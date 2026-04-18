#!/usr/bin/env python3
"""Run repeatable postllm benchmark suites from JSON scenario definitions."""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import re
import statistics
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Queue
from typing import Any

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_DSN = os.environ.get(
    "POSTLLM_BENCH_DSN",
    "postgresql://postgres:postgres@127.0.0.1:5440/postllm",
)
DEFAULT_OUTPUT_DIR = ROOT_DIR / "target" / "benchmarks"
ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run postllm benchmark scenarios and emit JSON/Markdown summaries."
    )
    parser.add_argument(
        "--suite",
        default=str(ROOT_DIR / "benchmarks" / "runtime_matrix.json"),
        help="Path to the benchmark suite JSON file.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Run only the named scenario. May be repeated.",
    )
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="PostgreSQL DSN used by psql.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for benchmark result files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the suite and print the resolved plan without executing benchmarks.",
    )
    return parser.parse_args()


def expand_env(value: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        name = match.group(1)
        default = match.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(f"missing required environment variable '{name}'")

    return ENV_PATTERN.sub(replacer, value)


def resolve_env(value: Any) -> Any:
    if isinstance(value, str):
        return expand_env(value)
    if isinstance(value, list):
        return [resolve_env(item) for item in value]
    if isinstance(value, dict):
        return {key: resolve_env(item) for key, item in value.items()}
    return value


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite float is not allowed in SQL literals: {value}")
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (dict, list)):
        json_text = json.dumps(value, separators=(",", ":"), sort_keys=True)
        return "'" + json_text.replace("'", "''") + "'::jsonb"
    raise TypeError(f"unsupported SQL literal type: {type(value)!r}")


def build_configure_sql(configure: dict[str, Any]) -> str:
    assignments = ", ".join(
        f"{key} => {sql_literal(value)}" for key, value in configure.items()
    )
    return f"SELECT postllm.configure({assignments});"


def build_compose_base_command() -> list[str]:
    files = os.environ.get("POSTLLM_BENCH_COMPOSE_FILES", "")
    if not files:
        return []
    command = ["docker", "compose"]
    for file_path in files.split(":"):
        if file_path:
            command.extend(["-f", file_path])
    project = os.environ.get("POSTLLM_BENCH_COMPOSE_PROJECT")
    if project:
        command.extend(["-p", project])
    return command


def measure_process_rss_kb(pid: int) -> int | None:
    docker_service = os.environ.get("POSTLLM_BENCH_DOCKER_SERVICE")
    if docker_service:
        compose_command = build_compose_base_command()
        if not compose_command:
            raise RuntimeError(
                "POSTLLM_BENCH_DOCKER_SERVICE requires POSTLLM_BENCH_COMPOSE_FILES"
            )
        command = compose_command + [
            "exec",
            "-T",
            docker_service,
            "sh",
            "-lc",
            f"ps -o rss= -p {pid} | tr -d '[:space:]'",
        ]
    else:
        command = ["ps", "-o", "rss=", "-p", str(pid)]

    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    output = completed.stdout.strip()
    if not output:
        return None
    return int(output)


class PsqlSession:
    def __init__(self, dsn: str) -> None:
        self.process = subprocess.Popen(
            [
                "psql",
                dsn,
                "-X",
                "-q",
                "-t",
                "-A",
                "-v",
                "ON_ERROR_STOP=1",
                "-P",
                "pager=off",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("failed to open psql pipes")
        self.stderr_queue: Queue[str] = Queue()
        self.stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self.stderr_thread.start()

    def _drain_stderr(self) -> None:
        assert self.process.stderr is not None
        for line in self.process.stderr:
            self.stderr_queue.put(line)

    def query(self, sql: str) -> str:
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        token = f"__postllm_end__{uuid.uuid4().hex}"
        statement = sql.strip()
        if not statement.endswith(";"):
            statement += ";"
        self.process.stdin.write(f"{statement}\nSELECT '{token}';\n")
        self.process.stdin.flush()

        lines: list[str] = []
        while True:
            line = self.process.stdout.readline()
            if line == "":
                stderr_text = "".join(list(self.stderr_queue.queue)).strip()
                raise RuntimeError(
                    f"psql exited unexpectedly while running SQL: {statement}\n{stderr_text}"
                )
            stripped = line.rstrip("\n")
            if stripped == token:
                return "\n".join(lines).strip()
            lines.append(stripped)

    def close(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()
        self.process.wait(timeout=5)

    def __enter__(self) -> "PsqlSession":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


@dataclass
class ScenarioResult:
    name: str
    runtime: str
    model_size: str
    kind: str
    configure: dict[str, Any]
    warmup: int
    iterations: int
    concurrency: int
    sample_result: str
    latency_ms: dict[str, float]
    throughput: dict[str, float]
    memory_kb: dict[str, int | None]


def percentile_ms(samples: list[float], ratio: float) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return samples[0]
    ordered = sorted(samples)
    index = ratio * (len(ordered) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def run_iteration(session: PsqlSession, statement: str) -> tuple[str, float]:
    started = time.perf_counter()
    result = session.query(statement)
    duration_ms = (time.perf_counter() - started) * 1000.0
    return result, duration_ms


def run_sequential_probe(dsn: str, scenario: dict[str, Any]) -> ScenarioResult:
    configure_sql = build_configure_sql(scenario["configure"])
    prewarm_sql = scenario.get("prewarm_sql")
    warmup = int(scenario.get("warmup", 1))
    iterations = int(scenario.get("iterations", 5))
    concurrency = int(scenario.get("concurrency", 1))
    statement = scenario["statement"]

    with PsqlSession(dsn) as session:
        session.query(configure_sql)
        backend_pid = int(session.query("SELECT pg_backend_pid();"))
        rss_before = measure_process_rss_kb(backend_pid)
        if prewarm_sql:
            session.query(prewarm_sql)
        rss_after_prewarm = measure_process_rss_kb(backend_pid)

        for _ in range(warmup):
            session.query(statement)

        samples: list[float] = []
        sample_result = ""
        for _ in range(iterations):
            sample_result, duration_ms = run_iteration(session, statement)
            samples.append(duration_ms)

        rss_after_benchmark = measure_process_rss_kb(backend_pid)

    latency = {
        "min_ms": min(samples),
        "avg_ms": statistics.mean(samples),
        "p50_ms": percentile_ms(samples, 0.50),
        "p95_ms": percentile_ms(samples, 0.95),
        "max_ms": max(samples),
    }
    total_seconds = sum(samples) / 1000.0
    throughput = {
        "requests": float(iterations),
        "requests_per_second": (float(iterations) / total_seconds) if total_seconds else 0.0,
    }
    memory = {
        "rss_before_kb": rss_before,
        "rss_after_prewarm_kb": rss_after_prewarm,
        "rss_after_benchmark_kb": rss_after_benchmark,
        "rss_delta_warmup_kb": (
            None
            if rss_before is None or rss_after_prewarm is None
            else rss_after_prewarm - rss_before
        ),
        "rss_delta_benchmark_kb": (
            None
            if rss_after_prewarm is None or rss_after_benchmark is None
            else rss_after_benchmark - rss_after_prewarm
        ),
    }

    return ScenarioResult(
        name=scenario["name"],
        runtime=scenario["runtime"],
        model_size=scenario["model_size"],
        kind=scenario["kind"],
        configure=scenario["configure"],
        warmup=warmup,
        iterations=iterations,
        concurrency=concurrency,
        sample_result=sample_result,
        latency_ms=latency,
        throughput=throughput,
        memory_kb=memory,
    )


def run_parallel_probe(dsn: str, scenario: dict[str, Any]) -> dict[str, float]:
    configure_sql = build_configure_sql(scenario["configure"])
    prewarm_sql = scenario.get("prewarm_sql")
    statement = scenario["statement"]
    concurrency = int(scenario.get("concurrency", 1))
    iterations = int(scenario.get("iterations", 5))

    per_worker = [iterations // concurrency for _ in range(concurrency)]
    for index in range(iterations % concurrency):
        per_worker[index] += 1

    def worker(count: int) -> int:
        if count == 0:
            return 0
        with PsqlSession(dsn) as session:
            session.query(configure_sql)
            if prewarm_sql:
                session.query(prewarm_sql)
            for _ in range(count):
                session.query(statement)
        return count

    start = time.perf_counter()
    threads: list[threading.Thread] = []
    errors: list[BaseException] = []
    completed = 0
    completed_lock = threading.Lock()

    def runner(count: int) -> None:
        nonlocal completed
        try:
            finished = worker(count)
            with completed_lock:
                completed += finished
        except BaseException as exc:  # pragma: no cover - propagated immediately
            errors.append(exc)

    for count in per_worker:
        thread = threading.Thread(target=runner, args=(count,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(str(errors[0]))

    elapsed_seconds = time.perf_counter() - start
    return {
        "requests": float(completed),
        "wall_clock_seconds": elapsed_seconds,
        "requests_per_second": (float(completed) / elapsed_seconds) if elapsed_seconds else 0.0,
    }


def scenario_plan(suite: dict[str, Any], selected: list[str]) -> list[dict[str, Any]]:
    scenarios = suite["scenarios"]
    if selected:
        names = set(selected)
        scenarios = [scenario for scenario in scenarios if scenario["name"] in names]
    if not scenarios:
        raise ValueError("no scenarios matched the requested filters")
    return scenarios


def write_results(
    suite_name: str,
    output_dir: pathlib.Path,
    results: list[ScenarioResult],
) -> tuple[pathlib.Path, pathlib.Path]:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{suite_name}-{timestamp}.json"
    markdown_path = output_dir / f"{suite_name}-{timestamp}.md"

    payload = {
        "suite": suite_name,
        "generated_at": timestamp,
        "results": [result.__dict__ for result in results],
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    lines = [
        f"# Benchmark Report: {suite_name}",
        "",
        f"Generated at {timestamp}.",
        "",
        "| scenario | runtime | size | kind | p50 latency (ms) | p95 latency (ms) | req/s | rss warmup delta (KB) | rss benchmark delta (KB) |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(
            "| {name} | {runtime} | {size} | {kind} | {p50:.2f} | {p95:.2f} | {rps:.2f} | {warmup} | {bench} |".format(
                name=result.name,
                runtime=result.runtime,
                size=result.model_size,
                kind=result.kind,
                p50=result.latency_ms["p50_ms"],
                p95=result.latency_ms["p95_ms"],
                rps=result.throughput["requests_per_second"],
                warmup=result.memory_kb["rss_delta_warmup_kb"],
                bench=result.memory_kb["rss_delta_benchmark_kb"],
            )
        )
    markdown_path.write_text("\n".join(lines) + "\n")
    return json_path, markdown_path


def main() -> int:
    args = parse_args()
    suite_path = pathlib.Path(args.suite)
    suite = resolve_env(json.loads(suite_path.read_text()))
    scenarios = scenario_plan(suite, args.scenario)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "suite": suite["name"],
                    "description": suite.get("description"),
                    "scenarios": scenarios,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    results: list[ScenarioResult] = []
    for scenario in scenarios:
        result = run_sequential_probe(args.dsn, scenario)
        if result.concurrency > 1:
            result.throughput = run_parallel_probe(args.dsn, scenario)
        results.append(result)
        print(
            f"{result.name}: p50={result.latency_ms['p50_ms']:.2f}ms "
            f"p95={result.latency_ms['p95_ms']:.2f}ms "
            f"req/s={result.throughput['requests_per_second']:.2f}",
            flush=True,
        )

    output_dir = pathlib.Path(args.output_dir)
    json_path, markdown_path = write_results(suite["name"], output_dir, results)
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
