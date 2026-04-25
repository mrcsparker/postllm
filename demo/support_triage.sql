\pset pager off
\pset tuples_only off
\pset null '(null)'

\echo ''
\echo 'postllm demo: support ticket triage'
\echo ''

DROP SCHEMA IF EXISTS postllm_demo CASCADE;
CREATE SCHEMA postllm_demo;

CREATE TABLE postllm_demo.kb_articles (
    id bigserial PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL
);

CREATE TABLE postllm_demo.tickets (
    id bigserial PRIMARY KEY,
    customer text NOT NULL,
    subject text NOT NULL,
    body text NOT NULL
);

CREATE TABLE postllm_demo.ticket_responses (
    ticket_id bigint PRIMARY KEY REFERENCES postllm_demo.tickets(id),
    draft_response text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO postllm_demo.kb_articles (title, body)
VALUES
    (
        'Reset a read replica slot',
        'If a read replica falls behind after a network outage, check replication lag, verify disk space, restart the replica, and only recreate the replication slot after confirming no required WAL is needed.'
    ),
    (
        'Autovacuum and table bloat',
        'Autovacuum removes dead tuples left behind by MVCC. If bloat keeps growing, check long-running transactions, autovacuum thresholds, and whether manual VACUUM is needed during a quiet window.'
    ),
    (
        'Index maintenance',
        'Use REINDEX when an index is suspected to be bloated or corrupted. Prefer CONCURRENTLY on production systems when write availability matters.'
    );

INSERT INTO postllm_demo.tickets (customer, subject, body)
VALUES (
    'Acme Analytics',
    'Orders table keeps growing after deletes',
    'We delete old rows from public.orders every night, but disk usage keeps growing. What should we check first?'
);

WITH ticket AS (
    SELECT *
    FROM postllm_demo.tickets
    WHERE customer = 'Acme Analytics'
),
candidates AS (
    SELECT
        row_number() OVER ()::integer AS document_id,
        format('%s: %s', title, body) AS document
    FROM postllm_demo.kb_articles
),
ranked_context AS (
    SELECT string_agg(
        format('[%s] %s', ranked.rank, ranked.document),
        E'\n\n'
        ORDER BY ranked.rank
    ) AS context
    FROM ticket
    CROSS JOIN LATERAL postllm.keyword_rank(
        ticket.subject || E'\n' || ticket.body,
        ARRAY(SELECT document FROM candidates),
        top_n => 2
    ) AS ranked
),
draft AS (
    SELECT
        ticket.id AS ticket_id,
        postllm.chat_text(
            ARRAY[
                postllm.system('Draft a concise support reply from the provided context. Include one next step.'),
                postllm.user(format(
                    'Ticket subject: %s\n\nTicket body: %s\n\nKnowledge-base context:\n%s',
                    ticket.subject,
                    ticket.body,
                    ranked_context.context
                ))
            ],
            max_tokens => 80,
            temperature => 0.0
        ) AS draft_response
    FROM ticket
    CROSS JOIN ranked_context
)
INSERT INTO postllm_demo.ticket_responses (ticket_id, draft_response)
SELECT ticket_id, draft_response
FROM draft;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM postllm_demo.ticket_responses
        WHERE length(trim(draft_response)) > 0
    ) THEN
        RAISE EXCEPTION 'postllm demo did not produce a draft response';
    END IF;
END;
$$;

\echo ''
\echo 'Demo ticket'
TABLE postllm_demo.tickets;

\echo ''
\echo 'Drafted response'
SELECT
    ticket.customer,
    ticket.subject,
    response.draft_response
FROM postllm_demo.ticket_responses AS response
JOIN postllm_demo.tickets AS ticket ON ticket.id = response.ticket_id;

\echo ''
\echo 'postllm demo completed successfully.'
