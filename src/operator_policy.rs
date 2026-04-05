#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]
#![allow(unsafe_code)]

use crate::error::{Error, Result};
use pgrx::pg_sys;
use std::ffi::CStr;

#[must_use]
pub(crate) fn caller_is_superuser() -> bool {
    unsafe { pg_sys::superuser_arg(caller_role_oid()) }
}

pub(crate) fn caller_role_name() -> String {
    unsafe {
        let ptr = pg_sys::GetUserNameFromId(caller_role_oid(), false);
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    }
}

pub(crate) fn require_operator(function_name: &str) -> Result<()> {
    if caller_is_superuser() {
        Ok(())
    } else {
        let role_name = caller_role_name();
        Err(Error::Config(format!(
            "{function_name}(...) requires a PostgreSQL superuser for role '{role_name}'; fix: run it as a superuser and let application roles use postllm.configure(api_key_secret => ...) or postllm.profile_apply(...)"
        )))
    }
}

pub(crate) fn run_operator_operation<T>(
    function_name: &str,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    require_operator(function_name)?;
    operation()
}

#[must_use]
pub(crate) fn caller_role_oid() -> pg_sys::Oid {
    unsafe { pg_sys::GetOuterUserId() }
}
