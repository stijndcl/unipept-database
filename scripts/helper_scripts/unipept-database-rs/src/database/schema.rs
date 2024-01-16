use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, Context, Error, Result};

use crate::database::{DatabaseContext, setup_psql};
use crate::utils::subprocess::{cat_file_stdout, handle_process_status};

pub fn create_schema(ctx: &DatabaseContext, schema_file: &PathBuf) -> Result<()> {
    let cat = cat_file_stdout(schema_file).spawn().context("Error catting database schema file in subprocess")?;
    let cat_stdout = cat.stdout.context("Unable to access stdout of cat subprocess")?;

    let mut cmd = setup_psql(ctx);
    cmd.stdin(cat_stdout).stdout(Stdio::inherit());

    let process = cmd.spawn().context("Error spawning database schema subprocess")?;
    handle_process_status(process, "Database schema creation")
}