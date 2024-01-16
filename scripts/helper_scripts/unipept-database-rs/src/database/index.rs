use std::io::Write;
use std::sync::Arc;
use anyhow::{Context, Result};
use crate::database::{DatabaseContext, execute_statement, setup_psql};

pub fn parallel_index(ctx: DatabaseContext) -> Result<()> {
    Ok(())
}

fn index(ctx: Arc<DatabaseContext>, table_name: String, column_name: String) -> Result<()> {
    let statement = format!("CREATE INDEX idx_{column_name} ON unipept.{table_name}({column_name});");

    let mut cmd = execute_statement(&ctx, &statement);
    let mut process = cmd.spawn().context("Error spawning subprocess for database index")?;
    let mut stdin = process.stdin.take().context("Unable to access stdin of database index subprocess")?;
    stdin.write_all(statement.as_bytes()).context("Error writing to stdin of database index subprocess")?;

    let output = process.wait_with_output().context("Error ")?;
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));

    Ok(())
}
