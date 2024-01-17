use std::io::Write;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};

use crate::database::{DatabaseContext, execute_statement};
use crate::utils::subprocess::handle_process_status;

const TABLE_COLUMNS: [&str; 13] = [
    "uniprot_entries:taxon_id",
    "uniprot_entries:uniprot_accession_number",
    "ec_numbers:code",
    "go_terms:code",
    "sequences:sequence",
    "sequences:lca",
    "sequences:lca_il",
    "peptides:sequence_id",
    "peptides:uniprot_entry_id",
    "peptides:original_sequence_id",
    "go_cross_references:uniprot_entry_id",
    "ec_cross_references:uniprot_entry_id",
    "interpro_cross_references:uniprot_entry_id",
];

pub fn parallel_index(ctx: Arc<DatabaseContext>) -> Result<()> {
    let mut handles = vec![];

    for entry in TABLE_COLUMNS {
        let (table_name, column_name) = entry.split_once(":").with_context(|| format!("Unable to split {entry} on ':'"))?;

        let arc_ctx = Arc::clone(&ctx);
        let table_name = table_name.to_string();
        let column_name = column_name.to_string();

        let handle = thread::spawn(move || {
            if let Err(e) = index(arc_ctx, table_name, column_name) {
                eprintln!("Error loading database table: {e}");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Error joining threads");
    }

    Ok(())
}

fn index(ctx: Arc<DatabaseContext>, table_name: String, column_name: String) -> Result<()> {
    let statement = format!("CREATE INDEX idx_{column_name} ON unipept.{table_name}({column_name});");

    let mut cmd = execute_statement(&ctx, &statement);
    let mut process = cmd.spawn().context("Error spawning subprocess for database index")?;
    let mut stdin = process.stdin.take().context("Unable to access stdin of database index subprocess")?;
    stdin.write_all(statement.as_bytes()).context("Error writing to stdin of database index subprocess")?;
    handle_process_status(process, format!("Database index for column {table_name}:{column_name}"))
}
