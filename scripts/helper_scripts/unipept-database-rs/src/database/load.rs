use std::{fs, thread};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::database::{DatabaseContext, execute_statement};
use crate::utils::subprocess::{decompress_file_stdout, handle_process_status};

/// Load all *.tsv.lz4 files in a given directory into the database in parallel
pub fn parallel_load(ctx: Arc<DatabaseContext>, data_dir: &PathBuf) -> Result<()> {
    let mut handles = vec![];

    for entry in fs::read_dir(data_dir).context("Unable to read data directory")? {
        let entry = entry.context("Error reading entry from data directory")?;
        let path = entry.path();

        // Not a file
        if !path.is_file() {
            continue;
        }

        let base_name = match path.file_name() {
            None => { continue; }
            Some(n) => n.to_str().context("Error creating string from file path")?
        };

        let Some(table_name) = base_name.strip_suffix(".tsv.lz4") else { continue; };

        let table_name = table_name.to_string();
        let arc_ctx = Arc::clone(&ctx);

        // Load the table in a subprocess
        let handle = thread::spawn(move || {
            if let Err(e) = load(arc_ctx, &path, table_name) {
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

fn load(ctx: Arc<DatabaseContext>, fp: &PathBuf, table: String) -> Result<()> {
    let lz4 = decompress_file_stdout(fp).spawn().context("Error spawning decompression subprocess")?;
    let lz4_stdout = lz4.stdout.context("Unable to access stdout of decompression subprocess")?;

    let stmt = format!(r"COPY unipept.{table} FROM STDIN WITH (FORMAT text, DELIMITER E'\t', HEADER false, NULL '\N');");
    let mut cmd = execute_statement(&ctx, &stmt);
    cmd.stdin(lz4_stdout).stdout(Stdio::inherit());

    let process = cmd.spawn().context("Error spawning database import subprocess")?;
    handle_process_status(process, format!("Database import for table {table}"))
}
