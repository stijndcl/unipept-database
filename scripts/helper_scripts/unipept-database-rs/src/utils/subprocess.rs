use anyhow::{anyhow, Error, Result};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

/// Create a process::Command instance to write a file to stdout
pub fn cat_file_stdout(fp: &PathBuf) -> Command {
    let mut cmd = Command::new("cat");
    cmd.arg(format!("{}", fp.display()));
    cmd.stdout(Stdio::piped());

    cmd
}

/// Create a process::Command instance to decompress a given file using lz4
/// Output is written to stdout to be piped to other Commands
pub fn decompress_file_stdout(fp: &PathBuf) -> Command {
    let mut cmd = Command::new("lz4");
    cmd.args(["-d", "-c"]);
    cmd.stdout(Stdio::piped());

    cmd
}

pub fn handle_process_status(mut process: Child, name: &str) -> Result<()> {
    match process.wait() {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                Err(anyhow!("{name} exited with status {:?}", status.code()))
            }
        }
        Err(e) => Err(Error::from(e))
    }
}