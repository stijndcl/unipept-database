use std::process::Command;

pub mod load;
pub mod index;
pub mod schema;

pub struct DatabaseContext {
    pub user: String,
    pub pass: String,
    pub container: Option<String>,
}

pub fn setup_psql(ctx: &DatabaseContext) -> Command {
    match &ctx.container {
        None => {
            let mut cmd = Command::new("psql");

            cmd.env("PGPASSWORD", &ctx.pass)
                .args(["-U", &ctx.user]);

            cmd
        }
        Some(container) => {
            let mut cmd = Command::new("docker");

            cmd.env("PGPASSWORD", &ctx.pass)
                .args(["exec", "-i", &container, "psql", "-U", &ctx.user]);

            cmd
        }
    }
}

/// Create a process.Command that runs a database statement
/// If a value for ctx.container is passed, this runs in Docker instead
pub fn execute_statement(ctx: &DatabaseContext, stmt: &str) -> Command {
    let mut cmd = setup_psql(ctx);
    cmd.arg("-c").arg(stmt);

    cmd
}

