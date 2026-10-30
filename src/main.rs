use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use yino::run::{self, Config};

#[derive(Parser)]
#[command(version, about = "Durable BTCUSDT market-data demo")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Live {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 900)]
        seconds: u64,
        #[arg(long, default_value = run::ENDPOINT)]
        endpoint: String,
        #[command(flatten)]
        config: Config,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// 0 = unlimited, 1 = original speed, 10 = ten times faster.
        #[arg(long, default_value_t = 0.0)]
        speed: f64,
        /// Validate a complete prefix only; exits nonzero and never passes acceptance.
        #[arg(long)]
        recover: bool,
        /// Write a fresh WAL using the live persistence and compute pipeline.
        #[arg(long)]
        persist: bool,
        #[command(flatten)]
        config: Config,
    },
    Verify {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        recover: bool,
    },
    /// Display recorded statistics; not the final performance report.
    Report {
        #[arg(long)]
        input: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Live {
            out,
            seconds,
            endpoint,
            config,
        } => {
            run::prepare_output(&out)?;
            let summary = run::live(&out, &endpoint, seconds, config).await?;
            run::save_summary(&out, &summary)?;
            ensure!(
                summary.complete,
                "live sample failed: {}",
                summary.errors.join("; ")
            );
        }
        Command::Replay {
            input,
            out,
            speed,
            recover,
            persist,
            config,
        } => {
            let shutdown = yino::shutdown::Shutdown::new()?;
            run::prepare_output(&out)?;
            let summary =
                run::replay(&input, &out, speed, recover, persist, config, shutdown).await?;
            run::save_summary(&out, &summary)?;
            ensure!(
                summary.complete,
                "replay is incomplete: {}",
                summary.errors.join("; ")
            );
        }
        Command::Verify { input, recover } => {
            if recover {
                println!("{}", serde_json::to_string(&run::verify_recovery(&input)?)?);
                anyhow::bail!("recovery-only verification; sample remains incomplete");
            }
            println!(
                "{}",
                serde_json::json!({"verified": true, "stats": run::verify(&input)?})
            );
        }
        Command::Report { input } => {
            println!("{}", run::report(&input)?);
        }
    }
    Ok(())
}
