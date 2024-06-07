use clap::Parser;
use dfut::{GlobalScheduler, GlobalSchedulerCfg};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:8220")]
    address: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    GlobalScheduler::serve_forever(GlobalSchedulerCfg {
        address: args.address,
        ..Default::default()
    })
    .await;
}
