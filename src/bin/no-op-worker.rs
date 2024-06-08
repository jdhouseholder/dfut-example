use dfut::WorkerServerConfig;

use clap::Parser;
use dfut_example::NoOpWorker;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:8220")]
    global_scheduler_address: String,
    #[arg(short, long)]
    local_server_address: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let args = Args::parse();

    NoOpWorker::serve_forever(WorkerServerConfig {
        local_server_address: args.local_server_address.to_string(),
        global_scheduler_address: args.global_scheduler_address.to_string(),
        ..Default::default()
    })
    .await;
}
