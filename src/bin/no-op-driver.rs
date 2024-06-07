use std::time::{Duration, Instant, SystemTime};

use clap::Parser;
use csv::Writer;
use tokio::time::sleep;

use dfut_example::NoOpWorkerRootClient;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:8220")]
    global_scheduler_address: String,

    #[arg(short, long)]
    n_workers: u64,

    #[arg(short, long)]
    exp: u64,

    #[arg(short, long)]
    n_calls: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let n_tx = u64::max(args.n_workers / 5, 1);

    let exp = args.exp;

    let mut js = tokio::task::JoinSet::new();
    for id in 0..n_tx {
        js.spawn({
            let root_client = NoOpWorkerRootClient::new(
                &args.global_scheduler_address,
                &format!("unique-id-{id}"),
            )
            .await;

            async move {
                // Wait for hbto to setup to avoid emitting retry errors for the demo.
                sleep(Duration::from_secs(2)).await;
                let client = root_client.new_client();

                let mut data = Vec::new();
                for i in 0..args.n_calls {
                    let start = Instant::now();
                    let fut = client.nop_fanout(5, 2 << exp).await.unwrap();

                    let _output = client.d_await(fut).await.unwrap();
                    let dur = start.elapsed();
                    let when = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap();
                    data.push((when, dur));

                    if i % 10 == 0 {
                        println!("{id}: {i}");
                    }
                }
                data
            }
        });
    }

    let mut data = Vec::new();
    while let Some(d) = js.join_next().await {
        let mut d = d.unwrap();
        data.append(&mut d);
    }

    let mut wtr = Writer::from_path(format!("no-op-data-{}.csv", args.n_workers)).unwrap();
    wtr.write_record(&["when", "dur"]).unwrap();
    for (when, dur) in &data {
        wtr.write_record(&[
            format!("{}", when.as_millis()),
            format!("{:?}", dur.as_millis()),
        ])
        .unwrap();
    }

    println!();
    println!("metrics");
    println!("{}", prometheus_handle.render());
}
