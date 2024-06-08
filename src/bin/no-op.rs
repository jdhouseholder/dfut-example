use std::time::{Duration, Instant, SystemTime};

use clap::Parser;
use csv::Writer;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use dfut::{GlobalScheduler, GlobalSchedulerCfg, WorkerServerConfig};
use dfut_example::{NoOpWorker, NoOpWorkerRootClient};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:8220")]
    global_scheduler_address: String,

    #[arg(short, long, default_value_t = 10)]
    n_workers: u64,

    #[arg(short, long, default_value_t = 20)]
    exp: u64,

    #[arg(short, long, default_value_t = 5)]
    fan_out_by: u64,

    #[arg(short, long, default_value_t = 5)]
    heart_beat_timeout_secs: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    tokio::spawn(GlobalScheduler::serve_forever(GlobalSchedulerCfg {
        address: args.global_scheduler_address.to_string(),
        heart_beat_timeout: Duration::from_secs(args.heart_beat_timeout_secs),
        ..Default::default()
    }));

    let base_port_number = 8120;
    (0..args.n_workers).for_each(|i| {
        tokio::spawn({
            let local_server_address = format!("http://127.0.0.1:{}", base_port_number + i);
            let global_scheduler_address = args.global_scheduler_address.clone();
            async move {
                NoOpWorker::serve_forever(WorkerServerConfig {
                    local_server_address,
                    global_scheduler_address,
                    ..Default::default()
                })
                .await;
            }
        });
    });

    let n_senders = u64::max(args.n_workers / 5, 1);

    println!("Using n_senders={}", n_senders);
    let ct = CancellationToken::new();

    let mut js = tokio::task::JoinSet::new();
    for id in 0..n_senders {
        js.spawn({
            let ct = ct.clone();
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
                for i in 0.. {
                    let start = Instant::now();
                    let fut = client
                        .nop_fanout(args.fan_out_by, 1 << args.exp)
                        .await
                        .unwrap();

                    let _output = client.d_await(fut).await.unwrap();
                    let dur = start.elapsed();
                    let when = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap();
                    data.push((when, dur));

                    if i % 10 == 0 {
                        println!("{id}: {i}");
                    }
                    if ct.is_cancelled() {
                        break;
                    }
                }
                data
            }
        });
    }

    sleep(Duration::from_secs(30)).await;
    ct.cancel();

    let mut data = Vec::new();
    while let Some(d) = js.join_next().await {
        let mut d = d.unwrap();
        data.append(&mut d);
    }

    let mut wtr =
        Writer::from_path(format!("no-op-data-{}-{}.csv", args.exp, args.n_workers)).unwrap();
    wtr.write_record(&["t", "dur"]).unwrap();
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
