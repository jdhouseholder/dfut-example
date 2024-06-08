use std::time::{Duration, Instant};

use clap::Parser;
use dfut::{
    d_await, into_dfut, DFut, DResult, GlobalScheduler, GlobalSchedulerCfg, Runtime,
    WorkerServerConfig,
};
use rand::seq::SliceRandom;
use rayon::prelude::*;
use tokio_util::sync::CancellationToken;

use dfut_example::now;

const BASE: u64 = 200_000;
const N: u32 = 6;

const P_FAIL: &[f64] = &[0., 0.01, 0.1];

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value_t = 100)]
    n: u32,
}

pub fn partition(mut v: Vec<u64>) -> (Vec<u64>, u64, Vec<u64>) {
    let p = v.pop().unwrap();
    let mut l = Vec::new();
    let mut g = Vec::new();
    for e in v {
        if e > p {
            g.push(e);
        } else {
            l.push(e);
        }
    }

    (l, p, g)
}

pub fn local_quick_sort(mut v: Vec<u64>) -> Vec<u64> {
    if v.len() < 200_000 {
        v.sort();
        return v;
    }
    let (l, p, g) = partition(v);
    let l = local_quick_sort(l);
    let g = local_quick_sort(g);
    let mut out = Vec::new();
    out.extend(l);
    out.push(p);
    out.extend(g);
    out
}

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    pub async fn quick_sort(&self, p_fail: f64, mut v: Vec<u64>) -> DResult<Vec<u64>> {
        if rand::random::<f64>() < p_fail {
            return Err(dfut::Error::System);
        }
        if v.len() < 200_000 {
            v.sort();
            return Ok(v);
        }

        let (l, p, g) = tokio::task::spawn_blocking(move || partition(v))
            .await
            .unwrap();

        let l_fut = self.quick_sort(p_fail, l).await?;
        let g_fut = self.quick_sort(p_fail, g).await?;

        let mut out = Vec::new();
        out.extend(d_await!(l_fut));
        out.push(p);
        out.extend(d_await!(g_fut));

        Ok(out)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let global_scheduler_address = "http://127.0.0.1:8220";

    tokio::spawn(GlobalScheduler::serve_forever(GlobalSchedulerCfg {
        address: global_scheduler_address.to_string(),
        heart_beat_timeout: Duration::from_secs(20),
        ..Default::default()
    }));

    let base_port_number = 8120;
    (0..10).for_each(|i| {
        tokio::spawn(Worker::serve_forever(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:{}", base_port_number + i),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;

    let n_cpus = num_cpus::get();

    let mut data = Vec::new();
    for i in 1..N {
        let size = BASE * 2u64.pow(i);

        let exp_start = Instant::now();
        let datas: Vec<Vec<_>> = (0..n_cpus)
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|_| {
                let mut data = Vec::new();
                loop {
                    let mut v: Vec<u64> = (0..size).collect();
                    v.shuffle(&mut rand::thread_rng());

                    let mut v_clone = v.clone();
                    let start = Instant::now();
                    v_clone.sort();
                    let elapsed = start.elapsed();
                    assert_eq!(v_clone, (0..size).collect::<Vec<_>>());
                    data.push((
                        now().as_millis().to_string(),
                        size.to_string(),
                        "std".to_string(),
                        elapsed.as_secs_f64().to_string(),
                    ));
                    if exp_start.elapsed() >= Duration::from_secs(30) {
                        break;
                    }
                }
                data
            })
            .collect();
        for d in datas {
            data.extend(d);
        }

        let exp_start = Instant::now();
        let datas: Vec<Vec<_>> = (0..n_cpus)
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|_| {
                let mut data = Vec::new();
                loop {
                    let mut v: Vec<u64> = (0..size).collect();
                    v.shuffle(&mut rand::thread_rng());

                    let v_clone = v.clone();
                    let start = Instant::now();
                    let got = local_quick_sort(v_clone);
                    let elapsed = start.elapsed();
                    assert_eq!(got, (0..size).collect::<Vec<_>>());
                    data.push((
                        now().as_millis().to_string(),
                        size.to_string(),
                        "local".to_string(),
                        elapsed.as_secs_f64().to_string(),
                    ));
                    if exp_start.elapsed() >= Duration::from_secs(30) {
                        break;
                    }
                }
                data
            })
            .collect();
        for d in datas {
            data.extend(d);
        }

        for p_fail in P_FAIL {
            let mut js = tokio::task::JoinSet::new();
            let ct = CancellationToken::new();

            for _ in 0..10 {
                js.spawn({
                    let ct = ct.clone();
                    let exp_id = format!("p_fail={p_fail}");
                    let client = root_client.new_client();

                    async move {
                        let mut data = Vec::new();
                        loop {
                            let mut v: Vec<u64> = (0..size).collect();
                            v.shuffle(&mut rand::thread_rng());

                            let v_clone = v.clone();
                            let start = Instant::now();
                            let f = client.quick_sort(*p_fail, v_clone).await.unwrap();
                            let got = client.d_await(f).await.unwrap();
                            let elapsed = start.elapsed();
                            assert_eq!(got, (0..size).collect::<Vec<_>>());
                            data.push((
                                now().as_millis().to_string(),
                                size.to_string(),
                                exp_id.clone(),
                                elapsed.as_secs_f64().to_string(),
                            ));
                            if ct.is_cancelled() {
                                break;
                            }
                        }
                        data
                    }
                });
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
            ct.cancel();

            while let Some(r) = js.join_next().await {
                data.append(&mut r.unwrap());
            }
        }
    }

    let mut wtr = csv::Writer::from_path("sort-with-errors-data.csv").unwrap();
    wtr.write_record(&["t", "size", "exp_id", "dur"]).unwrap();

    for (now, size, exp_id, dur) in &data {
        wtr.write_record(&[now, size, exp_id, dur]).unwrap();
    }

    println!("DONE");

    // tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    if std::env::var("SHOW_METRICS").ok().is_some() {
        println!();
        println!("metrics");
        println!("{}", prometheus_handle.render());
    }
}
