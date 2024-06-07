use std::time::{Duration, Instant};

use dfut::{
    d_await, into_dfut, DFut, DResult, GlobalScheduler, GlobalSchedulerCfg, Runtime,
    WorkerServerConfig,
};
use rand::seq::SliceRandom;

const BASE: u64 = 200_000;
const N: u32 = 6;

const P_FAIL: &[f64] = &[0., 0.01, 0.1];

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
        tokio::spawn(async move {
            let root_runtime_handle = Worker::serve(WorkerServerConfig {
                local_server_address: format!("http://127.0.0.1:{}", base_port_number + i),
                global_scheduler_address: global_scheduler_address.to_string(),
                ..Default::default()
            })
            .await;

            if std::env::var("DEBUG").ok().is_some() {
                loop {
                    root_runtime_handle.emit_debug_output();
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            }
        });
    });

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(48));

    let n = 100;

    let mut data = Vec::new();
    let mut js = tokio::task::JoinSet::new();
    for i in 1..N {
        let size = BASE * 2u64.pow(i);

        let mut record = Vec::new();
        record.push(size.to_string());

        let mut std_dur = Vec::new();
        for _ in 0..n {
            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let mut v_clone = v.clone();
            let start = Instant::now();
            v_clone.sort();
            let elapsed = start.elapsed();
            assert_eq!(v_clone, (0..size).collect::<Vec<_>>());
            std_dur.push(elapsed.as_secs_f64().to_string());
        }

        let mut local_dur = Vec::new();
        for _ in 0..n {
            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let v_clone = v.clone();
            let start = Instant::now();
            let got = local_quick_sort(v_clone);
            let elapsed = start.elapsed();
            assert_eq!(got, (0..size).collect::<Vec<_>>());
            local_dur.push(elapsed.as_secs_f64().to_string());
        }

        let mut p_durs = Vec::new();
        for p_fail in P_FAIL {
            let mut dur = Vec::new();
            for _ in 0..n {
                js.spawn({
                    let client = root_client.new_client();
                    let sem = sem.clone();

                    async move {
                        let _g = sem.acquire().await.unwrap();

                        let mut v: Vec<u64> = (0..size).collect();
                        v.shuffle(&mut rand::thread_rng());

                        let v_clone = v.clone();
                        let start = Instant::now();
                        let f = client.quick_sort(*p_fail, v_clone).await.unwrap();
                        let got = client.d_await(f).await.unwrap();
                        let elapsed = start.elapsed();
                        assert_eq!(got, (0..size).collect::<Vec<_>>());
                        elapsed.as_secs_f64().to_string()
                    }
                });
            }

            while let Some(r) = js.join_next().await {
                dur.push(r.unwrap());
            }
            p_durs.push(dur);
        }

        for i in 0..n {
            let mut record = Vec::new();
            record.push(format!("{size}"));
            record.push(std_dur[i].clone());
            record.push(local_dur[i].clone());
            for j in 0..P_FAIL.len() {
                record.push(p_durs[j][i].clone());
            }
            data.push(record);
        }
    }

    let mut wtr = csv::Writer::from_path("sort-with-errors-data.csv").unwrap();
    wtr.write_record(&[
        "size",
        "std",
        "local",
        "fail prob 0",
        "fail prob 0.01",
        "fail prob 0.1",
    ])
    .unwrap();

    for record in &data {
        wtr.write_record(record).unwrap();
    }

    println!("DONE");

    // tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    if std::env::var("SHOW_METRICS").ok().is_some() {
        println!();
        println!("metrics");
        println!("{}", prometheus_handle.render());
    }
}
