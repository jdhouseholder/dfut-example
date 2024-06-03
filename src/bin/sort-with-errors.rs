use std::time::{Duration, Instant};

use dfut::{d_await, into_dfut, DFut, DResult, GlobalScheduler, Runtime, WorkerServerConfig};
use rand::seq::SliceRandom;

const BASE: u64 = 200_000;
const N: u32 = 5;

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
        let (l, p, g) = partition(v);
        let l_fut = self.quick_sort(p_fail, l).await?;
        let g_fut = self.quick_sort(p_fail, g).await?;
        let l = d_await!(l_fut);
        let g = d_await!(g_fut);
        let mut out = Vec::new();
        out.extend(l);
        out.push(p);
        out.extend(g);
        Ok(out)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let global_scheduler_address = "http://127.0.0.1:8120";

    tokio::spawn(GlobalScheduler::serve(
        global_scheduler_address,
        vec![],
        Duration::from_secs(5),
    ));

    (1..=9).for_each(|i| {
        tokio::spawn(Worker::serve(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;
    let client = root_client.new_client();

    // Sort.
    {
        println!(
            "size, std, local, {}",
            P_FAIL
                .iter()
                .map(|v| format!("distributed (p_fail={v})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        for i in 1..N {
            let size = BASE * 2u64.pow(i);

            print!("{size}, ");
            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let mut v_clone = v.clone();
            let start = Instant::now();
            v_clone.sort();
            let elapsed = start.elapsed();
            assert_eq!(v_clone, (0..size).collect::<Vec<_>>());
            print!("{:?}, ", elapsed.as_secs_f64());

            let v_clone = v.clone();
            let start = Instant::now();
            let got = local_quick_sort(v_clone);
            let elapsed = start.elapsed();
            assert_eq!(got, (0..size).collect::<Vec<_>>());
            print!("{:?}", elapsed.as_secs_f64());

            for p_fail in P_FAIL {
                let v_clone = v.clone();
                let start = Instant::now();
                let f = client.quick_sort(*p_fail, v_clone).await.unwrap();
                let got = client.d_await(f).await.unwrap();
                let elapsed = start.elapsed();
                print!(", {:?}", elapsed.as_secs_f64());

                assert_eq!(got, (0..size).collect::<Vec<_>>());
            }
            println!();
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    if !std::env::var("SHOW_METRICS").ok().is_some() {
        println!();
        println!("metrics");
        println!("{}", prometheus_handle.render());
    }
}
