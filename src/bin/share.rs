use dfut::{
    d_await, d_box, d_cancel, into_dfut, DFut, DResult, GlobalScheduler, GlobalSchedulerCfg,
    Runtime, WorkerServerConfig,
};

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    // Supervisor. Inspired by:
    // https://docs.ray.io/en/latest/ray-core/patterns/tree-of-actors.html.
    pub async fn supervised_train(
        &self,
        hyperparams: Vec<f64>,
        data: Vec<f64>,
    ) -> DResult<Vec<Vec<f64>>> {
        let data = d_box!(data);

        let data_dfuts = self
            .runtime
            .share_n(&data, hyperparams.len() as u64)
            .await?;

        let mut v = Vec::new();
        for (hyperparam, data) in hyperparams.into_iter().zip(data_dfuts) {
            v.push(self.train_epoch(hyperparam, data).await?);
        }

        let mut o = Vec::new();
        for f in v {
            o.push(d_await!(f));
        }

        d_cancel!(data);

        Ok(o)
    }

    pub async fn train_epoch(&self, hyperparam: f64, data: DFut<Vec<f64>>) -> DResult<Vec<f64>> {
        let mut v = Vec::new();
        for data in d_await!(data) {
            v.push(self.train(hyperparam, data).await?);
        }

        let mut o = Vec::new();
        for f in v {
            o.push(d_await!(f));
        }

        Ok(o)
    }

    pub async fn train(&self, hyperparam: f64, data: f64) -> DResult<f64> {
        Ok(hyperparam * data)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let global_scheduler_address = "http://127.0.0.1:8120";

    tokio::spawn(GlobalScheduler::serve_forever(GlobalSchedulerCfg {
        address: global_scheduler_address.to_string(),
        ..Default::default()
    }));

    (1..=9).for_each(|i| {
        tokio::spawn(Worker::serve_forever(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;
    let client = root_client.new_client();

    let hyperparams = vec![1., 2., 3., 4.];
    let data = vec![1., 2., 3.];
    let fut = client.supervised_train(hyperparams, data).await.unwrap();
    let model1 = client.d_await(fut).await.unwrap();
    assert_eq!(
        model1,
        vec![
            vec![1., 2., 3.],
            vec![2., 4., 6.],
            vec![3., 6., 9.],
            vec![4., 8., 12.]
        ]
    );

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    println!();
    println!("metrics");
    println!("{}", prometheus_handle.render());
}
