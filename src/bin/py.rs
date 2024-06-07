use std::collections::HashMap;

use dfut::{
    into_dfut, DFut, DResult, GlobalScheduler, GlobalSchedulerCfg, Runtime, WorkerServerConfig,
};
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Error {
    PyError,
}

const F_NAME: &str = "do_work";

const SCRIPT: &str = r#"
def do_work(**kwargs):
    name = kwargs['name']
    if name:
        print(f'hello world, my name is {name}')
    else:
        print('hello world')
    return 42
"#;

fn run_py(f_name: String, script: String, kwargs: HashMap<String, String>) -> PyResult<u64> {
    Python::with_gil(|py| {
        let fun: Py<PyAny> = PyModule::from_code_bound(py, &script, "", "")?
            .getattr(f_name.as_str())?
            .into();
        let r = fun.call_bound(py, (), Some(&kwargs.into_py_dict_bound(py)))?;
        r.extract(py)
    })
}

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    pub async fn run_py(
        &self,
        f_name: String,
        script: String,
        kwargs: HashMap<String, String>,
    ) -> DResult<Result<u64, Error>> {
        Ok(run_py(f_name, script, kwargs).map_err(|_| Error::PyError))
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
        tokio::spawn(Worker::serve(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;
    let client = root_client.new_client();

    let mut kwargs = HashMap::new();
    kwargs.insert("name".to_string(), "bob".to_string());
    let fut = client
        .run_py(F_NAME.to_string(), SCRIPT.to_string(), kwargs)
        .await
        .unwrap();
    let result = client.d_await(fut).await.unwrap().unwrap();
    assert_eq!(result, 42);

    println!();
    println!("metrics");
    println!("{}", prometheus_handle.render());
}
