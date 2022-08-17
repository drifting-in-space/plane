use crate::container::{ContainerResource, ContainerSpec};
use crate::scratch_dir;
use crate::util::wait_for_port;
use anyhow::{Result, anyhow};
use dis_spawner::nats::TypedNats;
use dis_spawner::nats_connection::NatsConnection;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddrV4;

const NATS_TOKEN: &str = "mytoken";
pub struct Nats {
    container: ContainerResource,
    #[allow(unused)]
    log_handle: JoinHandle<()>,
}

impl Nats {
    fn connection_string(&self) -> String {
        format!("nats://{}@{}", NATS_TOKEN, self.container.ip)
    }

    pub async fn connection(&self) -> Result<TypedNats> {
        let nc = NatsConnection::new(self.connection_string())?;
        nc.connection().await
    }

    pub async fn new() -> Result<Nats> {
        let spec = ContainerSpec {
            name: "nats".into(),
            image: "docker.io/nats:2.8".into(),
            environment: HashMap::new(),
            command: vec!["--jetstream".into(), "--auth".into(), NATS_TOKEN.into()],
            volumes: Vec::new(),
        };

        let container = ContainerResource::new(&spec).await?;

        wait_for_port(SocketAddrV4::new(container.ip, 4222), 10_000).await?;

        let conn = async_nats::ConnectOptions::with_token(NATS_TOKEN.into())
            .connect(container.ip.to_string())
            .await?;

        let mut output = File::create(scratch_dir("logs").join("nats-wiretap.txt"))?;
        let mut wiretap = conn.subscribe(">".into()).await.map_err(|_| anyhow!("Couldn't subscribe to NATS wiretap."))?;

        let log_handle = tokio::spawn(async move {
            while let Some(v) = wiretap.next().await {
                let message = std::str::from_utf8(&v.payload).unwrap();
                output.write_fmt(format_args!("{}: {}\n", v.subject, message)).unwrap();
            }
        });
        
        Ok(Nats { container, log_handle })
    }
}
