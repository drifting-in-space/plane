use super::record_stats::Stats;
use super::ErrorObj;
use async_nats as nats;
use serde::Serialize;
use std::boxed::Box;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct NatsSubjectComponent(pub String);

#[derive(Serialize)]
pub struct DroneStatsMessage {
    #[serde(flatten)]
    pub stats: Stats,
    pub cluster: String,
    pub drone: String,
}

impl FromStr for NatsSubjectComponent {
    type Err = String;
    fn from_str(a: &str) -> Result<Self, Self::Err> {
        if a.contains(char::is_whitespace) {
            return Err("provided nats subject components contain whitespace!".into());
        }
        Ok(NatsSubjectComponent(a.replace(".", "_").into()))
    }
}

pub type Sender = Box<dyn Fn(&'static str) -> Result<(), ErrorObj>>;
pub async fn get_nats_sender(nats_url: &str, subject: &str) -> Result<Sender, ErrorObj> {
    let nc = nats::connect(nats_url).await?;
    let sub_clone = subject.to_owned();

    Ok(Box::new(move |msg: &'static str| {
		let msg = msg.clone();
		let sub_clone = sub_clone.clone();
		let nc = nc.clone();
		tokio::spawn(async move { nc.publish(sub_clone.clone(), msg.clone().into()).await });
        Ok(())
    }))
}
