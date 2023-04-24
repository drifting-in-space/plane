use crate::nats::JetStreamable;
use anyhow::anyhow;
pub mod agent;
pub mod cert;
pub mod dns;
pub mod drone_state;
pub mod logging;
pub mod scheduler;
pub mod state;

async fn add_jetstream_stream<T: JetStreamable>(
    jetstream: &async_nats::jetstream::Context,
) -> anyhow::Result<()> {
    let config = T::config();
    tracing::debug!(name = config.name, "Creating jetstream stream.");
    jetstream
        .get_or_create_stream(config)
        .await
        .map_err(|d| anyhow!("Error: {d:?}"))?;

    Ok(())
}

pub async fn initialize_jetstreams(
    jetstream: &async_nats::jetstream::Context,
) -> anyhow::Result<()> {
    add_jetstream_stream::<state::WorldStateMessage>(jetstream).await?;
    add_jetstream_stream::<agent::DroneLogMessage>(jetstream).await?;
    add_jetstream_stream::<agent::BackendStateMessage>(jetstream).await?;
    add_jetstream_stream::<dns::SetDnsRecord>(jetstream).await?;

    Ok(())
}
