use super::{backend_manager::BackendManager, state_store::StateStore};
use crate::{
    drone::runtime::Runtime,
    names::BackendName,
    protocol::{BackendAction, BackendEventId, BackendStateMessage},
    types::BackendState,
    util::GuardHandle,
};
use anyhow::Result;
use dashmap::DashMap;
use futures_util::StreamExt;
use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};
use valuable::Valuable;

pub struct Executor<R: Runtime> {
    pub runtime: Arc<R>,
    state_store: Arc<Mutex<StateStore>>,
    backends: Arc<DashMap<BackendName, Arc<BackendManager<R>>>>,
    ip: IpAddr,
    _backend_event_listener: GuardHandle,
}

impl<R: Runtime> Executor<R> {
    pub fn new(runtime: Arc<R>, state_store: StateStore, ip: IpAddr) -> Self {
        let backends: Arc<DashMap<BackendName, Arc<BackendManager<R>>>> = Arc::default();

        let backend_event_listener = {
            let docker = runtime.clone();
            let backends = backends.clone();

            GuardHandle::new(async move {
                let mut events = Box::pin(docker.events());
                while let Some(event) = events.next().await {
                    if let Some((_, manager)) = backends.remove(&event.backend_id) {
                        tracing::info!(
                            backend_id = event.backend_id.as_value(),
                            exit_code = event.exit_code.unwrap_or(-1),
                            "Backend terminated.",
                        );

                        if let Err(err) = manager.mark_terminated(event.exit_code) {
                            tracing::error!(?err, "Error marking backend as terminated.");
                        }
                    }
                }

                tracing::info!("Backend event listener stopped.");
            })
        };

        Self {
            runtime,
            state_store: Arc::new(Mutex::new(state_store)),
            backends,
            ip,
            _backend_event_listener: backend_event_listener,
        }
    }

    pub fn register_listener<F>(&self, listener: F) -> Result<()>
    where
        F: Fn(BackendStateMessage) + Send + Sync + 'static,
    {
        self.state_store
            .lock()
            .expect("State store lock poisoned.")
            .register_listener(listener)
    }

    pub fn ack_event(&self, event_id: BackendEventId) -> Result<()> {
        self.state_store
            .lock()
            .expect("State store lock poisoned.")
            .ack_event(event_id)
    }

    pub async fn apply_action(
        &self,
        backend_id: &BackendName,
        action: &BackendAction,
    ) -> Result<()> {
        match action {
            BackendAction::Spawn {
                executable,
                key,
                static_token,
            } => {
                let callback = {
                    let state_store = self.state_store.clone();
                    let backend_id = backend_id.clone();
                    let timestamp = chrono::Utc::now();
                    move |state: &BackendState| {
                        state_store
                            .lock()
                            .expect("State store lock poisoned.")
                            .register_event(&backend_id, state, timestamp)?;

                        Ok(())
                    }
                };

                let backend_config: R::BackendConfig = serde_json::from_value(executable.clone())?;

                let manager = BackendManager::new(
                    backend_id.clone(),
                    backend_config,
                    BackendState::default(),
                    self.runtime.clone(),
                    callback,
                    self.ip,
                    key.clone(),
                    static_token.clone(),
                );
                tracing::info!(backend_id = backend_id.as_value(), "Inserting backend.");
                self.backends.insert(backend_id.clone(), manager);
            }
            BackendAction::Terminate { kind, reason } => {
                tracing::info!("Terminating backend {}.", backend_id);

                let manager = {
                    // We need to be careful here not to hold the lock when we call terminate, or
                    // else we can deadlock.
                    let Some(manager) = self.backends.get(backend_id) else {
                        tracing::warn!(backend_id = backend_id.as_value(), "Backend not found when handling terminate action (assumed terminated).");
                        return Ok(());
                    };
                    manager.clone()
                };

                manager.terminate(*kind, *reason).await?;
            }
        }

        Ok(())
    }
}
