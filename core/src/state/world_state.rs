use crate::{
    messages::{
        agent,
        state::{
            BackendLockMessage, BackendLockMessageType, BackendMessageType, ClusterStateMessage,
            DroneMessageType, DroneMeta, WorldStateMessage,
        },
    },
    types::{BackendId, ClusterName, DroneId, PlaneLockName, PlaneLockState},
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
    future::ready,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tokio::sync::broadcast::{channel, Sender};

#[derive(Default, Debug, Clone)]
pub struct StateHandle {
    state: Arc<RwLock<WorldState>>,
}

impl StateHandle {
    pub fn new(world_state: WorldState) -> Self {
        Self {
            state: Arc::new(RwLock::new(world_state)),
        }
    }

    pub fn wait_for_seq(
        // note: this is exclusively borrowed to prevent deadlocks
        // that can result from awaiting wait_for_seq
        // while holding a handle to .state()
        &mut self,
        sequence: u64,
    ) -> Pin<Box<dyn core::future::Future<Output = ()> + Send + Sync>> {
        let mut state = self.state.write().unwrap();
        if state.logical_time >= sequence {
            return Box::pin(ready(()));
        }
        let mut recv = match state.listeners.entry(sequence) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (send, recv) = channel(1024);
                entry.insert(send);
                recv
            }
        };

        Box::pin(async move {
            recv.recv().await.unwrap();
        })
    }

    pub fn get_ready_drones(
        &self,
        cluster: &ClusterName,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<DroneId>> {
        let world_state = self
            .state
            .read()
            .expect("Could not acquire world_state lock.");

        let min_keepalive = timestamp - chrono::Duration::seconds(30);

        Ok(world_state
            .cluster(cluster)
            .ok_or_else(|| anyhow!("Cluster not found."))?
            .drones
            .iter()
            .filter(|(_, drone)| {
                drone.state() == Some(agent::DroneState::Ready)
                    && drone.meta.is_some()
                    && drone.last_seen > Some(min_keepalive)
            })
            .map(|(drone_id, _)| drone_id.clone())
            .collect())
    }

    pub fn state(&self) -> RwLockReadGuard<WorldState> {
        self.state
            .read()
            .expect("Could not acquire world_state lock.")
    }

    pub(crate) fn write_state(&self) -> RwLockWriteGuard<WorldState> {
        self.state
            .write()
            .expect("Could not acquire world_state lock.")
    }
}

#[derive(Default, Debug)]
pub struct WorldState {
    logical_time: u64,
    pub clusters: BTreeMap<ClusterName, ClusterState>,
    listeners: BTreeMap<u64, Sender<()>>,
}

impl WorldState {
    pub fn logical_time(&self) -> u64 {
        self.logical_time
    }

    pub fn apply(&mut self, message: WorldStateMessage, sequence: u64) {
        let cluster = self.clusters.entry(message.cluster.clone()).or_default();
        cluster.apply(message.message);
        self.logical_time = sequence;

        self.listeners = {
            let outstanding_listeners = self.listeners.split_off(&(sequence + 1));
            for (_, sender) in self.listeners.iter() {
                sender.send(()).unwrap();
            }
            outstanding_listeners
        };
    }

    pub fn cluster(&self, cluster: &ClusterName) -> Option<&ClusterState> {
        self.clusters.get(cluster)
    }
}

#[derive(Default, Debug)]
pub struct ClusterState {
    pub drones: BTreeMap<DroneId, DroneState>,
    pub backends: BTreeMap<BackendId, BackendState>,
    pub txt_records: VecDeque<String>,
    pub locks: BTreeMap<PlaneLockName, PlaneLockState>,
}

impl ClusterState {
    fn apply(&mut self, message: ClusterStateMessage) {
        match message {
            ClusterStateMessage::LockMessage(message) => match message.message {
                crate::messages::state::ClusterLockMessageType::Announce { uid } => {
                    if let Entry::Vacant(entry) = self.locks.entry(message.lock) {
                        entry.insert(PlaneLockState::Announced { uid });
                    };
                }
            },
            ClusterStateMessage::DroneMessage(message) => {
                let drone = self.drones.entry(message.drone).or_default();
                drone.apply(message.message);
            }
            ClusterStateMessage::BackendMessage(message) => {
                let backend = self.backends.entry(message.backend.clone()).or_default();

                // replace this with a way to cache from backendstate
                // If the message is an assignment and includes a lock, we want to record it.
                if let BackendMessageType::LockMessage(BackendLockMessage {
                    ref lock,
                    message: crate::messages::state::BackendLockMessageType::Assign,
                }) = message.message
                {
                    match self.locks.entry(lock.clone()) {
                        Entry::Vacant(_) => panic!("lock must be announced before assignment"),
                        Entry::Occupied(mut v) => {
                            *v.get_mut() = PlaneLockState::Assigned {
                                backend: message.backend,
                            }
                        }
                    }
                }

                if let BackendMessageType::State { state, .. } = message.message {
                    if state.terminal() {
                        for lock in &backend.locks {
                            self.locks.remove(lock);
                        }
                    }
                }

                backend.apply(message.message);
            }
            ClusterStateMessage::AcmeMessage(message) => {
                if self.txt_records.len() >= 10 {
                    self.txt_records.pop_front();
                }

                self.txt_records.push_back(message.value);
            }
        }
    }

    pub fn a_record_lookup(&self, backend: &BackendId) -> Option<IpAddr> {
        let backend = self.backends.get(backend)?;

        let drone = backend.drone.as_ref()?;
        let drone = self.drones.get(drone)?;

        drone.meta.as_ref().map(|d| d.ip)
    }

    pub fn drone(&self, drone: &DroneId) -> Option<&DroneState> {
        self.drones.get(drone)
    }

    pub fn backend(&self, backend: &BackendId) -> Option<&BackendState> {
        self.backends.get(backend)
    }

    pub fn locked(&self, lock: &str) -> PlaneLockState {
        if let Some(lock_state) = self.locks.get(lock) {
            lock_state.clone()
        } else {
            PlaneLockState::Unlocked
        }
    }
}

#[derive(Default, Debug)]
pub struct DroneState {
    pub meta: Option<DroneMeta>,
    pub states: BTreeSet<(chrono::DateTime<chrono::Utc>, agent::DroneState)>,
    pub last_seen: Option<chrono::DateTime<chrono::Utc>>,
}

impl DroneState {
    fn apply(&mut self, message: DroneMessageType) {
        match message {
            DroneMessageType::Metadata(meta) => self.meta = Some(meta),
            DroneMessageType::State { state, timestamp } => {
                self.states.insert((timestamp, state));
            }
            DroneMessageType::KeepAlive { timestamp } => self.last_seen = Some(timestamp),
        }
    }

    /// Return the most recent state, or None.
    pub fn state(&self) -> Option<agent::DroneState> {
        self.states.last().map(|(_, state)| *state)
    }
}

#[derive(Default, Debug)]
pub struct BackendState {
    pub drone: Option<DroneId>,
    pub bearer_token: Option<String>,
    pub locks: Vec<PlaneLockName>,
    pub states: BTreeSet<(chrono::DateTime<chrono::Utc>, agent::BackendState)>,
}

impl BackendState {
    fn apply(&mut self, message: BackendMessageType) {
        match message {
            BackendMessageType::LockMessage(BackendLockMessage {
                lock,
                message: BackendLockMessageType::Assign,
            }) => {
                self.locks.push(lock);
            }
            BackendMessageType::Assignment {
                drone,
                bearer_token,
                ..
            } => {
                self.drone = Some(drone);
                self.bearer_token = bearer_token;
            }
            BackendMessageType::State {
                state: status,
                timestamp,
            } => {
                self.states.insert((timestamp, status));
                if status.terminal() {
                    self.locks.clear();
                }
            }
        }
    }

    pub fn state(&self) -> Option<agent::BackendState> {
        self.states.last().map(|(_, state)| *state)
    }

    pub fn state_timestamp(&self) -> Option<(chrono::DateTime<chrono::Utc>, agent::BackendState)> {
        self.states.last().copied()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::state::{AcmeDnsRecord};
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_listener() {
        let mut state = StateHandle::default();

        let zerolistener = state.wait_for_seq(0);
        timeout(Duration::from_secs(0), zerolistener)
            .await
            .expect("zerolistener should return immediately");

        let onelistener = state.wait_for_seq(1);
        let onelistener_2 = state.wait_for_seq(1);
        let twolistener = state.wait_for_seq(2);
        let twolistener_2 = state.wait_for_seq(2);
        // onelistener should block.
        {
            let result = timeout(Duration::from_secs(0), onelistener).await;
            assert!(result.is_err());
        }

        state.write_state().apply(
            WorldStateMessage {
                cluster: ClusterName::new("cluster"),
                message: ClusterStateMessage::AcmeMessage(AcmeDnsRecord {
                    value: "value".into(),
                }),
            },
            1,
        );

        // onelistener should now return.
        timeout(Duration::from_secs(0), onelistener_2)
            .await
            .expect("onelistener should return immediately");

        // twolistener should block
        {
            let result = timeout(Duration::from_secs(0), twolistener).await;
            assert!(result.is_err());
        }

        state.write_state().apply(
            WorldStateMessage {
                cluster: ClusterName::new("cluster"),
                message: ClusterStateMessage::AcmeMessage(AcmeDnsRecord {
                    value: "value".into(),
                }),
            },
            2,
        );

        // twolistener should now return.
        {
            timeout(Duration::from_secs(0), twolistener_2)
                .await
                .expect("wait_for_seq(2) should return immediately");
        }
    }

    #[test]
    fn test_locks() {
        /*
        let mut state = ClusterState::default();
        let backend = BackendId::new_random();

        // Initially, no locks are held.
        assert!(state.locked("mylock").is_none());

        // Assign a backend to a drone and acquire a lock.
        state.apply(ClusterStateMessage::BackendMessage(BackendMessage {
            backend: backend.clone(),
            message: BackendMessageType::Assignment {
                drone: DroneId::new("drone".into()),
                bearer_token: None,
            },
        }));

        assert_eq!(backend, state.locked("mylock").unwrap());

        // Update the backend state to loading.
        state.apply(ClusterStateMessage::BackendMessage(BackendMessage {
            backend: backend.clone(),
            message: BackendMessageType::State {
                state: agent::BackendState::Loading,
                timestamp: Utc::now(),
            },
        }));

        assert_eq!(backend, state.locked("mylock").unwrap());

        // Update the backend state to starting.
        state.apply(ClusterStateMessage::BackendMessage(BackendMessage {
            backend: backend.clone(),
            message: BackendMessageType::State {
                state: agent::BackendState::Starting,
                timestamp: Utc::now(),
            },
        }));

        assert_eq!(backend, state.locked("mylock").unwrap());

        // Update the backend state to ready.
        state.apply(ClusterStateMessage::BackendMessage(BackendMessage {
            backend: backend.clone(),
            message: BackendMessageType::State {
                state: agent::BackendState::Ready,
                timestamp: Utc::now(),
            },
        }));

        assert_eq!(backend, state.locked("mylock").unwrap());

        // Update the backend state to swept.
        state.apply(ClusterStateMessage::BackendMessage(BackendMessage {
            backend: backend.clone(),
            message: BackendMessageType::State {
                state: agent::BackendState::Swept,
                timestamp: Utc::now(),
            },
        }));

        // The lock should now be free.
        assert!(state.locked("mylock").is_none());

        state.backends.remove(&BackendId::new("backend".into()));

        // The lock should still be free.
        assert!(state.locked("mylock").is_none());
        */
    }
}
