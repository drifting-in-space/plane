use crate::{nats::TypedNats, retry::do_with_retry};
use anyhow::Result;
use async_nats::ConnectOptions;
use std::{cell::RefCell, fmt::Debug, sync::Arc, time::Duration};
use url::Url;

/// This matches NATS' Authorization struct, which is crate-private.
/// https://github.com/nats-io/nats.rs/blob/2f53feab2eac4c01fb470309a3af2c9920f9224a/async-nats/src/lib.rs#L1249
#[derive(Clone)]
pub enum Authorization {
    /// No authentication.
    None,

    /// Authenticate using a token.
    Token(String),

    /// Authenticate using a username and password.
    UserAndPassword(String, String),
    // TODO: JWT
}

impl Authorization {
    pub fn connect_options(&self) -> ConnectOptions {
        match self {
            Authorization::None => ConnectOptions::new(),
            Authorization::Token(token) => ConnectOptions::with_token(token.to_string()),
            Authorization::UserAndPassword(user, pass) => {
                ConnectOptions::with_user_and_password(user.to_string(), pass.to_string())
            }
        }
    }
}

/// Represents a shared, lazy connection to NATS.
/// No connection is made until connection().await is first
/// called. Once a successful connection is made, it is
/// cached and a clone of it is returned.
#[derive(Clone)]
pub struct NatsConnection {
    connection_string: String,
    authorization: Authorization,
    connection: Arc<RefCell<Option<TypedNats>>>,
}

impl PartialEq for NatsConnection {
    fn eq(&self, other: &Self) -> bool {
        self.connection_string == other.connection_string
    }
}

impl Debug for NatsConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsConnection")
            .field("connection_string", &self.connection_string)
            .finish()
    }
}

impl NatsConnection {
    pub fn new(connection_string: String) -> Result<Self> {
        let url = Url::parse(&connection_string)?;

        let authorization = if let Some(password) = url.password().as_ref() {
            Authorization::UserAndPassword(url.username().to_string(), password.to_string())
        } else if !url.username().is_empty() {
            Authorization::Token(url.username().to_string())
        } else {
            Authorization::None
        };

        Ok(NatsConnection {
            connection_string,
            authorization,
            connection: Arc::default(),
        })
    }

    pub async fn connection(&self) -> Result<TypedNats> {
        if let Some(nats) = self.connection.borrow().as_ref() {
            return Ok(nats.clone());
        }

        let nats = do_with_retry(
            || {
                TypedNats::connect(
                    &self.connection_string,
                    self.authorization.connect_options(),
                )
            },
            30,
            Duration::from_secs(10),
        )
        .await?;

        self.connection.replace(Some(nats.clone()));

        Ok(nats)
    }
}
