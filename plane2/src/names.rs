use crate::types::NodeKind;
use clap::error::ErrorKind;
use std::fmt::{Debug, Display};

const MAX_NAME_LENGTH: usize = 30;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum NameError {
    #[error("invalid prefix: {0}")]
    InvalidAnyPrefix(String),

    #[error("invalid prefix: {0}, expected {1}-")]
    InvalidPrefix(String, String),

    #[error("invalid character: {0} at position {1}")]
    InvalidCharacter(char, usize),

    #[error(
        "too long ({0} characters; max is {} including prefix)",
        MAX_NAME_LENGTH
    )]
    TooLong(usize),
}

pub trait Name:
    Display + ToString + Debug + Clone + Send + Sync + 'static + TryFrom<String, Error = NameError>
{
    fn as_str(&self) -> &str;

    fn new_random() -> Self;

    fn prefix() -> &'static str;
}

macro_rules! entity_name {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name(String);

        impl Name for $name {
            fn as_str(&self) -> &str {
                &self.0
            }

            fn new_random() -> Self {
                let st = crate::util::random_prefixed_string($prefix);
                Self(st)
            }

            fn prefix() -> &'static str {
                $prefix
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = NameError;

            fn try_from(s: String) -> Result<Self, NameError> {
                if !s.starts_with($prefix) {
                    return Err(NameError::InvalidPrefix(s, $prefix.to_string()));
                }

                if s.len() > MAX_NAME_LENGTH {
                    return Err(NameError::TooLong(s.len()));
                }

                for (i, c) in s.chars().enumerate() {
                    if !c.is_ascii_alphanumeric() && c != '-' {
                        return Err(NameError::InvalidCharacter(c, i));
                    }
                }

                Ok(Self(s))
            }
        }

        impl clap::builder::ValueParserFactory for $name {
            type Parser = NameParser<$name>;
            fn value_parser() -> Self::Parser {
                NameParser::<$name>::new()
            }
        }
    };
}

#[derive(Clone)]
pub struct NameParser<T: Name> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: Name> NameParser<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Name> clap::builder::TypedValueParser for NameParser<T> {
    type Value = T;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let st = value
            .to_str()
            .ok_or_else(|| clap::Error::new(ErrorKind::InvalidUtf8))?;
        match T::try_from(st.to_string()) {
            Ok(val) => Ok(val),
            Err(err) => Err(cmd.clone().error(ErrorKind::InvalidValue, &err.to_string())),
        }
    }
}

entity_name!(ControllerName, "co");
entity_name!(BackendName, "ba");
entity_name!(ProxyName, "pr");
entity_name!(DroneName, "dr");
entity_name!(AcmeDnsServerName, "ns");
entity_name!(BackendActionName, "ak");

pub trait NodeName: Name {
    fn kind(&self) -> NodeKind;
}

impl NodeName for ProxyName {
    fn kind(&self) -> NodeKind {
        NodeKind::Proxy
    }
}

impl NodeName for DroneName {
    fn kind(&self) -> NodeKind {
        NodeKind::Drone
    }
}

impl NodeName for AcmeDnsServerName {
    fn kind(&self) -> NodeKind {
        NodeKind::AcmeDnsServer
    }
}

pub enum AnyNodeName {
    Proxy(ProxyName),
    Drone(DroneName),
    AcmeDnsServer(AcmeDnsServerName),
}

impl Display for AnyNodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyNodeName::Proxy(name) => write!(f, "{}", name),
            AnyNodeName::Drone(name) => write!(f, "{}", name),
            AnyNodeName::AcmeDnsServer(name) => write!(f, "{}", name),
        }
    }
}

impl TryFrom<String> for AnyNodeName {
    type Error = NameError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.starts_with(ProxyName::prefix()) {
            Ok(AnyNodeName::Proxy(ProxyName::try_from(s)?))
        } else if s.starts_with(DroneName::prefix()) {
            Ok(AnyNodeName::Drone(DroneName::try_from(s)?))
        } else if s.starts_with(AcmeDnsServerName::prefix()) {
            Ok(AnyNodeName::AcmeDnsServer(AcmeDnsServerName::try_from(s)?))
        } else {
            Err(NameError::InvalidAnyPrefix(s))
        }
    }
}

impl AnyNodeName {
    pub fn kind(&self) -> NodeKind {
        match self {
            AnyNodeName::Proxy(_) => NodeKind::Proxy,
            AnyNodeName::Drone(_) => NodeKind::Drone,
            AnyNodeName::AcmeDnsServer(_) => NodeKind::AcmeDnsServer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_controller_name() {
        let name = ControllerName::new_random();
        assert!(name.to_string().starts_with("co-"));
    }

    #[test]
    fn test_valid_name() {
        assert_eq!(
            Ok(ControllerName("co-abcd".to_string())),
            ControllerName::try_from("co-abcd".to_string())
        );
    }

    #[test]
    fn test_invalid_prefix() {
        assert_eq!(
            Err(NameError::InvalidPrefix(
                "invalid".to_string(),
                "co".to_string()
            )),
            ControllerName::try_from("invalid".to_string())
        );
    }

    #[test]
    fn test_invalid_chars() {
        assert_eq!(
            Err(NameError::InvalidCharacter('*', 3)),
            ControllerName::try_from("co-*a".to_string())
        );
    }

    #[test]
    fn test_too_long() {
        let name = "co-".to_string() + &"a".repeat(100 - 3);
        assert_eq!(Err(NameError::TooLong(100)), ControllerName::try_from(name));
    }
}