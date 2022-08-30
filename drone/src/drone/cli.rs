use super::{
    agent::{AgentOptions, DockerApiTransport, DockerOptions},
    proxy::ProxyOptions,
};
use crate::{database_connection::DatabaseConnection, keys::KeyCertPathPair};
use anyhow::Result;
use clap::{Parser, Subcommand};
use dis_spawner::nats_connection::NatsConnection;
use dis_spawner::messages::scheduler::ClusterId;
use reqwest::Url;
use std::{
    fmt::Debug,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Parser)]
pub struct Opts {
    /// Path to sqlite3 database file to use for getting route information.
    ///
    /// This may be a file that does not exist. In this case, it will be created.
    #[clap(long, action)]
    pub db_path: Option<String>,

    /// The domain of the cluster that this drone serves.
    #[clap(long, action)]
    pub cluster_domain: Option<String>,

    /// IP:PORT pair to listen on.
    #[clap(long, action, default_value = "0.0.0.0:8080")]
    pub bind_address: SocketAddr,

    /// Path to read private key from.
    #[clap(long, action)]
    pub https_private_key: Option<PathBuf>,

    /// Path to read certificate from.
    #[clap(long, action)]
    pub https_certificate: Option<PathBuf>,

    /// Hostname for connecting to NATS.
    #[clap(long, action)]
    pub nats_url: Option<String>,

    /// Server to use for certificate signing.
    #[clap(long, action)]
    pub acme_server: Option<String>,

    /// email to use as mailto for cert issuance
    #[clap(long, action)]
    pub cert_email: Option<String>,

    /// Acme External Account Binding (EAB) ID (NOTE: NOT USED FOR LetsEncrypt)
    #[clap(long, action)]
    pub acme_eab_kid: Option<String>,

    /// Acme External Account Binding (EAB) key (NOTE: NOT USED FOR LetsEncrypt)
    #[clap(long, action)]
    pub acme_eab_key: Option<String>,

    /// Public IP of this drone, used for directing traffic outside the host.
    #[clap(long, action)]
    pub public_ip: Option<IpAddr>,

    /// API endpoint which returns the requestor's IP.
    #[clap(long, action)]
    pub ip_api: Option<Url>,

    /// Runtime to use with docker. Default is runc; runsc is an alternative if gVisor
    /// is available.
    #[clap(long, action)]
    pub docker_runtime: Option<String>,

    /// Unix socket through which to send Docker commands.
    #[clap(long, action)]
    pub docker_socket: Option<String>,

    /// HTTP url through which to send Docker commands. Mutually exclusive with --docker-socket.
    #[clap(long, action)]
    pub docker_http: Option<String>,

    #[clap(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, PartialEq)]
pub struct EabKeypair {
    pub eab_kid: String,
    pub eab_key: Vec<u8>,
}

impl EabKeypair {
    pub fn new(eab_kid: &str, eab_key_b64: &str) -> Result<EabKeypair> {
        let eab_key = base64::decode_config(&eab_key_b64, base64::URL_SAFE)?;

        Ok(EabKeypair {
            eab_key,
            eab_kid: eab_kid.to_string(),
        })
    }

    pub fn eab_key_b64(&self) -> String {
        base64::encode_config(&self.eab_key, base64::URL_SAFE)
    }
}

#[derive(Subcommand)]
enum Command {
    /// Migrate the database, and then exit.
    Migrate,

    /// Refresh the certificate, and the exit.
    Cert,

    /// Run one or more components as a service, indefinitely. Components are selected with --proxy, --agent, and --refresh.
    Serve {
        /// Run the proxy server.
        #[clap(long, action)]
        proxy: bool,

        /// Run the agent.
        #[clap(long, action)]
        agent: bool,

        /// Run the certificate refresh loop.
        #[clap(long, action)]
        cert_refresh: bool,
    },
}

impl Default for Command {
    fn default() -> Self {
        Command::Serve {
            proxy: true,
            agent: true,
            cert_refresh: true,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct CertOptions {
    pub cluster_domain: String,
    pub nats: NatsConnection,
    pub key_paths: KeyCertPathPair,
    pub email: String,
    pub acme_server_url: String,
    pub acme_eab_keypair: Option<EabKeypair>,
}

#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Debug)]
pub enum DronePlan {
    RunService {
        proxy_options: Option<ProxyOptions>,
        agent_options: Option<AgentOptions>,
        cert_options: Option<CertOptions>,
        nats: Option<NatsConnection>,
    },
    DoMigration {
        db: DatabaseConnection,
    },
    DoCertificateRefresh(CertOptions),
}

#[derive(PartialEq, Eq, Debug)]
pub enum IpProvider {
    Api(Url),
    Literal(IpAddr),
}

impl IpProvider {
    pub async fn get_ip(&self) -> Result<IpAddr> {
        match self {
            IpProvider::Literal(ip) => Ok(*ip),
            IpProvider::Api(url) => {
                let result = reqwest::get(url.as_ref()).await?.text().await?;
                let ip: IpAddr = result.parse()?;
                Ok(ip)
            }
        }
    }
}

impl From<Opts> for DronePlan {
    fn from(opts: Opts) -> Self {
        let key_cert_pair = if let (Some(private_key_path), Some(certificate_path)) =
            (&opts.https_private_key, &opts.https_certificate)
        {
            Some(KeyCertPathPair {
                certificate_path: certificate_path.clone(),
                private_key_path: private_key_path.clone(),
            })
        } else {
            assert!(
                opts.https_private_key.is_none(),
                "Expected --https-certificate if --https-private-key is provided."
            );
            assert!(
                opts.https_certificate.is_none(),
                "Expected --https-private-key if --https-certificate is provided."
            );

            None
        };

        let nats = opts
            .nats_url
            .map(NatsConnection::new)
            .transpose()
            .expect("Error parsing NATS URL.");

        let db = opts.db_path.map(DatabaseConnection::new);

        let acme_eab_keypair = match (opts.acme_eab_key, opts.acme_eab_kid) {
            (Some(eab_key), Some(eab_kid)) => Some(
                EabKeypair::new(&eab_kid, &eab_key)
                    .expect("Couldn't decode --acme-eab-key value as (url-encodable) base64."),
            ),
            (None, None) => None,
            _ => panic!(
                "If one of --acme-eab-key or --acme-eab-kid is provided, the other must be too."
            ),
        };

        match opts.command.unwrap_or_default() {
            Command::Migrate => DronePlan::DoMigration {
                db: db.expect("Expected --db-path when using migrate."),
            },
            Command::Cert => {
                DronePlan::DoCertificateRefresh(CertOptions {
                    cluster_domain: opts.cluster_domain.expect("Expected --cluster-domain when using cert command."),
                    nats: nats.expect("Expected --nats-host when using cert command."),
                    key_paths: key_cert_pair.expect("Expected --https-certificate and --https-private-key to point to location to write cert and key."),
                    acme_server_url: opts.acme_server.expect("Expected --acme-server when using cert command."),
                    email: opts.cert_email.expect("Expected --cert-email when using cert command"),
                    acme_eab_keypair,
                })
            },
            Command::Serve { proxy, agent, cert_refresh } => {
                let cert_options = if cert_refresh {
                    Some(CertOptions {
                        acme_server_url: opts.acme_server.clone().expect("Expected --acme-server for certificate refreshing."),
                        email: opts.cert_email.expect("Expected --cert-email when using cert command"),
                        cluster_domain: opts.cluster_domain.clone().expect("Expected --cluster-domain for certificate refreshing."),
                        key_paths: key_cert_pair.clone().expect("Expected --https-certificate and --https-private-key for certificate refresh."),
                        nats: nats.clone().expect("Expected --nats-url."),
                        acme_eab_keypair,
                    })
                } else {
                    None
                };

                let proxy_options = if proxy {
                    Some(ProxyOptions {
                        cluster_domain: opts
                            .cluster_domain.clone()
                            .expect("Expected --cluster-domain for serving proxy."),
                        db: db
                            .clone()
                            .expect("Expected --db-path for serving proxy."),
                        bind_address: opts.bind_address,
                        key_pair: key_cert_pair,
                    })
                } else {
                    None
                };

                let agent_options = if agent {
                    let docker_transport = if let Some(docker_socket) = opts.docker_socket {
                        DockerApiTransport::Socket(docker_socket)
                    } else if let Some(docker_http) = opts.docker_http {
                        DockerApiTransport::Http(docker_http)
                    } else {
                        DockerApiTransport::default()
                    };

                    let ip = if let Some(ip) = opts.public_ip {
                        IpProvider::Literal(ip)
                    } else if let Some(ip_api) = opts.ip_api {
                        IpProvider::Api(ip_api)
                    } else {
                        panic!("Expected one of --ip or --ip-api.")
                    };

                    Some(AgentOptions {
                        cluster_domain: ClusterId::new(opts.cluster_domain.as_ref().expect("Expected --cluster-domain for running agent.")),
                        db: db.expect("Expected --db-path for running agent."),
                        docker_options: DockerOptions {
                            runtime: opts.docker_runtime.clone(),
                            transport: docker_transport,
                        },
                        nats: nats.clone().expect("Expected --nats-url for running agent."),
                        ip,
                    })
                } else {
                    None
                };

                assert!(
                    proxy_options.is_some() || agent_options.is_some() || cert_options.is_some(),
                    "Expected at least one of --proxy, --agent, --cert-refresh if `serve` is provided explicitly."
                );

                DronePlan::RunService { proxy_options, agent_options, cert_options, nats }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;

    fn parse_args(args: &[&str]) -> Result<DronePlan> {
        let mut full_args = vec!["drone"];
        full_args.extend(args.iter());
        Ok(Opts::try_parse_from(full_args)?.try_into()?)
    }

    #[test]
    fn test_migrate() {
        let opts = parse_args(&["--db-path", "mydatabase", "migrate"]).unwrap();
        assert_eq!(
            DronePlan::DoMigration {
                db: DatabaseConnection::new("mydatabase".to_string())
            },
            opts
        );
    }

    #[test]
    fn test_cert() {
        let opts = parse_args(&[
            "--db-path",
            "mydatabase",
            "--https-certificate",
            "mycert.cert",
            "--https-private-key",
            "mycert.key",
            "--nats-url",
            "nats://foo@bar",
            "--cluster-domain",
            "mydomain.test",
            "--acme-server",
            "https://acme.server/dir",
            "--cert-email",
            "test@test.com",
            "cert",
        ])
        .unwrap();
        assert_eq!(
            DronePlan::DoCertificateRefresh(CertOptions {
                cluster_domain: "mydomain.test".to_string(),
                nats: NatsConnection::new("nats://foo@bar".to_string()).unwrap(),
                key_paths: KeyCertPathPair {
                    private_key_path: PathBuf::from("mycert.key"),
                    certificate_path: PathBuf::from("mycert.cert"),
                },
                acme_server_url: "https://acme.server/dir".to_string(),
                email: "test@test.com".to_string(),
                acme_eab_keypair: None,
            }),
            opts
        );
    }

    #[test]
    fn test_proxy() {
        let opts = parse_args(&[
            "--db-path",
            "mydatabase",
            "--cluster-domain",
            "mycluster.test",
            "serve",
            "--proxy",
        ])
        .unwrap();
        assert_eq!(
            DronePlan::RunService {
                proxy_options: Some(ProxyOptions {
                    db: DatabaseConnection::new("mydatabase".to_string()),
                    cluster_domain: "mycluster.test".to_string(),
                    bind_address: "0.0.0.0:8080".parse().unwrap(),
                    key_pair: None,
                }),
                agent_options: None,
                cert_options: None,
                nats: None,
            },
            opts
        );
    }

    #[test]
    #[should_panic(expected = "Expected ")]
    fn test_proxy_no_cluster_domain() {
        parse_args(&["--db-path", "mydatabase"]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected ")]
    fn test_proxy_no_db_path() {
        parse_args(&["--cluster-domain", "blah"]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected ")]
    fn test_migrate_no_db_path() {
        parse_args(&["migrate"]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected --https-certificate")]
    fn test_key_but_no_cert() {
        parse_args(&[
            "--db-path",
            "mydatabase",
            "--cluster-domain",
            "mycluster.test",
            "--https-private-key",
            "mycert.key",
        ])
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "Expected --https-private-key")]
    fn test_cert_but_no_key() {
        parse_args(&[
            "--db-path",
            "mydatabase",
            "--cluster-domain",
            "mycluster.test",
            "--https-certificate",
            "mycert.cert",
        ])
        .unwrap();
    }

    #[test]
    fn test_proxy_with_https() {
        let opts = parse_args(&[
            "--db-path",
            "mydatabase",
            "--cluster-domain",
            "mycluster.test",
            "--https-certificate",
            "mycert.cert",
            "--https-private-key",
            "mycert.key",
            "--public-ip",
            "123.123.123.123",
            "--nats-url",
            "nats://foo@bar",
            "serve",
            "--proxy",
            "--agent",
        ])
        .unwrap();
        assert_eq!(
            DronePlan::RunService {
                proxy_options: Some(ProxyOptions {
                    db: DatabaseConnection::new("mydatabase".to_string()),
                    cluster_domain: "mycluster.test".to_string(),
                    bind_address: "0.0.0.0:8080".parse().unwrap(),
                    key_pair: Some(KeyCertPathPair {
                        private_key_path: PathBuf::from("mycert.key"),
                        certificate_path: PathBuf::from("mycert.cert"),
                    }),
                }),
                agent_options: Some(AgentOptions {
                    db: DatabaseConnection::new("mydatabase".to_string()),
                    cluster_domain: ClusterId::new("mycluster.test"),
                    docker_options: DockerOptions {
                        transport: DockerApiTransport::Socket("/var/run/docker.sock".to_string()),
                        runtime: None,
                    },
                    ip: IpProvider::Literal("123.123.123.123".parse().unwrap()),
                    nats: NatsConnection::new("nats://foo@bar".to_string()).unwrap(),
                }),
                cert_options: None,
                nats: Some(NatsConnection::new("nats://foo@bar".to_string()).unwrap()),
            },
            opts
        );
    }

    #[test]
    fn test_proxy_with_ports() {
        let opts = parse_args(&[
            "--db-path",
            "mydatabase",
            "--cluster-domain",
            "mycluster.test",
            "--https-certificate",
            "mycert.cert",
            "--https-private-key",
            "mycert.key",
            "--bind-address",
            "127.1.1.1:8080",
            "--public-ip",
            "123.123.123.123",
            "--nats-url",
            "nats://foo@bar",
            "--acme-server",
            "https://acme-server",
            "--cert-email",
            "test@test.com",
        ])
        .unwrap();
        assert_eq!(
            DronePlan::RunService {
                proxy_options: Some(ProxyOptions {
                    db: DatabaseConnection::new("mydatabase".to_string()),
                    cluster_domain: "mycluster.test".to_string(),
                    bind_address: "127.1.1.1:8080".parse().unwrap(),
                    key_pair: Some(KeyCertPathPair {
                        private_key_path: PathBuf::from("mycert.key"),
                        certificate_path: PathBuf::from("mycert.cert"),
                    }),
                }),
                agent_options: Some(AgentOptions {
                    db: DatabaseConnection::new("mydatabase".to_string()),
                    cluster_domain: ClusterId::new("mycluster.test"),
                    docker_options: DockerOptions {
                        transport: DockerApiTransport::Socket("/var/run/docker.sock".to_string()),
                        runtime: None,
                    },
                    ip: IpProvider::Literal("123.123.123.123".parse().unwrap()),
                    nats: NatsConnection::new("nats://foo@bar".to_string()).unwrap(),
                }),
                cert_options: Some(CertOptions {
                    acme_server_url: "https://acme-server".to_string(),
                    cluster_domain: "mycluster.test".to_string(),
                    key_paths: KeyCertPathPair {
                        private_key_path: PathBuf::from("mycert.key"),
                        certificate_path: PathBuf::from("mycert.cert"),
                    },
                    email: "test@test.com".to_string(),
                    nats: NatsConnection::new("nats://foo@bar".to_string()).unwrap(),
                    acme_eab_keypair: None,
                }),
                nats: Some(NatsConnection::new("nats://foo@bar".to_string()).unwrap()),
            },
            opts
        );
    }
}
