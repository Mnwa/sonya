use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;
use std::env::VarError;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

/// Extracts config from yaml, json or environment
/// You can manage it with env var `CONFIG`
///
/// Example:
/// ```sh
/// CONFIG=ENV ADDR=0.0.0.0:8080 ./bin
/// CONFIG=./config.yaml ./bin
/// CONFIG=./config.json ./bin
/// ```
///
/// Available envs when `CONFIG=ENV` was set:
/// ```env
/// ADDR=addr:port // service address which will be listened
/// // Tls options
/// TLS_PRIVATE_KEY=key.pem
/// TLS_CERT=key.pem
/// SECURE_SERVICE_TOKEN=xxx // Service token
/// SECURE_JWT_EXPIRATION_TIME=60 // Jwt expiration time
/// QUEUE_DEFAULT=test1;test // Default queues splits by ;, queue server only
/// QUEUE_DB_PATH=/tmp/sonya // DB data path, queue server only
/// QUEUE_MAX_KEY_UPDATES=10 // Maximum key version to store
/// SERVICE_DISCOVERY_TYPE=API // Possible service discovery types is API, ETCD
/// SERVICE_DISCOVERY_HOSTS=http://etcd_host:port;http://etcd_host2:port // Hosts splits by ;, required by ETCD type
/// SERVICE_DISCOVERY_DEFAULT_SHARDS=http://queue:port;http://queue2:port // Hosts splits by ;, required by ETCD type
/// SERVICE_DISCOVERY_PREFIX=sonya // Prefix for service discovery key
/// SERVICE_DISCOVERY_INSTANCE_ADDR=http://queue:port // instance addr which will be registered in service discovery, required by server
/// SERVICE_DISCOVERY_INSTANCE_id=123 // instance id which will be registered in service discovery
/// WEBSOCKET_KEY=SGVsbG8sIHdvcmxkIQ== // Sec Web Socket header, proxy only
/// WEBSOCKET_VERSION=13 // Web Socket version, proxy only
/// GARBAGE_COLLECTOR_INTERVAL=60 // Time in seconds when proxy storage will be cleared, proxy only
/// ```
pub fn get_config() -> Config {
    env_logger::init();
    let config_path = std::env::var("CONFIG").unwrap_or_else(|e| match e {
        VarError::NotPresent => String::from("ENV"),
        e => panic!("{}", e),
    });

    match ConfigParsingStrategy::from_str(&config_path).unwrap() {
        ConfigParsingStrategy::Env => from_env().unwrap(),
        ConfigParsingStrategy::Yaml(r) => from_yaml(&r).unwrap(),
        ConfigParsingStrategy::Json(r) => from_json(&r).unwrap(),
    }
}

fn from_env() -> Result<Config, VarError> {
    Ok(Config {
        addr: from_env_optional("ADDR")?.map(|a| SocketAddr::from_str(&a).expect("invalid addr")),
        tls: tls_from_env()?,
        secure: secure_from_env()?,
        queue: queue_from_env()?,
        service_discovery: service_discovery_from_env()?,
        websocket: websocket_from_env()?,
        garbage_collector: garbage_collector_from_env()?,
    })
}

fn tls_from_env() -> Result<Option<Tls>, VarError> {
    let private_key = from_env_optional("TLS_PRIVATE_KEY")?;
    let cert = from_env_optional("TLS_CERT")?;

    Ok(private_key
        .and_then(|p| Some((p, cert?)))
        .map(|(p, c)| Tls {
            private_key: p,
            cert: c,
        }))
}

fn secure_from_env() -> Result<Option<Secure>, VarError> {
    let jwt_token_expiration = from_env_optional("SECURE_JWT_EXPIRATION_TIME")?
        .map(|e| e.parse().expect("invalid jwt expiration time"))
        .unwrap_or_else(default_jwt_token_expiration);
    let service_token = from_env_optional("SECURE_SERVICE_TOKEN")?.map(|st| Secure {
        service_token: st,
        jwt_token_expiration,
    });
    Ok(service_token)
}

fn garbage_collector_from_env() -> Result<GarbageCollector, VarError> {
    let gb = from_env_optional("GARBAGE_COLLECTOR_INTERVAL")?
        .map(|interval| GarbageCollector {
            interval: interval.parse().expect("invalid garbage interval"),
        })
        .unwrap_or_default();
    Ok(gb)
}

fn websocket_from_env() -> Result<WebSocket, VarError> {
    let mut websocket = WebSocket::default();
    if let Some(key) = from_env_optional("WEBSOCKET_KEY")? {
        websocket.key = key;
    }
    if let Some(version) = from_env_optional("WEBSOCKET_VERSION")? {
        websocket.version = version;
    }
    Ok(websocket)
}

fn queue_from_env() -> Result<Queue, VarError> {
    let default: DefaultQueues = from_env_optional("QUEUE_DEFAULT")?
        .map(|d| {
            d.split(';')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    let db_path = from_env_optional("QUEUE_DB_PATH")?.map(PathBuf::from);
    let max_key_updates = from_env_optional("QUEUE_MAX_KEY_UPDATES")?
        .map(|mku| mku.parse().expect("invalid max keys updates value"));
    Ok(Queue {
        default,
        db_path,
        max_key_updates,
    })
}

fn service_discovery_from_env() -> Result<Option<ServiceDiscovery>, VarError> {
    let service_discovery_type =
        from_env_optional("SERVICE_DISCOVERY_TYPE")?.unwrap_or_else(|| String::from("API"));
    let default_shards: Option<Shards> = from_env_optional("SERVICE_DISCOVERY_DEFAULT_SHARDS")?
        .map(|d| {
            d.split(';')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        });

    let service_discovery = match service_discovery_type.as_str() {
        "API" => ServiceDiscovery::Api {
            default: default_shards,
        },
        "ETCD" => ServiceDiscovery::Etcd {
            default: default_shards,
            hosts: std::env::var("SERVICE_DISCOVERY_HOSTS")
                .expect("empty service discovery hosts")
                .split(';')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            prefix: from_env_optional("SERVICE_DISCOVERY_PREFIX")?
                .unwrap_or_else(default_sd_prefix),
            instance_opts: instance_opts_from_env()?,
        },
        _ => panic!("Invalid service discovery type"),
    };

    Ok(Some(service_discovery))
}

fn instance_opts_from_env() -> Result<Option<ServiceDiscoveryInstanceOptions>, VarError> {
    let instance_addr = from_env_optional("SERVICE_DISCOVERY_INSTANCE_ADDR")?;
    let instance_id = from_env_optional("SERVICE_DISCOVERY_INSTANCE_id")?;

    Ok(instance_addr.map(|ia| ServiceDiscoveryInstanceOptions {
        instance_addr: ia,
        instance_id,
    }))
}

fn from_env_optional(env_var: &str) -> Result<Option<String>, VarError> {
    std::env::var(env_var).map(Some).or_else(|e| match e {
        VarError::NotPresent => Ok(None),
        e => Err(e),
    })
}

fn from_yaml(path: &str) -> serde_yaml::Result<Config> {
    let reader = match File::open(path) {
        Ok(r) => BufReader::new(r),
        Err(e) => return Err(serde_yaml::Error::custom(e)),
    };
    serde_yaml::from_reader(reader)
}

fn from_json(path: &str) -> serde_json::Result<Config> {
    let reader = match File::open(path) {
        Ok(r) => BufReader::new(r),
        Err(e) => return Err(serde_json::Error::custom(e)),
    };
    serde_json::from_reader(reader)
}

enum ConfigParsingStrategy<T> {
    Env,
    Yaml(T),
    Json(T),
}

impl FromStr for ConfigParsingStrategy<String> {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ENV" => Ok(Self::Env),
            s if s.ends_with(".yaml") => Ok(Self::Yaml(String::from(s))),
            s if s.ends_with(".json") => Ok(Self::Json(String::from(s))),
            _ => Err("invalid config type"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub addr: Option<SocketAddr>,
    pub tls: Option<Tls>,
    pub secure: Option<Secure>,
    pub queue: Queue,
    pub service_discovery: Option<ServiceDiscovery>,
    #[serde(default)]
    pub websocket: WebSocket,
    #[serde(default)]
    pub garbage_collector: GarbageCollector,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Tls {
    pub private_key: String,
    pub cert: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct Secure {
    pub service_token: SecureToken,
    #[serde(default = "default_jwt_token_expiration")]
    pub jwt_token_expiration: u64,
}

#[derive(Deserialize)]
#[serde(remote = "Secure")]
struct SecureDef {
    pub service_token: SecureToken,
    #[serde(default = "default_jwt_token_expiration")]
    pub jwt_token_expiration: u64,
}

pub fn default_jwt_token_expiration() -> u64 {
    60
}

pub type SecureToken = String;

impl From<SecureToken> for Secure {
    fn from(service_token: SecureToken) -> Self {
        Self {
            service_token,
            jwt_token_expiration: default_jwt_token_expiration(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WebSocket {
    pub key: String,
    #[serde(default = "default_websocket_v")]
    pub version: String,
}

fn default_websocket_v() -> String {
    "13".into()
}

impl Default for WebSocket {
    fn default() -> Self {
        Self {
            key: "SGVsbG8sIHdvcmxkIQ==".into(),
            version: default_websocket_v(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Queue {
    #[serde(default)]
    pub default: DefaultQueues,
    pub db_path: Option<PathBuf>,
    pub max_key_updates: Option<usize>,
}

pub type DefaultQueues = BTreeSet<String>;

#[derive(Serialize, Clone, Debug)]
pub struct GarbageCollector {
    pub interval: u64,
}

#[derive(Deserialize)]
#[serde(remote = "GarbageCollector")]
struct GarbageCollectorDef {
    pub interval: u64,
}

impl From<u64> for GarbageCollector {
    fn from(interval: u64) -> Self {
        Self { interval }
    }
}

impl Default for GarbageCollector {
    fn default() -> Self {
        Self::from(60)
    }
}

pub type Shards = Vec<String>;

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServiceDiscovery {
    Api {
        default: Option<Shards>,
    },
    Etcd {
        default: Option<Shards>,
        hosts: ServiceDiscoveryHosts,
        #[serde(default = "default_sd_prefix")]
        prefix: String,
        instance_opts: Option<ServiceDiscoveryInstanceOptions>,
    },
}

fn default_sd_prefix() -> String {
    "sonya".into()
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[serde(remote = "ServiceDiscovery")]
enum ServiceDiscoveryDef {
    Api {
        default: Option<Shards>,
    },
    Etcd {
        default: Option<Shards>,
        hosts: ServiceDiscoveryHosts,
        #[serde(default = "default_sd_prefix")]
        prefix: String,
        instance_opts: Option<ServiceDiscoveryInstanceOptions>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServiceDiscoveryInstanceOptions {
    pub instance_id: Option<String>,
    pub instance_addr: String,
}

impl Display for ServiceDiscovery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ServiceDiscovery::Api { .. } => "api",
                ServiceDiscovery::Etcd { .. } => "etcd",
            }
        )
    }
}

impl From<Shards> for ServiceDiscovery {
    fn from(default: Shards) -> Self {
        Self::Api {
            default: Some(default),
        }
    }
}

pub type ServiceDiscoveryHosts = Vec<String>;

struct StringOrStruct<T>(PhantomData<T>);
struct VecOrStruct<T>(PhantomData<T>);
struct U64OrStruct<T>(PhantomData<T>);

#[macro_export]
macro_rules! string_or_struct_impl {
    ($struct_name: ident, $struct_name_remote: ident) => {
        impl<'de> Visitor<'de> for StringOrStruct<$struct_name> {
            type Value = $struct_name;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    formatter,
                    "string or struct {} expected",
                    std::any::type_name::<Self::Value>()
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::from(value.to_owned()))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                $struct_name_remote::deserialize(de::value::MapAccessDeserializer::new(map))
            }
        }

        impl<'de> Deserialize<'de> for $struct_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(StringOrStruct::<Self>(PhantomData))
            }
        }
    };
}

#[macro_export]
macro_rules! vec_or_struct_impl {
    ($struct_name: ident, $struct_name_remote: ident) => {
        impl<'de> Visitor<'de> for VecOrStruct<$struct_name> {
            type Value = $struct_name;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    formatter,
                    "list of strings or struct {} expected",
                    std::any::type_name::<Self::Value>()
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::from(vec![value.to_owned()]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = seq.next_element::<String>()? {
                    vec.push(elem);
                }

                Ok(Self::Value::from(vec))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                $struct_name_remote::deserialize(de::value::MapAccessDeserializer::new(map))
            }
        }

        impl<'de> Deserialize<'de> for $struct_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(VecOrStruct::<Self>(PhantomData))
            }
        }
    };
}

#[macro_export]
macro_rules! u64_or_struct {
    ($struct_name: ident, $struct_name_remote: ident) => {
        impl<'de> Visitor<'de> for U64OrStruct<$struct_name> {
            type Value = $struct_name;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    formatter,
                    "list of strings or struct {} expected",
                    std::any::type_name::<Self::Value>()
                )
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::from(value))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                $struct_name_remote::deserialize(de::value::MapAccessDeserializer::new(map))
            }
        }

        impl<'de> Deserialize<'de> for $struct_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(U64OrStruct::<Self>(PhantomData))
            }
        }
    };
}

string_or_struct_impl!(Secure, SecureDef);
vec_or_struct_impl!(ServiceDiscovery, ServiceDiscoveryDef);
u64_or_struct!(GarbageCollector, GarbageCollectorDef);
