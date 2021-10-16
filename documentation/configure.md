# Setup and configure

## Setup

SonyaWQ is easily set up.

### Docker setup

#### Queue

**Queue** images places in docker hub with tag [mnwamnowich/sonya:queue-latest](https://hub.docker.com/r/mnwamnowich/sonya/tags)

Pull and run sonya queue image with docker:
```shell
docker pull mnwamnowich/sonya:queue-latest
docker run --name sonya_queue -p 8080:8080 -d mnwamnowich/sonya:queue-latest
```

#### Proxy

**Proxy** images places in docker hub with tag [mnwamnowich/sonya:proxy-latest](https://hub.docker.com/r/mnwamnowich/sonya/tags)

Pull and run sonya queue image with docker:
```shell
docker pull mnwamnowich/sonya:proxy-latest
docker run --name sonya_queue -p 8081:8081 -d mnwamnowich/sonya:queue-latest
```

### Cargo setup

#### Queue

Install and run **queue** from cargo.
```shell
cargo install sonya
cargo run sonya
```

#### Proxy

Install and run **proxy** from cargo.
```shell
cargo install sonya-proxy
cargo run sonya-proxy
```

### Docker compose example

```yaml
version: "3.9"
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    hostname: etcd-00
    command:
      - etcd
      - --name=etcd-00
      - --data-dir=data.etcd
      - --advertise-client-urls=http://etcd-00:2379,http://localhost:2379
      - --listen-client-urls=http://0.0.0.0:2379
      - --initial-advertise-peer-urls=http://etcd-00:2380,http://localhost:2380
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-cluster-state=new
      - --initial-cluster-token=etcd-cluster-1
  sonya_server-1:
    hostname: sonya-1
    image: mnwamnowich/sonya:queue-latest
    environment:
      - QUEUE_DEFAULT=test
      - SERVICE_DISCOVERY_TYPE=ETCD
      - SERVICE_DISCOVERY_HOSTS=http://etcd-00:2379
      - SERVICE_DISCOVERY_INSTANCE_ADDR=http://sonya-1:8080
    depends_on:
      - etcd
  sonya_server-2:
    hostname: sonya-2
    image: mnwamnowich/sonya:queue-latest
    environment:
      - QUEUE_DEFAULT=test
      - SERVICE_DISCOVERY_TYPE=ETCD
      - SERVICE_DISCOVERY_HOSTS=http://etcd-00:2379
      - SERVICE_DISCOVERY_INSTANCE_ADDR=http://sonya-2:8080
    depends_on:
      - etcd
  sonya_proxy:
    image: mnwamnowich/sonya:proxy-latest
    ports:
      - "8081:8081"
    environment:
      - SERVICE_DISCOVERY_TYPE=ETCD
      - SERVICE_DISCOVERY_HOSTS=http://etcd-00:2379
    depends_on:
      - etcd
```

## Configure

For choosing configuration type, you need to provide `CONFIG` environment to process.

**Yaml:**
```shell
CONFIG=./config.yaml sonya
```

**JSON:**
```shell
CONFIG=./config.json sonya
```

**ENV:**
```shell
CONFIG=ENV sonya
```

You have three ways to configure services.

### Queue

#### Yaml

**Base Config:**
```yaml
addr: 0.0.0.0:8080 # optional string, default 0.0.0.0:8080. Address which will be listened.
secure: # optional object. Will checks service token in header or query string if set.
  service_token: service_token_test # required string. Service token string.
  jwt_token_expiration: 60 # optional number, default 60. Jwt expiration time in seconds.
queue: # optional object, default {default: [], db_path: "/tmp/sonya"}. Will setup default queues.
  default: # optional array of strings, default empty. Queues with these names will create automatically on queue startup
    - queue_name
  db_path: # optional string. Path to local storage, if not set, db works from RAM.
  max_key_updates: 10 # optional positive number, default null. Max keys versions which will be possible to ask with sequence query parameter. Set 0 to disable sequences.
tls: # optional object. Will enable tls.
  private_key: /private/key/path.pem # required string. Path to private key.
  cert: /cert/path.pem # required string. Path to cert.
service_discovery: # optional object, default type: api. Will enable service discovery support.
  type: api # required string, enum of api and etcd.
```

**Service discovery variants:**
* api (default):
```yaml
service_discovery:
  type: api
```
* etcd:
```yaml
service_discovery:
  type: etcd
  hosts:
    - http://etcd-host:2379
  prefix: sonya # optional, default sonya. etcd prefix.
  instance_opts:
    instance_id: 1 # optional, default random UUID. With this id instance will be registered in etcd.
    instance_addr: http://sonya-queue:8080 # required. With this address queue will be registered in etcd.
```

**Short variant**
```yaml
addr: 0.0.0.0:8080
secure: service_token_test 
service_discovery: api
```

#### JSON

**Base Config:**
```json
{
  "addr": "0.0.0.0:8080",
  "secure": {
    "service_token": "service_token_test",
    "jwt_token_expiration": 60
  },
  "queue": {
    "default": [
        "queue_name"
    ],
    "db_path": "/tmp/sonya",
    "max_key_updates": 10
  },
  "tls": {
    "private_key": "/private/key/path.pem",
    "cert": "/cert/path.pem"
  },
  "service_discovery": {
    "type": "api"
  }
}
```

**Service discovery variants:**
* api (default):
```json
{
  "service_discovery": {
    "type": "api"
  }
}
```
* etcd:
```json
{
  "service_discovery": {
    "type": "etcd",
    "hosts": [
      "http://etcd-host:2379"
    ],
    "prefix": "sonya",
    "instance_opts": {
      "instance_id": 1,
      "instance_addr": "http://sonya-queue:8080"
    }
  }
}
```

**Short variant**
```json
{
  "addr": "0.0.0.0:8080",
  "secure": "service_token_test",
  "service_discovery": "api"
}
```

#### ENV

```shell
ADDR=addr:port #service address which will be listened
#TLS options
TLS_PRIVATE_KEY=key.pem
TLS_CERT=key.pem

#Secure options
SECURE_SERVICE_TOKEN=service_token_test #Service token
SECURE_JWT_EXPIRATION_TIME=60 #Jwt expiration time

#Queue options
QUEUE_DEFAULT=test1;test #Default queues splits by ;, queue server only
QUEUE_DB_PATH=/tmp/sonya # DB data path, queue server only. If not set, db works from RAM.
QUEUE_MAX_KEY_UPDATES=10 # Max keys versions which will be possible to ask with sequence query parameter.

# Service discovery
SERVICE_DISCOVERY_TYPE=API #Possible service discovery types is API, ETCD
SERVICE_DISCOVERY_HOSTS=http://etcd_host:port;http://etcd_host2:port #Hosts splits by ;, required by ETCD type
SERVICE_DISCOVERY_PREFIX=sonya #Prefix for service discovery key
SERVICE_DISCOVERY_INSTANCE_ADDR=http://queue:port #instance addr which will be registered in service discovery, required by server
SERVICE_DISCOVERY_INSTANCE_id=123 #instance id which will be registered in service discovery
```

### Proxy

#### Yaml

**Base Config:**
```yaml
addr: 0.0.0.0:8081 # optional string, default 0.0.0.0:8081. Address which will be listened.
secure: # optional object. Will checks service token in header or query string if set.
  service_token: service_token_test # required string. Service token string.
  jwt_token_expiration: 60 # optional number, default 60. Jwt expiration time in seconds.
tls: # optional object. Will enable tls.
  private_key: /private/key/path.pem # required string. Path to private key.
  cert: /cert/path.pem # required string. Path to cert.
service_discovery: # optional object, default type: api. Will enable service discovery support.
  type: api # required string, enum of api and etcd.
  default: # optional array of strings. Default queue shards list.
    - http://sonya-queue:8080
websocket: #optional object, fields has default values. Websocket params which will provided to shards.
  key: SGVsbG8sIHdvcmxkIQ== # required string, default SGVsbG8sIHdvcmxkIQ==. Sec-WebSocket-Key value for connecting with shards.
  version: 13 # optional string, default 13. Websocket version.
garbage_collector: #optional object, fields has default values. Options for clearing useless proxy connections.
  interval: 60 #optional number, default 60. Time interval for clearing useless proxy connections.
```

**Service discovery variants:**
* api (default):
```yaml
service_discovery:
  type: api
  default:
    - http://sonya-queue:8080
```
* etcd:
```yaml
service_discovery:
  type: etcd
  hosts:
    - http://etcd-host:2379
  prefix: sonya # optional, default sonya. etcd prefix.
  default:
    - http://sonya-queue:8080
```

**Short variant**
```yaml
addr: 0.0.0.0:8080
secure: service_token_test 
queue: queue_name
service_discovery: api
```

#### JSON

**Base Config:**
```json
{
  "addr": "0.0.0.0:8081",
  "secure": {
    "service_token": "service_token_test",
    "jwt_token_expiration": 60
  },
  "tls": {
    "private_key": "/private/key/path.pem",
    "cert": "/cert/path.pem"
  },
  "service_discovery": {
    "type": "api",
    "default": [
      "http://sonya-queue:8080"
    ]
  },
  "websocket": {
    "key": "SGVsbG8sIHdvcmxkIQ==",
    "version": 13
  },
  "garbage_collector": {
    "interval": 60
  }
}
```

**Service discovery variants:**
* api (default):
```json
{
  "service_discovery": {
    "type": "api",
    "default": [
      "http://sonya-queue:8080"
    ]
  }
}
```
* etcd:
```json
{
  "service_discovery": {
    "type": "etcd",
    "hosts": [
      "http://etcd-host:2379"
    ],
    "prefix": "sonya",
    "default": [
      "http://sonya-queue:8080"
    ]
  }
}
```

**Short variant**
```json
{
  "addr": "0.0.0.0:8080",
  "secure": "service_token_test",
  "queue": "queue_name",
  "service_discovery": "api"
}
```

#### ENV

```shell
ADDR=addr:port #service address which will be listened
#TLS options
TLS_PRIVATE_KEY=key.pem
TLS_CERT=key.pem

#Secure options
SECURE_SERVICE_TOKEN=service_token_test #Service token
SECURE_JWT_EXPIRATION_TIME=60 #Jwt expiration time

# Service discovery
SERVICE_DISCOVERY_TYPE=API #Possible service discovery types is API, ETCD
SERVICE_DISCOVERY_HOSTS=http://etcd_host:port;http://etcd_host2:port #Hosts splits by ;, required by ETCD type
SERVICE_DISCOVERY_PREFIX=sonya #Prefix for service discovery key
SERVICE_DISCOVERY_DEFAULT_SHARDS=http://queue:port;http://queue2:port // Hosts splits by ;, required by ETCD type

# Web socket
WEBSOCKET_KEY=SGVsbG8sIHdvcmxkIQ== # Sec Web Socket header, proxy only
WEBSOCKET_VERSION=13 # Web Socket version, proxy only

# Garbage collector
GARBAGE_COLLECTOR_INTERVAL=60 # Time in seconds when proxy storage will be cleared, proxy only
```