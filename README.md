<h1 align="center">Welcome to SonyaWQ 👋</h1>
<p>
  <img alt="Version" src="https://img.shields.io/badge/version-0.1-blue.svg?cacheSeconds=2592000" />
  <a href="#" target="_blank">
    <img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg" />
  </a>
</p>

> **sonya** is a fast, distributed queue that provides a flexible realization of the `Web Queue` in [Web Queue Worker](https://principles.green/principles/applied/web-queue-worker/) architecture.
> **sonya** provides Service Mesh architecture and supports `etcd` as a Service Discovery.

### 🏠 [Homepage](https://github.com/Mnwa/sonya)

## Features
### Flexible configuring
**sonya** supports simple and flexible configuring.
> You don't need to use any configurations to up the simple service

Also, the service has a lot of options that can be configured from the environment, YAML, and JSON.

#### [Configure documentation](https://github.com/Mnwa/sonya/documentation/configure.md)

#### Env example
```env
CONFIG=ENV
ADDR=0.0.0.0:8080
```

#### Yaml example
> You need to pass `CONFIG=./config.yaml` env to using yaml config file.
> Where `./config.yaml` is the path of your configuration file.
```yaml
addr: 0.0.0.0:8080
```

#### JSON example
> You need to pass `CONFIG=./config.json` env to using yaml config file.
> Where `./config.json` is the path of your configuration file.
```json
{
  "addr": "0.0.0.0:8080"
}
```
### Simple interface
**sonya** provides a simple API that can be used from websites frontend, backends, etc.

#### [API documentation](https://github.com/Mnwa/sonya/documentation/api.md)

#### Curl example subscribe
> where `my_queue_name` and `my_id` are the queue name and id that you will listen to.
```bash
curl --include \
    --no-buffer \
    --header "Connection: Upgrade" \
    --header "Upgrade: websocket" \
    --header "Host: localhost:8081" \
    --header "Origin: http://localhost:8081" \
    --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
    --header "Sec-WebSocket-Version: 13" \
    http://localhost:8081/queue/listen/ws/my_queue_name/my_id
```

#### Curl example send message
> where `my_queue_name` and `my_id` is the queue name and id whose listeners will be received this message.
```bash
curl -X POST --location "http://localhost:8081/queue/send/my_queue_name" \
    -H "Host: localhost:8081" \
    -H "Content-Type: application/json" \
    -d "{
          \"id\": \"my_id\",
          \"payload\": {
            \"message\": \"hello\"
          }
        }"
```

### Service mesh architecture and service discovery support
**sonya** consists of two services that provide `service mesh` architecture support.

#### [Sharding documentation](https://github.com/Mnwa/sonya/documentation/sharding.md)

* **queue** - is the service that provides queue functionality and may be easily sharded.
* **proxy** - is the service that will be balancing queues and ids between **queue** shards.

Every request must be sent to one of the **proxy** services, and they will be routing it to the needed **queue** service.

For registering **queue** services in **proxy**, you can use **proxy** api or `etcd`.

### Secure
**sonya** provides the JWT and service token requests support.
#### [Secure documentation](https://github.com/Mnwa/sonya/documentation/configure.md)

## Install and usage

### Install queue
```sh
cargo install sonya
```

### Install proxy
```sh
cargo install sonya-proxy
```

### Run server
```bash
CONFIG=ENV QUEUE_DEFAULT=test sonya
```

### Run proxy
```bash
CONFIG=ENV SERVICE_DISCOVERY_DEFAULT_SHARDS=http://localhost:8080 sonya-proxy
```

### Docker compose with `etcd` example

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

### Usage

#### Subscribe to updates
```bash
```bash
curl --include \
    --no-buffer \
    --header "Connection: Upgrade" \
    --header "Upgrade: websocket" \
    --header "Host: localhost:8081" \
    --header "Origin: http://localhost:8081" \
    --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
    --header "Sec-WebSocket-Version: 13" \
    http://localhost:8081/queue/listen/ws/test/1
```

#### Send message
```bash
```bash
curl -X POST --location "http://localhost:8081/queue/send/test" \
    -H "Host: localhost:8081" \
    -H "Content-Type: application/json" \
    -d "{
          \"id\": \"1\",
          \"payload\": {
            \"message\": \"hello\"
          }
        }"
```

## Author

👤 **Mikhail Panfilov**

* Github: [@Mnwa](https://github.com/Mnwa)
* LinkedIn: [@https:\/\/www.linkedin.com\/in\/mikhail-panfilov-020615133\/](https://linkedin.com/in/https:\/\/www.linkedin.com\/in\/mikhail-panfilov-020615133\/)

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!<br />Feel free to check [issues page](https://github.com/Mnwa/sonya/issues).

## Show your support

Give a ⭐️ if this project helped you!