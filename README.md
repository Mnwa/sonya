<h1 align="center">Welcome to Web Queue 👋</h1>
<p>
  <img alt="Version" src="https://img.shields.io/badge/version-0.1-blue.svg?cacheSeconds=2592000" />
  <a href="#" target="_blank">
    <img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg" />
  </a>
</p>

> It is an extremely fast distributed queue that will allow you to deliver any updates to your project using a simple api.

### 🏠 [Homepage](https://github.com/Mnwa/web-queue)

## Install

### Installing server
```sh
cargo install web-queue-server
```

### Installing proxy
```sh
cargo install web-queue-proxy
```

## Usage

### Running server
```sh
ADDR=0.0.0.0:8080 web-queue-server
```

### Running proxy
```sh
ADDR=0.0.0.0:8081 SHARDS=http://127.0.0.1:8080 web-queue-proxy
```

## Author

👤 **Mikhail Panfilov**

* Github: [@Mnwa](https://github.com/Mnwa)
* LinkedIn: [@https:\/\/www.linkedin.com\/in\/mikhail-panfilov-020615133\/](https://linkedin.com/in/https:\/\/www.linkedin.com\/in\/mikhail-panfilov-020615133\/)

## 🤝 Contributing

Contributions, issues and feature requests are welcome!<br />Feel free to check [issues page](https://github.com/Mnwa/web-queue/issues). 

## Show your support

Give a ⭐️ if this project helped you!