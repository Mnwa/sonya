# API

## Methods

SonyaWQ methods description.

### Manage queue

Endpoints for queues management.

#### List

* [Create queue:](./api/queue/create.md) `POST /queue/create/{queue_name}`
* [Close queue:](./api/queue/close.md) `POST /queue/close/{queue_name}`
* [Send message to queue:](./api/queue/send.md) `POST /queue/send/{queue_name}`

#### Security

When service tokens are provided, methods from these sections are available 
with the `Authorization` header or `access_token` query param and `service token`.
Example:
```http request
POST http://{host}:{port}/queue/create/{queue_name}
Authorization: Bearer {service_token}
```

### Listen queue

Endpoints for listening queues.

#### List

* [Long poll subscription:](./api/queue/longpoll.md) `POST /queue/listen/longpoll/{queue_name}/{id?}`
* [WebSocket subscription:](./api/queue/websocket.md) `POST /queue/listen/ws/{queue_name}/{id?}`

#### Security

When service tokens are provided, methods from these sections are available
with the `Authorization` header or `access_token` query param and `jwt token`.
Example:
```http request
POST http://{host}:{port}/queue/longpoll/{queue_name}
Authorization: Bearer {jwt_token}
```

### Secure

Endpoints for JWT tokens management.

#### List
* [Generate JWT tokens:](./api/queue/jwt.md) `POST /generate_jwt/{queue}/{uniq_id}`

#### Security

When service tokens are provided, methods from these sections are available
with the `Authorization` header or `access_token` query param and `service token`.
Example:
```http request
POST http://{host}:{port}/queue/generate_jwt/{queue_name}/{key}
Authorization: Bearer {service_token}
```