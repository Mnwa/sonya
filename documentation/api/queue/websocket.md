# Subscribe to queue with id

Return messages with specific id received from queue

**URL** : `/queue/listen/ws/{queue_name}/{id}`

**Method** : `GET`

**Headers**
```text
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: {websocket_token}
Sec-WebSocket-Version: 13
```

**Query parameters**
```http request
?access_token={jwt_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

If successful, will respond with websocket byte messages:

```json
{
  "id": "1",
  "timestamp": 1631282055,
  "payload": {
    "message": "hello"
  }
}
```

**Code examples**

**CURL**
```bash
  curl --include \
    --no-buffer \
    --header "Connection: Upgrade" \
    --header "Upgrade: websocket" \
    --header "Host: localhost:8081" \
    --header "Origin: http://localhost:8081" \
    --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
    --header "Sec-WebSocket-Version: 13" \
    "http://localhost:8081/queue/listen/ws/test/1"
```

**Java Script**
```js
const socket = new WebSocket("ws://localhost:8081/queue/listen/ws/test/1");

socket.onmessage = function(event) {
  console.log("received", JSON.parse(event.data));
};
```

# Subscribe to all queue messages

Return all messages received from queue

**URL** : `/queue/listen/ws/{queue_name}`

**Method** : `GET`

**Headers**
```text
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: {websocket_token}
Sec-WebSocket-Version: 13
```

**Query parameters**
```http request
?access_token={service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

If successful, will respond with websocket byte messages:

```json
{
  "id": "1",
  "timestamp": 1631282055,
  "payload": {
    "message": "hello"
  }
}
```

**Code examples**

**CURL**
```bash
  curl --include \
    --no-buffer \
    --header "Connection: Upgrade" \
    --header "Upgrade: websocket" \
    --header "Host: localhost:8081" \
    --header "Origin: http://localhost:8081" \
    --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
    --header "Sec-WebSocket-Version: 13" \
    "http://localhost:8081/queue/listen/ws/test"
```

**Java Script**
```js
const socket = new WebSocket("ws://localhost:8081/queue/listen/ws/test");

socket.onmessage = function(event) {
    console.log("received", JSON.parse(event.data));
};
```

## Notes
* This method will subscribe to all queue updates on every shard. That's maybe a little slow.