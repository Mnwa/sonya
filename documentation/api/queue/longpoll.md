# Subscribe to queue with id

Return first message with specific id received from queue

**URL** : `/queue/listen/longpoll/{queue_name}/{key}`

**Method** : `GET`

**Headers**
```text
Content-Type: application/json
Authorization: Bearer {jwt_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
GET http://localhost:8081/queue/listen/longpoll/test/1
Host: localhost:8081
Content-Type: application/json
```

If successful, will respond with:

```json
{
  "id": "1",
  "sequence": 1631471292599930200,
  "payload": {
    "message": "hello"
  }
}
```

**Code examples**

**CURL**
```bash
curl -X GET --location "http://localhost:8081/queue/listen/longpoll/test/1" \
    -H "Host: localhost:8081"
```

**Java Script**
```js
fetch('http://localhost:8081/queue/listen/longpoll/test/1');
```

# Subscribe to all queue updates

Return first message received from queue

**URL** : `/queue/listen/longpoll/{queue_name}`

**Method** : `GET`

**Headers**
```text
Content-Type: application/json
Authorization: Bearer {service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
GET http://localhost:8081/queue/listen/longpoll/test
Host: localhost:8081
Content-Type: application/json
```

If successful, will respond with:

```json
{
  "id": "1",
  "sequence": 1631471292599930200,
  "payload": {
    "message": "hello"
  }
}
```

**Code examples**

**CURL**
```bash
curl -X GET --location "http://localhost:8081/queue/listen/longpoll/test" \
    -H "Host: localhost:8081"
```

**Java Script**
```js
fetch('http://localhost:8081/queue/listen/longpoll/test');
```

## Notes
* This method will subscribe to all queue updates on every shard. That's maybe a little slow.