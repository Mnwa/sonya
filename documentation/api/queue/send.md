# Send to queue

Send a message to one of the shards of SonyaWQ.

**URL** : `/queue/send/{queue_name}`

**Method** : `POST`

**Body** :

```json
{
  "id": "",
  "sequence_id": 1,
  "payload": {}
}
```

* `id` is any `string`
* `payload` available type in json
* `sequence_id` is an **optional unique** integer. Not unique sequence will rewrite previous message.

[More about sequence.](../../sequence.md)

**Headers**
```text
Content-Type: application/json
Authorization: Bearer {service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
POST http://localhost:8081/queue/send/test
Host: localhost:8081
Content-Type: application/json

{
  "id": "1",
  "payload": {
    "message": "hello"
  }
}
```

If successful, will respond with:

```json
{
  "success": true,
  "sequence_id": 1
}
```

**Code examples**

**CURL**
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

**Java Script**
```js
fetch('http://localhost:8081/queue/send/test', {
  method: 'POST',
  headers: {
    'Host': 'localhost:8081',
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ "id": "1", "payload": { "message": "hello" } })
});
```
