# Create queue

Create a queue on every shard of SonyaWQ.

**URL** : `/queue/create/{queue_name}`

**Method** : `POST`

**Headers**
```text
Authorization: Bearer {service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
POST http://localhost:8081/queue/create/test
Host: localhost:8081
```

If successful, will respond with:

```json
{
  "success": true
}
```

**Code examples**

**CURL**
```bash
curl -X POST --location "http://localhost:8081/queue/create/test" \
    -H "Host: localhost:8081"
```

**Java Script**
```js
fetch("http://localhost:8081/queue/create/test", {
  method: "POST"
})
```

## Notes

* Method will not recreate the existing queue.