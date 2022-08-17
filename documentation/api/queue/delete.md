# Delete queue

Delete all events from queue by id on every shard of SonyaWQ.

**URL** : `/queue/delete/{queue_name}/{key}`

**Method** : `POST`

**Headers**
```text
Authorization: Bearer {service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
POST http://localhost:8081/queue/delete/test/123
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
curl -X POST --location "http://localhost:8081/queue/delete/test/123" \
    -H "Host: localhost:8081"
```

**Java Script**
```js
fetch("http://localhost:8081/queue/delete/test/123", {
  method: "POST"
})
```
