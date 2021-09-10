# Generate jwt token

Will return jwt token, which will make it possible to subscribe to the queue.

**URL** : `/queue/generate_jwt/{queue_name}/{key}`

**Method** : `POST`

**Headers**
```text
Authorization: Bearer {service_token}
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
POST http://localhost:8081/queue/generate_jwt/test/1
Host: localhost:8081
Authorization: Bearer {service_token}
```

If successful, will respond with:

```json
{
  "token": "token",
  "expiration": 1946850792
}
```

**Code examples**

**CURL**
```bash
curl -X POST --location "http://localhost:8081/queue/generate_jwt/test/1" \
    -H "Host: localhost:8081" \
    -H "Authorization: Bearer {service_token}"
```

**Java Script**
```js
fetch('http://localhost:8081/queue/generate_jwt/test/1', {
  method: 'POST',
  headers: {
    'Authorization': 'Bearer {service_token}'
  }
});

```

## Notes

* After the expiration date, jwt token will be invalid, but alive subscribe will not be closed.