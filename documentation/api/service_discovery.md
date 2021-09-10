# Update queues list

Send a message to one of the shards of SonyaWQ.

**URL** : `/registry`

**Method** : `POST`

**Body** : 
```json
[
  ""
]
```
Array of queue urls.

**Headers**
```text
Content-Type: application/json
Authorization: Bearer {service_token} // required if secure mode is enabled
```

## Success Response

**Code** : `200 OK`

**Request examples**

```http request
POST http://localhost:8081/registry
Host: localhost:8081
Content-Type: application/json

[
  "http://queue-1:8080"
]
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
curl -X POST --location "http://localhost:8081/registry" \
    -H "Host: localhost:8081" \
    -H "Authorization: Bearer dasdsadfaafwfwafwe" \
    -H "Content-Type: application/json" \
    -d "[
          \"http://queue-1:8080\"
        ]"
```

**Java Script**
```js
fetch('http://localhost:8081/registry', {
    method: 'POST',
    headers: {
        'Host': 'localhost:8081',
        'Authorization': 'Bearer dasdsadfaafwfwafwe',
        'Content-Type': 'application/json'
    },
    body: JSON.stringify([ "http://queue-1:8080" ])
});

```
