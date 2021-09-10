# Secure mode

SonyaWQ provides two token types:
* `Service Token`. This token setup once in the service configuration. It's make possible to modify queues, sent messages and generate `JWT` tokens.
* `JWT` token. This token maybe generated with `Service Token`.  It's make possible to subscribe to queue with key.

[Read more about generating JWT.](./api/queue/jwt.md)

> If you configure secure mode only on `proxy`, all unauthorized requests will not be passed to queue shards.
> This could help you optimize load.

## Configure

You can provide `Service Token` on instance startup with `yaml`, `json` or `environment`.

### Yaml
```yaml
secure: 
  service_token: service_token_test 
  jwt_token_expiration: 60 
```

### JSON
```json
{
  "secure": {
    "service_token": "service_token_test",
    "jwt_token_expiration": 60
  }
}
```

### ENV
```shell
SECURE_SERVICE_TOKEN=service_token_test
SECURE_JWT_EXPIRATION_TIME=60
```

[Read more about tokens configure.](./configure.md)

## Making requests

SonyaWQ provides two ways of making authorization requests:
* [Query parameter](https://self-issued.info/docs/draft-ietf-oauth-v2-bearer.html#query-param) `?access_token={token}`
* [Header value](https://self-issued.info/docs/draft-ietf-oauth-v2-bearer.html#authz-header) `Authorization: Bearer {token}`

### Header value

```http request
POST http://localhost:8081/queue/create/test
Host: localhost:8081
Authorization: Bearer {token}
```

### Query parameter
```http request
POST http://localhost:8081/queue/create/test?access_token={token}
Host: localhost:8081
```