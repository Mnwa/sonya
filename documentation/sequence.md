# Sequence

It's the mechanism for storing `queue-id` updates history.
When you send any message like this to queue:
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

All messages getting `sequence_id`.
`sequence_id` - it is a local id that starts with `1` and atomically increments on every sent message with the same `queue` and `id`.

`sequence_id` will return on subscribe responses in message body.

With the `sequence_id` you can receive missed updates of your key.

`GET http://localhost:8081/queue/listen/longpoll/test?sequence={sequence_id}` will return the message with set `sequence_id`.
If a message with set `sequence_id` does not exist, the method will wait for the next sent message.

Also, `sequence_id` may accept `first` and `last` value. It's return to you first and last key versions from api.

Long poll without data lost example:

**Java Script**
```js
let sequence_id = 1;
while (true) {
    const response = await fetch('http://localhost:8081/queue/listen/longpoll/test?sequence=' + sequence_id)
    const data = await response.json()
    sequence_id = data.sequence + 1
}
```

### Custom sequence_id
You can replace auto-generated `sequence_id` to own realization.
Just set your `sequence` to message body on sending.
```http request
POST http://localhost:8081/queue/send/test
Host: localhost:8081
Content-Type: application/json

{
  "id": "1",
  "sequence": 999,
  "payload": {
    "message": "hello"
  }
}
```

### Configure history size
You can set the maximum message versions for storing on config file or env.
```yaml
queue:
  max_key_updates: 10
```

The default value is `null`, the queue will not remove historical versions of messages.
Set `max_key_updates` to `0` and message versions will not store anymore.

When the `max_key_updates` value is more than `0`, 
every update of the message will clean old message versions
if the count of all message versions is more than `max_key_updates`.

**Example:**

If we set `max_key_updates` to `1`. 
The only previous version with the max `sequence_id` will be stored.