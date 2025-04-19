# automower2MQTT
Automower WebSocket to MQTT bridge


Receive Husqvarna Automower® events via WebSocket and mirror it to MQTT.


Using WebSocket you can subscribe to events from your mower(s). Instead of polling the Automower® Connect API (REST) for changes you get notifications when the status or positions change.

## Example Config File
```yaml
{
  "mqtt": {
    "broker_url": "mqtt://localhost:1883",
    "topic": "automower",
    "client_id": "mqttjs_12345678",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD"
  },
  "husqvarna": {
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_SECRET"
  }
}
```

## References:
- https://developer.husqvarnagroup.cloud/apis/authentication-api
- https://developer.husqvarnagroup.cloud/apis/automower-connect-api
- https://developer.husqvarnagroup.cloud/
