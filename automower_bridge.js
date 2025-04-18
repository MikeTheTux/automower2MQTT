//const normalizePort = require('normalize-port');
const mqtt = require('mqtt');
const WebSocket = require('ws');


// MQTT settings
const MQTT_BROKER_URL = 'mqtt://localhost:1883';
const MQTT_TOPIC = 'automower';
const MQTT_PASSWORD = 'yyy';

// Husqvarna Automower Connect WebSocket endpoint
const WEBSOCKET_URL = 'wss://ws.openapi.husqvarna.dev/v1';
// OAuth2 access token (get this via REST API)
const ACCESS_TOKEN = 'xxx';

// Connect to MQTT broker
const mqttClient  = mqtt.connect(MQTT_BROKER_URL, {
   username: 'openhabian',
   password: MQTT_PASSWORD,
   clientId: 'mqttjs_' + Math.random().toString(16).slice(2, 10),
   will: {
      topic: `${MQTT_TOPIC}/bridge/availability`,
      payload: 'offline',
      qos: 1,
      retain: true
   }
});

mqttClient.on('connect', function () {
    console.log('Connected to MQTT broker');
});

// Connect to Husqvarna WebSocket API
let ws = new WebSocket(WEBSOCKET_URL, {
    headers: {
        'Authorization': `Bearer ${ACCESS_TOKEN}`
    }
});

// Ping server - to keep alive
setInterval(function(){ ws.send('ping'); }, 60000);

ws.on('open', () => {
    console.log('Connected to Husqvarna WebSocket');
    mqttClient.publish(`${MQTT_TOPIC}/bridge/availability`, 'online', {retain: true});
    // Subscribe to all mower events
    ws.send(JSON.stringify({
        "type": "subscribe",
        "attributes": {
            "categories": ["*"]
        }
    }));
});

ws.on('message', (data) => {
    try {
        const event = data.toString().trim();
        if (event !== "") {
            console.log('Received from WebSocket:', event);
            const msg = JSON.parse(event);
            const mowerId = msg.id;
            const eventType = msg.type;
            if ( mowerId && eventType) {
                const topic = `${MQTT_TOPIC}/${mowerId}/${eventType}`;
                const attributes = msg.attributes ? JSON.stringify(msg.attributes) : '{}';
                mqttClient.publish(topic, attributes);
            }
        }
    } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
    }
});

ws.on('close', () => {
    console.log('WebSocket connection closed');
    mqttClient.publish(`${MQTT_TOPIC}/bridge/availability`, 'offline', {retain: true});

    ws = new WebSocket(WEBSOCKET_URL, {
        headers: {
            'Authorization': `Bearer ${ACCESS_TOKEN}`
        }
    });
});

ws.on('error', (err) => {
    console.error('WebSocket error:', err);
});
