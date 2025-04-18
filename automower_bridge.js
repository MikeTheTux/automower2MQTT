//const normalizePort = require('normalize-port');
const mqtt = require('mqtt');
const WebSocket = require('ws');
const axios = require('axios');

// MQTT settings
const MQTT_BROKER_URL = 'mqtt://localhost:1883';
const MQTT_TOPIC = 'automower'; // Replace with your desired MQTT topic
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || 'YOUR_MQTT_PASSWORD'; // Replace with your actual MQTT password
const MQTT_USERNAME = process.env.MQTT_USERNAME || 'YOUR_MQTT_USERNAME'; // Replace with your actual MQTT username
// MQTT client ID (optional, can be generated randomly)
// Note: If you use the same client ID for multiple clients, they will disconnect each other
const MQTT_CLIENT_ID = process.env.MQTT_CLIENT_ID || 'mqttjs_' + Math.random().toString(16).slice(2, 10);

// Husqvarna Automower Connect WebSocket endpoint
const WEBSOCKET_URL = 'wss://ws.openapi.husqvarna.dev/v1';
// Husqvarna API credentials (replace with your actual credentials)
const HUSQVARNA_CLIENT_ID = process.env.HUSQVARNA_CLIENT_ID || 'YOUR_HUSQVARNA_CLIENT_ID'; // Replace with your actual Husqvarna client ID
const HUSQVARNA_CLIENT_SECRET = process.env.HUSQVARNA_CLIENT_SECRET || 'YOUR_HUSQVARNA_CLIENT_SECRET'; // Replace with your actual Husqvarna client secret
const HUSQVARNA_TOKEN_URL = 'https://api.authentication.husqvarnagroup.dev/v1/oauth2/token';

let accessToken = null;
let accessTokenExpiresAt = 0;

async function fetchAccessToken() {
    try {
        const response = await axios.post(HUSQVARNA_TOKEN_URL, new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: HUSQVARNA_CLIENT_ID,
            client_secret: HUSQVARNA_CLIENT_SECRET
        }), {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        });
        accessToken = response.data.access_token;
        // expires_in is in seconds
        accessTokenExpiresAt = Date.now() + (response.data.expires_in - 60) * 1000; // renew 1 min before expiry
        console.log('Fetched new Husqvarna access token');
        return accessToken;
    } catch (err) {
        console.error('Failed to fetch Husqvarna access token:', err.response ? err.response.data : err);
        throw err;
    }
}

async function getValidAccessToken() {
    if (!accessToken || Date.now() > accessTokenExpiresAt) {
        return await fetchAccessToken();
    }
    return accessToken;
}

async function connectWebSocketWithToken() {
    const token = await getValidAccessToken();
    ws = new WebSocket(WEBSOCKET_URL, {
        headers: {
            'Authorization': `Bearer ${token}`
        }
    });

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
            if (event !== "") {     // Ignore empty messages (e.g. ping)
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
        connectWebSocketWithToken();
    });

    ws.on('error', (err) => {
        console.error('WebSocket error:', err);
    });
}

// Connect to MQTT broker
const mqttClient  = mqtt.connect(MQTT_BROKER_URL, {
   username: MQTT_USERNAME,
   password: MQTT_PASSWORD,
   clientId: MQTT_CLIENT_ID,
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

// On startup, fetch token and connect
connectWebSocketWithToken();

// Optionally, set up a timer to renew the token and reconnect WebSocket before expiry
setInterval(async () => {
    if (Date.now() > accessTokenExpiresAt - 60000) { // 1 min before expiry
        console.log('Renewing Husqvarna access token and reconnecting WebSocket...');
        try {
            await fetchAccessToken();
            if (ws) ws.terminate();
            connectWebSocketWithToken();
        } catch (e) {
            console.error('Error renewing token or reconnecting WebSocket:', e);
        }
    }
}, 60000);
