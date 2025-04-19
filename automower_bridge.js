//const normalizePort = require('normalize-port');
const mqtt = require('mqtt');
const WebSocket = require('ws');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Load config
const configPath = path.join(__dirname, 'automower_config.json');
let mqttConfig = {
    broker_url: 'mqtt://localhost:1883',
    topic: 'automower',
    client_id: 'mqttjs_' + Math.random().toString(16).slice(2, 10),
    username: 'YOUR_USERNAME',
    password: 'YOUR_PASSWORD'
};
let husqvarnaConfig = {
    client_id: 'YOUR_CLIENT_ID',
    client_secret: 'YOUR_CLIENT_SECRET'
};
try {
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    if (config.mqtt) {
        mqttConfig = {
            ...mqttConfig,
            ...config.mqtt
        };
    }
    if (config.husqvarna) {
        husqvarnaConfig = {
            ...husqvarnaConfig,
            ...config.husqvarna
        };
    }
} catch (e) {
    console.warn('Could not load automower config file, using defaults:', e.message);
}

// MQTT settings
const MQTT_BROKER_URL = mqttConfig.broker_url;
const MQTT_TOPIC = mqttConfig.topic;
const MQTT_CLIENT_ID = mqttConfig.client_id;
const MQTT_USERNAME = mqttConfig.username;
const MQTT_PASSWORD = mqttConfig.password;

// Husqvarna Automower Connect WebSocket endpoint
const WEBSOCKET_URL = 'wss://ws.openapi.husqvarna.dev/v1';
const HUSQVARNA_TOKEN_URL = 'https://api.authentication.husqvarnagroup.dev/v1/oauth2/token';
const HUSQVARNA_CLIENT_ID = husqvarnaConfig.client_id;
const HUSQVARNA_CLIENT_SECRET = husqvarnaConfig.client_secret;

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
