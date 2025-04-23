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
let logConfig = {
    logTo: 'console',
    logFilePath: path.join(__dirname, 'automower_bridge.log')
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
    if (config.log) {
        logConfig = {
            ...logConfig,
            ...config.log
        };
    }
} catch (e) {
    console.warn('Could not load automower config file, using defaults:', e.message);
}

function log(...args) {
    const message = args.map(arg => (typeof arg === 'object' ? JSON.stringify(arg) : String(arg))).join(' ');
    const line = formatDateTime(new Date()) + ' ' + message + '\n';
    if (logConfig.logTo === 'logfile') {
        try {
            fs.appendFileSync(logConfig.logFilePath, line);
        } catch (err) {
            console.error('Failed to write to log file:', err);
            console.log(message);
        }
    } else {
        console.log(message);
    }
}

function formatDateTime(date) {
    return date.getFullYear() + '-' +
        String(date.getMonth() + 1).padStart(2, '0') + '-' +
        String(date.getDate()).padStart(2, '0') + 'T' +
        String(date.getHours()).padStart(2, '0') + ':' +
        String(date.getMinutes()).padStart(2, '0') + ':' +
        String(date.getSeconds()).padStart(2, '0');
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
let wsReconnectTimer = null;
let pingTimer = null;
let ws = null;

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
        accessTokenExpiresAt = Date.now() + (response.data.expires_in * 1000) - (150 * 60 * 1000); // renew 150 min before expiry (token valid for entire 2 hours session)
        log('Fetched new Husqvarna access token');
        return accessToken;
    } catch (err) {
        log('Failed to fetch Husqvarna access token:', err.response ? err.response.data : err);
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
        log('Connected to Husqvarna WebSocket');
        mqttClient.publish(`${MQTT_TOPIC}/bridge/availability`, 'online', {retain: true});
        // Subscribe to all mower events
        ws.send(JSON.stringify({
            "type": "subscribe",
            "attributes": {
                "categories": ["*"]
            }
        }));

        // Ping server - to keep alive
        if ( !pingTimer ) {
            pingTimer = setInterval(function(){ ws.send('ping'); }, 60000);
        }
    });

    ws.on('message', (data) => {
        try {
            const event = data.toString().trim();
            const heartbeat = formatDateTime(new Date());
            if (event !== "") {     // Ignore empty messages (e.g. ping)
                log('Received from WebSocket:', event);
                console.log('Received from WebSocket:', event);
                const msg = JSON.parse(event);
                const mowerId = msg.id;
                const eventType = msg.type;
                if ( mowerId && eventType) {
                    const topic = `${MQTT_TOPIC}/${mowerId}/${eventType}`;
                    const heartbeatTopic = `${MQTT_TOPIC}/${mowerId}/heartbeat`;
                    const attributes = msg.attributes ? JSON.stringify(msg.attributes) : '{}';
                    mqttClient.publish(topic, attributes);
                    mqttClient.publish(heartbeatTopic, heartbeat);
                }
            }
            const heartbeatTopic = `${MQTT_TOPIC}/bridge/heartbeat`;
            mqttClient.publish(heartbeatTopic, heartbeat);
            //log('Heartbeat sent to MQTT:', heartbeat);
        } catch (e) {
            log('Failed to parse WebSocket message:', e);
        }
    });

    ws.on('close', async (code, reason) => {
		// https://docs.w3cub.com/dom/websocket/close
		// https://docs.w3cub.com/dom/closeevent/code
		// ws.terminate():					    ws.readyState: 3; data: 1006 (Abnormal Closure)
		// ws.close():						    ws.readyState: 3; data: 1005 (No Status Received)
		// ws.close(1000, "Work complete"): 	ws.readyState: 3; data: 1000, reason: Work complete

		// every 2 hour:			            ws.readyState: 3; data: 1001; reason: Going away -> connectWebSocketWithToken()
		// every 1 day:				            ws.readyState: 3; data: 1006 (Abnormal Closure)  -> fetchAccessToken() and connectWebSocketWithToken()

        log('WebSocket connection closed: code=' + code + ', reason=' + reason);
        mqttClient.publish(`${MQTT_TOPIC}/bridge/availability`, 'offline', {retain: true});
        if (pingTimer) {
            clearInterval(pingTimer);
            pingTimer = null;
        }

		if (code === 1000) {
			// do not restart because of shut down of connection from the adapter
		} else if (code === 1001) {
			// every 2 hours
            log('Reconnecting WebSocket...');
			connectWebSocketWithToken();
		} else if (code === 1006) {
			// every 1 day
			log('Renewing Husqvarna access token and reconnecting WebSocket...');
            await fetchAccessToken();
            if (ws) ws.terminate();
            connectWebSocketWithToken();
		} else if (code === 1012) {
			// 1012 = Service Restart (The server is terminating the connection because it is restarting)
			log('Renewing Husqvarna access token and reconnecting WebSocket...');
            await fetchAccessToken();
            if (ws) ws.terminate();
            connectWebSocketWithToken();
		} else {
            log('Unexpected code: Reconnecting WebSocket...');
			connectWebSocketWithToken();
		}
    });

    ws.on('error', (err) => {
        log('WebSocket error:', err);
    });
/*
    // WebSocket has a max time limit of 2 hours.
    // To keep the connection a live you have to reconnect before 2 hours have passed.
    // Set a timer to reconnect before 2 hours (e.g., after 1 hour 55 minutes)
    if (wsReconnectTimer) clearTimeout(wsReconnectTimer);
    wsReconnectTimer = setTimeout(() => {
        log('WebSocket reconnect timer triggered (max 2h limit), reconnecting...');
        if (ws) ws.terminate();
        connectWebSocketWithToken();
    }, 115 * 60 * 1000); // 1 hour 55 minutes
*/
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
    log('Connected to MQTT broker');
});

mqttClient.on('close', function () {
    log('MQTT connection closed, attempting to reconnect...');
    if (!mqttClient.reconnecting) {
        mqttClient.reconnect();
    }
});

mqttClient.on('offline', function () {
    log('MQTT client is offline, attempting to reconnect...');
    if (!mqttClient.reconnecting) {
        mqttClient.reconnect();
    }
});

mqttClient.on('error', function (err) {
    log('MQTT error:', err);
    if (!mqttClient.reconnecting) {
        mqttClient.reconnect();
    }
});

// On startup, fetch token and connect
connectWebSocketWithToken();

// Optionally, set up a timer to renew the token and reconnect WebSocket before expiry
/*
setInterval(async () => {
    if (Date.now() > accessTokenExpiresAt - 60000) { // 1 min before expiry
        log('Renewing Husqvarna access token and reconnecting WebSocket...');
        try {
            await fetchAccessToken();
            if (ws) ws.terminate();
            connectWebSocketWithToken();
        } catch (e) {
            log('Error renewing token or reconnecting WebSocket:', e);
        }
    }
}, 60000);
*/
