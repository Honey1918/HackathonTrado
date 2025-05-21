import mqtt from "mqtt";
import { config } from "../config";

export function createClient(): mqtt.MqttClient {
  const connectUrl = `mqtts://${config.mqtt.host}:${config.mqtt.port}`;
  console.log(connectUrl);
  const options: mqtt.IClientOptions = {
    clientId: config.mqtt.clientId,
    clean: true,
    connectTimeout: 4000,
    username: config.mqtt.username,
    password: config.mqtt.password,
    reconnectPeriod: 1000,
    rejectUnauthorized: true // For SSL
  };

  const client = mqtt.connect(connectUrl, options);

  // Configure event handlers
  client.on('connect', () => {
    console.log('Connected to MQTT broker');
  });

  client.on('error', (err) => {
    console.error('MQTT connection error:', err);
  });

  client.on('reconnect', () => {
    console.log('Attempting to reconnect to MQTT broker');
  });

  client.on('close', () => {
    console.log('MQTT connection closed');
  });

  return client;
}