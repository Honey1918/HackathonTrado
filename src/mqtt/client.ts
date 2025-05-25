import mqtt from "mqtt";
import { Pool } from "pg";
import { config } from "../config";
import { PG_CONFIG } from "../config/constants";

// Enhanced database pool configuration
const pool = new Pool({
  ...PG_CONFIG,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Topic cache to reduce database queries
const topicCache = new Map<string, number>();

export function createClient(): mqtt.MqttClient {
  const connectUrl = `mqtts://${config.mqtt.host}:${config.mqtt.port}`;
  
  const options: mqtt.IClientOptions = {
    clientId: `${config.mqtt.clientId}-${Date.now()}`,
    clean: true,
    connectTimeout: 10000,
    username: config.mqtt.username,
    password: config.mqtt.password,
    reconnectPeriod: 5000,
    rejectUnauthorized: true,
    keepalive: 60,
  };

  const client = mqtt.connect(connectUrl, options);

  // Configure enhanced event handlers
  client.on('connect', () => {
    console.log('✅ Connected to MQTT broker');
    
    // Subscribe to topics with QoS 1 for reliable delivery
    client.subscribe([
      'index/NIFTY',
      'index/BANKNIFTY',
      'index/FINNIFTY',
      'index/MIDCPNIFTY',
      'option/+/+/+/+' // Format: option/INDEX/STRIKE/EXPIRY/TYPE
    ], { qos: 1 }, (err) => {
      if (err) {
        console.error('❌ Subscription error:', err);
      } else {
        console.log('🔔 Subscribed to market data topics');
      }
    });
  });

  // Message processing with rate limiting
  let lastProcessed = Date.now();
  client.on('message', async (topic, message) => {
    try {
      // Rate limiting (max 100 messages/sec)
      const now = Date.now();
      if (now - lastProcessed < 10) {
        return;
      }
      lastProcessed = now;

      const data = JSON.parse(message.toString());
      const ltp = parseFloat(data.ltp);
      
      if (isNaN(ltp)) {
        console.warn(`⚠️ Invalid LTP in message on ${topic}`);
        return;
      }

      const topicId = await getOrCreateTopic(topic);
      const timestamp = new Date(data.timestamp || new Date());
      
      await storeMarketData(topicId, ltp, timestamp);
      
      // Debug logging (1% of messages)
      if (Math.random() < 0.01) {
        console.log(`📊 ${timestamp.toISOString()} | ${topic} @ ${ltp}`);
      }
    } catch (error) {
      console.error(`❌ Failed to process ${topic}:`, error);
    }
  });

  // Enhanced error handling
  client.on('error', (err) => {
    console.error('‼️ MQTT connection error:', err);
  });

  client.on('reconnect', () => {
    console.log('↩️ Attempting MQTT reconnection...');
  });

  client.on('close', () => {
    console.log('🔴 MQTT connection closed');
    // Implement reconnection logic if needed
  });

  return client;
}

// Optimized database operations
async function storeMarketData(topicId: number, ltp: number, timestamp: Date) {
  const client = await pool.connect();
  try {
    await client.query(
      `INSERT INTO ltp_data (topic_id, ltp, received_at)
       VALUES ($1, $2, $3)
       ON CONFLICT DO NOTHING`,
      [topicId, ltp, timestamp]
    );
  } finally {
    client.release();
  }
}

async function getOrCreateTopic(topicName: string): Promise<number> {
  // Check cache first
  if (topicCache.has(topicName)) {
    return topicCache.get(topicName)!;
  }

  const client = await pool.connect();
  try {
    // Try to get existing topic
    const res = await client.query(
      `SELECT topic_id FROM topics WHERE topic_name = $1 LIMIT 1`,
      [topicName]
    );
    
    if (res.rows.length > 0) {
      topicCache.set(topicName, res.rows[0].topic_id);
      return res.rows[0].topic_id;
    }
    
    // Parse and create new topic
    const parts = topicName.split('/');
    const isOption = parts[0] === 'option';
    
    const insertRes = await client.query(
      `INSERT INTO topics (topic_name, index_name, type, strike, expiry)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING topic_id`,
      [
        topicName,
        parts[1], // index name
        isOption ? parts[4] : null, // CE/PE
        isOption ? parseFloat(parts[2]) : null, // strike
        isOption ? parts[3] : null // expiry (format: DD-MM-YYYY)
      ]
    );
    
    const newTopicId = insertRes.rows[0].topic_id;
    topicCache.set(topicName, newTopicId);
    return newTopicId;
  } finally {
    client.release();
  }
}

// Graceful shutdown handler
process.on('SIGINT', async () => {
  console.log('🛑 Shutting down gracefully...');
  await pool.end();
  process.exit(0);
});