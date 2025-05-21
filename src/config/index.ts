// Configuration for the application
export const config = {
  mqtt: {
    host: process.env.MQTT_HOST || "emqx.trado.trade",
    port: parseInt(process.env.MQTT_PORT || "8883"),
    username: process.env.MQTT_USERNAME || "hack_iitrpr",
    password: process.env.MQTT_PASSWORD || "hack_iitrpr",
    clientId: `hackathon-client-${Math.random().toString(16).substring(2, 8)}`,
  },
  
  db: {
    host: process.env.PG_HOST || "localhost",
    port: parseInt(process.env.PG_PORT || "5432"),
    user: process.env.PG_USER || "postgres",
    password: process.env.PG_PASSWORD || "postgres",
    database: process.env.PG_DATABASE || "market_data",
  },
  app: {
    indexPrefix: process.env.INDEX_PREFIX || "index",
    batchSize: parseInt(process.env.BATCH_SIZE || "100"),
    batchInterval: parseInt(process.env.BATCH_INTERVAL || "5000"), // 5 seconds
  },
};

// List of indices to track
export const INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"];

// Expiry dates for each index
export const EXPIRY_DATES = {
  NIFTY: "22-05-2025",
  BANKNIFTY: "29-05-2025",
  FINNIFTY: "29-05-2025",
  MIDCPNIFTY: "29-05-2025",
};

// Number of strikes above and below ATM to subscribe to
export const STRIKE_RANGE = 5;
