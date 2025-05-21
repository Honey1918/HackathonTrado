import * as dotenv from "dotenv";
dotenv.config();

import { config } from "./config";
import * as db  from "./db";
import * as mqttClient from "./mqtt/client";
import * as messageProcessor from "./mqtt/messageProcessor";
import * as subscriptionManager from "./mqtt/subscriptionManager";

async function start() {
  try {
    console.log("Initializing application...");
    
    // Initialize the database
    const pool = db.createPool();
    db.initialize(pool);

    // Initialize subscription manager
    subscriptionManager.initializeFirstMessageTracking();

    // Create MQTT client
    const client = mqttClient.createClient();

    // MQTT event handlers
    client.on("connect", () => {
      console.log("Connected to MQTT broker");
      subscriptionManager.subscribeToAllIndices(client);
    });

    client.on("message", (topic: string, message: Buffer) => {
      messageProcessor.processMessage(topic, message, client);
    });

    // Error handling
    client.on("error", (err: Error) => {
      console.error("MQTT connection error:", err);
    });

    // Graceful shutdown
    process.on("SIGINT", async () => {
      console.log("Shutting down gracefully...");
      await db.cleanupDatabase();
      client.end();
      process.exit();
    });

    console.log("Application started successfully");
  } catch (error) {
    console.error("Application startup error:", error);
    process.exit(1);
  }
}

start();