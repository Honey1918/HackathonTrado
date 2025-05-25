import mqtt from "mqtt";
import * as marketdata from "../proto/market_data_pb";
import * as subscriptionManager from "./subscriptionManager";
import * as db from "../db";
import * as utils from "../utils";
import { config } from "../config";
import { isFirstIndexMessage } from "./subscriptionManager";
import { getAtmStrike } from "../utils";

// Store LTP values for indices
const indexLtpMap = new Map<string, number>();
const atmStrikeMap = new Map<string, number>();

export function processMessage(
  topic: string,
  message: Buffer,
  client: mqtt.MqttClient
) {
  console.log(`>> Received message on topic: ${topic}`);

  try {
    let decoded: any = null;
    let ltpValues: number[] = [];

    // Unified message decoding logic
    const decodeAttempts = [
      () => marketdata.marketdata.MarketData.decode(new Uint8Array(message)),
      () => marketdata.marketdata.MarketDataBatch.decode(new Uint8Array(message)),
      () => JSON.parse(message.toString())
    ];

    for (const attempt of decodeAttempts) {
      try {
        decoded = attempt();
        if ('data' in decoded) { // Batch processing
          ltpValues = decoded.data.map((d: any) => d.ltp).filter(Number.isFinite);
          console.log(`>> Decoded batch with ${ltpValues.length} valid LTPs`);
        } else if ('ltp' in decoded) { // Single message
          if (Number.isFinite(decoded.ltp)) ltpValues.push(decoded.ltp);
        }
        break;
      } catch {}
    }

    if (ltpValues.length === 0) {
      console.error('No valid LTP values found in message');
      return;
    }

    // Process each LTP value
    ltpValues.forEach(ltp => {
      console.log(`>> Decoded LTP: ${ltp} from topic: ${topic}`);

      const isOptionTopic = topic.includes("|");
      const [_, indexName] = topic.split('/');

      if (!isOptionTopic && indexName) {
        // Index processing
        handleIndexMessage(topic, indexName, ltp, client);
      } else {
        // Option processing
        db.saveToDatabase(topic, ltp);
      }
    });
  } catch (error) {
    console.error('Error processing message:', error);
  }
}

function handleIndexMessage(
  topic: string,
  indexName: string,
  ltp: number,
  client: mqtt.MqttClient
) {
  indexLtpMap.set(indexName, ltp);
  
  if (isFirstIndexMessage.get(indexName)) {
    console.log(`>> First message for ${indexName}, subscribing to options`);
    isFirstIndexMessage.set(indexName, false);
    
    const atmStrike = getAtmStrike(indexName, ltp);
    atmStrikeMap.set(indexName, atmStrike);
    
    subscriptionManager.subscribeToAtmOptions(client, indexName, atmStrike);
  }

  db.saveToDatabase(topic, ltp, indexName, 'index');
}