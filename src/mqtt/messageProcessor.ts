import mqtt from "mqtt";
import * as marketdata from "../proto/market_data_pb";
import * as subscriptionManager from "./subscriptionManager";
import * as db from "../db";
import * as utils from "../utils";
import { config } from "../config"; // Add this import
import { isFirstIndexMessage } from "./subscriptionManager"; // Add this import
import { getAtmStrike } from "../utils"; // Add this import

// Store LTP values for indices
const indexLtpMap = new Map<string, number>();
const atmStrikeMap = new Map<string, number>();

export function processMessage(
  topic: string,
  message: Buffer,
  client: mqtt.MqttClient
) {
  console.log(`>> Received message on topic: ${topic}`); // ✅ Always log

  try {
    let decoded: any = null;
    let ltpValues: number[] = [];

    try {
      decoded = marketdata.marketdata.MarketData.decode(new Uint8Array(message));
      console.log(">> Successfully decoded MarketData:", decoded); // ✅
      if (typeof decoded.ltp === 'number') ltpValues.push(decoded.ltp);
    } catch {
      try {
        decoded = marketdata.marketdata.MarketDataBatch.decode(new Uint8Array(message));
        console.log(">> Successfully decoded MarketDataBatch:", decoded.data.length); // ✅
        ltpValues = decoded.data.map((d: any) => d.ltp).filter((v: any) => typeof v === 'number');
      } catch {
        try {
          decoded = JSON.parse(message.toString());
          // console.log(">> Successfully parsed JSON:", decoded); // ✅
          if (typeof decoded.ltp === 'number') ltpValues.push(decoded.ltp);
        } catch {
          console.error('Failed to decode message for topic:', topic);
          console.log(">> Raw message:", message.toString()); // ✅
          return;
        }
      }
    }

    for (const ltp of ltpValues) {
      console.log(`>> Decoded LTP: ${ltp} from topic: ${topic}`); // ✅

      const isIndexTopic = topic.startsWith(`${config.app.indexPrefix}/`);
      if (isIndexTopic) {
        const indexName = topic.split('/')[1];
        console.log(`>> First message check for ${indexName}:`, isFirstIndexMessage.get(indexName));
        indexLtpMap.set(indexName, ltp);

        if (isFirstIndexMessage.get(indexName)) {
          isFirstIndexMessage.set(indexName, false);
          const atmStrike = getAtmStrike(indexName, ltp);
          atmStrikeMap.set(indexName, atmStrike);
          subscriptionManager.subscribeToAtmOptions(client, indexName, atmStrike);
        }

        db.saveToDatabase(topic, ltp, indexName, 'index');
      } else {
        db.saveToDatabase(topic, ltp);
      }
    }
  } catch (error) {
    console.error('Error processing message:', error);
  }
}
