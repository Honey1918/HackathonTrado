import mqtt from "mqtt";
import { config, INDICES, EXPIRY_DATES, STRIKE_RANGE } from "../config";
import * as utils from "../utils";

// Set of active subscriptions to avoid duplicates
export const activeSubscriptions = new Set<string>();

// Track if we've received the first message for each index
export const isFirstIndexMessage = new Map<string, boolean>();

// Subscribe to all index topics (e.g., index/NIFTY, index/BANKNIFTY)
export function subscribeToAllIndices(client: mqtt.MqttClient) {
  INDICES.forEach((indexName) => {
    const topic = `${config.app.indexPrefix}/${indexName}`;
    console.log(`Subscribing to index: ${topic}`);
    client.subscribe(topic);
    activeSubscriptions.add(topic);
  });
}

// Initialize first-message tracking for each index
export function initializeFirstMessageTracking() {
  INDICES.forEach((indexName) => {
    isFirstIndexMessage.set(indexName, true);
  });
}

// Subscribe to CE and PE option topics around ATM
export async function subscribeToAtmOptions(
  client: mqtt.MqttClient,
  indexName: string,
  atmStrike: number
) {
  const strikeDiff = utils.getStrikeDiff(indexName);
  const roundedAtm = utils.roundToNearestStrike(atmStrike, strikeDiff);

  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    const strike = roundedAtm + i * strikeDiff;

    for (const optionType of ["ce", "pe"] as const) {
      try {
        const token = await getOptionToken(indexName, strike, optionType);
        if (token) {
          const topic = utils.getOptionTopic(indexName, token);
          if (!activeSubscriptions.has(topic)) {
            console.log(`Subscribing to option: ${topic}`);
            client.subscribe(topic);
            activeSubscriptions.add(topic);
          }
        }
      } catch (error) {
        console.error(`Error subscribing to ${indexName} ${strike} ${optionType}:`, error);
      }
    }
  }
}

// Fetch token for option contract using Trado API
export async function getOptionToken(
  indexName: string,
  strikePrice: number,
  optionType: "ce" | "pe"
): Promise<string | null> {
  try {
    const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
    if (!expiryDate) throw new Error(`No expiry date for index ${indexName}`);

    const url = new URL("https://api.trado.trade/token");
    url.searchParams.append("index", indexName);
    url.searchParams.append("expiryDate", expiryDate);
    url.searchParams.append("optionType", optionType);
    url.searchParams.append("strikePrice", strikePrice.toString());


    const response = await fetch(url.toString());

    if (response.status === 404) {
      console.warn(`Option not found: ${indexName} ${strikePrice} ${optionType}`);
      return null;
    }

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`Token API failed: ${response.status} - ${errorBody}`);
    }

    const data = await response.json();
    return data.token || null;

  } catch (error) {
    console.error(`Error fetching token for ${indexName} ${strikePrice} ${optionType}:`, error);
    return null;
  }
}
