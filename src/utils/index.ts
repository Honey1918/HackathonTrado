// Function to get the strike difference for an index
export function getStrikeDiff(indexName: string): number {
  switch (indexName.toLowerCase()) {
    case "nifty":
      return 50;
    case "banknifty":
      return 100;
    case "finnifty":
      return 50;
    case "midcpnifty":
      return 25;
    case "bankex":
      return 100;
    case "sensex":
      return 100;
    default:
      return 100; // Default
  }
}

// Calculate ATM strike based on index LTP
export function getAtmStrike(indexName: string, ltp: number): number {
  const strikeDiff = getStrikeDiff(indexName);
  return Math.round(ltp / strikeDiff) * strikeDiff;
}
export function roundToNearestStrike(strike: number, step: number): number {
  return Math.round(strike / step) * step;
}


// Format option topic for subscription
export function getOptionTopic(index: string, expiry: string, strike: number, type: "ce" | "pe"): string {
  return `index/${index}/${expiry}/${strike}/${type}`;
}

