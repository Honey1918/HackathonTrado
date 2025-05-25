import * as dotenv from "dotenv";

dotenv.config();

// Index configuration
export const INDEX_NAME = process.env.INDEX_NAME || "BANKNIFTY";

// Strategy parameters
export const SL_PERCENT = Number(process.env.SL_PERCENT || 25);
export const PNL_TARGET_PERCENT = Number(process.env.PNL_TARGET_PERCENT || 25);
export const PNL_SL_PERCENT = Number(process.env.PNL_SL_PERCENT || 10);

// Market timing
export const ENTRY_TIME = process.env.ENTRY_TIME || "09:25";
export const ADJUSTMENT_TIME = process.env.ADJUSTMENT_TIME || "14:00";
export const EXIT_TIME = process.env.EXIT_TIME || "15:15";

// Database configuration
export const PG_CONFIG = {
  host: process.env.PG_HOST || "localhost",
  port: Number(process.env.PG_PORT) || 5432,
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "postgres",
  database: process.env.PG_DATABASE || "ltpdb",
};

// Other constants (add more as needed)
export const TIME_WINDOW_MINUTES = 2; // Window to look for price data
export const DAYS_TO_BACKTEST = 3; // Number of days to backtest
