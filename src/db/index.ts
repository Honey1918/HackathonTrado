import { Pool } from "pg";
import { config } from "../config";

// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

let isShuttingDown = false;

export async function cleanupDatabase() {
  isShuttingDown = true;

  if (dataBatch.length > 0) {
    await flushBatch();
  }

  topicCache.clear();

  if (pool) {
    await pool.end();
    pool = null as unknown as Pool;
  }

  console.log("Database cleanup completed");
}



// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;

// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

export function createPool(): Pool {
  return new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
  });
}

export function initialize(dbPool: Pool) {
  pool = dbPool;
  console.log("Database initialized");

  // TODO: Preload topic cache from database
}

export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  if (!pool) {
    throw new Error("getTopicId() called after pool was closed");
  }

  if (topicCache.has(topicName)) {
    return topicCache.get(topicName)!;
  }

  const client = await pool.connect();
  try {
    const res = await client.query(
      'SELECT topic_id FROM topics WHERE topic_name = $1',
      [topicName]
    );

    if (res.rows.length > 0) {
      const topicId = res.rows[0].topic_id;
      topicCache.set(topicName, topicId);
      return topicId;
    }

    const insertRes = await client.query(
      `INSERT INTO topics (topic_name, index_name, type, strike)
       VALUES ($1, $2, $3, $4)
       RETURNING topic_id`,
      [topicName, indexName, type, strike]
    );

    const topicId = insertRes.rows[0].topic_id;
    topicCache.set(topicName, topicId);
    return topicId;
  } finally {
    client.release();
  }
}


export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  if (isShuttingDown || !pool) {
    console.warn("saveToDatabase called during shutdown or after pool was closed");
    return;
  }

  dataBatch.push({ topic, ltp, indexName, type, strike });

  if (!batchTimer) {
    batchTimer = setTimeout(() => {
      flushBatch().catch(console.error);
    }, config.app.batchInterval);
  }

  if (dataBatch.length >= config.app.batchSize) {
    if (batchTimer) {
      clearTimeout(batchTimer);
      batchTimer = null;
    }
    flushBatch().catch(console.error);
  }
}


export async function flushBatch() {
  if (isShuttingDown || !pool) {
    console.warn("flushBatch() called during shutdown or after pool was closed");
    return;
  }

  if (dataBatch.length === 0) return;

  const batchToProcess = [...dataBatch];
  dataBatch = [];
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const item of batchToProcess) {
      const topicId = await getTopicId(item.topic, item.indexName, item.type, item.strike);
      await client.query(
        `INSERT INTO ltp_data (topic_id, ltp)
         VALUES ($1, $2)`,
        [topicId, item.ltp]
      );
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error flushing batch:', error);
  } finally {
    client.release();
  }
}

