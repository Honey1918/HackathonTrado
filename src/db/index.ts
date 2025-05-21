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

export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Clear the topic cache
  topicCache.clear();

  // Close the database pool if it exists
  if (pool) {
    await pool.end();
    pool = null as unknown as Pool; // Reset pool
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
  // Check cache first
  if (topicCache.has(topicName)) {
    return topicCache.get(topicName)!;
  }

  // Check database
  const client = await pool.connect();
  try {
    // Check if topic exists
    const res = await client.query(
      'SELECT topic_id FROM topics WHERE topic_name = $1',
      [topicName]
    );

    if (res.rows.length > 0) {
      const topicId = res.rows[0].topic_id;
      topicCache.set(topicName, topicId);
      return topicId;
    }

    // Insert new topic
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
  // Add to batch
  dataBatch.push({ topic, ltp, indexName, type, strike });

  // Start timer if not running
  if (!batchTimer) {
    batchTimer = setTimeout(() => {
      flushBatch().catch(console.error);
    }, config.app.batchInterval);
  }

  // Check batch size
  if (dataBatch.length >= config.app.batchSize) {
    if (batchTimer) {
      clearTimeout(batchTimer);
      batchTimer = null;
    }
    flushBatch().catch(console.error);
  }
}

export async function flushBatch() {
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

    // Process all items in batch
    for (const item of batchToProcess) {
      const topicId = await getTopicId(
        item.topic,
        item.indexName,
        item.type,
        item.strike
      );

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
    // Consider re-queueing failed items
  } finally {
    client.release();
  }
}