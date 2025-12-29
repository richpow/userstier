import express from "express";
import pkg from "pg";

const { Pool } = pkg;

// Log what the service actually sees for DATABASE_URL
console.log("DATABASE_URL env:", process.env.DATABASE_URL);

const app = express();
const port = process.env.PORT || 3000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Use the real fasttrack_daily columns, quoted where needed
// One latest row per creator based on "Data period" then _ingested_at
const SNAPSHOT_SQL = `
  select distinct on (creator_id)
    creator_id,
    "Creator's username" as creator_handle,
    "group"               as manager,
    valid_go_live_days    as live_days_mtd,
    "LIVE streams"        as live_streams_mtd,
    "LIVE duration"       as live_duration_raw,
    "Diamonds"            as diamonds_mtd,
    "Data period"         as data_period
  from fasttrack_daily
  where is_demo_data is not true
  order by creator_id, "Data period" desc, _ingested_at desc;
`;

app.get("/fasttrack/snapshot", async (req, res) => {
  try {
    const client = await pool.connect();
    try {
      const result = await client.query(SNAPSHOT_SQL);
      res.json(result.rows);
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Snapshot error", err);
    res.status(500).json({ error: "snapshot_failed" });
  }
});

app.get("/", (req, res) => {
  res.json({ status: "ok" });
});

app.listen(port, () => {
  console.log(`FastTrack sheets bridge listening on ${port}`);
});
