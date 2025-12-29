import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
const port = process.env.PORT || 3000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const SNAPSHOT_SQL = `
  select distinct on (creator_id)
    creator_id,
    username       as creator_handle,
    assigned_group as manager,
    live_days_mtd,
    live_hours_mtd,
    diamonds_mtd,
    response_status,
    date           as data_date
  from fasttrack_daily
  where date <= current_date - interval '1 day'
  order by creator_id, date desc;
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
