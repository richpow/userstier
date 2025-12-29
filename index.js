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

// Month to date snapshot per creator
// Logic:
// 1) Find latest "Data period" in fasttrack_daily (ignoring demo data)
// 2) If latest day-of-month = 1, then report the previous calendar month
//    else report the month of latest date
// 3) Sum per creator across that month between month_start and month_end
const SNAPSHOT_SQL = `
  with latest as (
    select max("Data period") as latest_date
    from fasttrack_daily
    where is_demo_data is not true
  ),
  month_bounds as (
    select
      case
        when extract(day from latest_date) = 1
          then (date_trunc('month', latest_date) - interval '1 month')::date
        else
          date_trunc('month', latest_date)::date
      end as month_start,
      case
        when extract(day from latest_date) = 1
          then (date_trunc('month', latest_date) - interval '1 day')::date
        else
          latest_date
      end as month_end
    from latest
  )
  select
    f.creator_id,
    f."Creator's username" as creator_handle,
    f."group"              as manager,
    sum(f.valid_go_live_days) as live_days_mtd,
    sum(f."LIVE streams")     as live_streams_mtd,
    sum(f."LIVE duration")    as live_duration_raw,
    sum(f."Diamonds")         as diamonds_mtd,
    max(f."Data period")      as data_period
  from fasttrack_daily f
  cross join month_bounds mb
  where f.is_demo_data is not true
    and f."Data period" between mb.month_start and mb.month_end
  group by
    f.creator_id,
    f."Creator's username",
    f."group";
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
