-- Run this once in your Supabase SQL editor

CREATE TABLE IF NOT EXISTS pitwall_news (
  id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  tag         text        NOT NULL,
  headline    text        NOT NULL,
  summary     text        NOT NULL,
  url         text        UNIQUE,          -- prevents duplicate articles
  scraped_at  timestamptz NOT NULL DEFAULT now()
);

-- Public read (anon key can SELECT)
ALTER TABLE pitwall_news ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public read"
  ON pitwall_news FOR SELECT
  USING (true);

-- Index for fast ordered fetches
CREATE INDEX IF NOT EXISTS pitwall_news_scraped_at_idx
  ON pitwall_news (scraped_at DESC);
