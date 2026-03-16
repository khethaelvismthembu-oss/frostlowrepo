-- ─────────────────────────────────────────────────────────────────────────────
-- FrostFlow — Supabase Database Setup
-- Run this entire file in: Supabase Dashboard → SQL Editor → New Query → Run
-- ─────────────────────────────────────────────────────────────────────────────

-- Client accounts table
create table if not exists public.clients (
  user_id         text        primary key,
  first_name      text        not null,
  last_name       text        not null default '',
  email           text        unique not null,
  phone           text        not null default '',
  plan            text        not null default '',
  password_hash   text        not null,
  password_salt   text        not null,
  created_at      text        not null,
  updated_at      text,
  service_history jsonb       not null default '[]'::jsonb
);

-- Our Node.js server handles all auth using the service_role key.
-- RLS would block service_role reads, so we disable it.
alter table public.clients disable row level security;

-- Index for fast email lookups during login
create index if not exists clients_email_idx on public.clients (email);

-- ─────────────────────────────────────────────────────────────────────────────
-- Done. Copy these two values into Render → Environment Variables:
--
--   SUPABASE_URL  →  Settings → API → Project URL
--                    e.g. https://abcdefghijkl.supabase.co
--
--   SUPABASE_KEY  →  Settings → API → service_role key  (NOT the anon key)
--                    Starts with: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
-- ─────────────────────────────────────────────────────────────────────────────
