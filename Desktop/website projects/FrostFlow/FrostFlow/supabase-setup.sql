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

-- ── Email verification + extended columns (run once after initial setup) ──────
alter table public.clients
  add column if not exists email_verified      boolean  not null default false,
  add column if not exists email_verif_token   text     default null,
  add column if not exists email_verif_expiry  text     default null,
  add column if not exists status              text     not null default 'active',
  add column if not exists appliances          jsonb    not null default '[]'::jsonb,
  add column if not exists fault_reports       jsonb    not null default '[]'::jsonb,
  add column if not exists settings            jsonb    not null default '{}'::jsonb;

-- Index for fast token lookups during email verification
create index if not exists clients_verif_token_idx on public.clients (email_verif_token);

-- ─────────────────────────────────────────────────────────────────────────────
-- RESEND_API_KEY (for email verification) → https://resend.com → free 100/day
-- Add on Render → Environment Variables:
--   RESEND_API_KEY  →  re_xxxxxxxxxxxxxxxxxxxxxxxx
--   EMAIL_FROM      →  FrostFlow <noreply@frostflowrefridgerations.co.za>
-- ─────────────────────────────────────────────────────────────────────────────

-- ─────────────────────────────────────────────────────────────────────────────
-- Done. Copy these two values into Render → Environment Variables:
--
--   SUPABASE_URL  →  Settings → API → Project URL
--                    e.g. https://abcdefghijkl.supabase.co
--
--   SUPABASE_KEY  →  Settings → API → service_role key  (NOT the anon key)
--                    Starts with: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
-- ─────────────────────────────────────────────────────────────────────────────
