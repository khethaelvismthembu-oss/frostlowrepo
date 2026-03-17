const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const ROOT = path.resolve(__dirname, '..');

const MIME = {
  '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css',
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
  '.svg': 'image/svg+xml', '.xml': 'application/xml', '.txt': 'text/plain',
  '.json': 'application/json', '.ico': 'image/x-icon', '.webp': 'image/webp',
  '.zip': 'application/zip'
};

// In-memory YouTube data cache: { videoId: { data, timestamp } }
const ytCache = {};
const YT_CACHE_TTL = 60 * 60 * 1000; // 1 hour

function fetchYouTubeData(videoId) {
  return new Promise((resolve, reject) => {
    if (ytCache[videoId] && Date.now() - ytCache[videoId].timestamp < YT_CACHE_TTL) {
      return resolve(ytCache[videoId].data);
    }
    const options = {
      hostname: 'www.youtube.com',
      path: `/watch?v=${videoId}`,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      }
    };

    const req = https.get(options, (ytRes) => {
      // Follow redirects
      if (ytRes.statusCode >= 300 && ytRes.statusCode < 400 && ytRes.headers.location) {
        return fetchYouTubeData(videoId).then(resolve).catch(reject);
      }
      let body = '';
      ytRes.on('data', chunk => { body += chunk; if (body.length > 2000000) body = body.slice(0, 2000000); });
      ytRes.on('end', () => {
        try {
          // Extract ytInitialData JSON from page
          const match = body.match(/var ytInitialData\s*=\s*({.+?});\s*(?:var |<\/script>)/s);
          if (!match) throw new Error('ytInitialData not found in page');
          const data = JSON.parse(match[1]);

          // Dig into the nested structure
          const contents = data?.contents?.twoColumnWatchNextResults?.results?.results?.contents || [];
          const primary   = contents.find(c => c.videoPrimaryInfoRenderer)?.videoPrimaryInfoRenderer;
          const secondary = contents.find(c => c.videoSecondaryInfoRenderer)?.videoSecondaryInfoRenderer;

          const title         = primary?.title?.runs?.[0]?.text || '';
          const viewCountText = primary?.viewCount?.videoViewCountRenderer?.viewCount?.simpleText
                              || primary?.viewCount?.videoViewCountRenderer?.originalViewCount || '';
          const publishDate   = primary?.relativeDateText?.simpleText
                              || primary?.dateText?.simpleText || '';
          const channelName   = secondary?.owner?.videoOwnerRenderer?.title?.runs?.[0]?.text || '';
          const avatars       = secondary?.owner?.videoOwnerRenderer?.thumbnail?.thumbnails || [];
          const channelAvatar = avatars[avatars.length - 1]?.url || '';

          const result = { title, viewCount: viewCountText, publishDate, channelName, channelAvatar };
          ytCache[videoId] = { data: result, timestamp: Date.now() };
          console.log(`[YT] Fetched metadata for ${videoId}: "${title}"`);
          resolve(result);
        } catch (e) {
          console.error(`[YT] Parse error for ${videoId}:`, e.message);
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('YouTube fetch timeout')); });
  });
}

// ── Auth & Client Data Infrastructure ────────────────────────────────────────

// Supabase (persistent, production) — set SUPABASE_URL + SUPABASE_KEY on Render
const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_KEY = process.env.SUPABASE_KEY || '';
const useSupabase  = !!(SUPABASE_URL && SUPABASE_KEY);

// File-based fallback (local dev — no env vars needed)
const DATA_DIR   = path.join(ROOT, 'data', 'clients');
const SESS_FILE  = path.join(ROOT, 'data', 'sessions.json');
const EMAIL_FILE = path.join(ROOT, 'data', 'email_index.json');
if (!useSupabase) {
  [path.join(ROOT, 'data'), DATA_DIR].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });
}

// In-memory session store — users re-login after server restart (acceptable for free tier)
let sessions = {};
try {
  if (!useSupabase && fs.existsSync(SESS_FILE)) {
    sessions = JSON.parse(fs.readFileSync(SESS_FILE, 'utf8'));
    const now = Date.now();
    Object.keys(sessions).forEach(k => { if (sessions[k].expires < now) delete sessions[k]; });
  }
} catch(e) {}

// Email → userId index (file fallback only; Supabase queries email column directly)
let emailIndex = {};
try { if (!useSupabase && fs.existsSync(EMAIL_FILE)) emailIndex = JSON.parse(fs.readFileSync(EMAIL_FILE, 'utf8')); } catch(e) {}

function saveSessions()   { if (!useSupabase) try { fs.writeFileSync(SESS_FILE,  JSON.stringify(sessions),   'utf8'); } catch(e) {} }
function saveEmailIndex() { if (!useSupabase) try { fs.writeFileSync(EMAIL_FILE, JSON.stringify(emailIndex), 'utf8'); } catch(e) {} }

// ── Admin session store (in-memory, 4-hour expiry) ────────────────────────────
let adminSessions = {};
function createAdminSession() {
  const token = crypto.randomBytes(32).toString('hex');
  adminSessions[token] = { expires: Date.now() + 4 * 60 * 60 * 1000 };
  return token;
}
function getAdminSession(req) {
  const cookie = req.headers.cookie || '';
  const match  = cookie.match(/ff_admin=([a-f0-9]{64})/);
  if (!match) return null;
  const s = adminSessions[match[1]];
  if (!s || s.expires < Date.now()) { if (match) delete adminSessions[match[1]]; return null; }
  return s;
}
const ADMIN_COOKIE_OPTS = `HttpOnly; SameSite=Strict; Path=/; Max-Age=${4*3600}${process.env.NODE_ENV === 'production' ? '; Secure' : ''}`;
const CLEAR_ADMIN_COOKIE = `ff_admin=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0${process.env.NODE_ENV === 'production' ? '; Secure' : ''}`;

// ── Email sending via Resend API (zero npm — pure https) ──────────────────────
// Get a free key at https://resend.com (100 emails/day free)
// Add RESEND_API_KEY to Render → Environment Variables
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const EMAIL_FROM     = process.env.EMAIL_FROM || 'FrostFlow <noreply@frostflowrefridgerations.co.za>';
const SITE_URL       = process.env.SITE_URL  || 'https://www.frostflowrefridgerations.co.za';
const ADMIN_PASS     = process.env.ADMIN_PASS || '';
const ADMIN_EMAIL    = process.env.ADMIN_EMAIL || '';
const YOCO_SECRET_KEY     = process.env.YOCO_SECRET_KEY     || '';
const GOOGLE_CLIENT_ID    = process.env.GOOGLE_CLIENT_ID    || '';
const GOOGLE_CLIENT_SECRET= process.env.GOOGLE_CLIENT_SECRET|| '';
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || `${(process.env.SITE_URL||'http://localhost:3000')}/auth/google/callback`;

// ── Rate limiter & IP security ─────────────────────────────────────────────
const loginAttempts = {}; // { 'ip': { count, firstAttempt, blockedUntil } }
const MAX_LOGIN_ATTEMPTS = 5;
const ATTEMPT_WINDOW_MS  = 15 * 60 * 1000;   // 15 min rolling window
const BLOCK_DURATION_MS  = 60 * 60 * 1000;   // 1 hr block after max attempts
const BLOCKED_IPS        = new Set((process.env.BLOCKED_IPS || '').split(',').map(s => s.trim()).filter(Boolean));

function getClientIP(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'unknown';
}
function checkRateLimit(ip) {
  if (BLOCKED_IPS.has(ip)) return { blocked: true, remaining: 999, reason: 'blocked' };
  const now = Date.now();
  const e   = loginAttempts[ip];
  if (!e) return { blocked: false };
  if (e.blockedUntil && now < e.blockedUntil) {
    return { blocked: true, remaining: Math.ceil((e.blockedUntil - now) / 60000), reason: 'rate_limit' };
  }
  if (now - e.firstAttempt > ATTEMPT_WINDOW_MS) { delete loginAttempts[ip]; return { blocked: false }; }
  return { blocked: false, attemptsLeft: Math.max(0, MAX_LOGIN_ATTEMPTS - e.count) };
}
function recordFailedAttempt(ip) {
  const now = Date.now();
  if (!loginAttempts[ip]) loginAttempts[ip] = { count: 0, firstAttempt: now };
  loginAttempts[ip].count++;
  if (loginAttempts[ip].count >= MAX_LOGIN_ATTEMPTS) loginAttempts[ip].blockedUntil = now + BLOCK_DURATION_MS;
}
function clearLoginAttempts(ip) { delete loginAttempts[ip]; }

// ── 2FA pending store (in-memory, 10-min TTL) ─────────────────────────────
const pending2FA = {}; // { tempToken: { userId, email, expiresAt } }
function create2FASession(userId, email) {
  const tok = crypto.randomBytes(32).toString('hex');
  pending2FA[tok] = { userId, email, expiresAt: Date.now() + 10 * 60 * 1000 };
  return tok;
}
function get2FASession(tok) {
  const s = pending2FA[tok];
  if (!s || s.expiresAt < Date.now()) { if (tok) delete pending2FA[tok]; return null; }
  return s;
}

// ── File upload directory ─────────────────────────────────────────────────
const UPLOAD_DIR = path.join(ROOT, 'data', 'uploads');
if (!useSupabase && !fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

function sendEmail(to, subject, htmlBody) {
  return new Promise((resolve) => {
    if (!RESEND_API_KEY) {
      console.log('[EMAIL] No RESEND_API_KEY — skipping send to:', to, '|', subject);
      return resolve({ ok: false, reason: 'no_api_key' });
    }
    const payload = JSON.stringify({ from: EMAIL_FROM, to: [to], subject, html: htmlBody });
    const req = https.request({
      hostname: 'api.resend.com', port: 443, path: '/emails', method: 'POST',
      headers: { 'Authorization': 'Bearer ' + RESEND_API_KEY, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
    }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => { console.log('[EMAIL] →', to, 'status', r.statusCode); resolve({ ok: r.statusCode < 300 }); }); });
    req.on('error', e => { console.error('[EMAIL] Error:', e.message); resolve({ ok: false }); });
    req.write(payload); req.end();
  });
}

// ── Yoco payment capture (zero npm — pure https) ─────────────────────────────
function yocoCapture(token, amountInCents) {
  return new Promise((resolve, reject) => {
    if (!YOCO_SECRET_KEY) return reject(new Error('Yoco secret key not configured.'));
    const body = JSON.stringify({ token, amountInCents, currency: 'ZAR' });
    const req = https.request({
      hostname: 'online.yoco.com', port: 443, path: '/v1/charges/',
      method: 'POST',
      headers: {
        'X-Auth-Secret-Key': YOCO_SECRET_KEY,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    }, (resp) => {
      let data = '';
      resp.on('data', c => data += c);
      resp.on('end', () => {
        try {
          const json = JSON.parse(data);
          console.log('[YOCO] charge response status:', resp.statusCode, json.id || json.errorCode || json.displayMessage);
          if (resp.statusCode >= 200 && resp.statusCode < 300 && json.id) {
            resolve(json);
          } else {
            reject(new Error(json.displayMessage || json.errorCode || 'Card declined. Please try a different card.'));
          }
        } catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('Payment gateway timeout — please try again.')); });
    req.write(body);
    req.end();
  });
}

function makeVerifyEmailHtml(firstName, verifyUrl) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00337a);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Refrigeration &amp; Air-Conditioning</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Verify your email address</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 28px;">Hi ${firstName}, welcome to the FrostFlow Client Portal! Click the button below to verify your email and activate your account.</p>
      <div style="text-align:center;margin:32px 0;">
        <a href="${verifyUrl}" style="display:inline-block;background:#004aad;color:#fff;font-weight:800;font-size:14px;text-decoration:none;padding:14px 40px;border-radius:50px;text-transform:uppercase;letter-spacing:0.5px;">Verify My Email</a>
      </div>
      <p style="color:#94a3b8;font-size:11px;text-align:center;margin:0;">Link expires in 24 hours. If you didn't create this account, ignore this email.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · +27 73 816 0885 · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

function makePasswordResetEmailHtml(firstName, resetUrl) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#0f172a,#1e293b);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.5);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Password Reset Request</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Reset your password</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 28px;">Hi ${firstName}, we received a request to reset your FrostFlow account password. Click below to set a new password. This link expires in 1 hour.</p>
      <div style="text-align:center;margin:32px 0;">
        <a href="${resetUrl}" style="display:inline-block;background:#0f172a;color:#fff;font-weight:800;font-size:14px;text-decoration:none;padding:14px 40px;border-radius:50px;text-transform:uppercase;letter-spacing:0.5px;">Reset My Password</a>
      </div>
      <p style="color:#94a3b8;font-size:11px;text-align:center;margin:0;">If you didn't request this, you can safely ignore this email. Your password will not change.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · +27 73 816 0885</p>
    </div>
  </div></body></html>`;
}

function makeServiceBookingClientHtml(firstName, preferredDate, notes) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00337a);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Service Booking Received</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Booking Request Confirmed ✓</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 20px;">Hi ${firstName}, we've received your service booking request. Our team will contact you within 1 business day to confirm your appointment.</p>
      <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:20px;margin:0 0 24px;">
        <p style="margin:0 0 8px;font-size:12px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.05em;">Booking Details</p>
        <p style="margin:0 0 6px;font-size:14px;color:#0f172a;"><strong>Preferred Date:</strong> ${preferredDate || 'Flexible'}</p>
        ${notes ? `<p style="margin:0;font-size:14px;color:#0f172a;"><strong>Notes:</strong> ${notes}</p>` : ''}
      </div>
      <p style="color:#475569;font-size:13px;line-height:1.7;margin:0;">Need urgent assistance? Call us on <strong>+27 73 816 0885</strong> or WhatsApp us directly.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · +27 73 816 0885 · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

function makeServiceBookingAdminHtml(profile, preferredDate, notes) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#0f172a,#1e293b);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;">❄ FrostFlow Admin</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.5);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">New Service Booking</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:20px;font-weight:800;color:#0f172a;margin:0 0 16px;">New Service Booking Request</h1>
      <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:20px;margin:0 0 20px;">
        <p style="margin:0 0 8px;font-size:12px;font-weight:700;color:#94a3b8;text-transform:uppercase;">Client Details</p>
        <p style="margin:0 0 4px;font-size:14px;color:#0f172a;"><strong>Name:</strong> ${profile.firstName} ${profile.lastName || ''}</p>
        <p style="margin:0 0 4px;font-size:14px;color:#0f172a;"><strong>Email:</strong> ${profile.email}</p>
        <p style="margin:0 0 4px;font-size:14px;color:#0f172a;"><strong>Phone:</strong> ${profile.phone || 'Not provided'}</p>
        <p style="margin:0 0 4px;font-size:14px;color:#0f172a;"><strong>Plan:</strong> ${profile.plan || 'None'}</p>
        <p style="margin:0 0 4px;font-size:14px;color:#0f172a;"><strong>Preferred Date:</strong> ${preferredDate || 'Flexible'}</p>
        ${notes ? `<p style="margin:0;font-size:14px;color:#0f172a;"><strong>Notes:</strong> ${notes}</p>` : ''}
      </div>
      <a href="${SITE_URL}/admin.html" style="display:inline-block;background:#004aad;color:#fff;font-weight:800;font-size:13px;text-decoration:none;padding:12px 28px;border-radius:50px;text-transform:uppercase;">Open Admin Panel</a>
    </div>
  </div></body></html>`;
}

function makePlanUpgradeClientHtml(firstName, oldPlan, newPlan) {
  const labels = { domestic:'Domestic Fridge Cover', aircon:'Air Conditioning Cover', commercial:'Commercial Unit Cover' };
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#00b87c,#009565);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.75);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Plan Updated</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Your Plan Has Been Updated ✓</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 20px;">Hi ${firstName}, your maintenance plan has been successfully updated.</p>
      <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px;margin:0 0 24px;">
        ${oldPlan ? `<p style="margin:0 0 8px;font-size:13px;color:#64748b;"><s>${labels[oldPlan] || oldPlan}</s></p>` : ''}
        <p style="margin:0;font-size:16px;font-weight:800;color:#00b87c;">→ ${labels[newPlan] || newPlan}</p>
      </div>
      <p style="color:#475569;font-size:13px;">Your new coverage and benefits are active immediately. View your dashboard for details.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · +27 73 816 0885</p>
    </div>
  </div></body></html>`;
}

function makePlanUpgradeAdminHtml(profile, oldPlan, newPlan) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#0f172a,#1e293b);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;">❄ FrostFlow Admin</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.5);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Plan Change</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:20px;font-weight:800;color:#0f172a;margin:0 0 16px;">Client Changed Plan</h1>
      <p style="font-size:14px;color:#475569;margin:0 0 16px;"><strong>${profile.firstName} ${profile.lastName || ''}</strong> (${profile.email}) changed their plan from <strong>${oldPlan || 'none'}</strong> → <strong>${newPlan}</strong>.</p>
      <a href="${SITE_URL}/admin.html" style="display:inline-block;background:#004aad;color:#fff;font-weight:800;font-size:13px;text-decoration:none;padding:12px 28px;border-radius:50px;text-transform:uppercase;">Open Admin Panel</a>
    </div>
  </div></body></html>`;
}

function makeServiceReminderHtml(firstName, planLabel, lastServiceDate) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00337a);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Service Reminder</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Your Service Is Due 🔧</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 20px;">Hi ${firstName}, it's time for your scheduled maintenance service as part of your <strong>${planLabel}</strong> plan. Regular servicing keeps your unit running efficiently and prevents costly breakdowns.</p>
      <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:20px;margin:0 0 28px;">
        <p style="margin:0;font-size:13px;color:#1e40af;"><strong>Last service:</strong> ${lastServiceDate || 'Not on record'}</p>
      </div>
      <div style="text-align:center;margin:0 0 20px;">
        <a href="${SITE_URL}/dashboard.html" style="display:inline-block;background:#004aad;color:#fff;font-weight:800;font-size:14px;text-decoration:none;padding:14px 40px;border-radius:50px;text-transform:uppercase;letter-spacing:0.5px;">Book Your Service</a>
      </div>
      <p style="color:#94a3b8;font-size:12px;text-align:center;">Or call us on <strong>+27 73 816 0885</strong></p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

function makeFaultAckHtml(firstName, reportId, urgency) {
  const urgLabel = urgency === 'emergency' ? '🚨 Emergency' : urgency === 'high' ? '⚠ High' : 'Normal';
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#dc2626,#b91c1c);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.75);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Fault Report Logged</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Fault Report Received ✓</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 20px;">Hi ${firstName}, we've logged your fault report. Our team has been notified and will respond based on your plan's SLA.</p>
      <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:20px;margin:0 0 24px;">
        <p style="margin:0 0 6px;font-size:13px;color:#64748b;"><strong>Reference:</strong> ${reportId}</p>
        <p style="margin:0;font-size:13px;color:#64748b;"><strong>Urgency:</strong> ${urgLabel}</p>
      </div>
      <p style="color:#475569;font-size:13px;">For emergencies, please also call us directly on <strong>+27 73 816 0885</strong>.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

// ── Google OAuth helpers ───────────────────────────────────────────────────
function googlePost(path2, body) {
  return new Promise((resolve, reject) => {
    const payload = new URLSearchParams(body).toString();
    const req = https.request({
      hostname: 'oauth2.googleapis.com', port: 443, path: path2, method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(payload) }
    }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(e); } }); });
    req.on('error', reject); req.write(payload); req.end();
  });
}
function googleGet(path2, accessToken) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'www.googleapis.com', port: 443, path: path2, method: 'GET',
      headers: { Authorization: 'Bearer ' + accessToken }
    }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(e); } }); });
    req.on('error', reject); req.end();
  });
}
async function exchangeGoogleCode(code) {
  return googlePost('/token', {
    code, client_id: GOOGLE_CLIENT_ID, client_secret: GOOGLE_CLIENT_SECRET,
    redirect_uri: GOOGLE_REDIRECT_URI, grant_type: 'authorization_code'
  });
}
async function getGoogleUserInfo(accessToken) {
  return googleGet('/oauth2/v2/userinfo', accessToken);
}

// ── Activity log helper ────────────────────────────────────────────────────
function appendLoginHistory(profile, req, success) {
  const entry = { ts: new Date().toISOString(), ip: getClientIP(req), ua: (req.headers['user-agent'] || '').slice(0, 120), success };
  const s = profile.settings = profile.settings || {};
  s.loginHistory = [entry, ...(s.loginHistory || [])].slice(0, 50); // keep last 50
}

// ── Password reset email ───────────────────────────────────────────────────
function makePasswordResetHtml(firstName, resetUrl) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00337a);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Password Reset</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Reset your password</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 28px;">Hi ${firstName}, we received a request to reset your password. Click the button below — this link expires in 1 hour.</p>
      <div style="text-align:center;margin:32px 0;">
        <a href="${resetUrl}" style="display:inline-block;background:#004aad;color:#fff;font-weight:800;font-size:14px;text-decoration:none;padding:14px 40px;border-radius:50px;text-transform:uppercase;letter-spacing:0.5px;">Reset My Password</a>
      </div>
      <p style="color:#94a3b8;font-size:12px;">If you didn't request this, you can safely ignore this email. Your password won't change.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

// ── 2FA OTP email ──────────────────────────────────────────────────────────
function make2FAEmailHtml(firstName, otp) {
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00337a);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Sign-In Code</div>
    </div>
    <div style="padding:36px;text-align:center;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Your verification code</h1>
      <p style="color:#475569;font-size:14px;margin:0 0 28px;">Hi ${firstName}, use this code to complete your sign-in. It expires in 10 minutes.</p>
      <div style="background:#f0f7ff;border:2px solid #bfdbfe;border-radius:16px;padding:24px;display:inline-block;margin:0 auto 24px;">
        <span style="font-size:42px;font-weight:900;letter-spacing:12px;color:#004aad;">${otp}</span>
      </div>
      <p style="color:#94a3b8;font-size:12px;">Never share this code. FrostFlow will never ask for it by phone.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town</p>
    </div>
  </div></body></html>`;
}

function makePlanActivationClientHtml(firstName, plan, amountRand) {
  const planNames = { domestic: 'Domestic Fridge Cover', aircon: 'Air Conditioning Cover', commercial: 'Commercial Unit Cover' };
  const planName = planNames[plan] || plan;
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00b87c);padding:32px;text-align:center;">
      <div style="font-size:22px;font-weight:900;color:#fff;text-transform:uppercase;letter-spacing:-0.5px;">❄ FrostFlow</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Plan Activated</div>
    </div>
    <div style="padding:36px;">
      <h1 style="font-size:21px;font-weight:800;color:#0f172a;margin:0 0 10px;">Welcome aboard, ${firstName}! 🎉</h1>
      <p style="color:#475569;font-size:14px;line-height:1.7;margin:0 0 20px;">Your <strong>${planName}</strong> has been activated. You're now covered and our team is ready to assist you.</p>
      <div style="background:#f0fdf4;border:1px solid #86efac;border-radius:12px;padding:20px;margin:0 0 24px;">
        <p style="margin:0 0 6px;font-size:13px;color:#166534;font-weight:700;">✓ Plan: ${planName}</p>
        <p style="margin:0;font-size:13px;color:#166534;font-weight:700;">✓ First month payment: R${Number(amountRand).toFixed(2)} — Received</p>
      </div>
      <p style="color:#475569;font-size:13px;">Log in to your client portal to register appliances, book services, or report faults.</p>
    </div>
    <div style="background:#f8fafc;padding:18px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#94a3b8;font-size:10px;margin:0;">FrostFlow · Blackheath, Cape Town · frostflowrefridgerations.co.za</p>
    </div>
  </div></body></html>`;
}

function makePlanActivationAdminHtml(profile, plan) {
  const planNames = { domestic: 'Domestic Fridge Cover', aircon: 'Air Conditioning Cover', commercial: 'Commercial Unit Cover' };
  return `<!DOCTYPE html><html><body style="margin:0;padding:32px;background:#f1f5f9;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
    <div style="background:linear-gradient(135deg,#004aad,#00b87c);padding:24px;text-align:center;">
      <div style="font-size:18px;font-weight:900;color:#fff;">❄ FrostFlow Admin</div>
    </div>
    <div style="padding:28px;">
      <h2 style="font-size:17px;font-weight:800;color:#0f172a;margin:0 0 12px;">💳 New Plan Activation</h2>
      <p style="font-size:13px;color:#475569;margin:0 0 6px;"><strong>Client:</strong> ${profile.firstName} ${profile.lastName||''}</p>
      <p style="font-size:13px;color:#475569;margin:0 0 6px;"><strong>Email:</strong> ${profile.email}</p>
      <p style="font-size:13px;color:#475569;margin:0 0 6px;"><strong>Phone:</strong> ${profile.phone||'—'}</p>
      <p style="font-size:13px;color:#475569;margin:0;"><strong>Plan:</strong> ${planNames[plan]||plan}</p>
    </div>
  </div></body></html>`;
}

// ── Supabase PostgREST helper (uses Node built-in https — zero npm dependencies) ──
function supaFetch(table, method, body, query, prefer) {
  return new Promise((resolve, reject) => {
    const base    = new URL(SUPABASE_URL);
    const apiPath = '/rest/v1/' + table + (query ? '?' + query : '');
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'apikey'       : SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type' : 'application/json',
      'Prefer'       : prefer || 'return=representation'
    };
    if (payload) headers['Content-Length'] = Buffer.byteLength(payload);
    const req = https.request(
      { hostname: base.hostname, port: 443, path: apiPath, method: method || 'GET', headers },
      r => { let d = ''; r.on('data', c => d += c); r.on('end', () => {
        try { resolve({ status: r.statusCode, rows: d ? JSON.parse(d) : [] }); }
        catch(e) { resolve({ status: r.statusCode, rows: [] }); }
      }); }
    );
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ── Column mapping: JS camelCase ↔ Supabase snake_case ──
function toRow(p) {
  return {
    user_id: p.userId, first_name: p.firstName, last_name: p.lastName || '',
    email: p.email, phone: p.phone || '', plan: p.plan || '',
    password_hash: p.passwordHash, password_salt: p.passwordSalt,
    created_at: p.createdAt, updated_at: p.updatedAt || null,
    service_history: p.serviceHistory || [],
    email_verified: p.emailVerified || false,
    email_verif_token: p.emailVerifToken || null,
    email_verif_expiry: p.emailVerifExpiry || null,
    status: p.status || 'active',
    appliances: p.appliances || [],
    fault_reports: p.faultReports || [],
    settings: p.settings || {}
  };
}
function fromRow(r) {
  return {
    userId: r.user_id, firstName: r.first_name, lastName: r.last_name,
    email: r.email, phone: r.phone, plan: r.plan,
    passwordHash: r.password_hash, passwordSalt: r.password_salt,
    createdAt: r.created_at, updatedAt: r.updated_at,
    serviceHistory: r.service_history || [],
    emailVerified: r.email_verified || false,
    emailVerifToken: r.email_verif_token || null,
    emailVerifExpiry: r.email_verif_expiry || null,
    status: r.status || 'active',
    appliances: r.appliances || [],
    faultReports: r.fault_reports || [],
    settings: r.settings || {}
  };
}

// ── Storage functions (auto-select Supabase or file system) ──
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
}
function createSession(userId, email, rememberMe) {
  const token = crypto.randomBytes(48).toString('hex');
  const ttl = rememberMe ? 30 * 24 * 60 * 60 * 1000 : 24 * 60 * 60 * 1000;
  sessions[token] = { userId, email, expires: Date.now() + ttl };
  saveSessions();
  return token;
}
function sessionCookieOpts(rememberMe) {
  const maxAge = rememberMe ? 30 * 24 * 3600 : 24 * 3600;
  return `HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAge}${isProd ? '; Secure' : ''}`;
}
function getSession(req) {
  const cookie = req.headers.cookie || '';
  const match  = cookie.match(/ff_session=([a-f0-9]{96})/);
  if (!match) return null;
  const s = sessions[match[1]];
  if (!s || s.expires < Date.now()) return null;
  return s;
}
function clientDir(userId) { return path.join(DATA_DIR, userId); }

async function readProfile(userId) {
  if (useSupabase) {
    const { rows } = await supaFetch('clients', 'GET', null, 'user_id=eq.' + userId + '&select=*');
    return rows[0] ? fromRow(rows[0]) : null;
  }
  try { return JSON.parse(fs.readFileSync(path.join(clientDir(userId), 'profile.json'), 'utf8')); } catch(e) { return null; }
}
async function writeProfile(userId, data) {
  if (useSupabase) {
    await supaFetch('clients', 'POST', toRow(data), null, 'resolution=merge-duplicates,return=minimal');
    return;
  }
  const d = clientDir(userId);
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  fs.writeFileSync(path.join(d, 'profile.json'), JSON.stringify(data, null, 2), 'utf8');
}
async function findUserByEmail(email) {
  if (useSupabase) {
    const { rows } = await supaFetch('clients', 'GET', null, 'email=eq.' + encodeURIComponent(email) + '&select=*');
    return rows[0] ? fromRow(rows[0]) : null;
  }
  const uid = emailIndex[email];
  return uid ? readProfile(uid) : null;
}
async function emailExists(email) {
  if (useSupabase) {
    const { rows } = await supaFetch('clients', 'GET', null, 'email=eq.' + encodeURIComponent(email) + '&select=user_id');
    return rows.length > 0;
  }
  return !!emailIndex[email];
}

function parseBody(req, maxBytes) {
  const limit = maxBytes || 50000;
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => { body += chunk; if (body.length > limit) { body = ''; req.destroy(); reject(new Error('Request too large')); } });
    req.on('end', () => { try { resolve(JSON.parse(body)); } catch(e) { resolve({}); } });
  });
}
function jsonRes(res, code, data, extraHeaders) {
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }, extraHeaders || {}));
  res.end(JSON.stringify(data));
}
const isProd = process.env.NODE_ENV === 'production';
const SESSION_COOKIE_OPTS = `HttpOnly; SameSite=Strict; Path=/; Max-Age=${30*24*3600}${isProd ? '; Secure' : ''}`;
const CLEAR_COOKIE = `ff_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0${isProd ? '; Secure' : ''}`;

console.log(useSupabase ? '[DB] Using Supabase persistent storage' : '[DB] Using local file storage (dev mode)');

// ── Plan service intervals (days between scheduled services) ─────────────────
const PLAN_SERVICE_INTERVAL_DAYS = { domestic: 365, aircon: 180, commercial: 90 };
const PLAN_LABELS = { domestic: 'Domestic Fridge Cover', aircon: 'Air Conditioning Cover', commercial: 'Commercial Unit Cover' };

// ── Daily service reminder cron ──────────────────────────────────────────────
async function runServiceReminders() {
  if (!RESEND_API_KEY) return;
  try {
    let clients = [];
    if (useSupabase) {
      const { rows } = await supaFetch('clients', 'GET', null, 'status=eq.active&email_verified=eq.true&select=*');
      clients = rows.map(fromRow);
    } else {
      for (const uid of Object.values(emailIndex)) {
        const p = await readProfile(uid);
        if (p && p.status === 'active' && p.emailVerified) clients.push(p);
      }
    }
    const now = Date.now();
    for (const profile of clients) {
      const intervalDays = PLAN_SERVICE_INTERVAL_DAYS[profile.plan];
      if (!intervalDays) continue;
      // Find last service date
      const history = profile.serviceHistory || [];
      const lastEntry = history.sort((a, b) => new Date(b.date||b.loggedAt||0) - new Date(a.date||a.loggedAt||0))[0];
      const lastServiceAt = lastEntry ? new Date(lastEntry.date || lastEntry.loggedAt).getTime() : new Date(profile.createdAt).getTime();
      const dueSince = now - lastServiceAt;
      const intervalMs = intervalDays * 24 * 60 * 60 * 1000;
      if (dueSince < intervalMs) continue; // not due yet
      // Check we haven't recently sent a reminder (7-day cooldown)
      const lastReminder = profile.settings && profile.settings.lastReminderSentAt ? new Date(profile.settings.lastReminderSentAt).getTime() : 0;
      if ((now - lastReminder) < 7 * 24 * 60 * 60 * 1000) continue;
      // Send reminder
      const lastServiceDateStr = lastEntry ? (lastEntry.date || new Date(lastEntry.loggedAt).toLocaleDateString('en-ZA')) : 'Not on record';
      await sendEmail(profile.email, 'Your FrostFlow service is due', makeServiceReminderHtml(profile.firstName, PLAN_LABELS[profile.plan] || profile.plan, lastServiceDateStr));
      // Update lastReminderSentAt
      profile.settings = Object.assign(profile.settings || {}, { lastReminderSentAt: new Date().toISOString() });
      profile.updatedAt = new Date().toISOString();
      await writeProfile(profile.userId, profile);
      console.log('[REMINDER] Sent to', profile.email);
    }
  } catch(e) { console.error('[REMINDER cron]', e.message); }
}
// Run once at startup (after 60s for DB to be ready), then every 24h
setTimeout(runServiceReminders, 60000);
setInterval(runServiceReminders, 24 * 60 * 60 * 1000);

http.createServer((req, res) => {
  const parsed = new URL(req.url, `http://${req.headers.host}`);

  // ── Auth API ──────────────────────────────────────────────────────────────

  // POST /api/auth/register
  if (parsed.pathname === '/api/auth/register' && req.method === 'POST') {
    (async () => {
      try {
        const { firstName, lastName, email, phone, password, plan } = await parseBody(req);
        if (!firstName || !email || !password || password.length < 8)
          return jsonRes(res, 400, { error: 'Missing required fields. Password must be at least 8 characters.' });
        const norm = email.trim().toLowerCase();
        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(norm))
          return jsonRes(res, 400, { error: 'Invalid email address.' });
        // Strict uniqueness — email cannot be registered on multiple accounts
        if (await emailExists(norm))
          return jsonRes(res, 409, { error: 'An account with this email already exists. Please sign in or use a different email.' });
        const userId      = crypto.randomBytes(16).toString('hex');
        const salt        = crypto.randomBytes(32).toString('hex');
        const verifToken  = crypto.randomBytes(32).toString('hex');
        const verifExpiry = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        const profile = {
          userId, firstName: firstName.trim(), lastName: (lastName || '').trim(),
          email: norm, phone: (phone || '').trim(), plan: plan || '',
          passwordHash: hashPassword(password, salt), passwordSalt: salt,
          createdAt: new Date().toISOString(), serviceHistory: [],
          emailVerified: false, emailVerifToken: verifToken, emailVerifExpiry: verifExpiry,
          status: 'pending_verification'
        };
        await writeProfile(userId, profile);
        if (!useSupabase) { emailIndex[norm] = userId; saveEmailIndex(); }
        // Send verification email (non-blocking — registration succeeds regardless)
        const verifyUrl = `${SITE_URL}/verify-email.html?token=${verifToken}`;
        sendEmail(norm, 'Verify your FrostFlow account', makeVerifyEmailHtml(profile.firstName, verifyUrl))
          .catch(e => console.error('[register email]', e.message));
        jsonRes(res, 200, { ok: true, requiresVerification: true, firstName: profile.firstName, email: norm });
      } catch(e) {
        console.error('[register]', e.message);
        jsonRes(res, 500, { error: 'Registration failed. Please try again.' });
      }
    })();
    return;
  }

  // POST /api/auth/login
  if (parsed.pathname === '/api/auth/login' && req.method === 'POST') {
    (async () => {
      const ip = getClientIP(req);
      try {
        // Rate limit check
        const rl = checkRateLimit(ip);
        if (rl.blocked) {
          return jsonRes(res, 429, {
            error: rl.reason === 'blocked'
              ? 'Access from your IP is restricted. Contact support.'
              : `Too many failed attempts. Please try again in ${rl.remaining} minute${rl.remaining !== 1 ? 's' : ''}.`,
            errorCode: 'RATE_LIMITED', remaining: rl.remaining || 0
          });
        }
        const { email, password, rememberMe } = await parseBody(req);
        if (!email || !password) return jsonRes(res, 400, { error: 'Email and password required.' });
        const norm    = email.trim().toLowerCase();
        const profile = await findUserByEmail(norm);

        // Invalid credentials
        if (!profile || hashPassword(password, profile.passwordSalt) !== profile.passwordHash) {
          recordFailedAttempt(ip);
          const rlAfter = checkRateLimit(ip);
          const hint = rlAfter.attemptsLeft !== undefined && rlAfter.attemptsLeft <= 2
            ? ` (${rlAfter.attemptsLeft} attempt${rlAfter.attemptsLeft !== 1 ? 's' : ''} left)` : '';
          if (profile) { appendLoginHistory(profile, req, false); profile.updatedAt = new Date().toISOString(); await writeProfile(profile.userId, profile); }
          return jsonRes(res, 401, { error: 'Invalid email or password.' + hint });
        }
        if (!profile.emailVerified)
          return jsonRes(res, 403, { error: 'Please verify your email before signing in.', errorCode: 'EMAIL_NOT_VERIFIED', email: norm });

        // 2FA check
        const twoFaEnabled = profile.settings && profile.settings.twoFaEnabled;
        if (twoFaEnabled) {
          // Generate 6-digit OTP and email it
          const otp = String(Math.floor(100000 + Math.random() * 900000));
          profile.settings.twoFaOtp = { code: otp, expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() };
          profile.updatedAt = new Date().toISOString();
          await writeProfile(profile.userId, profile);
          const tempToken = create2FASession(profile.userId, norm);
          sendEmail(norm, 'Your FrostFlow sign-in code', make2FAEmailHtml(profile.firstName, otp))
            .catch(e => console.error('[2fa email]', e.message));
          clearLoginAttempts(ip);
          return jsonRes(res, 200, { ok: false, requires2FA: true, tempToken, firstName: profile.firstName });
        }

        // Success — log it and create session
        clearLoginAttempts(ip);
        appendLoginHistory(profile, req, true);
        profile.updatedAt = new Date().toISOString();
        await writeProfile(profile.userId, profile);
        const token = createSession(profile.userId, norm, rememberMe);
        jsonRes(res, 200, { ok: true, firstName: profile.firstName, email: norm, plan: profile.plan },
          { 'Set-Cookie': `ff_session=${token}; ${sessionCookieOpts(rememberMe)}` });
      } catch(e) {
        console.error('[login]', e.message);
        jsonRes(res, 500, { error: 'Login failed. Please try again.' });
      }
    })();
    return;
  }

  // GET /api/auth/verify-email?token=xxx
  if (parsed.pathname === '/api/auth/verify-email' && req.method === 'GET') {
    (async () => {
      const token = parsed.searchParams.get('token');
      if (!token || token.length !== 64) return jsonRes(res, 400, { error: 'Invalid verification token.' });
      try {
        // Find user by token — Supabase path
        let profile = null;
        if (useSupabase) {
          const { rows } = await supaFetch('clients', 'GET', null, 'email_verif_token=eq.' + token + '&select=*');
          if (rows[0]) profile = fromRow(rows[0]);
        } else {
          // File fallback: scan all profiles
          for (const uid of Object.values(emailIndex)) {
            const p = await readProfile(uid);
            if (p && p.emailVerifToken === token) { profile = p; break; }
          }
        }
        if (!profile) return jsonRes(res, 404, { error: 'Verification link is invalid or has already been used.' });
        if (profile.emailVerified) return jsonRes(res, 200, { ok: true, alreadyVerified: true, firstName: profile.firstName });
        if (profile.emailVerifExpiry && new Date(profile.emailVerifExpiry) < new Date())
          return jsonRes(res, 410, { error: 'Verification link has expired. Please request a new one.', errorCode: 'TOKEN_EXPIRED', email: profile.email });
        // Mark verified
        profile.emailVerified   = true;
        profile.emailVerifToken  = null;
        profile.emailVerifExpiry = null;
        profile.status           = 'active';
        profile.updatedAt        = new Date().toISOString();
        await writeProfile(profile.userId, profile);
        // Auto-login the user
        const sessionToken = createSession(profile.userId, profile.email);
        jsonRes(res, 200, { ok: true, firstName: profile.firstName, email: profile.email, plan: profile.plan },
          { 'Set-Cookie': `ff_session=${sessionToken}; ${SESSION_COOKIE_OPTS}` });
      } catch(e) {
        console.error('[verify-email]', e.message);
        jsonRes(res, 500, { error: 'Verification failed. Please try again.' });
      }
    })();
    return;
  }

  // POST /api/auth/resend-verification
  if (parsed.pathname === '/api/auth/resend-verification' && req.method === 'POST') {
    (async () => {
      try {
        const { email } = await parseBody(req);
        if (!email) return jsonRes(res, 400, { error: 'Email is required.' });
        const norm    = email.trim().toLowerCase();
        const profile = await findUserByEmail(norm);
        // Always return 200 to prevent email enumeration
        if (!profile || profile.emailVerified) return jsonRes(res, 200, { ok: true });
        const verifToken  = crypto.randomBytes(32).toString('hex');
        const verifExpiry = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        profile.emailVerifToken  = verifToken;
        profile.emailVerifExpiry = verifExpiry;
        profile.updatedAt        = new Date().toISOString();
        await writeProfile(profile.userId, profile);
        const verifyUrl = `${SITE_URL}/verify-email.html?token=${verifToken}`;
        sendEmail(norm, 'Verify your FrostFlow account', makeVerifyEmailHtml(profile.firstName, verifyUrl))
          .catch(e => console.error('[resend-verif email]', e.message));
        jsonRes(res, 200, { ok: true });
      } catch(e) {
        console.error('[resend-verification]', e.message);
        jsonRes(res, 500, { error: 'Could not resend verification email.' });
      }
    })();
    return;
  }

  // POST /api/auth/logout
  if (parsed.pathname === '/api/auth/logout' && req.method === 'POST') {
    const cookie = req.headers.cookie || '';
    const match  = cookie.match(/ff_session=([a-f0-9]{96})/);
    if (match && sessions[match[1]]) { delete sessions[match[1]]; saveSessions(); }
    jsonRes(res, 200, { ok: true }, { 'Set-Cookie': CLEAR_COOKIE });
    return;
  }

  // GET /api/auth/me
  if (parsed.pathname === '/api/auth/me' && req.method === 'GET') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 401, { error: 'Not authenticated.' });
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, safe);
      } catch(e) {
        jsonRes(res, 500, { error: 'Profile unavailable.' });
      }
    })();
    return;
  }

  // PUT /api/client/profile
  if (parsed.pathname === '/api/client/profile' && req.method === 'PUT') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const body    = await parseBody(req);
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        if (body.firstName !== undefined) profile.firstName = body.firstName.trim();
        if (body.lastName  !== undefined) profile.lastName  = body.lastName.trim();
        if (body.phone     !== undefined) profile.phone     = body.phone.trim();
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, safe);
      } catch(e) {
        console.error('[profile update]', e.message);
        jsonRes(res, 500, { error: 'Update failed. Please try again.' });
      }
    })();
    return;
  }

  // POST /api/client/change-password
  if (parsed.pathname === '/api/client/change-password' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { currentPassword, newPassword } = await parseBody(req);
        if (!currentPassword || !newPassword || newPassword.length < 8)
          return jsonRes(res, 400, { error: 'New password must be at least 8 characters.' });
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        if (hashPassword(currentPassword, profile.passwordSalt) !== profile.passwordHash)
          return jsonRes(res, 401, { error: 'Current password is incorrect.' });
        const newSalt = crypto.randomBytes(32).toString('hex');
        profile.passwordHash = hashPassword(newPassword, newSalt);
        profile.passwordSalt = newSalt;
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true });
      } catch(e) {
        console.error('[change-password]', e.message);
        jsonRes(res, 500, { error: 'Password change failed.' });
      }
    })();
    return;
  }

  // POST /api/client/request-cancellation
  if (parsed.pathname === '/api/client/request-cancellation' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        profile.status = 'Cancellation Pending';
        profile.cancellationRequestedAt = new Date().toISOString();
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true });
      } catch(e) {
        console.error('[request-cancellation]', e.message);
        jsonRes(res, 500, { error: 'Request failed. Please try again.' });
      }
    })();
    return;
  }

  // GET /api/client/appliances
  if (parsed.pathname === '/api/client/appliances' && req.method === 'GET') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        jsonRes(res, 200, { appliances: profile.appliances || [] });
      } catch(e) {
        jsonRes(res, 500, { error: 'Could not load appliances.' });
      }
    })();
    return;
  }

  // POST /api/client/appliances — add appliance
  if (parsed.pathname === '/api/client/appliances' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { name, model, serialNo, location } = await parseBody(req);
        if (!name) return jsonRes(res, 400, { error: 'Appliance name is required.' });
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        if (!profile.appliances) profile.appliances = [];
        const appliance = {
          id: crypto.randomBytes(8).toString('hex'),
          name: name.trim(),
          model: (model || '').trim(),
          serialNo: (serialNo || '').trim(),
          location: (location || '').trim(),
          addedAt: new Date().toISOString()
        };
        profile.appliances.push(appliance);
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true, appliance });
      } catch(e) {
        console.error('[add-appliance]', e.message);
        jsonRes(res, 500, { error: 'Could not add appliance.' });
      }
    })();
    return;
  }

  // DELETE /api/client/appliances/:id
  if (parsed.pathname.startsWith('/api/client/appliances/') && req.method === 'DELETE') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const applianceId = parsed.pathname.split('/').pop();
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        profile.appliances = (profile.appliances || []).filter(a => a.id !== applianceId);
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true });
      } catch(e) {
        jsonRes(res, 500, { error: 'Could not remove appliance.' });
      }
    })();
    return;
  }

  // GET /api/client/settings
  if (parsed.pathname === '/api/client/settings' && req.method === 'GET') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        jsonRes(res, 200, { settings: profile.settings || {} });
      } catch(e) { jsonRes(res, 500, { error: 'Could not load settings.' }); }
    })();
    return;
  }

  // PUT /api/client/settings
  if (parsed.pathname === '/api/client/settings' && req.method === 'PUT') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const body    = await parseBody(req);
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        profile.settings = Object.assign(profile.settings || {}, body.settings || {});
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, safe);
      } catch(e) {
        console.error('[settings]', e.message);
        jsonRes(res, 500, { error: 'Could not save settings.' });
      }
    })();
    return;
  }

  // POST /api/client/fault-report
  if (parsed.pathname === '/api/client/fault-report' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { description, applianceId, urgency } = await parseBody(req);
        if (!description) return jsonRes(res, 400, { error: 'Description is required.' });
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        if (!profile.faultReports) profile.faultReports = [];
        const report = {
          id: crypto.randomBytes(8).toString('hex'),
          description: description.trim(),
          applianceId: applianceId || null,
          urgency: urgency || 'normal',
          status: 'Logged',
          loggedAt: new Date().toISOString()
        };
        profile.faultReports.push(report);
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        // Send ack email to client
        sendEmail(profile.email, 'FrostFlow fault report received', makeFaultAckHtml(profile.firstName, report.id, report.urgency)).catch(e => console.error('[fault-report ack email]', e.message));
        // Notify admin
        if (ADMIN_EMAIL) sendEmail(ADMIN_EMAIL, `Fault report from ${profile.firstName}: ${urgency || 'normal'} urgency`, makeServiceBookingAdminHtml(profile, '', `FAULT: ${description}`)).catch(e => console.error('[fault-report admin email]', e.message));
        jsonRes(res, 200, { ok: true, report });
      } catch(e) {
        console.error('[fault-report]', e.message);
        jsonRes(res, 500, { error: 'Could not log fault.' });
      }
    })();
    return;
  }

  // ── Admin: Login ─────────────────────────────────────────────────────────
  if (parsed.pathname === '/api/admin/login' && req.method === 'POST') {
    (async () => {
      try {
        const { password } = await parseBody(req);
        if (!ADMIN_PASS) return jsonRes(res, 503, { error: 'Admin panel not configured. Set ADMIN_PASS environment variable.' });
        if (!password || password !== ADMIN_PASS) return jsonRes(res, 401, { error: 'Incorrect password.' });
        const token = createAdminSession();
        jsonRes(res, 200, { ok: true }, { 'Set-Cookie': `ff_admin=${token}; ${ADMIN_COOKIE_OPTS}` });
      } catch(e) { jsonRes(res, 500, { error: 'Login failed.' }); }
    })();
    return;
  }

  // ── Admin: Logout ─────────────────────────────────────────────────────────
  if (parsed.pathname === '/api/admin/logout' && req.method === 'POST') {
    const cookie = req.headers.cookie || '';
    const match  = cookie.match(/ff_admin=([a-f0-9]{64})/);
    if (match && adminSessions[match[1]]) delete adminSessions[match[1]];
    jsonRes(res, 200, { ok: true }, { 'Set-Cookie': CLEAR_ADMIN_COOKIE });
    return;
  }

  // ── Admin: Get all clients ─────────────────────────────────────────────────
  if (parsed.pathname === '/api/admin/clients' && req.method === 'GET') {
    (async () => {
      if (!getAdminSession(req)) return jsonRes(res, 401, { error: 'Admin authentication required.' });
      try {
        let clients = [];
        if (useSupabase) {
          const { rows } = await supaFetch('clients', 'GET', null, 'select=*&order=created_at.desc');
          clients = rows.map(r => { const p = fromRow(r); const { passwordHash, passwordSalt, emailVerifToken, ...safe } = p; return safe; });
        } else {
          for (const uid of Object.values(emailIndex)) {
            const p = await readProfile(uid);
            if (p) { const { passwordHash, passwordSalt, emailVerifToken, ...safe } = p; clients.push(safe); }
          }
          clients.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        }
        jsonRes(res, 200, { clients });
      } catch(e) { console.error('[admin/clients]', e.message); jsonRes(res, 500, { error: 'Could not load clients.' }); }
    })();
    return;
  }

  // ── Admin: Log a service visit ─────────────────────────────────────────────
  if (parsed.pathname === '/api/admin/service-visit' && req.method === 'POST') {
    (async () => {
      if (!getAdminSession(req)) return jsonRes(res, 401, { error: 'Admin authentication required.' });
      try {
        const { userId, type, technician, date, notes, amount } = await parseBody(req);
        if (!userId || !type || !date) return jsonRes(res, 400, { error: 'userId, type and date are required.' });
        const profile = await readProfile(userId);
        if (!profile) return jsonRes(res, 404, { error: 'Client not found.' });
        const visit = {
          id: 'SV-' + crypto.randomBytes(6).toString('hex').toUpperCase(),
          type: type.trim(),
          technician: (technician || '').trim(),
          date: date,
          notes: (notes || '').trim(),
          amount: parseFloat(amount) || 0,
          status: 'Paid',
          loggedAt: new Date().toISOString()
        };
        if (!profile.serviceHistory) profile.serviceHistory = [];
        profile.serviceHistory.push(visit);
        profile.settings = Object.assign(profile.settings || {}, { lastServiceAt: date });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(userId, profile);
        jsonRes(res, 200, { ok: true, visit });
      } catch(e) { console.error('[admin/service-visit]', e.message); jsonRes(res, 500, { error: 'Could not log visit.' }); }
    })();
    return;
  }

  // ── Admin: Update fault report status ──────────────────────────────────────
  if (parsed.pathname === '/api/admin/fault-report' && req.method === 'PUT') {
    (async () => {
      if (!getAdminSession(req)) return jsonRes(res, 401, { error: 'Admin authentication required.' });
      try {
        const { userId, reportId, status } = await parseBody(req);
        if (!userId || !reportId || !status) return jsonRes(res, 400, { error: 'userId, reportId and status are required.' });
        const validStatuses = ['Logged', 'In Progress', 'Resolved', 'Closed'];
        if (!validStatuses.includes(status)) return jsonRes(res, 400, { error: 'Invalid status value.' });
        const profile = await readProfile(userId);
        if (!profile) return jsonRes(res, 404, { error: 'Client not found.' });
        const report = (profile.faultReports || []).find(r => r.id === reportId);
        if (!report) return jsonRes(res, 404, { error: 'Fault report not found.' });
        report.status = status;
        report.updatedAt = new Date().toISOString();
        profile.updatedAt = new Date().toISOString();
        await writeProfile(userId, profile);
        jsonRes(res, 200, { ok: true, report });
      } catch(e) { console.error('[admin/fault-report]', e.message); jsonRes(res, 500, { error: 'Could not update fault report.' }); }
    })();
    return;
  }

  // ── Admin: Send reminder email manually ────────────────────────────────────
  if (parsed.pathname === '/api/admin/send-reminder' && req.method === 'POST') {
    (async () => {
      if (!getAdminSession(req)) return jsonRes(res, 401, { error: 'Admin authentication required.' });
      try {
        const { userId } = await parseBody(req);
        if (!userId) return jsonRes(res, 400, { error: 'userId required.' });
        const profile = await readProfile(userId);
        if (!profile) return jsonRes(res, 404, { error: 'Client not found.' });
        const history = (profile.serviceHistory || []).sort((a, b) => new Date(b.date||b.loggedAt||0) - new Date(a.date||a.loggedAt||0));
        const last = history[0];
        const lastDateStr = last ? (last.date || new Date(last.loggedAt).toLocaleDateString('en-ZA')) : 'Not on record';
        await sendEmail(profile.email, 'Your FrostFlow service is due', makeServiceReminderHtml(profile.firstName, PLAN_LABELS[profile.plan] || profile.plan, lastDateStr));
        profile.settings = Object.assign(profile.settings || {}, { lastReminderSentAt: new Date().toISOString() });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(userId, profile);
        jsonRes(res, 200, { ok: true });
      } catch(e) { console.error('[admin/send-reminder]', e.message); jsonRes(res, 500, { error: 'Could not send reminder.' }); }
    })();
    return;
  }

  // ── Client: Upgrade / change plan ─────────────────────────────────────────
  if (parsed.pathname === '/api/client/upgrade-plan' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { newPlan } = await parseBody(req);
        const validPlans = ['domestic', 'aircon', 'commercial'];
        if (!newPlan || !validPlans.includes(newPlan)) return jsonRes(res, 400, { error: 'Invalid plan selection.' });
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        const oldPlan = profile.plan;
        if (oldPlan === newPlan) return jsonRes(res, 400, { error: 'You are already on this plan.' });
        profile.plan = newPlan;
        profile.settings = Object.assign(profile.settings || {}, { planUpdatedAt: new Date().toISOString() });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        // Email client + admin
        sendEmail(profile.email, 'Your FrostFlow plan has been updated', makePlanUpgradeClientHtml(profile.firstName, oldPlan, newPlan)).catch(e => console.error('[upgrade-plan client email]', e.message));
        if (ADMIN_EMAIL) sendEmail(ADMIN_EMAIL, `Plan change: ${profile.firstName} → ${newPlan}`, makePlanUpgradeAdminHtml(profile, oldPlan, newPlan)).catch(e => console.error('[upgrade-plan admin email]', e.message));
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, { ok: true, profile: safe });
      } catch(e) { console.error('[upgrade-plan]', e.message); jsonRes(res, 500, { error: 'Could not update plan.' }); }
    })();
    return;
  }

  // ── Client: Book a service visit ───────────────────────────────────────────
  if (parsed.pathname === '/api/client/book-service' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { preferredDate, notes } = await parseBody(req);
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        const booking = { preferredDate: preferredDate || '', notes: (notes || '').trim(), requestedAt: new Date().toISOString(), status: 'Pending' };
        profile.settings = Object.assign(profile.settings || {}, { pendingBooking: booking });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        // Email client confirmation
        sendEmail(profile.email, 'FrostFlow service booking received', makeServiceBookingClientHtml(profile.firstName, preferredDate, notes)).catch(e => console.error('[book-service client email]', e.message));
        // Email admin notification
        if (ADMIN_EMAIL) sendEmail(ADMIN_EMAIL, `New booking: ${profile.firstName} ${profile.lastName || ''}`, makeServiceBookingAdminHtml(profile, preferredDate, notes)).catch(e => console.error('[book-service admin email]', e.message));
        jsonRes(res, 200, { ok: true, booking });
      } catch(e) { console.error('[book-service]', e.message); jsonRes(res, 500, { error: 'Could not submit booking.' }); }
    })();
    return;
  }

  // Update fault-report endpoint to also send ack email
  // POST /api/client/fault-report (enhanced — send ack email)

  // ── Google OAuth: redirect ─────────────────────────────────────────────────
  if (parsed.pathname === '/auth/google' && req.method === 'GET') {
    if (!GOOGLE_CLIENT_ID) return jsonRes(res, 503, { error: 'Google login not configured.' });
    const params = new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID, redirect_uri: GOOGLE_REDIRECT_URI,
      response_type: 'code', scope: 'openid email profile', access_type: 'online', prompt: 'select_account'
    });
    res.writeHead(302, { Location: 'https://accounts.google.com/o/oauth2/v2/auth?' + params.toString() });
    return res.end();
  }

  // ── Google OAuth: callback ─────────────────────────────────────────────────
  if (parsed.pathname === '/auth/google/callback' && req.method === 'GET') {
    const code = parsed.searchParams.get('code');
    const fail = (msg) => { res.writeHead(302, { Location: '/signin.html?error=' + encodeURIComponent(msg) }); res.end(); };
    if (!code) return fail('Google sign-in was cancelled.');
    (async () => {
      try {
        const tokens   = await exchangeGoogleCode(code);
        if (!tokens.access_token) return fail('Google sign-in failed. Please try again.');
        const gUser    = await getGoogleUserInfo(tokens.access_token);
        if (!gUser.email || !gUser.verified_email) return fail('Could not retrieve a verified Google email.');
        const norm = gUser.email.toLowerCase();
        let profile    = await findUserByEmail(norm);
        if (!profile) {
          // Auto-register via Google
          const userId = crypto.randomBytes(16).toString('hex');
          profile = {
            userId, firstName: gUser.given_name || gUser.name || 'User',
            lastName: gUser.family_name || '', email: norm, phone: '',
            plan: '', passwordHash: '', passwordSalt: '',
            createdAt: new Date().toISOString(), serviceHistory: [],
            emailVerified: true, emailVerifToken: null, emailVerifExpiry: null,
            status: 'active',
            settings: { googleId: gUser.id, profilePicture: gUser.picture || '', loginHistory: [] }
          };
          await writeProfile(userId, profile);
          if (!useSupabase) { emailIndex[norm] = userId; saveEmailIndex(); }
        } else {
          // Link Google ID
          profile.settings = Object.assign(profile.settings || {}, { googleId: gUser.id });
          if (!profile.settings.profilePicture && gUser.picture) profile.settings.profilePicture = gUser.picture;
          appendLoginHistory(profile, req, true);
          profile.emailVerified = true;
          profile.status        = profile.status === 'pending_verification' ? 'active' : profile.status;
          profile.updatedAt     = new Date().toISOString();
          await writeProfile(profile.userId, profile);
        }
        const token = createSession(profile.userId, norm, true);
        res.writeHead(302, {
          Location: '/dashboard.html',
          'Set-Cookie': `ff_session=${token}; ${sessionCookieOpts(true)}`
        });
        res.end();
      } catch(e) { console.error('[google/callback]', e.message); fail('Google sign-in failed: ' + e.message); }
    })();
    return;
  }

  // ── 2FA: verify OTP ────────────────────────────────────────────────────────
  if (parsed.pathname === '/api/auth/verify-2fa' && req.method === 'POST') {
    (async () => {
      try {
        const { tempToken, otp, rememberMe } = await parseBody(req);
        if (!tempToken || !otp) return jsonRes(res, 400, { error: 'Missing code.' });
        const session2fa = get2FASession(tempToken);
        if (!session2fa) return jsonRes(res, 401, { error: 'Code expired or invalid. Please sign in again.' });
        const profile = await readProfile(session2fa.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Account not found.' });
        const stored = profile.settings && profile.settings.twoFaOtp;
        if (!stored || new Date(stored.expiresAt) < new Date())
          return jsonRes(res, 401, { error: 'Code has expired. Please sign in again.' });
        if (stored.code !== String(otp).trim())
          return jsonRes(res, 401, { error: 'Incorrect code. Please check your email.' });
        // Clear OTP + create full session
        profile.settings.twoFaOtp = null;
        appendLoginHistory(profile, req, true);
        profile.updatedAt = new Date().toISOString();
        await writeProfile(profile.userId, profile);
        delete pending2FA[tempToken];
        const token = createSession(profile.userId, session2fa.email, rememberMe);
        clearLoginAttempts(getClientIP(req));
        jsonRes(res, 200, { ok: true, firstName: profile.firstName, email: session2fa.email, plan: profile.plan },
          { 'Set-Cookie': `ff_session=${token}; ${sessionCookieOpts(rememberMe)}` });
      } catch(e) { console.error('[verify-2fa]', e.message); jsonRes(res, 500, { error: 'Verification failed.' }); }
    })();
    return;
  }

  // ── Password reset: request ────────────────────────────────────────────────
  if (parsed.pathname === '/api/auth/forgot-password' && req.method === 'POST') {
    (async () => {
      try {
        const { email } = await parseBody(req);
        if (!email) return jsonRes(res, 400, { error: 'Email required.' });
        const norm = email.trim().toLowerCase();
        const profile = await findUserByEmail(norm);
        // Always return 200 to prevent email enumeration
        if (profile) {
          const resetToken  = crypto.randomBytes(32).toString('hex');
          const resetExpiry = new Date(Date.now() + 60 * 60 * 1000).toISOString();
          profile.settings = Object.assign(profile.settings || {}, { resetToken, resetExpiry });
          profile.updatedAt = new Date().toISOString();
          await writeProfile(profile.userId, profile);
          const resetUrl = `${SITE_URL}/reset-password.html?token=${resetToken}`;
          sendEmail(norm, 'Reset your FrostFlow password', makePasswordResetHtml(profile.firstName, resetUrl))
            .catch(e => console.error('[forgot-password email]', e.message));
        }
        jsonRes(res, 200, { ok: true, message: 'If an account with that email exists, a reset link has been sent.' });
      } catch(e) { console.error('[forgot-password]', e.message); jsonRes(res, 500, { error: 'Could not process request.' }); }
    })();
    return;
  }

  // ── Password reset: verify token ───────────────────────────────────────────
  if (parsed.pathname === '/api/auth/check-reset-token' && req.method === 'GET') {
    (async () => {
      const token = parsed.searchParams.get('token');
      if (!token || token.length !== 64) return jsonRes(res, 400, { error: 'Invalid reset link.' });
      try {
        let profile = null;
        if (useSupabase) {
          // Scan via settings — look for matching resetToken
          const { rows } = await supaFetch('clients', 'GET', null, 'select=user_id,settings,first_name');
          for (const r of rows) {
            const p = fromRow(r);
            if (p.settings && p.settings.resetToken === token) { profile = p; break; }
          }
        } else {
          for (const uid of Object.values(emailIndex)) {
            const p = await readProfile(uid);
            if (p && p.settings && p.settings.resetToken === token) { profile = p; break; }
          }
        }
        if (!profile || !profile.settings.resetToken) return jsonRes(res, 404, { error: 'Reset link is invalid or has already been used.' });
        if (new Date(profile.settings.resetExpiry) < new Date()) return jsonRes(res, 410, { error: 'Reset link has expired. Please request a new one.', errorCode: 'EXPIRED' });
        jsonRes(res, 200, { ok: true, firstName: profile.firstName });
      } catch(e) { console.error('[check-reset-token]', e.message); jsonRes(res, 500, { error: 'Could not verify link.' }); }
    })();
    return;
  }

  // ── Password reset: set new password ──────────────────────────────────────
  if (parsed.pathname === '/api/auth/reset-password' && req.method === 'POST') {
    (async () => {
      try {
        const { token, newPassword } = await parseBody(req);
        if (!token || !newPassword || newPassword.length < 8)
          return jsonRes(res, 400, { error: 'Token and a password of at least 8 characters are required.' });
        let profile = null;
        if (useSupabase) {
          const { rows } = await supaFetch('clients', 'GET', null, 'select=*');
          for (const r of rows) { const p = fromRow(r); if (p.settings && p.settings.resetToken === token) { profile = p; break; } }
        } else {
          for (const uid of Object.values(emailIndex)) { const p = await readProfile(uid); if (p && p.settings && p.settings.resetToken === token) { profile = p; break; } }
        }
        if (!profile) return jsonRes(res, 404, { error: 'Reset link is invalid or has already been used.' });
        if (new Date(profile.settings.resetExpiry) < new Date()) return jsonRes(res, 410, { error: 'Reset link has expired.' });
        const newSalt = crypto.randomBytes(32).toString('hex');
        profile.passwordHash    = hashPassword(newPassword, newSalt);
        profile.passwordSalt    = newSalt;
        profile.settings.resetToken  = null;
        profile.settings.resetExpiry = null;
        profile.updatedAt = new Date().toISOString();
        await writeProfile(profile.userId, profile);
        jsonRes(res, 200, { ok: true, message: 'Password updated. You can now sign in.' });
      } catch(e) { console.error('[reset-password]', e.message); jsonRes(res, 500, { error: 'Could not reset password.' }); }
    })();
    return;
  }

  // ── Client: upload avatar (base64 JSON) ────────────────────────────────────
  if (parsed.pathname === '/api/client/upload-avatar' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { data: b64, type } = await parseBody(req, 8 * 1024 * 1024); // 8 MB max
        const ALLOWED_IMG = ['image/jpeg','image/jpg','image/png','image/webp'];
        if (!b64 || !ALLOWED_IMG.includes(type)) return jsonRes(res, 400, { error: 'Invalid file. Please upload a JPEG, PNG, or WebP image.' });
        const raw = Buffer.from(b64, 'base64');
        if (raw.length > 5 * 1024 * 1024) return jsonRes(res, 400, { error: 'Image too large. Maximum size is 5 MB.' });
        const ext      = type.split('/')[1].replace('jpeg','jpg');
        const filename = 'avatar-' + session.userId + '.' + ext;
        const filepath = path.join(UPLOAD_DIR, filename);
        fs.writeFileSync(filepath, raw);
        const avatarUrl = '/uploads/' + filename;
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        profile.settings = Object.assign(profile.settings || {}, { profilePicture: avatarUrl });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true, avatarUrl });
      } catch(e) { console.error('[upload-avatar]', e.message); jsonRes(res, 500, { error: 'Upload failed.' }); }
    })();
    return;
  }

  // ── Client: upload file (fault photo / document) ───────────────────────────
  if (parsed.pathname === '/api/client/upload-file' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { data: b64, type, originalName } = await parseBody(req, 8 * 1024 * 1024);
        const ALLOWED = ['image/jpeg','image/jpg','image/png','image/webp','application/pdf'];
        if (!b64 || !ALLOWED.includes(type)) return jsonRes(res, 400, { error: 'Invalid file type. Allowed: JPEG, PNG, WebP, PDF.' });
        const raw = Buffer.from(b64, 'base64');
        if (raw.length > 5 * 1024 * 1024) return jsonRes(res, 400, { error: 'File too large. Maximum 5 MB.' });
        const ext      = type === 'application/pdf' ? 'pdf' : type.split('/')[1].replace('jpeg','jpg');
        const fileId   = crypto.randomBytes(16).toString('hex');
        const filename = fileId + '.' + ext;
        const filepath = path.join(UPLOAD_DIR, filename);
        fs.writeFileSync(filepath, raw);
        const url = '/uploads/' + filename;
        console.log('[UPLOAD] saved', filename, 'for', session.userId, '(' + (raw.length/1024).toFixed(1) + ' KB)');
        jsonRes(res, 200, { ok: true, url, filename, originalName: (originalName || filename).slice(0, 120) });
      } catch(e) { console.error('[upload-file]', e.message); jsonRes(res, 500, { error: 'Upload failed.' }); }
    })();
    return;
  }

  // ── Client: get activity/login log ─────────────────────────────────────────
  if (parsed.pathname === '/api/client/activity-log' && req.method === 'GET') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const profile = await readProfile(session.userId);
        const history = (profile && profile.settings && profile.settings.loginHistory) || [];
        jsonRes(res, 200, { ok: true, loginHistory: history });
      } catch(e) { jsonRes(res, 500, { error: 'Could not load activity log.' }); }
    })();
    return;
  }

  // ── Client: toggle 2FA ─────────────────────────────────────────────────────
  if (parsed.pathname === '/api/client/toggle-2fa' && req.method === 'PUT') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { enable } = await parseBody(req);
        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });
        profile.settings = Object.assign(profile.settings || {}, { twoFaEnabled: !!enable });
        profile.updatedAt = new Date().toISOString();
        await writeProfile(session.userId, profile);
        jsonRes(res, 200, { ok: true, twoFaEnabled: !!enable });
      } catch(e) { jsonRes(res, 500, { error: 'Could not update 2FA setting.' }); }
    })();
    return;
  }

  // ── Serve uploaded files ───────────────────────────────────────────────────
  if (parsed.pathname.startsWith('/uploads/') && req.method === 'GET') {
    const session = getSession(req);
    if (!session) { res.writeHead(401); return res.end('Unauthorized'); }
    const fname = path.basename(parsed.pathname);
    if (!/^[a-f0-9\-]+\.(jpg|jpeg|png|webp|pdf)$/i.test(fname)) { res.writeHead(400); return res.end('Bad request'); }
    const fpath = path.join(UPLOAD_DIR, fname);
    if (!fs.existsSync(fpath)) { res.writeHead(404); return res.end('Not found'); }
    const ext  = path.extname(fname).toLowerCase();
    const mime = { '.jpg':'image/jpeg','.jpeg':'image/jpeg','.png':'image/png','.webp':'image/webp','.pdf':'application/pdf' }[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'private, max-age=86400' });
    fs.createReadStream(fpath).pipe(res);
    return;
  }

  // ── Client: Capture Yoco charge ────────────────────────────────────────────
  // POST /api/client/yoco-charge   body: { token, amountInCents, purpose:'plan_fee'|'invoice' }
  if (parsed.pathname === '/api/client/yoco-charge' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { token, amountInCents, purpose } = await parseBody(req);
        if (!token || !amountInCents) return jsonRes(res, 400, { error: 'Missing token or amount.' });
        if (!YOCO_SECRET_KEY) return jsonRes(res, 500, { error: 'Payment processing not configured on server.' });

        // Capture with Yoco
        const charge = await yocoCapture(token, parseInt(amountInCents, 10));

        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });

        const now = new Date().toISOString();
        const amountRand = parseInt(amountInCents, 10) / 100;
        const shortRef = charge.id.slice(-8).toUpperCase();

        // Mark all existing unpaid service invoices as paid
        const history = profile.serviceHistory || [];
        history.forEach(v => { if (v.amount && !v.paid) v.paid = true; });

        // Record this payment as a history entry
        history.push({
          id: 'PAY-' + shortRef,
          type: purpose === 'plan_fee' ? 'Monthly Plan Fee' : 'Invoice Payment',
          amount: amountRand,
          paid: true,
          date: now,
          notes: 'Paid via Yoco · Ref: ' + charge.id,
          technician: ''
        });

        profile.serviceHistory = history;
        profile.settings = Object.assign(profile.settings || {}, { lastPaymentAt: now, lastPaymentRef: charge.id });
        profile.updatedAt = now;
        await writeProfile(session.userId, profile);

        console.log('[YOCO] charge captured:', charge.id, 'for', profile.email, 'R' + amountRand);
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, { ok: true, chargeId: charge.id, receiptRef: shortRef, profile: safe });
      } catch(e) {
        console.error('[yoco-charge]', e.message);
        jsonRes(res, 400, { error: e.message || 'Payment failed. Please try again or use a different card.' });
      }
    })();
    return;
  }

  // ── Client: Select plan + pay first month ─────────────────────────────────
  // POST /api/client/select-plan   body: { plan, token, amountInCents }
  if (parsed.pathname === '/api/client/select-plan' && req.method === 'POST') {
    (async () => {
      const session = getSession(req);
      if (!session) return jsonRes(res, 401, { error: 'Not authenticated.' });
      try {
        const { plan, token, amountInCents } = await parseBody(req);
        const validPlans = ['domestic', 'aircon', 'commercial'];
        if (!plan || !validPlans.includes(plan)) return jsonRes(res, 400, { error: 'Invalid plan selection.' });
        if (!token || !amountInCents) return jsonRes(res, 400, { error: 'Payment token required to activate plan.' });
        if (!YOCO_SECRET_KEY) return jsonRes(res, 500, { error: 'Payment processing not configured on server.' });

        // Capture first month payment
        const charge = await yocoCapture(token, parseInt(amountInCents, 10));

        const profile = await readProfile(session.userId);
        if (!profile) return jsonRes(res, 404, { error: 'Profile not found.' });

        const now = new Date().toISOString();
        const amountRand = parseInt(amountInCents, 10) / 100;
        const planLabel = PLAN_LABELS[plan] || plan;

        profile.plan = plan;
        if (profile.status !== 'Cancellation Pending') profile.status = 'active';
        profile.settings = Object.assign(profile.settings || {}, {
          planUpdatedAt: now,
          lastPaymentAt: now,
          lastPaymentRef: charge.id
        });
        profile.serviceHistory = [...(profile.serviceHistory || []), {
          id: 'PAY-' + charge.id.slice(-8).toUpperCase(),
          type: 'Plan Activation — ' + planLabel,
          amount: amountRand,
          paid: true,
          date: now,
          notes: 'First month payment · Yoco Ref: ' + charge.id,
          technician: ''
        }];
        profile.updatedAt = now;
        await writeProfile(session.userId, profile);

        // Emails
        sendEmail(profile.email, 'Welcome to FrostFlow — Plan Activated! 🎉', makePlanActivationClientHtml(profile.firstName, plan, amountRand))
          .catch(e => console.error('[select-plan client email]', e.message));
        if (ADMIN_EMAIL) sendEmail(ADMIN_EMAIL, `New plan activation: ${profile.firstName} ${profile.lastName||''} → ${planLabel}`, makePlanActivationAdminHtml(profile, plan))
          .catch(e => console.error('[select-plan admin email]', e.message));

        console.log('[SELECT-PLAN] activated', plan, 'for', profile.email, 'charge:', charge.id);
        const { passwordHash, passwordSalt, ...safe } = profile;
        jsonRes(res, 200, { ok: true, profile: safe });
      } catch(e) {
        console.error('[select-plan]', e.message);
        jsonRes(res, 400, { error: e.message || 'Could not activate plan. Please try again.' });
      }
    })();
    return;
  }

  // ── YouTube metadata proxy ──────────────────────────────────────────────
  if (parsed.pathname === '/api/yt') {
    const videoId = parsed.searchParams.get('v');
    if (!videoId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Missing video ID' }));
    }
    fetchYouTubeData(videoId)
      .then(data => {
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=3600',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify(data));
      })
      .catch(err => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // ── Static file serving ─────────────────────────────────────────────────
  let urlPath = decodeURIComponent(parsed.pathname);
  // Security: never serve the data directory (client passwords/sessions live here)
  if (urlPath.startsWith('/data/') || urlPath === '/data') {
    res.writeHead(403); return res.end('Forbidden');
  }
  if (urlPath.endsWith('/')) urlPath += 'index.html';
  const filePath = path.join(ROOT, urlPath);
  // Extra guard: ensure the resolved path stays within ROOT
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403); return res.end('Forbidden');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
}).listen(PORT, () => console.log(`Serving ${ROOT} on port ${PORT}`));
