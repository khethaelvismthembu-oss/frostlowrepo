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
    service_history: p.serviceHistory || []
  };
}
function fromRow(r) {
  return {
    userId: r.user_id, firstName: r.first_name, lastName: r.last_name,
    email: r.email, phone: r.phone, plan: r.plan,
    passwordHash: r.password_hash, passwordSalt: r.password_salt,
    createdAt: r.created_at, updatedAt: r.updated_at,
    serviceHistory: r.service_history || []
  };
}

// ── Storage functions (auto-select Supabase or file system) ──
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
}
function createSession(userId, email) {
  const token = crypto.randomBytes(48).toString('hex');
  sessions[token] = { userId, email, expires: Date.now() + 30 * 24 * 60 * 60 * 1000 };
  saveSessions();
  return token;
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

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => { body += chunk; if (body.length > 50000) { body = ''; req.destroy(); reject(new Error('Too large')); } });
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
        if (await emailExists(norm))
          return jsonRes(res, 409, { error: 'An account with this email already exists.' });
        const userId = crypto.randomBytes(16).toString('hex');
        const salt   = crypto.randomBytes(32).toString('hex');
        const profile = {
          userId, firstName: firstName.trim(), lastName: (lastName || '').trim(),
          email: norm, phone: (phone || '').trim(), plan: plan || '',
          passwordHash: hashPassword(password, salt), passwordSalt: salt,
          createdAt: new Date().toISOString(), serviceHistory: []
        };
        await writeProfile(userId, profile);
        if (!useSupabase) { emailIndex[norm] = userId; saveEmailIndex(); }
        const token = createSession(userId, norm);
        jsonRes(res, 200, { ok: true, firstName: profile.firstName, email: norm, plan: profile.plan },
          { 'Set-Cookie': `ff_session=${token}; ${SESSION_COOKIE_OPTS}` });
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
      try {
        const { email, password } = await parseBody(req);
        if (!email || !password) return jsonRes(res, 400, { error: 'Email and password required.' });
        const norm    = email.trim().toLowerCase();
        const profile = await findUserByEmail(norm);
        if (!profile || hashPassword(password, profile.passwordSalt) !== profile.passwordHash)
          return jsonRes(res, 401, { error: 'Invalid email or password.' });
        const token = createSession(profile.userId, norm);
        jsonRes(res, 200, { ok: true, firstName: profile.firstName, email: norm, plan: profile.plan },
          { 'Set-Cookie': `ff_session=${token}; ${SESSION_COOKIE_OPTS}` });
      } catch(e) {
        console.error('[login]', e.message);
        jsonRes(res, 500, { error: 'Login failed. Please try again.' });
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
        jsonRes(res, 200, { ok: true, report });
      } catch(e) {
        console.error('[fault-report]', e.message);
        jsonRes(res, 500, { error: 'Could not log fault.' });
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
