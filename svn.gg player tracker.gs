/**
 * @typedef {Object} ServerInfo
 * @property {string} ip - Server IP address
 * @property {number} port - Server port number
 * @property {string} map - Current map name
 * @property {string} status - Server status
 * @property {number} players - Current player count
 * @property {number} maxPlayers - Maximum player capacity
 */

/**
 * @typedef {Object} TrackyData
 * @property {string[]} names - List of player names
 * @property {number} updatedAtMs - Last update timestamp in milliseconds
 * @property {string} source - Data source identifier
 */

/**
 * @typedef {Object} GameTrackerData
 * @property {string[]} names - List of player names
 * @property {number} updatedAtMs - Last update timestamp in milliseconds
 * @property {string} map - Current map name
 * @property {string} source - Data source identifier
 */

/**
 * Validates the configuration object
 * @param {Object} C - Configuration object
 * @throws {Error} If any required fields are missing or invalid
 */
function validateConfig_(C) {
  const required = ['SERVER_IP', 'SERVER_PORT', 'BACTA', 'DOCTOR'];
  for (const key of required) {
    if (!C[key]) throw new Error(`Missing required config: ${key}`);
  }
  if (!isFinite(C.MAX_PLAYERS_CAP) || C.MAX_PLAYERS_CAP < 1) {
    throw new Error('Invalid MAX_PLAYERS_CAP');
  }
  if (!C.TZ) throw new Error('Missing timezone configuration');
  if (!C.TS_FMT) throw new Error('Missing timestamp format configuration');
}

function updateServerSheets() {
  const C = {
    // ---- YOUR SERVER (exact match for BM) ----
    SERVER_IP: "51.161.198.234",
    SERVER_PORT: 27016,
    MAX_PLAYERS_CAP: 80, // for Tracky fallback count

    // Sources
    GT_URL: "https://www.gametracker.com/server_info/51.161.198.234:27016/",
    TS_WIDGET: "https://api.trackyserver.com/widget/index.php?id=3282714", // Tracky API (names)
    BM_SEARCH: "https://api.battlemetrics.com/servers?filter[game]=gmod&filter[search]=51.161.198.234%3A27016",

    // Sheets
    BACTA: "Bacta Data",
    DOCTOR: "Doctor Roster",

    // Bacta list + count
    NAMES_A1: "U2:U129",
    COUNT_A1: "T2",

    // Bacta header (W2..W8 = 7 lines)
    HEADER_COL: 23,        // W
    HEADER_START_ROW: 2,   // W2
    HEADER_ROWS: 7,

    // Logs region (merged W:AB). Write-only, add at top, push down.
    LOG_COL: 23,           // W
    LOG_FIRST_ROW: 9,
    LOG_LAST_ROW: 1029,

    // Elapsed panel
    ELAPSE_A1: "AD2:AF82", // Name | Start | Elapsed

    // Doctor Roster info block (3 cols wide)
    DOCTOR_INFO_A1: "AB24:AD32",

    // Doctor Roster Last Seen stamping
    DR_HEADER_ROW: 6,
    DR_FIRST_ROW: 7,
    DR_LAST_SEEN_COL: 22,  // V
    DR_STATUS_COL: 19,     // S (optional “Online”)
    DR_NAME_COL: 24,       // X
    DR_STEAMID_COL: 25,    // Y

    // Prefs
    MAX_PLAYERS: 128,
    FRESHNESS_BIAS_MS: 2 * 60 * 1000, // prefer Tracky unless GT >2m fresher (for names)
    GT_FRESH_WINDOW_MS: 10 * 60 * 1000, // GT map considered fresh if scanned within 10m
    MAP_CONFIRM_RUNS: 2,  // require BM to report the *same* new map for N consecutive runs
    TZ: "Australia/Sydney",
    TS_FMT: "yyyy-MM-dd HH:mm:ss z",

    // Persistent state keys
    PROP_STATE: "SOVR_STATE_STABLE",   // map/players/joinTimes
    PROP_MAPPEND: "SOVR_PENDING_MAP",  // {pendingMap, seenCount}

    // HTTP
    UA: "Mozilla/5.0 (AppsScript; Sovereign 2nd MedCo Sheet)",
    TIMEOUT_MS: 15000
  };

  const t0 = Date.now();
  const now = new Date();
  const nowMs = now.getTime();
  Logger.log("=== run start ===");

  // Validate configuration
  try {
    validateConfig_(C);
  } catch (e) {
    Logger.log("Configuration error: " + e.message);
    throw e;
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const bacta  = ss.getSheetByName(C.BACTA);
  const doctor = ss.getSheetByName(C.DOCTOR);
  if (!bacta || !doctor) throw new Error("Missing sheets (Bacta/Doctor)");

  // ---- BattleMetrics (authoritative facts; exact ip:port only) ----
  let bm = null;
  try {
    bm = fetchBMExact_(C.BM_SEARCH, C.UA, C.SERVER_IP, C.SERVER_PORT);
    if (bm) Logger.log(`BM OK: map=${bm.map} players=${bm.players}/${bm.maxPlayers} status=${bm.status}`);
    else Logger.log("BM: no exact ip:port match; skipping BM facts this tick.");
  } catch (e) {
    Logger.log("BM API failed: %s", e);
  }

  // ---- Names: Tracky API primary; GT freshness cross-check (names only) ----
  let tk = { names: [], updatedAtMs: nowMs, source: "Tracky API" };
  let gt  = { names: [], updatedAtMs: 0, map: "", source: "GameTracker (HTML)" };

  // Tracky API
  try {
    const j = getCachedResponse_(C.TS_WIDGET, req_(C.UA, C.TIMEOUT_MS), 1); // 1 minute cache
    tk.names = unique_(parseTrackyJSON_(j).map(clean_).filter(valid_)).slice(0, C.MAX_PLAYERS);
    tk.updatedAtMs = nowMs;
    Logger.log("Tracky names: %d", tk.names.length);
  } catch (e) {
    Logger.log("Tracky API fail: %s", e);
  }

  // GameTracker (also parse its map + "last scanned" freshness)
  let gtHtml = "";
  try {
    const response = rateLimitedFetch_(C.GT_URL, req_(C.UA, C.TIMEOUT_MS));
    if (response.getResponseCode() !== 200) throw new Error("GT HTTP " + response.getResponseCode());
    gtHtml = response.getContentText() || "";
    const p = parseGT_(gtHtml);
    gt.names = unique_(p.names.map(clean_).filter(valid_)).slice(0, C.MAX_PLAYERS);
    gt.updatedAtMs = p.updatedAtMs || 0;
    gt.map = p.map || "";
    Logger.log("GT names=%d, GT map=%s, GT freshness(ms)=%s", gt.names.length, gt.map, gt.updatedAtMs);
  } catch (e) {
    Logger.log("GT fail: %s", e);
  }

  // Pick names source by freshness (bias to Tracky)
  let picked = tk;
  if (gt.names.length && (!tk.names.length || (gt.updatedAtMs - tk.updatedAtMs) > C.FRESHNESS_BIAS_MS)) {
    picked = gt;
  }
  const names = picked.names;
  const chosenSource = picked.source + (picked === tk ? " (preferred)" : " (fresher)");

  // ---- Write names + count to Bacta ----
  bacta.getRange(C.NAMES_A1).clearContent();
  bacta.getRange(C.COUNT_A1).clearContent();
  if (names.length) {
    bacta.getRange(2, 21, names.length, 1).setValues(names.map(n => [n]));
    bacta.getRange(C.COUNT_A1).setValue(names.length);
  } else {
    bacta.getRange("U2").setValue("No players online");
    bacta.getRange(C.COUNT_A1).setValue(0);
  }

  // ---- Temporary header (Last Seen count will be filled later) ----
  const headerBase = {
    ts: Utilities.formatDate(now, C.TZ, C.TS_FMT),
    source: picked.source,
    online: isFinite(bm?.players) ? bm.players : names.length,
    range: names.length ? `U2:U${names.length + 1}` : "U2:U2",
    runMs: Date.now() - t0
  };
  setHeader_(bacta, C, headerBase, 0);

  // ---- Deltas with normalized keys ----
  const state = readState_(C.PROP_STATE) || {};
  const prevMap = state.map || "";
  const prevPlayers = Array.isArray(state.players) ? state.players : [];
  const prevSet = new Set(prevPlayers.map(normKey_));
  const joinTimes = state.joinTimes || {};

  const curNormList = names.map(normKey_);
  const curSet = new Set(curNormList);

  const joins = [];
  const leaves = [];
  for (let i = 0; i < names.length; i++) {
    const nk = curNormList[i];
    if (!prevSet.has(nk)) joins.push(names[i]);
  }
  for (const pn of prevPlayers) {
    const nk = normKey_(pn);
    if (!curSet.has(nk)) leaves.push(pn);
  }

  joins.forEach(n => { joinTimes[normKey_(n)] = nowMs; });
  const leaveLogs = leaves.map(n => {
    const nk = normKey_(n);
    const start = joinTimes[nk] || nowMs;
    delete joinTimes[nk];
    return { n, dur: nowMs - start };
  });

  // ---- MAP DECISION: BM exact match + debounce + GT veto if fresh and disagrees ----
  const pend = readState_(C.PROP_MAPPEND) || {}; // {pendingMap, seenCount}
  const decision = decideMap_(bm, gt, prevMap, pend, nowMs, C);
  const curMap = decision.map;
  const mapChanged = decision.changed;

  // persist pending info and stable state ASAP
  writeState_(C.PROP_MAPPEND, { pendingMap: decision.pendingMap || "", seenCount: decision.seenCount || 0 });
  writeState_(C.PROP_STATE, { map: curMap || prevMap, players: names, joinTimes });

  // ---- Logs (top-append, push-down; no row inserts/merges touched) ----
  pushLogsTop_(bacta, C, now, curMap, mapChanged, joins, leaveLogs);

  // ---- Elapsed panel (best effort) ----
  try { writeElapsed_(bacta, C.ELAPSE_A1, names, joinTimes, nowMs); } catch (e) { Logger.log("Elapsed block error: %s", e); }

  // ---- Doctor Roster: Server Information with BM authority but Tracky status fallback ----
  try {
    const bmOnline = !!bm && String(bm.status || "").toLowerCase() === "online" && isFinite(bm.players);
    const trackyHasPlayers = names.length > 0;
    let info;

    if (bmOnline) {
      info = {
        when: Utilities.formatDate(now, C.TZ, C.TS_FMT),
        source: "BattleMetrics (API)",
        ip: `${bm.ip}:${bm.port}`,
        map: curMap || "Unknown", // curMap already BM-confirmed/debounced
        players: `${bm.players}/${bm.maxPlayers || C.MAX_PLAYERS_CAP}`,
        status: "online"
      };
    } else if (trackyHasPlayers) {
      info = {
        when: Utilities.formatDate(now, C.TZ, C.TS_FMT),
        source: "Tracky API (fallback)",
        ip: `${C.SERVER_IP}:${C.SERVER_PORT}`,
        map: curMap || "Unknown", // don’t change without BM confirmation
        players: `${names.length}/${C.MAX_PLAYERS_CAP}`,
        status: "online"
      };
    } else {
      info = {
        when: Utilities.formatDate(now, C.TZ, C.TS_FMT),
        source: bm ? "BattleMetrics (API)" : "No exact BM match",
        ip: `${C.SERVER_IP}:${C.SERVER_PORT}`,
        map: curMap || "Unknown",
        players: `${isFinite(bm?.players) ? bm.players : 0}/${bm?.maxPlayers || C.MAX_PLAYERS_CAP}`,
        status: bm ? (bm.status || "unknown") : "unknown"
      };
    }
    writeDoctorInfo_(doctor, C.DOCTOR_INFO_A1, info);
  } catch (e) {
    Logger.log("Doctor Info block error: %s", e);
  }

  // ---- Doctor Roster: Last Seen (V) ----
  let lastSeenUpdatesCount = 0;
  try { lastSeenUpdatesCount = updateRosterLastSeen_(doctor, C, new Set(names)); } catch (e) { Logger.log("Last Seen update error: %s", e); }

  // ---- Final header with real Last Seen count ----
  setHeader_(bacta, C, headerBase, lastSeenUpdatesCount);

  Logger.log("Picked names source: %s", chosenSource);
  Logger.log("MAP decision -> prev=%s, bm=%s, gt=%s, cur=%s, changed=%s", prevMap, bm?.map || "", gt.map || "", curMap, String(mapChanged));
  Logger.log("joins=%d leaves=%d", joins.length, leaves.length);
  Logger.log("=== run end (%d ms) ===", Date.now() - t0);
}

/* ---------- MAP decision (debounced + GT veto) ---------- */
function decideMap_(bm, gt, prevMap, pend, nowMs, C) {
  const result = { map: prevMap || "", changed: false, pendingMap: pend.pendingMap || "", seenCount: Number(pend.seenCount || 0) };

  const bmMap = (bm && bm.map) ? String(bm.map).trim() : "";
  const bmOnline = !!bm && String(bm.status || "").toLowerCase() === "online";
  const gtFresh = gt && (nowMs - (gt.updatedAtMs || 0)) <= C.GT_FRESH_WINDOW_MS;
  const gtMap = (gt && gt.map) ? String(gt.map).trim() : "";

  // If BM is offline/unavailable -> keep prev, clear pending
  if (!bmOnline || !bmMap) {
    result.pendingMap = "";
    result.seenCount = 0;
    return result;
  }

  // If BM map equals previous -> accept as stable; clear pending
  if (bmMap === prevMap) {
    result.map = bmMap;
    result.pendingMap = "";
    result.seenCount = 0;
    return result;
  }

  // If GT is fresh and explicitly contradicts BM, veto this tick (don’t advance pending)
  if (gtFresh && gtMap && gtMap !== bmMap) {
    Logger.log("MAP veto: GT fresh (%s) != BM (%s). Holding prev=%s.", gtMap, bmMap, prevMap);
    return result; // keep prev; pending unchanged
  }

  // Debounce: require BM to repeat the same *new* map N times
  if (result.pendingMap === bmMap) {
    result.seenCount += 1;
  } else {
    result.pendingMap = bmMap;
    result.seenCount = 1;
  }

  if (result.seenCount >= C.MAP_CONFIRM_RUNS) {
    result.map = bmMap;
    result.changed = true;
    result.pendingMap = "";
    result.seenCount = 0;
  }

  return result;
}

/* ---------- header W2..W8 ---------- */
function setHeader_(sheet, C, base, lastSeenCount) {
  const lines = [
    `[${base.ts}] Update from ${base.source}`,
    `Source: ${base.source}`,
    `Online: ${base.online}`,
    `Range: ${base.range}`,
    `Last Seen updated: ${lastSeenCount}`,
    `Run: complete • Responded (${base.source}) in ${base.runMs} ms`,
    `${base.source} • Steam visibility`
  ];
  sheet.getRange(C.HEADER_START_ROW, C.HEADER_COL, C.HEADER_ROWS, 1).setValues(lines.map(s => [s]));
}

/* ---------- logs (top-append) ---------- */
function pushLogsTop_(sheet, C, now, curMap, mapChanged, joins, leaveLogs) {
  const lines = [];
  if (mapChanged) lines.push(`Server deployed to map: ${curMap}`);
  if (joins.length || leaveLogs.length) {
    lines.push(`[${Utilities.formatDate(now, C.TZ, C.TS_FMT)}] Changes`);
    joins.forEach(n => lines.push(`+ ${n}`));
    leaveLogs.forEach(x => lines.push(`- ${x.n}`));
  }
  if (!lines.length) return;

  const total = C.LOG_LAST_ROW - C.LOG_FIRST_ROW + 1;
  const rng = sheet.getRange(C.LOG_FIRST_ROW, C.LOG_COL, total, 1);
  const existing = rng.getValues().map(r => (r[0] || "").toString());
  const column = lines.concat(existing).slice(0, total).map(s => [s]);
  rng.setValues(column);
}

/* ---------- Doctor Roster: Last Seen ---------- */
function updateRosterLastSeen_(doctor, C, onlineNamesSet) {
  const lastRow = doctor.getLastRow();
  const startRow = Math.max(C.DR_FIRST_ROW, C.DR_HEADER_ROW + 1);
  if (lastRow < startRow) return 0;

  const numRows = lastRow - startRow + 1;
  const namesR = doctor.getRange(startRow, C.DR_NAME_COL,   numRows, 1).getValues(); // X
  const seenR  = doctor.getRange(startRow, C.DR_LAST_SEEN_COL, numRows, 1).getValues();// V
  let statusR = null;
  try { statusR = doctor.getRange(startRow, C.DR_STATUS_COL, numRows, 1).getValues(); } catch(_) {}

  const normOnline = new Set([...onlineNamesSet].map(normKey_));
  let updates = 0;
  for (let i = 0; i < numRows; i++) {
    const nameOnline = !!namesR[i][0] && normOnline.has(normKey_(namesR[i][0]));
    const statusOnline = statusR ? String(statusR[i][0] || "").trim().toLowerCase() === "online" : false;
    if (nameOnline || statusOnline) {
      seenR[i][0] = new Date();
      updates++;
    }
  }
  if (updates) doctor.getRange(startRow, C.DR_LAST_SEEN_COL, numRows, 1).setValues(seenR);
  return updates;
}

/* ---------- elapsed panel ---------- */
function writeElapsed_(sheet, a1, names, joinTimes, nowMs) {
  const rng = sheet.getRange(a1);
  rng.clearContent();
  if (!names.length) return;

  const rows = Math.min(rng.getNumRows(), names.length);
  const data = [];
  for (let i = 0; i < rows; i++) {
    const n = names[i];
    const nk = normKey_(n);
    const startMs = joinTimes[nk];
    const start = startMs ? new Date(startMs) : "";
    const elapsed = startMs ? durFmt_(nowMs - startMs) : "";
    data.push([n, start, elapsed]);
  }
  if (data.length) rng.offset(0, 0, data.length, 3).setValues(data);
}

/* ---------- writers ---------- */
function writeDoctorInfo_(sheet, a1, info) {
  const rows = [
    ["Server Information", "", ""],
    [`Last Updated - ${info.when}`, "", ""],
    [`Source: ${info.source}`, "", ""],
    [`Server IP: ${info.ip}`, "", ""],
    [`Current Map: ${info.map}`, "", ""],
    [`Players: ${info.players}`, "", ""],
    [`Server Status: ${info.status}`, "", ""],
    ["", "", ""]
  ];
  const range = sheet.getRange(a1);
  range.clearContent();
  const r = range.getRow(), c = range.getColumn();
  sheet.getRange(r, c, rows.length, 3).setValues(rows);
}

/* ---------- parsers ---------- */
function parseTrackyJSON_(json) {
  const list = Array.isArray(json?.playerslist) ? json.playerslist : [];
  const out = [];
  for (let i = 0; i < list.length && out.length < 128; i++) {
    const name = clean_(list[i]?.name);
    if (valid_(name)) out.push(name);
  }
  return out;
}

// GT HTML → { names[], updatedAtMs, map }
function parseGT_(html) {
  let updatedAtMs = 0;
  const m = (html || "").match(/LAST\s*SCANNED[^<]*?(\d+)\s*minute/i);
  if (m) updatedAtMs = Date.now() - parseInt(m[1], 10) * 60 * 1000;

  // names
  const block = one_(html, /id=["']HTML_online_players["'][^>]*>([\s\S]*?)<\/div>\s*<\/div>/i)
             || one_(html, /id=["']HTML_online_players["'][^>]*>([\s\S]*?)$/i) || "";
  let tbody = one_(block, /<table[^>]*class=["'][^"']*table_lst_stp[^"']*["'][^>]*>[\s\S]*?<tbody[^>]*>([\s\S]*?)<\/tbody>/i);
  if (!tbody) {
    const afterHdr = block || section_(html, />\s*ONLINE\s*PLAYERS\s*</i, /<\/div>/i) || "";
    tbody = one_(afterHdr, /<tbody[^>]*>([\s\S]*?)<\/tbody>/i) || "";
  }
  const names = [];
  if (tbody) {
    const rows = (tbody.match(/<tr[\s\S]*?<\/tr>/gi) || []);
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (/>[\s\r\n]*Rank[\s\r\n]*</i.test(row)) continue;
      const tds = row.match(/<td[\s\S]*?<\/td>/gi) || [];
      if (tds.length < 2) continue;
      let nm = one_(tds[1], /<a[^>]*>([\s\S]*?)<\/a>/i) || stripTags_(tds[1]);
      nm = clean_(nm);
      if (valid_(nm)) names.push(nm);
    }
  }

  // map
  let gtMap = "";
  const mapBlock = section_(html, /<b>\s*Map:\s*<\/b>/i, /<\/div>/i) || "";
  gtMap = clean_(stripTags_(mapBlock)).replace(/^Map:\s*/i, "").trim();

  return { names, updatedAtMs, map: gtMap };
}

/* ---------- BM exact match ---------- */
function fetchBMExact_(url, ua, wantIp, wantPort) {
  const r = rateLimitedFetch_(url, req_(ua, 15000));
  if (r.getResponseCode() !== 200) throw new Error("BM HTTP " + r.getResponseCode());
  const j = JSON.parse(r.getContentText() || "{}");
  const list = Array.isArray(j.data) ? j.data : [];
  const hit = list.find(d => {
    const a = d && d.attributes || {};
    return (String(a.ip || "").trim() === String(wantIp)) &&
           (Number(a.port) === Number(wantPort));
  });
  if (!hit) return null;
  const a = hit.attributes || {};
  const details = a.details || {};
  return {
    ip: a.ip || wantIp,
    port: a.port || wantPort,
    map: details.map || "",
    status: a.status || "unknown",
    players: Number(a.players) || 0,
    maxPlayers: Number(a.maxPlayers) || NaN
  };
}

/* ---------- utils ---------- */
/**
 * Rate-limited fetch with retries
 * @param {string} url - URL to fetch
 * @param {Object} options - Fetch options
 * @param {number} maxRetries - Maximum number of retries
 * @param {number} delayMs - Delay between retries in milliseconds
 * @returns {GoogleAppsScript.URL_Fetch.HTTPResponse}
 */
function rateLimitedFetch_(url, options, maxRetries = 3, delayMs = 1000) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = UrlFetchApp.fetch(url, options);
      if (response.getResponseCode() === 429) { // Too Many Requests
        Utilities.sleep(delayMs * (i + 1));
        continue;
      }
      return response;
    } catch (e) {
      if (i === maxRetries - 1) throw e;
      Utilities.sleep(delayMs * (i + 1));
    }
  }
}

/**
 * Fetch with caching
 * @param {string} url - URL to fetch
 * @param {Object} options - Fetch options
 * @param {number} cacheMinutes - Cache duration in minutes
 * @returns {Object} Parsed response data
 */
function getCachedResponse_(url, options, cacheMinutes = 5) {
  const cache = CacheService.getScriptCache();
  const cacheKey = Utilities.base64Encode(url);
  const cached = cache.get(cacheKey);
  
  if (cached) return JSON.parse(cached);
  
  const response = rateLimitedFetch_(url, options);
  const data = response.getContentText();
  cache.put(cacheKey, data, cacheMinutes * 60);
  
  return JSON.parse(data);
}

function req_(ua, timeout) {
  return { muteHttpExceptions: true, followRedirects: true, headers: { "User-Agent": ua, "Accept-Language": "en-AU,en;q=0.8" }, method: "get", timeout: timeout };
}
function one_(s, re) { const m = (s || "").match(re); return m ? m[1] : ""; }
function section_(s, startRe, endRe) { const a=(s||"").search(startRe); if(a<0) return ""; const sub=(s||"").slice(a); const b=sub.search(endRe); return b<0?sub:sub.slice(0,b); }
function stripTags_(s) { return (s || "").replace(/<[^>]+>/g, " "); }
function decode_(s) {
  let t = (s || "").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  t = t.replace(/&#(\d+);/g, (_, d) => String.fromCharCode(parseInt(d, 10)));
  t = t.replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
  return t;
}
function clean_(s) { return decode_(String(s || "").replace(/\s+/g, " ").trim()); }
function valid_(name) {
  if (!name) return false;
  if (/^(Rank|Name|Score|Time\s*Played|Online\s*players)$/i.test(name)) return false;
  if (name.length > 48) return false;
  return true;
}
function unique_(arr) { const seen = new Set(); const out = []; for (const n of arr) { const k = normKey_(n); if (!seen.has(k)) { seen.add(k); out.push(n); } } return out; }
function normKey_(s) {
  let t = (s || "").toString().trim().toLowerCase();
  t = t.replace(/^[\[\(][^\]\)]+[\]\)]\s*/,''); // strip one leading [TAG] or (TAG)
  t = t.replace(/\s+/g, ' ');
  return t;
}
function durFmt_(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), ss = s % 60;
  return (h ? (h + "h ") : "") + (m ? (m + "m ") : "") + ss + "s";
}
function readState_(k) { try { return JSON.parse(PropertiesService.getScriptProperties().getProperty(k) || "{}"); } catch (_) { return {}; } }
function writeState_(k, obj) { PropertiesService.getScriptProperties().setProperty(k, JSON.stringify(obj || {})); }
