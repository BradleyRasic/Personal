/******************************************************************************
 * Sovereign Player Tracker & Roster Automator
 *
 * @version 3.0.0
 * @author 2nd Medical Regiment Command
 * @description Monitors the Sovereign GMod server, logs player activity,
 *              and updates the Doctor Roster sheet for the 2nd Medical Regiment.
 *
 * This script uses a service-oriented architecture for robustness and maintainability.
 ******************************************************************************/

/******************************************************************************
 * CONFIGURATION
 * All user-configurable settings are centralized here.
 ******************************************************************************/
const Config = {
  // Server Identification
  SERVER_IP: "51.161.198.234",
  SERVER_PORT: 27016,
  MAX_PLAYERS_CAP: 80,

  // Data Source URLs
  SOURCES: {
    GAMETRACKER: "https://www.gametracker.com/server_info/51.161.198.234:27016/",
    TRACKYSERVER: "https://api.trackyserver.com/widget/index.php?id=3282714",
    BATTLEMETRICS: "https://api.battlemetrics.com/servers?filter[game]=gmod&filter[search]=51.161.198.234%3A27016",
  },

  // Sheet & Range Definitions
  SHEETS: {
    BACTA: "Bacta Data",
    DOCTOR: "Doctor Roster",
  },
  RANGES: {
    // Bacta Sheet
    PLAYER_NAMES: "U2:U129",
    PLAYER_COUNT: "T2",
    HEADER: "W2:W8",
    LOGS: "W9:W1029",
    ELAPSED_PANEL: "AD2:AF82",
    // Doctor Roster Sheet
    DOCTOR_INFO: "AB24:AD32",
    ROSTER_NAMES: "X7:X",
    ROSTER_LAST_SEEN: "V7:V",
    ROSTER_STATUS: "S7:S",
  },

  // Operational Parameters
  TIMEZONE: "Australia/Sydney",
  TIMESTAMP_FORMAT: "yyyy-MM-dd HH:mm:ss z",
  FRESHNESS_BIAS_MS: 2 * 60 * 1000, // Prefer Tracky unless GT is >2m fresher
  GT_FRESH_WINDOW_MS: 10 * 60 * 1000, // GT map is fresh if scanned within 10m
  MAP_CONFIRM_RUNS: 2, // Require N consecutive runs for a map change

  // State & Caching Keys
  STATE_KEYS: {
    MAIN: "SOVR_STATE_STABLE_V2",
    PENDING_MAP: "SOVR_PENDING_MAP_V2",
  },
  CACHE_KEYS: {
    BATTLEMETRICS: "cache_bm",
    TRACKYSERVER: "cache_ts",
    GAMETRACKER: "cache_gt",
  },
  CACHE_TTL_SECONDS: 60, // 1 minute cache for API responses

  // HTTP & Script Behavior
  USER_AGENT: "Mozilla/5.0 (AppsScript; Sovereign 2nd MedCo Tracker/3.0)",
  HTTP_TIMEOUT_MS: 15000,
  LOCK_TIMEOUT_MS: 45000, // 45 seconds to acquire lock
};


/******************************************************************************
 * SERVICE: LogService
 * Handles structured logging, execution tracking, and metrics.
 ******************************************************************************/
const LogService = {
  // ... (Implementation from previous steps)
};


/******************************************************************************
 * SERVICE: CacheService
 * Manages caching of API responses to reduce external calls.
 ******************************************************************************/
const CacheService = {
  // ... (Implementation based on CacheManager)
};


/******************************************************************************
 * SERVICE: StateService
 * Manages persistent script state using PropertiesService.
 ******************************************************************************/
const StateService = {
  // ... (Implementation based on StateManager)
};


/******************************************************************************
 * SERVICE: ApiService
 * Handles all external API requests, parsing, and error handling.
 ******************************************************************************/
const ApiService = {
  fetchServerData: function() {
    const requests = [
      { url: Config.SOURCES.BATTLEMETRICS, key: Config.CACHE_KEYS.BATTLEMETRICS },
      { url: Config.SOURCES.TRACKYSERVER, key: Config.CACHE_KEYS.TRACKYSERVER },
      { url: Config.SOURCES.GAMETRACKER, key: Config.CACHE_KEYS.GAMETRACKER },
    ];

    const responses = UrlFetchApp.fetchAll(requests.map(r => ({
      url: r.url,
      muteHttpExceptions: true,
      headers: { "User-Agent": Config.USER_AGENT },
      timeout: Config.HTTP_TIMEOUT_MS,
    })));

    const [bmResponse, tsResponse, gtResponse] = responses;

    const battlemetrics = this._parseBattlemetrics(bmResponse);
    const trackyserver = this._parseTrackyServer(tsResponse);
    const gametracker = this._parseGameTracker(gtResponse);

    return { battlemetrics, trackyserver, gametracker };
  },

  _parseBattlemetrics: function(response) {
    LogService.markStart('parse_battlemetrics');
    try {
      if (response.getResponseCode() !== 200) {
        throw new APIError(`BattleMetrics HTTP ${response.getResponseCode()}`, 'BattleMetrics');
      }

      const data = JSON.parse(response.getContentText() || '{}');
      const list = Array.isArray(data.data) ? data.data : [];
      
      // Find exact server match
      const server = list.find(d => {
        const a = d?.attributes || {};
        return String(a.ip || '').trim() === String(Config.SERVER_IP) &&
               Number(a.port) === Number(Config.SERVER_PORT);
      });

      if (!server) {
        return { status: 'not_found' };
      }

      const attrs = server.attributes || {};
      const details = attrs.details || {};

      return {
        status: 'success',
        data: {
          ip: attrs.ip || Config.SERVER_IP,
          port: attrs.port || Config.SERVER_PORT,
          map: DataValidator.sanitizeText(details.map || ''),
          serverStatus: DataValidator.sanitizeText(attrs.status || 'unknown').toLowerCase(),
          players: Math.max(0, Number(attrs.players) || 0),
          maxPlayers: Math.max(0, Number(attrs.maxPlayers) || Config.MAX_PLAYERS_CAP),
          lastUpdate: Date.now()
        }
      };
    } catch (e) {
      LogService.error('BattleMetrics parse failed', { error: e.message });
      return { status: 'error', error: e.message };
    } finally {
      LogService.markEnd('parse_battlemetrics');
    }
  },

  _parseTrackyServer: function(response) {
    LogService.markStart('parse_trackyserver');
    try {
      if (response.getResponseCode() !== 200) {
        throw new APIError(`TrackyServer HTTP ${response.getResponseCode()}`, 'TrackyServer');
      }

      const data = JSON.parse(response.getContentText() || '{}');
      if (!Array.isArray(data.playerslist)) {
        return { status: 'invalid_data', players: [] };
      }

      const players = data.playerslist
        .map(p => DataValidator.sanitizeText(p?.name))
        .filter(name => DataValidator.isValidPlayerName(name))
        .slice(0, Config.MAX_PLAYERS_CAP);

      return {
        status: 'success',
        data: {
          players: players,
          source: 'TrackyServer API',
          timestamp: Date.now()
        }
      };
    } catch (e) {
      LogService.error('TrackyServer parse failed', { error: e.message });
      return { status: 'error', error: e.message, players: [] };
    } finally {
      LogService.markEnd('parse_trackyserver');
    }
  },

  _parseGameTracker: function(response) {
    LogService.markStart('parse_gametracker');
    try {
      if (response.getResponseCode() !== 200) {
        throw new APIError(`GameTracker HTTP ${response.getResponseCode()}`, 'GameTracker');
      }

      const html = response.getContentText() || '';
      
      // Parse last scan time
      let updatedAtMs = 0;
      const timeMatch = html.match(/LAST\s*SCANNED[^<]*?(\d+)\s*minute/i);
      if (timeMatch) {
        updatedAtMs = Date.now() - (parseInt(timeMatch[1], 10) * 60 * 1000);
      }

      // Parse player names
      const playerBlock = this._extractHTMLSection(html, 'HTML_online_players');
      const playerRows = this._extractPlayerRows(playerBlock);
      const players = playerRows
        .map(row => this._extractPlayerName(row))
        .filter(name => DataValidator.isValidPlayerName(name));

      // Parse map name
      const mapBlock = this._extractHTMLSection(html, 'Map:');
      const mapName = DataValidator.sanitizeText(this._stripTags(mapBlock))
        .replace(/^Map:\s*/i, '')
        .trim();

      return {
        status: 'success',
        data: {
          players: players,
          map: mapName,
          updatedAtMs: updatedAtMs,
          source: 'GameTracker'
        }
      };
    } catch (e) {
      LogService.error('GameTracker parse failed', { error: e.message });
      return { status: 'error', error: e.message, players: [] };
    } finally {
      LogService.markEnd('parse_gametracker');
    }
  },

  // HTML parsing helpers
  _extractHTMLSection: function(html, identifier) {
    const sectionRegex = new RegExp(`id=["']${identifier}["'][^>]*>([\\s\\S]*?)<\\/div>`);
    const match = html.match(sectionRegex);
    return match ? match[1] : '';
  },

  _extractPlayerRows: function(block) {
    const rows = block.match(/<tr[\\s\\S]*?<\\/tr>/gi) || [];
    return rows.filter(row => !/>\\s*Rank\\s*</i.test(row));
  },

  _extractPlayerName: function(row) {
    const cells = row.match(/<td[\\s\\S]*?<\\/td>/gi) || [];
    if (cells.length < 2) return '';
    
    const nameCell = cells[1];
    const nameMatch = nameCell.match(/<a[^>]*>([\\s\\S]*?)<\\/a>/i);
    return DataValidator.sanitizeText(nameMatch ? nameMatch[1] : this._stripTags(nameCell));
  },

  _stripTags: function(html) {
    return (html || '').replace(/<[^>]+>/g, ' ').replace(/\\s+/g, ' ').trim();
  },
};


/******************************************************************************
 * SERVICE: SheetService
 * Encapsulates all interactions with the Google Sheet.
 ******************************************************************************/
const SheetService = {
  bactaSheet: null,
  doctorSheet: null,

  init: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    this.bactaSheet = ss.getSheetByName(Config.SHEETS.BACTA);
    this.doctorSheet = ss.getSheetByName(Config.SHEETS.DOCTOR);
    if (!this.bactaSheet || !this.doctorSheet) {
      throw new Error("Missing required sheets: Bacta Data or Doctor Roster");
    }
  },

  updatePlayerList: function(playerNames) {
    LogService.markStart('update_player_list');
    try {
      // Clear existing content
      this.bactaSheet.getRange(Config.RANGES.PLAYER_NAMES).clearContent();
      this.bactaSheet.getRange(Config.RANGES.PLAYER_COUNT).clearContent();

      if (!playerNames.length) {
        this.bactaSheet.getRange('U2').setValue('No players online');
        this.bactaSheet.getRange(Config.RANGES.PLAYER_COUNT).setValue(0);
        return;
      }

      // Write player names and count in batch
      const writeOperations = [{
        range: `U2:U${playerNames.length + 1}`,
        values: playerNames.map(name => [name])
      }, {
        range: Config.RANGES.PLAYER_COUNT,
        values: [[playerNames.length]]
      }];

      this._batchWrite(writeOperations);
      LogService.info('Updated player list', { count: playerNames.length });
    } catch (e) {
      LogService.error('Player list update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_player_list');
    }
  },

  updateHeader: function(headerData) {
    LogService.markStart('update_header');
    try {
      const lines = [
        `[${headerData.timestamp}] Update from ${headerData.source}`,
        `Source: ${headerData.source}`,
        `Online: ${headerData.playerCount}`,
        `Range: ${headerData.range}`,
        `Last Seen updated: ${headerData.lastSeenCount}`,
        `Run: ${headerData.status} • Response time: ${headerData.responseTime}ms`,
        `${headerData.source} • Steam visibility`
      ];

      const headerRange = this.bactaSheet.getRange(Config.RANGES.HEADER);
      headerRange.setValues(lines.map(line => [line]));
      
      LogService.info('Updated header');
    } catch (e) {
      LogService.error('Header update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_header');
    }
  },

  updateLogs: function(logEntries) {
    LogService.markStart('update_logs');
    try {
      if (!logEntries.length) return;

      const logRange = this.bactaSheet.getRange(Config.RANGES.LOGS);
      const existing = logRange.getValues().map(row => row[0]?.toString() || '');
      
      // Combine new logs with existing, maintaining size limit
      const combinedLogs = [...logEntries, ...existing]
        .slice(0, Config.LOG_MAX_ENTRIES)
        .map(log => [log]);

      logRange.setValues(combinedLogs);
      
      LogService.info('Updated logs', { newEntries: logEntries.length });
    } catch (e) {
      LogService.error('Log update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_logs');
    }
  },

  updateElapsedPanel: function(elapsedData) {
    LogService.markStart('update_elapsed');
    try {
      const range = this.bactaSheet.getRange(Config.RANGES.ELAPSED_PANEL);
      range.clearContent();

      if (!elapsedData.length) return;

      const data = elapsedData.map(entry => [
        entry.name,
        entry.joinTime ? new Date(entry.joinTime) : '',
        entry.elapsed || ''
      ]);

      range.offset(0, 0, data.length, 3).setValues(data);
      
      LogService.info('Updated elapsed panel', { entries: data.length });
    } catch (e) {
      LogService.error('Elapsed panel update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_elapsed');
    }
  },

  updateDoctorInfo: function(info) {
    LogService.markStart('update_doctor_info');
    try {
      const rows = [
        ['Server Information', '', ''],
        [`Last Updated - ${info.timestamp}`, '', ''],
        [`Source: ${info.source}`, '', ''],
        [`Server IP: ${info.ip}`, '', ''],
        [`Current Map: ${info.map}`, '', ''],
        [`Players: ${info.players}`, '', ''],
        [`Server Status: ${info.status}`, '', ''],
        ['', '', '']
      ];

      const range = this.doctorSheet.getRange(Config.RANGES.DOCTOR_INFO);
      range.setValues(rows);
      
      LogService.info('Updated doctor info');
    } catch (e) {
      LogService.error('Doctor info update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_doctor_info');
    }
  },

  updateLastSeen: function(onlinePlayers) {
    LogService.markStart('update_last_seen');
    try {
      const lastRow = this.doctorSheet.getLastRow();
      if (lastRow < Config.ROSTER_START_ROW) return 0;

      const numRows = lastRow - Config.ROSTER_START_ROW + 1;
      
      // Get current roster data
      const [names, status] = this._batchRead([
        { range: `${Config.RANGES.ROSTER_NAMES}${lastRow}` },
        { range: `${Config.RANGES.ROSTER_STATUS}${lastRow}` }
      ]);

      // Create normalized sets for quick lookups
      const normalizedOnline = new Set(onlinePlayers.map(name => 
        DataValidator.normalize(name)
      ));

      // Process updates
      const updates = [];
      let updateCount = 0;

      for (let i = 0; i < numRows; i++) {
        const name = names[i][0];
        const isOnline = name && normalizedOnline.has(DataValidator.normalize(name));
        const hasOnlineStatus = status[i][0]?.toString().toLowerCase() === 'online';

        if (isOnline || hasOnlineStatus) {
          updates.push(i + Config.ROSTER_START_ROW);
          updateCount++;
        }
      }

      // Batch update last seen timestamps
      if (updates.length) {
        const now = new Date();
        const updateRanges = updates.map(row => ({
          range: `${Config.RANGES.ROSTER_LAST_SEEN}${row}`,
          values: [[now]]
        }));
        this._batchWrite(updateRanges);
      }

      LogService.info('Updated last seen timestamps', { count: updateCount });
      return updateCount;
    } catch (e) {
      LogService.error('Last seen update failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('update_last_seen');
    }
  },

  _batchWrite: function(operations) {
    const ranges = [];
    const valuesList = [];
    
    operations.forEach(op => {
      ranges.push(this.bactaSheet.getRange(op.range));
      valuesList.push(op.values);
    });

    if (ranges.length) {
      SpreadsheetApp.getActiveSpreadsheet()
        .getRangeList(ranges.map(r => r.getA1Notation()))
        .setValues(valuesList);
    }
  },

  _batchRead: function(operations) {
    return operations.map(op => 
      this.bactaSheet.getRange(op.range).getValues()
    );
  },
},

const DataProcessor = {
  sanitizePlayers: function(rawPlayers) {
    LogService.markStart('sanitize_players');
    try {
      return rawPlayers
        .filter(player => DataValidator.isValidPlayerName(player))
        .map(player => DataValidator.sanitizeName(player))
        .sort((a, b) => a.localeCompare(b));
    } finally {
      LogService.markEnd('sanitize_players');
    }
  },

  processApiResponse: function(response, source) {
    LogService.markStart('process_api_response');
    try {
      const result = {
        players: [],
        serverInfo: {
          source: source,
          timestamp: new Date().toISOString(),
          status: 'OK',
          ip: '',
          map: '',
          players: '0/0',
          responseTime: response.responseTime || 0
        },
        isError: false
      };

      if (!response || response.error) {
        result.isError = true;
        result.serverInfo.status = 'ERROR';
        return result;
      }

      // Extract common server info
      if (response.serverInfo) {
        result.serverInfo = {
          ...result.serverInfo,
          ...response.serverInfo
        };
      }

      // Process players based on source format
      switch(source) {
        case 'BattleMetrics':
          result.players = this._processBattleMetricsPlayers(response);
          break;
        case 'TrackyServer':
          result.players = this._processTrackyServerPlayers(response);
          break;
        case 'GameTracker':
          result.players = this._processGameTrackerPlayers(response);
          break;
        default:
          LogService.warn('Unknown source type', { source });
          result.players = [];
      }

      result.players = this.sanitizePlayers(result.players);
      
      LogService.info('Processed API response', {
        source,
        playerCount: result.players.length,
        status: result.serverInfo.status
      });

      return result;
    } catch (e) {
      LogService.error('API response processing failed', {
        error: e.message,
        source
      });
      throw e;
    } finally {
      LogService.markEnd('process_api_response');
    }
  },

  calculateElapsedTimes: function(players, previousElapsed) {
    LogService.markStart('calculate_elapsed');
    try {
      const now = new Date();
      const result = new Map();

      // Process previous elapsed times
      if (previousElapsed) {
        previousElapsed.forEach(entry => {
          if (!entry.name || !entry.joinTime) return;
          result.set(DataValidator.normalize(entry.name), {
            name: entry.name,
            joinTime: entry.joinTime,
            elapsed: entry.elapsed
          });
        });
      }

      // Update with current players
      players.forEach(player => {
        const normalized = DataValidator.normalize(player);
        if (!result.has(normalized)) {
          result.set(normalized, {
            name: player,
            joinTime: now,
            elapsed: '0m'
          });
        }
      });

      // Calculate elapsed times and clean inactive players
      const activeResults = [];
      result.forEach((entry, normalized) => {
        if (players.some(p => DataValidator.normalize(p) === normalized)) {
          const elapsed = Math.floor((now - new Date(entry.joinTime)) / 60000);
          entry.elapsed = this._formatElapsedTime(elapsed);
          activeResults.push(entry);
        }
      });

      LogService.info('Calculated elapsed times', {
        players: activeResults.length
      });

      return activeResults.sort((a, b) => 
        new Date(b.joinTime) - new Date(a.joinTime)
      );
    } finally {
      LogService.markEnd('calculate_elapsed');
    }
  },

  _processBattleMetricsPlayers: function(response) {
    return response.players || [];
  },

  _processTrackyServerPlayers: function(response) {
    return response.players?.map(p => p.name) || [];
  },

  _processGameTrackerPlayers: function(response) {
    return response.players || [];
  },

  _formatElapsedTime: function(minutes) {
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    return `${hours}h${remainingMinutes}m`;
  }
},

const StateManager = {
  CACHE_KEYS: {
    LAST_RUN: 'last_run_timestamp',
    ELAPSED_TIMES: 'player_elapsed_times',
    API_ERRORS: 'api_error_count',
    RATE_LIMITS: 'rate_limit_status',
    EXECUTION_LOCK: 'execution_lock',
    MEMORY_STATS: 'memory_stats',
    API_CALLS: 'api_call_count',
    SHEET_OPS: 'sheet_op_count'
  },

  CACHE_TTL: {
    SHORT: 60, // 1 minute
    MEDIUM: 300, // 5 minutes
    LONG: 3600, // 1 hour
    VERY_LONG: 86400, // 24 hours
    API_WINDOW: 60, // 1 minute for rate limiting
    MEMORY_STATS: 300 // 5 minutes
  },

  // Rate limiting windows
  _apiCalls: new Map(),
  _sheetOps: new Map(),
  _memoryStats: {
    lastCheck: 0,
    peakUsage: 0,
    warningCount: 0
  },

  initialize: function() {
    LogService.markStart('state_init');
    try {
      this.cache = CacheService.getScriptCache();
      this._cleanStaleData();
      LogService.info('State manager initialized');
    } catch (e) {
      LogService.error('State initialization failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('state_init');
    }
  },

  acquireLock: function() {
    LogService.markStart('acquire_lock');
    try {
      const now = new Date().getTime();
      const lockExpiry = now + (this.CACHE_TTL.SHORT * 1000);
      
      // Attempt to acquire lock
      const success = this.cache.putIfAbsent(
        this.CACHE_KEYS.EXECUTION_LOCK,
        lockExpiry.toString(),
        this.CACHE_TTL.SHORT
      );

      if (!success) {
        // Check if existing lock is stale
        const currentLock = this.cache.get(this.CACHE_KEYS.EXECUTION_LOCK);
        if (currentLock && parseInt(currentLock) < now) {
          // Force acquire stale lock
          this.cache.put(
            this.CACHE_KEYS.EXECUTION_LOCK,
            lockExpiry.toString(),
            this.CACHE_TTL.SHORT
          );
          LogService.warn('Acquired stale lock');
          return true;
        }
        return false;
      }

      LogService.info('Lock acquired');
      return true;
    } catch (e) {
      LogService.error('Lock acquisition failed', { error: e.message });
      return false;
    } finally {
      LogService.markEnd('acquire_lock');
    }
  },

  releaseLock: function() {
    LogService.markStart('release_lock');
    try {
      this.cache.remove(this.CACHE_KEYS.EXECUTION_LOCK);
      LogService.info('Lock released');
    } catch (e) {
      LogService.error('Lock release failed', { error: e.message });
    } finally {
      LogService.markEnd('release_lock');
    }
  },

  updateLastRun: function() {
    this.cache.put(
      this.CACHE_KEYS.LAST_RUN,
      new Date().toISOString(),
      this.CACHE_TTL.LONG
    );
  },

  getLastRun: function() {
    return this.cache.get(this.CACHE_KEYS.LAST_RUN);
  },

  cacheElapsedTimes: function(elapsedData) {
    if (!elapsedData || !elapsedData.length) return;
    
    this.cache.put(
      this.CACHE_KEYS.ELAPSED_TIMES,
      JSON.stringify(elapsedData),
      this.CACHE_TTL.MEDIUM
    );
  },

  getElapsedTimes: function() {
    const cached = this.cache.get(this.CACHE_KEYS.ELAPSED_TIMES);
    return cached ? JSON.parse(cached) : null;
  },

  trackApiError: function(source) {
    const key = `${this.CACHE_KEYS.API_ERRORS}_${source}`;
    const current = parseInt(this.cache.get(key) || '0');
    
    this.cache.put(
      key,
      (current + 1).toString(),
      this.CACHE_TTL.LONG
    );

    // Check for rate limiting
    if (current + 1 >= Config.API_ERROR_THRESHOLD) {
      this.setRateLimit(source);
    }
  },

  isRateLimited: function(source) {
    const key = `${this.CACHE_KEYS.RATE_LIMITS}_${source}`;
    return !!this.cache.get(key);
  },

  setRateLimit: function(source) {
    const key = `${this.CACHE_KEYS.RATE_LIMITS}_${source}`;
    this.cache.put(key, 'true', this.CACHE_TTL.VERY_LONG);
    LogService.warn('Rate limit applied', { source });
  },

  _cleanStaleData: function() {
    // Remove stale locks
    const currentLock = this.cache.get(this.CACHE_KEYS.EXECUTION_LOCK);
    if (currentLock && parseInt(currentLock) < new Date().getTime()) {
      this.cache.remove(this.CACHE_KEYS.EXECUTION_LOCK);
    }
  },

  clearAll: function() {
    LogService.markStart('clear_state');
    try {
      Object.values(this.CACHE_KEYS).forEach(key => {
        this.cache.remove(key);
      });
      this._apiCalls.clear();
      this._sheetOps.clear();
      this._memoryStats = {
        lastCheck: 0,
        peakUsage: 0,
        warningCount: 0
      };
      LogService.info('Cleared all state data');
    } catch (e) {
      LogService.error('State clear failed', { error: e.message });
    } finally {
      LogService.markEnd('clear_state');
    }
  },

  checkMemoryUsage: function() {
    const now = Date.now();
    if (now - this._memoryStats.lastCheck < 5000) return; // Check every 5 seconds

    try {
      const usage = this._getCurrentMemoryUsage();
      this._memoryStats.lastCheck = now;
      this._memoryStats.peakUsage = Math.max(this._memoryStats.peakUsage, usage);

      // Check thresholds
      if (usage > Config.MEMORY.CRITICAL_THRESHOLD_MB) {
        this._handleCriticalMemory();
      } else if (usage > Config.MEMORY.WARNING_THRESHOLD_MB) {
        this._handleMemoryWarning();
      } else if (usage > Config.MEMORY.GC_THRESHOLD_MB) {
        this._triggerGarbageCollection();
      }

      // Update memory stats in cache
      this.cache.put(
        this.CACHE_KEYS.MEMORY_STATS,
        JSON.stringify(this._memoryStats),
        this.CACHE_TTL.MEMORY_STATS
      );
    } catch (e) {
      LogService.error('Memory check failed', { error: e.message });
    }
  },

  _getCurrentMemoryUsage: function() {
    try {
      return Math.round(
        (process.memoryUsage().heapUsed / 1024 / 1024) * 100
      ) / 100;
    } catch (e) {
      return 0; // Best effort
    }
  },

  _handleCriticalMemory: function() {
    LogService.error('Critical memory usage detected', {
      usage: this._memoryStats.peakUsage + 'MB'
    });

    // Emergency cleanup
    this.clearAll();
    this._triggerGarbageCollection();

    // Notify if multiple warnings
    if (++this._memoryStats.warningCount >= 3) {
      this._notifyMemoryIssue();
    }
  },

  _handleMemoryWarning: function() {
    LogService.warn('High memory usage', {
      usage: this._memoryStats.peakUsage + 'MB'
    });

    // Cleanup old data
    this._cleanupOldCache();
    this._triggerGarbageCollection();
  },

  _triggerGarbageCollection: function() {
    try {
      // Clear temporary objects
      this._apiCalls = new Map();
      this._sheetOps = new Map();
      
      // Force garbage collection if available
      if (global.gc) {
        global.gc();
      }
    } catch (e) {
      // Ignore GC errors
    }
  },

  _cleanupOldCache: function() {
    // Remove old elapsed times
    const elapsedTimes = this.getElapsedTimes() || [];
    if (elapsedTimes.length > 50) {
      this.cacheElapsedTimes(elapsedTimes.slice(0, 50));
    }

    // Clear old rate limiting data
    const now = Date.now();
    const windowMs = Config.RATE_LIMITS.API_CALLS.WINDOW_MS;
    
    this._apiCalls.forEach((timestamp, key) => {
      if (now - timestamp > windowMs) {
        this._apiCalls.delete(key);
      }
    });

    this._sheetOps.forEach((timestamp, key) => {
      if (now - timestamp > windowMs) {
        this._sheetOps.delete(key);
      }
    });
  },

  _notifyMemoryIssue: function() {
    const ui = SpreadsheetApp.getUi();
    ui.alert(
      'Memory Warning',
      'The script is experiencing high memory usage. Consider reducing data volume or increasing cleanup frequency.',
      ui.ButtonSet.OK
    );
  }
},

const Config = {
  // Sheet Configuration
  SHEET_NAMES: {
    BACTA: 'Bacta Data',
    DOCTOR: 'Doctor Roster'
  },

  RANGES: {
    // BACTA Sheet
    PLAYER_NAMES: 'U2:U100',
    PLAYER_COUNT: 'T2',
    HEADER: 'V2:V8',
    LOGS: 'W2:W50',
    ELAPSED_PANEL: 'X2:Z50',

    // Doctor Sheet
    DOCTOR_INFO: 'A1:C8',
    ROSTER_NAMES: 'A',
    ROSTER_STATUS: 'E',
    ROSTER_LAST_SEEN: 'G'
  },

  // API Configuration
  API_ENDPOINTS: {
    BATTLEMETRICS: 'https://api.battlemetrics.com/servers/',
    TRACKYSERVER: 'https://api.trackyserver.com/server/',
    GAMETRACKER: 'https://api.gametracker.com/server/'
  },

  API_CONFIG: {
    BATTLEMETRICS: {
      SERVER_ID: '5584777',
      RATE_LIMIT: 30, // requests per minute
      TIMEOUT: 10000 // 10 seconds
    },
    TRACKYSERVER: {
      SERVER_ID: 'swrp-bacta',
      RATE_LIMIT: 60,
      TIMEOUT: 8000
    },
    GAMETRACKER: {
      SERVER_ID: '74.91.124.165:27015',
      RATE_LIMIT: 30,
      TIMEOUT: 12000
    }
  },

  // Application Settings
  LOG_MAX_ENTRIES: 50,
  ROSTER_START_ROW: 2,
  API_ERROR_THRESHOLD: 5,
  MAX_RETRY_ATTEMPTS: 3,
  RETRY_DELAY: 1000, // 1 second

  // Execution Settings
  TRIGGER_INTERVALS: {
    PRIMARY: 5, // minutes
    BACKUP: 10, // minutes
    CLEANUP: 60 // minutes
  },

  // Validation Rules
  VALIDATION: {
    INVALID_CHARS: /[<>\\\/\{\}\[\]\(\)]/,
    NAME_PATTERN: /^[\w\s\-\.\(\)\[\]\{\}]+$/i  // Updated to allow more special characters
  },

  // HTML Parser Configuration
  HTML_PARSER: {
    PLAYER_TABLE: {
      TAG: 'table',
      CLASS: 'players-table'
    },
    PLAYER_ROW: {
      TAG: 'tr',
      CLASS: 'player-row'
    },
    NAME_CELL: {
      TAG: 'td',
      CLASS: 'name-cell'
    }
  },

  // Cache Configuration
  CACHE: {
    PREFIX: 'svn_gg_',
    DEFAULT_TTL: 300, // 5 minutes
    MAX_SIZE: 100000 // 100KB
  },

  // Logging Configuration
  LOG_LEVELS: {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3
  },

  LOG_RETENTION_DAYS: 7,
  
  // Feature Flags
  FEATURES: {
    USE_BACKUP_API: true,
    ENABLE_RATE_LIMITING: true,
    VERBOSE_LOGGING: false,
    AUTO_RETRY: true
  },

  // Server Information
  SERVER_INFO: {
    NAME: 'Imperial 2nd Medical Regiment',
    GAME: 'Garry\'s Mod',
    GAMEMODE: 'Star Wars RP',
    REGION: 'US East',
    WEBSITE: 'https://steamcommunity.com/groups/I2MR'
  },

  // System Constants
  SYSTEM: {
    VERSION: '2.0.0',
    AUTHOR: 'Bradley Rasic',
    LAST_UPDATED: '2025-10-20',
    SUPPORT_EMAIL: 'bradleyrasic@gmail.com'
  },

  getApiConfig: function(source) {
    return this.API_CONFIG[source.toUpperCase()] || null;
  },

  isFeatureEnabled: function(feature) {
    return !!this.FEATURES[feature];
  },

  getLogLevel: function() {
    return this.FEATURES.VERBOSE_LOGGING ? this.LOG_LEVELS.DEBUG : this.LOG_LEVELS.INFO;
  },

  getRetryConfig: function() {
    return {
      enabled: this.FEATURES.AUTO_RETRY,
      maxAttempts: this.MAX_RETRY_ATTEMPTS,
      delay: this.RETRY_DELAY
    };
  },

  getCacheKey: function(key) {
    return this.CACHE.PREFIX + key;
  }
},

const DataValidator = {
  ValidationRules: {
    PLAYER_NAME: {
      minLength: 2,
      maxLength: 32,
      pattern: /^[\w\s\-\.\(\)\[\]\{\}]+$/i,
      invalidChars: /[<>\\\/\{\}\[\]\(\)]/,
      reservedWords: ['server', 'admin', 'console', 'system']
    },
    MAP_NAME: {
      minLength: 1,
      maxLength: 64,
      pattern: /^[\w\s\-\.]+$/i
    },
    SERVER_STATUS: {
      validStates: ['online', 'offline', 'restarting', 'changing']
    }
  },

  isValidPlayerName: function(name) {
    if (!name || typeof name !== 'string') return false;
    
    const rules = this.ValidationRules.PLAYER_NAME;
    const trimmed = name.trim();
    
    // Basic validations
    if (!trimmed) return false;
    if (trimmed.length < rules.minLength || trimmed.length > rules.maxLength) return false;
    if (rules.invalidChars.test(trimmed)) return false;
    if (!rules.pattern.test(trimmed)) return false;
    
    // Check for reserved words
    const normalized = trimmed.toLowerCase();
    if (rules.reservedWords.includes(normalized)) return false;
    
    // Check for excessive special characters
    const specialCharCount = (trimmed.match(/[^a-zA-Z0-9\s]/g) || []).length;
    if (specialCharCount > trimmed.length / 3) return false;
    
    return true;
  },

  sanitizeName: function(name) {
    if (!name) return '';

    // Basic sanitization
    let sanitized = name.trim()
      .replace(/\s+/g, ' ')        // Normalize whitespace
      .replace(/[\x00-\x1F]/g, '') // Remove control characters
      .replace(/&/g, '&amp;')      // Encode HTML entities
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');

    return sanitized;
  },

  normalize: function(name) {
    if (!name) return '';
    
    // Create a normalized version for comparison
    return name.trim()
      .toLowerCase()
      .replace(/\s+/g, '')
      .replace(/[^a-z0-9]/g, '');
  },

  validateApiResponse: function(response, source) {
    if (!response) {
      throw new Error(`Empty response from ${source}`);
    }

    if (response.error) {
      throw new Error(`API error from ${source}: ${response.error}`);
    }

    if (!response.players && !Array.isArray(response.players)) {
      throw new Error(`Invalid players data from ${source}`);
    }

    return true;
  },

  sanitizeServerInfo: function(info) {
    const sanitized = {};
    
    // Only allow specific fields with appropriate types
    const allowedFields = {
      ip: 'string',
      map: 'string',
      players: 'string',
      status: 'string',
      name: 'string',
      timestamp: 'string'
    };

    Object.keys(allowedFields).forEach(field => {
      if (info[field] !== undefined) {
        const value = info[field];
        if (typeof value === allowedFields[field]) {
          sanitized[field] = this.sanitizeString(value);
        }
      }
    });

    return sanitized;
  },

  sanitizeString: function(str) {
    if (typeof str !== 'string') return '';
    
    return str.trim()
      .replace(/[\x00-\x1F]/g, '')  // Remove control characters
      .replace(/&/g, '&amp;')       // Encode HTML entities
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  },

  isValidDate: function(date) {
    if (!(date instanceof Date) && typeof date !== 'string') return false;
    
    const timestamp = new Date(date).getTime();
    return !isNaN(timestamp);
  },

  validateRange: function(range) {
    if (!range || typeof range !== 'string') return false;
    
    // Basic A1 notation validation
    const a1Pattern = /^[A-Z]+[0-9]+(:[A-Z]+[0-9]+)?$/;
    return a1Pattern.test(range);
  }
},

const LogService = {
  _buffer: [],
  _timers: new Map(),
  _maxBufferSize: 100,

  debug: function(message, data = {}) {
    if (Config.getLogLevel() <= Config.LOG_LEVELS.DEBUG) {
      this._log('DEBUG', message, data);
    }
  },

  info: function(message, data = {}) {
    if (Config.getLogLevel() <= Config.LOG_LEVELS.INFO) {
      this._log('INFO', message, data);
    }
  },

  warn: function(message, data = {}) {
    if (Config.getLogLevel() <= Config.LOG_LEVELS.WARN) {
      this._log('WARN', message, data);
    }
  },

  error: function(message, data = {}) {
    this._log('ERROR', message, data);
    
    // For errors, we also log to Apps Script's native logger
    console.error(message, data);
  },

  markStart: function(operation) {
    this._timers.set(operation, new Date().getTime());
  },

  markEnd: function(operation) {
    const startTime = this._timers.get(operation);
    if (startTime) {
      const duration = new Date().getTime() - startTime;
      this._timers.delete(operation);
      this.debug(`${operation} completed`, { duration: `${duration}ms` });
    }
  },

  flush: function() {
    if (!this._buffer.length) return;

    try {
      const sheet = SpreadsheetApp.getActive().getSheetByName('System Logs');
      if (!sheet) return;

      const now = new Date();
      const logs = this._buffer.map(entry => [
        now,
        entry.level,
        entry.message,
        JSON.stringify(entry.data)
      ]);

      // Append new logs
      sheet.getRange(sheet.getLastRow() + 1, 1, logs.length, 4)
        .setValues(logs);

      // Clear old logs beyond retention period
      this._cleanOldLogs(sheet);
    } catch (e) {
      console.error('Failed to flush logs:', e);
    } finally {
      this._buffer = [];
    }
  },

  _log: function(level, message, data) {
    const sanitizedMessage = Utils.sanitizeLogOutput(message);
    if (sanitizedMessage === null) return; // Skip logging if message was sanitized out

    this._buffer.push({
      timestamp: new Date(),
      level: level,
      message: sanitizedMessage,
      data: Utils.isEmptyObject(data) ? {} : data
    });

    // Auto-flush if buffer gets too large
    if (this._buffer.length >= this._maxBufferSize) {
      this.flush();
    }
  },

  _cleanOldLogs: function(sheet) {
    const retention = Config.LOG_RETENTION_DAYS;
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - retention);

    const values = sheet.getDataRange().getValues();
    let deleteCount = 0;

    // Find how many rows to delete
    for (let i = 1; i < values.length; i++) {
      if (new Date(values[i][0]) < cutoff) {
        deleteCount++;
      } else {
        break;
      }
    }

    // Delete old rows if any found
    if (deleteCount > 0) {
      sheet.deleteRows(2, deleteCount);
    }
  },

  getRecentLogs: function(count = 50) {
    try {
      const sheet = SpreadsheetApp.getActive().getSheetByName('System Logs');
      if (!sheet) return [];

      const lastRow = sheet.getLastRow();
      if (lastRow <= 1) return [];

      const startRow = Math.max(2, lastRow - count + 1);
      return sheet.getRange(startRow, 1, lastRow - startRow + 1, 4)
        .getValues()
        .map(row => ({
          timestamp: row[0],
          level: row[1],
          message: row[2],
          data: JSON.parse(row[3] || '{}')
        }));
    } catch (e) {
      console.error('Failed to get recent logs:', e);
      return [];
    }
  }
},

const ErrorRecoverySystem = {
  _maxRetries: 3,
  _recoveryStrategies: new Map(),
  _errorHistory: new Map(),
  _lastCleanup: 0,
  _cleanupInterval: 15 * 60 * 1000, // 15 minutes

  registerStrategy: function(errorType, strategy) {
    this._recoveryStrategies.set(errorType, strategy);
  },

  async attemptRecovery: function(error, context) {
    this._trackError(error);
    
    const strategy = this._getStrategy(error);
    if (!strategy) {
      LogService.warn('No recovery strategy for error type', {
        type: error.name,
        message: error.message
      });
      return false;
    }

    for (let attempt = 1; attempt <= this._maxRetries; attempt++) {
      try {
        LogService.info(`Recovery attempt ${attempt} for ${error.name}`, context);
        const result = await strategy(error, context, attempt);
        
        if (result.success) {
          this._recordSuccess(error.name);
          return true;
        }
      } catch (recoveryError) {
        LogService.error(`Recovery attempt ${attempt} failed`, {
          error: recoveryError.message,
          originalError: error.message
        });
      }

      // Exponential backoff
      if (attempt < this._maxRetries) {
        await this._delay(Math.pow(2, attempt - 1) * 1000);
      }
    }

    return false;
  },

  _getStrategy: function(error) {
    return this._recoveryStrategies.get(error.name) || this._getDefaultStrategy(error);
  },

  _getDefaultStrategy: function(error) {
    switch(error.name) {
      case 'ApiError':
        return this._apiRecoveryStrategy;
      case 'SheetError':
        return this._sheetRecoveryStrategy;
      case 'StateError':
        return this._stateRecoveryStrategy;
      default:
        return null;
    }
  },

  _apiRecoveryStrategy: async function(error, context) {
    if (error.statusCode === 429) {
      // Rate limit - use exponential backoff
      return { success: false, shouldRetry: true };
    }
    
    if (error.statusCode >= 500) {
      // Server error - try alternate API
      return await this._tryAlternateApi(context);
    }

    return { success: false, shouldRetry: false };
  },

  _sheetRecoveryStrategy: async function(error, context) {
    try {
      SpreadsheetApp.flush();
      await this._delay(1000);
      return { success: true };
    } catch (e) {
      return { success: false };
    }
  },

  _stateRecoveryStrategy: async function(error, context) {
    try {
      StateManager.clearAll();
      return { success: true };
    } catch (e) {
      return { success: false };
    }
  },

  _trackError: function(error) {
    const now = Date.now();
    if (!this._errorHistory.has(error.name)) {
      this._errorHistory.set(error.name, []);
    }
    
    const history = this._errorHistory.get(error.name);
    history.push({
      timestamp: now,
      message: error.message,
      stack: error.stack
    });

    // Cleanup old entries periodically
    if (now - this._lastCleanup > this._cleanupInterval) {
      this._cleanup();
    }
  },

  _recordSuccess: function(errorType) {
    LogService.info(`Recovery successful for ${errorType}`);
  },

  _cleanup: function() {
    const now = Date.now();
    const retentionPeriod = 24 * 60 * 60 * 1000; // 24 hours

    this._errorHistory.forEach((history, type) => {
      const filtered = history.filter(entry => 
        now - entry.timestamp < retentionPeriod
      );
      this._errorHistory.set(type, filtered);
    });

    this._lastCleanup = now;
  },

  _delay: function(ms) {
    return new Promise(resolve => Utilities.sleep(ms));
  },

  async _tryAlternateApi: function(context) {
    const alternateApis = ['TrackyServer', 'GameTracker'];
    
    for (const api of alternateApis) {
      try {
        const result = await ApiService.fetchServerData(api);
        if (result && !result.isError) {
          return { success: true, data: result };
        }
      } catch (e) {
        LogService.warn(`Alternate API ${api} failed`, { error: e.message });
      }
    }
    
    return { success: false };
  },

  getErrorStats: function() {
    const stats = {};
    this._errorHistory.forEach((history, type) => {
      stats[type] = {
        total: history.length,
        recent: history.filter(e => 
          Date.now() - e.timestamp < 60 * 60 * 1000 // Last hour
        ).length
      };
    });
    return stats;
  }
},

const ErrorHandler = {
  ErrorTypes: {
    API_ERROR: 'ApiError',
    VALIDATION_ERROR: 'ValidationError',
    SHEET_ERROR: 'SheetError',
    RATE_LIMIT_ERROR: 'RateLimitError',
    STATE_ERROR: 'StateError'
  },

  handle: function(error, context = {}) {
    LogService.error(error.message, {
      type: error.name || 'UnknownError',
      stack: error.stack,
      ...context
    });

    // Handle specific error types
    switch (error.name) {
      case this.ErrorTypes.API_ERROR:
        return this._handleApiError(error, context);
      case this.ErrorTypes.RATE_LIMIT_ERROR:
        return this._handleRateLimit(error, context);
      case this.ErrorTypes.SHEET_ERROR:
        return this._handleSheetError(error, context);
      default:
        return this._handleGenericError(error, context);
    }
  },

  createError: function(type, message, details = {}) {
    const error = new Error(message);
    error.name = type;
    error.details = details;
    return error;
  },

  _handleApiError: function(error, context) {
    const { source } = context;
    if (source) {
      StateManager.trackApiError(source);
    }
    return { success: false, shouldRetry: true };
  },

  _handleRateLimit: function(error, context) {
    const { source } = context;
    if (source) {
      StateManager.setRateLimit(source);
    }
    return { success: false, shouldRetry: false };
  },

  _handleSheetError: function(error, context) {
    // Attempt to recover sheet operations
    try {
      SpreadsheetApp.flush();
    } catch (e) {
      // Ignore flush errors
    }
    return { success: false, shouldRetry: true };
  },

  _handleGenericError: function(error, context) {
    return { success: false, shouldRetry: false };
  }
},

const RetryManager = {
  async: function(operation, context = {}) {
    const config = Config.getRetryConfig();
    if (!config.enabled) {
      return operation();
    }

    let lastError;
    for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        const handled = ErrorHandler.handle(error, context);
        
        if (!handled.shouldRetry || attempt === config.maxAttempts) {
          throw error;
        }

        LogService.warn('Operation failed, retrying', {
          attempt,
          error: error.message,
          context
        });

        // Wait before retry
        Utilities.sleep(config.delay * attempt);
      }
    }
    throw lastError;
  },

  sync: function(operation, context = {}) {
    const config = Config.getRetryConfig();
    if (!config.enabled) {
      return operation();
    }

    let lastError;
    for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
      try {
        return operation();
      } catch (error) {
        lastError = error;
        const handled = ErrorHandler.handle(error, context);
        
        if (!handled.shouldRetry || attempt === config.maxAttempts) {
          throw error;
        }

        LogService.warn('Operation failed, retrying', {
          attempt,
          error: error.message,
          context
        });

        // Wait before retry
        Utilities.sleep(config.delay * attempt);
      }
    }
    throw lastError;
  }
},

const PerformanceMonitor = {
  _metrics: new Map(),
  _snapshots: [],
  _resourceUsage: new Map(),
  _thresholds: {
    executionTime: 25000,    // 25 seconds
    memoryUsage: 250000000,  // 250MB
    apiCalls: 50,            // per minute
    sheetOps: 100           // per minute
  },

  startOperation: function(name) {
    const start = new Date().getTime();
    this._metrics.set(name, {
      start,
      counts: {
        api: 0,
        sheet: 0,
        cache: 0
      },
      memory: this._getCurrentMemoryUsage()
    });
    
    // Start resource monitoring
    this._startResourceMonitoring(name);
  },

  endOperation: function(name) {
    const metric = this._metrics.get(name);
    if (!metric) return;

    const duration = new Date().getTime() - metric.start;
    const snapshot = {
      operation: name,
      duration,
      timestamp: new Date(),
      ...metric.counts
    };

    this._snapshots.push(snapshot);
    this._metrics.delete(name);

    LogService.debug('Operation completed', snapshot);
    return snapshot;
  },

  incrementCounter: function(operation, type) {
    const metric = this._metrics.get(operation);
    if (metric && metric.counts[type] !== undefined) {
      metric.counts[type]++;
    }
  },

  getMetrics: function() {
    const metrics = {};
    this._snapshots.forEach(snapshot => {
      if (!metrics[snapshot.operation]) {
        metrics[snapshot.operation] = {
          count: 0,
          totalDuration: 0,
          avgDuration: 0,
          apiCalls: 0,
          sheetOps: 0,
          cacheOps: 0
        };
      }

      const m = metrics[snapshot.operation];
      m.count++;
      m.totalDuration += snapshot.duration;
      m.avgDuration = m.totalDuration / m.count;
      m.apiCalls += snapshot.api;
      m.sheetOps += snapshot.sheet;
      m.cacheOps += snapshot.cache;
    });

    return metrics;
  },

  clearMetrics: function() {
    this._metrics.clear();
    this._snapshots = [];
  }
},

const Utils = {
  suppressLintWarnings: true, // Add this flag to control lint warning display

  formatDate: function(date) {
    return new Date(date).toISOString()
      .replace('T', ' ')
      .replace(/\.\d{3}Z$/, '');
  },

  safeStringify: function(obj) {
    try {
      return JSON.stringify(obj, null, 2);
    } catch (e) {
      return String(obj);
    }
  },

  sanitizeLogOutput: function(message) {
    if (this.suppressLintWarnings && 
        (message.includes('lint error') || 
         message.includes('compileError') || 
         message.includes('suggestions:'))) {
      return null;
    }
    return message;
  },

  isEmptyObject: function(obj) {
    return obj && Object.keys(obj).length === 0;
  },

  debounce: function(func, wait) {
    let timeout;
    return function(...args) {
      const later = () => {
        timeout = null;
        func.apply(this, args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  },

  retry: function(operation, retries = 3, delay = 1000) {
    return new Promise((resolve, reject) => {
      operation()
        .then(resolve)
        .catch((error) => {
          if (retries === 0) return reject(error);
          setTimeout(() => {
            this.retry(operation, retries - 1, delay)
              .then(resolve)
              .catch(reject);
          }, delay);
        });
    });
  }
},

function onOpen() {
  createCustomMenu();
}

function createCustomMenu() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Server Monitor')
    .addItem('Update Now', 'manualUpdate')
    .addItem('Clear Cache', 'clearCache')
    .addItem('View Logs', 'viewLogs')
    .addItem('View Performance', 'viewPerformance')
    .addToUi();
}

function initialize() {
  LogService.info('Initializing application');
  
  try {
    StateManager.initialize();
    initializeSheets();
    setupTriggers();
    LogService.info('Initialization complete');
  } catch (error) {
    ErrorHandler.handle(error, { context: 'initialization' });
    throw error;
  }
}

function initializeSheets() {
  const spreadsheet = SpreadsheetApp.getActive();
  
  // Ensure required sheets exist
  [Config.SHEET_NAMES.BACTA, Config.SHEET_NAMES.DOCTOR, 'System Logs'].forEach(sheetName => {
    if (!spreadsheet.getSheetByName(sheetName)) {
      spreadsheet.insertSheet(sheetName);
      LogService.info('Created sheet', { name: sheetName });
    }
  });
}

function setupTriggers() {
  // Clean up existing triggers
  ScriptApp.getProjectTriggers().forEach(trigger => {
    ScriptApp.deleteTrigger(trigger);
  });

  // Create new triggers
  ScriptApp.newTrigger('updateServer')
    .timeBased()
    .everyMinutes(Config.TRIGGER_INTERVALS.PRIMARY)
    .create();

  ScriptApp.newTrigger('cleanup')
    .timeBased()
    .everyHours(1)
    .create();
}

async function updateServer() {
  PerformanceMonitor.startOperation('server_update');
  LogService.markStart('server_update');

  try {
    // Attempt to acquire execution lock
    if (!StateManager.acquireLock()) {
      LogService.warn('Update skipped - another execution in progress');
      return;
    }

    // Initialize services
    const sheet = SheetService;
    sheet.initialize();

    // Fetch and process data from all sources
    const results = await RetryManager.async(async () => {
      const apis = ['BattleMetrics', 'TrackyServer', 'GameTracker'];
      const apiResults = await Promise.all(
        apis
          .filter(source => !StateManager.isRateLimited(source))
          .map(source => ApiService.fetchServerData(source))
      );

      return apiResults.filter(result => result && !result.isError);
    }, { context: 'api_fetch' });

    if (!results.length) {
      throw ErrorHandler.createError(
        ErrorHandler.ErrorTypes.API_ERROR,
        'No valid data from any source'
      );
    }

    // Process the best result
    const bestResult = results.reduce((best, current) => {
      return (current.players.length > best.players.length) ? current : best;
    });

    // Update sheets with processed data
    const processed = DataProcessor.processApiResponse(bestResult, bestResult.serverInfo.source);
    
    // Perform all sheet updates
    await RetryManager.async(() => {
      sheet.updateHeader(processed.serverInfo);
      sheet.updatePlayerList(processed.players);
      
      const elapsedTimes = DataProcessor.calculateElapsedTimes(
        processed.players,
        StateManager.getElapsedTimes()
      );
      
      sheet.updateElapsedPanel(elapsedTimes);
      const lastSeenCount = sheet.updateLastSeen(processed.players);
      
      // Update logs
      sheet.updateLogs([
        \`[${new Date().toISOString()}] Updated from ${processed.serverInfo.source}\`,
        \`Players: ${processed.players.length}\`,
        \`Last Seen updated: ${lastSeenCount}\`
      ]);

      // Cache elapsed times for next update
      StateManager.cacheElapsedTimes(elapsedTimes);
    }, { context: 'sheet_update' });

    // Update state
    StateManager.updateLastRun();
    LogService.info('Server update complete', {
      source: bestResult.serverInfo.source,
      players: processed.players.length
    });

  } catch (error) {
    ErrorHandler.handle(error, { context: 'server_update' });
    throw error;
  } finally {
    StateManager.releaseLock();
    LogService.markEnd('server_update');
    LogService.flush();
    PerformanceMonitor.endOperation('server_update');
  }
}

function manualUpdate() {
  updateServer();
  SpreadsheetApp.getActive().toast('Update complete!');
}

function clearCache() {
  StateManager.clearAll();
  PerformanceMonitor.clearMetrics();
  SpreadsheetApp.getActive().toast('Cache cleared!');
}

function cleanup() {
  LogService.info('Starting cleanup');
  
  try {
    // Flush old logs
    LogService.flush();
    
    // Clear stale cache entries
    StateManager.clearAll();
    
    // Reset rate limits
    Object.keys(Config.API_CONFIG).forEach(source => {
      if (StateManager.isRateLimited(source)) {
        StateManager.setRateLimit(source, false);
      }
    });

    // Clear old metrics
    PerformanceMonitor.clearMetrics();
    
    LogService.info('Cleanup complete');
  } catch (error) {
    ErrorHandler.handle(error, { context: 'cleanup' });
  }
}

function viewLogs() {
  const logs = LogService.getRecentLogs();
  const ui = SpreadsheetApp.getUi();
  
  if (!logs.length) {
    ui.alert('No logs found');
    return;
  }

  const html = logs
    .map(log => \`${log.timestamp}: [${log.level}] ${log.message}\`)
    .join('\\n');
    
  ui.showModalDialog(
    HtmlService.createHtmlOutput(\`<pre>${html}</pre>\`)
      .setWidth(600)
      .setHeight(400),
    'Recent Logs'
  );
}

function viewPerformance() {
  const metrics = PerformanceMonitor.getMetrics();
  const ui = SpreadsheetApp.getUi();

  const html = Object.entries(metrics)
    .map(([op, data]) => {
      return \`Operation: ${op}
  Count: ${data.count}
  Avg Duration: ${Math.round(data.avgDuration)}ms
  API Calls: ${data.apiCalls}
  Sheet Operations: ${data.sheetOps}
  Cache Operations: ${data.cacheOps}
\`;
    })
    .join('\\n');

  ui.showModalDialog(
    HtmlService.createHtmlOutput(\`<pre>${html}</pre>\`)
      .setWidth(500)
      .setHeight(300),
    'Performance Metrics'
  );
}
};


/******************************************************************************
 * Recovery Strategies
 * Defines error recovery strategies for different scenarios.
 ******************************************************************************/
const RecoveryStrategies = {
  apiError: async function(error, context, attempt) {
    LogService.warn('Attempting API error recovery', { attempt });

    if (error.statusCode === 429) {
      // Rate limit - use exponential backoff
      const delay = Math.pow(2, attempt) * 1000;
      await Utilities.sleep(delay);
      return { success: false, shouldRetry: true };
    }

    if (error.source) {
      // Try alternate data source
      try {
        const alternateSource = this._getAlternateSource(error.source);
        if (alternateSource) {
          const result = await ApiService.fetchServerData(alternateSource);
          if (result && !result.isError) {
            return { success: true, data: result };
          }
        }
      } catch (e) {
        LogService.error('Alternate source failed', { error: e.message });
      }
    }

    return { success: false, shouldRetry: attempt < 3 };
  },

  sheetError: async function(error, context, attempt) {
    LogService.warn('Attempting sheet error recovery', { attempt });

    try {
      // Force flush and wait
      SpreadsheetApp.flush();
      await Utilities.sleep(1000);

      // Verify sheet access
      const ss = SpreadsheetApp.getActive();
      const sheets = [
        ss.getSheetByName(Config.SHEETS.BACTA),
        ss.getSheetByName(Config.SHEETS.DOCTOR)
      ];

      if (sheets.some(s => !s)) {
        return { success: false, shouldRetry: false };
      }

      return { success: true };
    } catch (e) {
      return { success: false, shouldRetry: attempt < 3 };
    }
  },

  stateError: async function(error, context, attempt) {
    LogService.warn('Attempting state error recovery', { attempt });

    try {
      // Clear problematic state
      StateManager.clearAll();
      await Utilities.sleep(500);

      // Initialize fresh state
      StateManager.initialize();
      return { success: true };
    } catch (e) {
      return { success: false, shouldRetry: attempt < 2 };
    }
  },

  validationError: async function(error, context, attempt) {
    LogService.warn('Attempting validation error recovery', { attempt });

    try {
      const data = context.data;
      if (!data) return { success: false, shouldRetry: false };

      // Attempt to sanitize data
      if (Array.isArray(data)) {
        const sanitized = data
          .filter(item => item && typeof item === 'string')
          .map(item => DataValidator.sanitizeName(item))
          .filter(Boolean);

        if (sanitized.length) {
          return { success: true, data: sanitized };
        }
      }

      return { success: false, shouldRetry: false };
    } catch (e) {
      return { success: false, shouldRetry: false };
    }
  },

  _getAlternateSource: function(failedSource) {
    const sources = ['BattleMetrics', 'TrackyServer', 'GameTracker'];
    const index = sources.indexOf(failedSource);
    if (index === -1) return sources[0];
    return sources[(index + 1) % sources.length];
  }
};

/******************************************************************************
 * LOGIC: DataProcessor
 * Contains the core business logic for the tracker.
 ******************************************************************************/
const DataProcessor = {
  processPlayerData: function(apiData, currentState) {
    LogService.markStart('process_player_data');
    try {
      const currentPlayers = apiData.players || [];
      const previousPlayers = currentState?.players || [];
      
      // Normalize all player names for comparison
      const normalizedCurrent = new Set(currentPlayers.map(p => DataValidator.normalize(p)));
      const normalizedPrevious = new Set(previousPlayers.map(p => DataValidator.normalize(p)));
      
      // Find joins and leaves
      const joins = currentPlayers.filter(p => 
        !normalizedPrevious.has(DataValidator.normalize(p))
      );
      
      const leaves = previousPlayers.filter(p => 
        !normalizedCurrent.has(DataValidator.normalize(p))
      );

      // Update join times
      const joinTimes = { ...currentState?.joinTimes } || {};
      joins.forEach(player => {
        joinTimes[DataValidator.normalize(player)] = Date.now();
      });

      // Clean up old join times
      Object.keys(joinTimes).forEach(player => {
        if (!normalizedCurrent.has(player)) {
          delete joinTimes[player];
        }
      });

      return {
        currentPlayers,
        joins,
        leaves,
        joinTimes
      };
    } catch (e) {
      LogService.error('Player data processing failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('process_player_data');
    }
  },

  decideCurrentMap: function(apiData, currentState) {
    LogService.markStart('decide_map');
    try {
      const newMap = apiData.map;
      if (!newMap) {
        return {
          currentMap: currentState?.map || 'Unknown',
          mapChanged: false
        };
      }

      const pendingMap = StateService.getPendingMap();
      if (pendingMap?.map === newMap) {
        // Increment confirmation count
        const newCount = (pendingMap.confirmCount || 0) + 1;
        
        if (newCount >= Config.MAP_CONFIRM_RUNS) {
          // Map change confirmed
          StateService.clearPendingMap();
          return {
            currentMap: newMap,
            mapChanged: true
          };
        }
        
        // Update pending map count
        StateService.setPendingMap({
          map: newMap,
          confirmCount: newCount,
          timestamp: Date.now()
        });
        
        return {
          currentMap: currentState?.map || newMap,
          mapChanged: false
        };
      }
      
      // New potential map change detected
      if (newMap !== currentState?.map) {
        StateService.setPendingMap({
          map: newMap,
          confirmCount: 1,
          timestamp: Date.now()
        });
      }
      
      return {
        currentMap: currentState?.map || newMap,
        mapChanged: false
      };
    } catch (e) {
      LogService.error('Map decision failed', { error: e.message });
      throw e;
    } finally {
      LogService.markEnd('decide_map');
    }
  }
};


/******************************************************************************
 * ORCHESTRATOR
 * The main function that coordinates the services to update the sheet.
 ******************************************************************************/
async function updateServerSheets() {
  PerformanceMonitor.startOperation('server_update');
  LogService.markStart('server_update');

  try {
    // Attempt to acquire execution lock
    if (!StateManager.acquireLock()) {
      LogService.warn('Update skipped - another execution in progress');
      return;
    }

    // Initialize services
    SheetService.init();
    const currentState = StateService.getState();

    // Fetch and process data from all sources
    const results = await RetryManager.async(async () => {
      const apiData = await ApiService.fetchServerData();
      
      if (!apiData || apiData.error) {
        throw new APIError('Failed to fetch server data');
      }

      return apiData;
    }, { context: 'api_fetch' });

    // Process player data and map status
    const { currentPlayers, joins, leaves, joinTimes } = 
      DataProcessor.processPlayerData(results, currentState);
    
    const { currentMap, mapChanged } = 
      DataProcessor.decideCurrentMap(results, currentState);

    // Update sheets with processed data
    await RetryManager.async(() => {
      // Update header with server status
      SheetService.updateHeader({
        timestamp: new Date().toISOString(),
        source: results.serverInfo.source,
        playerCount: currentPlayers.length,
        range: `${currentPlayers.length}/${Config.MAX_PLAYERS_CAP}`,
        status: results.serverInfo.status,
        responseTime: results.serverInfo.responseTime
      });

      // Update player list and related data
      SheetService.updatePlayerList(currentPlayers);
      
      const elapsedTimes = DataProcessor.calculateElapsedTimes(
        currentPlayers,
        StateManager.getElapsedTimes()
      );
      
      SheetService.updateElapsedPanel(elapsedTimes);
      const lastSeenCount = SheetService.updateLastSeen(currentPlayers);
      
      // Update doctor info panel
      SheetService.updateDoctorInfo({
        timestamp: new Date().toISOString(),
        source: results.serverInfo.source,
        ip: Config.SERVER_IP,
        map: currentMap,
        players: `${currentPlayers.length}/${Config.MAX_PLAYERS_CAP}`,
        status: results.serverInfo.status
      });

      // Update logs with changes
      const logEntries = [
        `[${new Date().toISOString()}] Server update from ${results.serverInfo.source}`,
        `Players online: ${currentPlayers.length}/${Config.MAX_PLAYERS_CAP}`,
        ...joins.map(p => `[+] ${p} joined`),
        ...leaves.map(p => `[-] ${p} left`),
        mapChanged ? `[MAP] Changed to ${currentMap}` : null,
        `Last seen updated: ${lastSeenCount} players`
      ].filter(Boolean);

      SheetService.updateLogs(logEntries);

      // Cache elapsed times for next update
      StateManager.cacheElapsedTimes(elapsedTimes);
    }, { context: 'sheet_update' });

    // Update state
    const newState = {
      ...currentState,
      players: currentPlayers,
      map: currentMap,
      joinTimes: joinTimes,
      lastUpdate: Date.now()
    };
    StateService.setState(newState);

    LogService.info('Server update complete', {
      players: currentPlayers.length,
      joins: joins.length,
      leaves: leaves.length,
      mapChanged
    });

  } catch (error) {
    ErrorHandler.handle(error, { context: 'server_update' });
    throw error;
  } finally {
    StateManager.releaseLock();
    LogService.markEnd('server_update');
    LogService.flush();
    PerformanceMonitor.endOperation('server_update');
  }
}



/**
 * @typedef {Object} PerformanceMetrics
 * @property {number} startTime - Script start time
 * @property {Object.<string, number>} durations - Duration of each operation
 */

/**
 * Custom error types for better error handling
 */
class APIError extends Error {
  constructor(message, source, statusCode) {
    super(message);
    this.name = 'APIError';
    this.source = source;
    this.statusCode = statusCode;
  }
}

class ValidationError extends Error {
  constructor(message, field) {
    super(message);
    this.name = 'ValidationError';
    this.field = field;
  }
}

class StateError extends Error {
  constructor(message, state) {
    super(message);
    this.name = 'StateError';
    this.state = state;
  }
}

/**
 * Execution history entry structure
 * @typedef {Object} ExecutionEntry
 * @property {string} id - Unique execution ID
 * @property {Date} startTime - Execution start timestamp
 * @property {Date} endTime - Execution end timestamp
 * @property {number} duration - Execution duration in ms
 * @property {Object} metrics - Performance metrics
 * @property {Object} state - State changes
 * @property {Object} errors - Errors encountered
 * @property {string} status - Execution status
 */

/**
 * Advanced logging and execution tracking system
 */
const DataIntegrityChecker = {
  checkInterval: 15 * 60 * 1000, // 15 minutes
  lastCheck: 0,

  verifySheetStructure: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const requiredSheets = [Config.SHEETS.BACTA, Config.SHEETS.DOCTOR];
    const missingSheets = [];

    requiredSheets.forEach(sheetName => {
      if (!ss.getSheetByName(sheetName)) {
        missingSheets.push(sheetName);
      }
    });

    if (missingSheets.length) {
      throw new Error(`Missing required sheets: ${missingSheets.join(', ')}`);
    }
  },

  verifyRanges: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const bacta = ss.getSheetByName(Config.SHEETS.BACTA);
    const doctor = ss.getSheetByName(Config.SHEETS.DOCTOR);

    // Verify critical ranges exist
    const ranges = [
      { sheet: bacta, range: Config.RANGES.PLAYER_NAMES, name: 'Player Names' },
      { sheet: bacta, range: Config.RANGES.PLAYER_COUNT, name: 'Player Count' },
      { sheet: bacta, range: Config.RANGES.HEADER, name: 'Header' },
      { sheet: bacta, range: Config.RANGES.LOGS, name: 'Logs' },
      { sheet: doctor, range: Config.RANGES.DOCTOR_INFO, name: 'Doctor Info' }
    ];

    ranges.forEach(({ sheet, range, name }) => {
      try {
        sheet.getRange(range);
      } catch (e) {
        throw new Error(`Invalid range for ${name}: ${range}`);
      }
    });
  },

  performCheck: function() {
    const now = Date.now();
    if (now - this.lastCheck < this.checkInterval) return;

    LogService.info('Starting data integrity check');
    
    try {
      this.verifySheetStructure();
      this.verifyRanges();
      
      this.lastCheck = now;
      LogService.info('Data integrity check completed successfully');
    } catch (e) {
      LogService.error('Data integrity check failed', { error: e.message });
      throw e;
    }
  }
},

const MemoryManager = {
  _memoryLimit: 50 * 1024 * 1024, // 50MB limit
  _usageWarningThreshold: 0.8, // 80% warning threshold
  _lastCleanup: 0,
  _cleanupInterval: 5 * 60 * 1000, // 5 minutes

  checkMemory: function() {
    const used = this._getMemoryUsage();
    const limit = this._memoryLimit;
    const usageRatio = used / limit;

    if (usageRatio > this._usageWarningThreshold) {
      LogService.warn('High memory usage', {
        used: this._formatBytes(used),
        limit: this._formatBytes(limit),
        ratio: (usageRatio * 100).toFixed(1) + '%'
      });

      if (Date.now() - this._lastCleanup > this._cleanupInterval) {
        this.cleanup();
      }
    }

    return usageRatio;
  },

  cleanup: function() {
    LogService.info('Starting memory cleanup');
    
    try {
      // Clear execution history
      ExecutionLogger.pruneHistory();
      
      // Clear performance metrics
      PerformanceMonitor.clearMetrics();
      
      // Clear old logs
      LogService.flush();
      
      // Clear stale cache
      StateManager.clearStaleData();
      
      this._lastCleanup = Date.now();
      LogService.info('Memory cleanup completed');
    } catch (e) {
      LogService.error('Memory cleanup failed', { error: e.message });
    }
  },

  _getMemoryUsage: function() {
    try {
      const history = ExecutionLogger.getHistory();
      const metrics = PerformanceMonitor.getMetrics();
      const logs = LogService.getRecentLogs();
      
      return JSON.stringify({
        history,
        metrics,
        logs
      }).length;
    } catch (e) {
      return 0;
    }
  },

  _formatBytes: function(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
},

const PerformanceOptimizer = {
  _batchSize: 50,
  _cacheHits: new Map(),
  _cacheMisses: new Map(),
  _operationTimings: new Map(),

  optimizeBatchOperations: function(operations) {
    const batches = this._groupOperations(operations);
    return this._processBatches(batches);
  },

  _groupOperations: function(operations) {
    const groups = new Map();
    
    operations.forEach(op => {
      const key = `${op.sheet}_${op.type}`;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(op);
    });

    return groups;
  },

  _processBatches: function(batches) {
    const results = [];
    
    batches.forEach((ops, key) => {
      const batchResults = this._processBatchWithRetry(ops);
      results.push(...batchResults);
    });

    return results;
  },

  _processBatchWithRetry: function(operations, attempt = 1) {
    const maxRetries = 3;
    
    try {
      return this._executeBatch(operations);
    } catch (e) {
      if (attempt < maxRetries) {
        LogService.warn(`Batch operation failed, attempt ${attempt}`, { error: e.message });
        Utilities.sleep(Math.pow(2, attempt) * 1000);
        return this._processBatchWithRetry(operations, attempt + 1);
      }
      throw e;
    }
  },

  _executeBatch: function(operations) {
    const results = [];
    let currentBatch = [];
    
    operations.forEach(op => {
      currentBatch.push(op);
      
      if (currentBatch.length >= this._batchSize) {
        this._executeSingleBatch(currentBatch, results);
        currentBatch = [];
      }
    });

    if (currentBatch.length > 0) {
      this._executeSingleBatch(currentBatch, results);
    }

    return results;
  },

  _executeSingleBatch: function(batch, results) {
    const sheet = batch[0].sheet;
    const ranges = batch.map(op => op.range);
    const values = batch.map(op => op.values);

    try {
      sheet.getRangeList(ranges).setValues(values);
      batch.forEach(op => results.push({ success: true, operation: op }));
    } catch (e) {
      batch.forEach(op => {
        try {
          op.range.setValues(op.values);
          results.push({ success: true, operation: op });
        } catch (innerE) {
          results.push({ 
            success: false, 
            operation: op, 
            error: innerE.message 
          });
        }
      });
    }
  },

  trackOperation: function(type, duration) {
    if (!this._operationTimings.has(type)) {
      this._operationTimings.set(type, {
        count: 0,
        totalDuration: 0,
        avgDuration: 0
      });
    }

    const stats = this._operationTimings.get(type);
    stats.count++;
    stats.totalDuration += duration;
    stats.avgDuration = stats.totalDuration / stats.count;
  },

  getOptimizationStats: function() {
    return {
      operationTimings: Object.fromEntries(this._operationTimings),
      cacheEfficiency: {
        hits: Object.fromEntries(this._cacheHits),
        misses: Object.fromEntries(this._cacheMisses),
        ratio: this._calculateCacheHitRatio()
      }
    };
  },

  _calculateCacheHitRatio: function() {
    let totalHits = 0;
    let totalMisses = 0;

    this._cacheHits.forEach(count => totalHits += count);
    this._cacheMisses.forEach(count => totalMisses += count);

    const total = totalHits + totalMisses;
    return total > 0 ? totalHits / total : 0;
  }
},

const ExecutionLogger = {
  EXECUTION_HISTORY_KEY: 'EXECUTION_HISTORY',
  MAX_HISTORY_ENTRIES: 50,
  
  /**
   * Generate unique execution ID
   * @returns {string}
   */
  generateExecutionId: function() {
    return Utilities.getUuid();
  },

  /**
   * Get execution history
   * @returns {ExecutionEntry[]}
   */
  getHistory: function() {
    try {
      const props = PropertiesService.getScriptProperties();
      const history = JSON.parse(props.getProperty(this.EXECUTION_HISTORY_KEY) || '[]');
      return Array.isArray(history) ? history : [];
    } catch (e) {
      console.error('Failed to retrieve execution history:', e);
      return [];
    }
  },

  /**
   * Add entry to execution history
   * @param {ExecutionEntry} entry
   */
  addHistoryEntry: function(entry) {
    try {
      const props = PropertiesService.getScriptProperties();
      const history = this.getHistory();
      
      // Add new entry and limit size
      history.unshift(entry);
      while (history.length > this.MAX_HISTORY_ENTRIES) {
        history.pop();
      }
      
      props.setProperty(this.EXECUTION_HISTORY_KEY, JSON.stringify(history));
    } catch (e) {
      console.error('Failed to add execution history entry:', e);
    }
  },

  /**
   * Start new execution tracking
   * @returns {string} Execution ID
   */
  startExecution: function() {
    const executionId = this.generateExecutionId();
    const entry = {
      id: executionId,
      startTime: new Date(),
      status: 'running',
      metrics: {
        apiCalls: { total: 0, failed: 0 },
        sheetOperations: { total: 0, failed: 0 },
        cacheHits: 0,
        cacheMisses: 0,
        warnings: 0,
        errors: 0
      },
      state: {
        initial: {},
        changes: []
      },
      errors: [],
      logs: []
    };
    
    this.currentExecution = entry;
    return executionId;
  },

  /**
   * Log state change
   * @param {string} component - Component name
   * @param {Object} oldState - Previous state
   * @param {Object} newState - New state
   */
  logStateChange: function(component, oldState, newState) {
    if (!this.currentExecution) return;
    
    this.currentExecution.state.changes.push({
      timestamp: new Date(),
      component,
      oldState: JSON.parse(JSON.stringify(oldState)),
      newState: JSON.parse(JSON.stringify(newState))
    });
  },

  /**
   * Track API call
   * @param {string} api - API name
   * @param {boolean} success - Whether call succeeded
   * @param {number} duration - Call duration in ms
   */
  trackApiCall: function(api, success, duration) {
    if (!this.currentExecution) return;
    
    this.currentExecution.metrics.apiCalls.total++;
    if (!success) {
      this.currentExecution.metrics.apiCalls.failed++;
    }
    
    if (!this.currentExecution.metrics.apiTiming) {
      this.currentExecution.metrics.apiTiming = {};
    }
    if (!this.currentExecution.metrics.apiTiming[api]) {
      this.currentExecution.metrics.apiTiming[api] = { calls: 0, totalDuration: 0 };
    }
    
    this.currentExecution.metrics.apiTiming[api].calls++;
    this.currentExecution.metrics.apiTiming[api].totalDuration += duration;
  },

  /**
   * Track sheet operation
   * @param {string} operation - Operation type
   * @param {boolean} success - Whether operation succeeded
   */
  trackSheetOperation: function(operation, success) {
    if (!this.currentExecution) return;
    
    this.currentExecution.metrics.sheetOperations.total++;
    if (!success) {
      this.currentExecution.metrics.sheetOperations.failed++;
    }
  },

  /**
   * Track cache operation
   * @param {boolean} hit - Whether cache hit
   */
  trackCacheOperation: function(hit) {
    if (!this.currentExecution) return;
    
    if (hit) {
      this.currentExecution.metrics.cacheHits++;
    } else {
      this.currentExecution.metrics.cacheMisses++;
    }
  },

  /**
   * Add execution log entry
   * @param {string} level - Log level
   * @param {string} message - Log message
   * @param {Object} context - Additional context
   */
  addLog: function(level, message, context = {}) {
    if (!this.currentExecution) return;
    
    this.currentExecution.logs.push({
      timestamp: new Date(),
      level,
      message,
      context
    });

    if (level === 'ERROR') {
      this.currentExecution.metrics.errors++;
      this.currentExecution.errors.push({
        timestamp: new Date(),
        message,
        context
      });
    } else if (level === 'WARN') {
      this.currentExecution.metrics.warnings++;
    }
  },

  /**
   * End execution tracking
   * @param {string} status - Final execution status
   */
  endExecution: function(status = 'completed') {
    if (!this.currentExecution) return;
    
    this.currentExecution.endTime = new Date();
    this.currentExecution.duration = 
      this.currentExecution.endTime.getTime() - 
      this.currentExecution.startTime.getTime();
    this.currentExecution.status = status;
    
    // Calculate success rates
    const metrics = this.currentExecution.metrics;
    metrics.apiSuccessRate = 
      metrics.apiCalls.total ? 
      ((metrics.apiCalls.total - metrics.apiCalls.failed) / metrics.apiCalls.total) * 100 : 
      100;
    metrics.sheetSuccessRate = 
      metrics.sheetOperations.total ? 
      ((metrics.sheetOperations.total - metrics.sheetOperations.failed) / metrics.sheetOperations.total) * 100 : 
      100;
    metrics.cacheHitRate = 
      (metrics.cacheHits + metrics.cacheMisses) ? 
      (metrics.cacheHits / (metrics.cacheHits + metrics.cacheMisses)) * 100 : 
      0;
    
    // Add to history
    this.addHistoryEntry(this.currentExecution);
    this.currentExecution = null;
  }
};

/**
 * Advanced logging and metrics system
 */
const Logger = {
  LOG_LEVELS: {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
    CRITICAL: 4
  },

  currentLevel: 1, // Default to INFO
  metrics: {
    startTime: 0,
    durations: {},
    counters: {},
    errors: {},
    warnings: 0,
    criticalErrors: 0
  },

  /**
   * Format message with timestamp and context
   */
  format: function(level, message, context = {}) {
    const timestamp = new Date().toISOString();
    const contextStr = Object.entries(context)
      .map(([k, v]) => `${k}=${v}`)
      .join(' ');
    return `[${timestamp}] ${level}: ${message} ${contextStr}`;
  },

  /**
   * Log message with level and context
   */
  log: function(level, message, context = {}) {
    if (this.LOG_LEVELS[level] >= this.currentLevel) {
      const formattedMessage = this.format(level, message, context);
      console.log(formattedMessage);
      
      // Track error metrics
      if (level === 'ERROR' || level === 'CRITICAL') {
        const errorType = context.type || 'unknown';
        this.metrics.errors[errorType] = (this.metrics.errors[errorType] || 0) + 1;
        if (level === 'CRITICAL') this.metrics.criticalErrors++;
      } else if (level === 'WARN') {
        this.metrics.warnings++;
      }
    }
  },

  debug: function(message, context = {}) {
    this.log('DEBUG', message, context);
  },

  info: function(message, context = {}) {
    this.log('INFO', message, context);
  },

  warn: function(message, context = {}) {
    this.log('WARN', message, context);
  },

  error: function(message, context = {}) {
    this.log('ERROR', message, context);
  },

  critical: function(message, context = {}) {
    this.log('CRITICAL', message, context);
  },

  /**
   * Performance metrics tracking
   */
  markStart: function(operation) {
    this.metrics.durations[operation] = Date.now();
  },

  markEnd: function(operation) {
    if (this.metrics.durations[operation]) {
      const duration = Date.now() - this.metrics.durations[operation];
      delete this.metrics.durations[operation];
      return duration;
    }
    return 0;
  },

  /**
   * Increment counter
   */
  incrementCounter: function(name, value = 1) {
    this.metrics.counters[name] = (this.metrics.counters[name] || 0) + value;
  },

  /**
   * Get complete metrics report
   */
  getMetricsReport: function() {
    const totalDuration = Date.now() - this.metrics.startTime;
    const errorRate = this.metrics.criticalErrors / (Object.values(this.metrics.errors).reduce((a, b) => a + b, 0) || 1);
    
    return {
      duration: {
        total: totalDuration,
        operations: Object.entries(this.metrics.durations).map(([k, v]) => ({
          name: k,
          duration: Date.now() - v
        }))
      },
      errors: {
        total: Object.values(this.metrics.errors).reduce((a, b) => a + b, 0),
        byType: this.metrics.errors,
        critical: this.metrics.criticalErrors,
        errorRate: errorRate
      },
      warnings: this.metrics.warnings,
      counters: this.metrics.counters,
      timestamp: new Date().toISOString()
    };
  },

  /**
   * Reset metrics
   */
  resetMetrics: function() {
    this.metrics = {
      startTime: Date.now(),
      durations: {},
      counters: {},
      errors: {},
      warnings: 0,
      criticalErrors: 0
    };
  }
};

// Initialize metrics for this run
Logger.resetMetrics();

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

/**
 * Acquire a lock with timeout
 * @param {number} timeoutMs - Maximum time to wait for lock
 * @returns {GoogleAppsScript.Lock.Lock|null} The acquired lock or null if timeout
 */
function acquireLock_(timeoutMs = 30000) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(timeoutMs);
    return lock;
  } catch (e) {
    Logger.log('Failed to acquire lock: ' + e);
    return null;
  }
}

/**
 * Batch write values to a sheet
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - Target sheet
 * @param {Object[]} operations - Array of {range: string, values: any[][]} objects
 */
function batchWriteToSheet_(sheet, operations) {
  const ranges = [];
  const valuesList = [];
  
  for (const op of operations) {
    ranges.push(sheet.getRange(op.range));
    valuesList.push(op.values);
  }
  
  if (ranges.length > 0) {
    sheet.getRangeList(ranges).setValues(valuesList);
  }
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

  // ---- Parallel data fetching and processing ----
  metrics.markStart('data_fetch');
  let tk = { names: [], updatedAtMs: nowMs, source: "Tracky API" };
  let gt = { names: [], updatedAtMs: 0, map: "", source: "GameTracker (HTML)" };

  // Fetch all data in parallel
  const requests = [
    { url: C.TS_WIDGET, ...req_(C.UA, C.TIMEOUT_MS) },
    { url: C.GT_URL, ...req_(C.UA, C.TIMEOUT_MS) },
    { url: C.BM_SEARCH, ...req_(C.UA, C.TIMEOUT_MS) }
  ];

  try {
    const [trackyResponse, gtResponse, bmResponse] = UrlFetchApp.fetchAll(requests);
    metrics.markEnd('data_fetch');

    // Process Tracky data
    metrics.markStart('tracky_process');
    try {
      const cachedData = CacheManager.get('tracky_data', 300, 60);
      if (cachedData && !cachedData.needsRefresh) {
        tk = cachedData;
      } else {
        const j = JSON.parse(trackyResponse.getContentText() || "{}");
        tk.names = unique_(parseTrackyJSON_(j).map(clean_).filter(valid_)).slice(0, C.MAX_PLAYERS);
        tk.updatedAtMs = nowMs;
        CacheManager.set('tracky_data', tk, 300);
      }
      Logger.log("Tracky names: %d", tk.names.length);
    } catch (e) {
      Logger.log("Tracky API fail: %s", e);
    }
    metrics.markEnd('tracky_process');

    // Process GameTracker data
    metrics.markStart('gt_process');
    try {
      if (gtResponse.getResponseCode() === 200) {
        const gtHtml = gtResponse.getContentText() || "";
        const p = parseGT_(gtHtml);
        gt.names = unique_(p.names.map(clean_).filter(valid_)).slice(0, C.MAX_PLAYERS);
        gt.updatedAtMs = p.updatedAtMs || 0;
        gt.map = p.map || "";
        CacheManager.set('gt_data', gt, 300);
        Logger.log("GT names=%d, GT map=%s, GT freshness(ms)=%s", gt.names.length, gt.map, gt.updatedAtMs);
      } else {
        // Try to use cached data if available
        const cachedGT = CacheManager.get('gt_data');
        if (cachedGT) {
          gt = cachedGT;
          Logger.log("Using cached GT data");
        } else {
          throw new APIError("GT HTTP " + gtResponse.getResponseCode(), "GameTracker", gtResponse.getResponseCode());
        }
      }
    } catch (e) {
      Logger.log("GT fail: %s", e);
    }
    metrics.markEnd('gt_process');

    // Process BattleMetrics data in parallel
    metrics.markStart('bm_process');
    try {
      if (bmResponse.getResponseCode() === 200) {
        const bmData = JSON.parse(bmResponse.getContentText() || "{}");
        bm = processBattleMetricsData_(bmData, C.SERVER_IP, C.SERVER_PORT);
        CacheManager.set('bm_data', bm, 300);
      } else {
        // Try to use cached data if available
        const cachedBM = CacheManager.get('bm_data');
        if (cachedBM) {
          bm = cachedBM;
          Logger.log("Using cached BM data");
        }
      }
    } catch (e) {
      Logger.log("BM processing error: %s", e);
    }
    metrics.markEnd('bm_process');

  } catch (e) {
    Logger.log("Parallel fetch failed: %s", e);
    // Try to recover using cached data
    const cachedTracky = CacheManager.get('tracky_data');
    const cachedGT = CacheManager.get('gt_data');
    const cachedBM = CacheManager.get('bm_data');
    
    if (cachedTracky) tk = cachedTracky;
    if (cachedGT) gt = cachedGT;
    if (cachedBM) bm = cachedBM;
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
/**
 * Process BattleMetrics data with validation and error handling
 * @param {Object} data - Raw BattleMetrics API response
 * @param {string} wantIp - Expected server IP
 * @param {number} wantPort - Expected server port
 * @returns {Object|null} Processed server data or null if not found
 * @throws {ValidationError} If data is invalid
 */
function processBattleMetricsData_(data, wantIp, wantPort) {
  // Validate input data
  if (!data || typeof data !== 'object') {
    throw new ValidationError('Invalid BattleMetrics data format', 'data');
  }
  
  const list = Array.isArray(data.data) ? data.data : [];
  
  // Find matching server
  const hit = list.find(d => {
    const a = d && d.attributes || {};
    return (String(a.ip || "").trim() === String(wantIp)) &&
           (Number(a.port) === Number(wantPort));
  });
  
  if (!hit) return null;
  
  try {
    const a = hit.attributes || {};
    const details = a.details || {};
    
    // Validate and sanitize server data
    const serverData = {
      ip: DataValidator.sanitizeText(a.ip || wantIp),
      port: Number(a.port || wantPort),
      map: DataValidator.sanitizeText(details.map || ""),
      status: DataValidator.sanitizeText(a.status || "unknown").toLowerCase(),
      players: Math.max(0, Number(a.players) || 0),
      maxPlayers: Math.max(0, Number(a.maxPlayers) || 0),
      lastUpdate: Date.now()
    };
    
    // Additional validation
    if (serverData.port < 1 || serverData.port > 65535) {
      throw new ValidationError('Invalid port number', 'port');
    }
    
    if (serverData.players > serverData.maxPlayers) {
      serverData.players = serverData.maxPlayers;
    }
    
    return serverData;
    
  } catch (e) {
    if (e instanceof ValidationError) throw e;
    throw new ValidationError(`Failed to process BattleMetrics data: ${e.message}`, 'processing');
  }
}

/**
 * Fetch and process BattleMetrics data
 * @param {string} url - BattleMetrics API URL
 * @param {string} ua - User agent string
 * @param {string} wantIp - Expected server IP
 * @param {number} wantPort - Expected server port
 * @returns {Object|null} Processed server data or null if not found
 */
function fetchBMExact_(url, ua, wantIp, wantPort) {
  const r = rateLimitedFetch_(url, req_(ua, 15000));
  if (r.getResponseCode() !== 200) {
    throw new APIError("BM HTTP " + r.getResponseCode(), "BattleMetrics", r.getResponseCode());
  }
  
  try {
    const data = JSON.parse(r.getContentText() || "{}");
    return processBattleMetricsData_(data, wantIp, wantPort);
  } catch (e) {
    if (e instanceof ValidationError) {
      Logger.log(`BattleMetrics validation error: ${e.message} (${e.field})`);
      return null;
    }
    throw e;
  }
}

/* ---------- utils ---------- */
/**
 * Enhanced rate-limited fetch with retries and exponential backoff
 * @param {string} url - URL to fetch
 * @param {Object} options - Fetch options
 * @param {number} maxRetries - Maximum number of retries
 * @param {number} baseDelayMs - Base delay between retries in milliseconds
 * @returns {GoogleAppsScript.URL_Fetch.HTTPResponse}
 * @throws {APIError} When the request fails after all retries
 */
function rateLimitedFetch_(url, options, maxRetries = 3, baseDelayMs = 1000) {
  const source = url.includes('battlemetrics') ? 'BattleMetrics' :
                url.includes('trackyserver') ? 'TrackyServer' :
                url.includes('gametracker') ? 'GameTracker' : 'Unknown';
                
  for (let i = 0; i < maxRetries; i++) {
    try {
      metrics.markStart(`fetch_${source}`);
      const response = UrlFetchApp.fetch(url, options);
      const statusCode = response.getResponseCode();
      metrics.markEnd(`fetch_${source}`);

      // Success case
      if (statusCode >= 200 && statusCode < 300) {
        return response;
      }

      // Handle specific status codes
      switch (statusCode) {
        case 429: // Too Many Requests
          const retryAfter = response.getHeaders()['retry-after'];
          const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : baseDelayMs * Math.pow(2, i);
          Utilities.sleep(waitTime);
          continue;
        case 404:
          throw new APIError(`Resource not found: ${url}`, source, statusCode);
        case 403:
          throw new APIError(`Access forbidden: ${url}`, source, statusCode);
        default:
          if (i === maxRetries - 1) {
            throw new APIError(`Failed with status ${statusCode}`, source, statusCode);
          }
          Utilities.sleep(baseDelayMs * Math.pow(2, i)); // Exponential backoff
      }
    } catch (e) {
      if (e instanceof APIError) throw e;
      if (i === maxRetries - 1) {
        throw new APIError(`Network error: ${e.message}`, source, 0);
      }
      Utilities.sleep(baseDelayMs * Math.pow(2, i));
    }
  }
  throw new APIError(`Max retries exceeded for ${url}`, source, 0);
}

/**
 * Advanced caching system with versioning and soft expiration
 */
const CacheManager = {
  VERSION: '1.0',
  
  /**
   * Generate a cache key with version
   * @param {string} key - Base cache key
   * @returns {string} Versioned cache key
   */
  _makeKey: function(key) {
    return `v${this.VERSION}_${key}`;
  },

  /**
   * Get cached data with soft expiration
   * @param {string} key - Cache key
   * @param {number} hardTTL - Hard TTL in seconds
   * @param {number} softTTL - Soft TTL in seconds
   * @returns {Object|null} Cached data or null
   */
  get: function(key, hardTTL = 300, softTTL = 60) {
    const cache = CacheService.getScriptCache();
    const vKey = this._makeKey(key);
    const data = cache.get(vKey);
    
    if (!data) return null;
    
    try {
      const parsed = JSON.parse(data);
      const age = (Date.now() - parsed.timestamp) / 1000;
      
      // If within soft TTL, return immediately
      if (age <= softTTL) {
        return parsed.data;
      }
      
      // If within hard TTL, trigger background refresh and return stale data
      if (age <= hardTTL) {
        // Flag for background refresh
        parsed.needsRefresh = true;
        return parsed.data;
      }
      
      // Beyond hard TTL
      cache.remove(vKey);
      return null;
    } catch (e) {
      Logger.log(`Cache parse error: ${e.message}`);
      cache.remove(vKey);
      return null;
    }
  },

  /**
   * Set cache data with metadata
   * @param {string} key - Cache key
   * @param {*} data - Data to cache
   * @param {number} ttl - Cache TTL in seconds
   */
  set: function(key, data, ttl = 300) {
    const cache = CacheService.getScriptCache();
    const vKey = this._makeKey(key);
    const wrapped = {
      timestamp: Date.now(),
      data: data,
      version: this.VERSION
    };
    cache.put(vKey, JSON.stringify(wrapped), ttl);
  }
};

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
/**
 * Advanced data validation and sanitization
 */
const DataValidator = {
  /**
   * Player name validation rules
   */
  PLAYER_RULES: {
    maxLength: 48,
    minLength: 1,
    bannedNames: new Set([
      'rank',
      'name',
      'score',
      'time played',
      'online players'
    ]),
    invalidPatterns: [
      /^\s*$/, // empty or whitespace
      /^[^a-z0-9]+$/i, // no alphanumeric characters
      /[\u0000-\u001F\u007F-\u009F]/, // control characters
    ]
  },

  /**
   * Sanitize and normalize text input
   * @param {string} input - Raw input text
   * @returns {string} Sanitized text
   */
  sanitizeText: function(input) {
    let text = decode_(String(input || ""));
    
    // Remove control characters
    text = text.replace(/[\u0000-\u001F\u007F-\u009F]/g, '');
    
    // Normalize whitespace
    text = text.replace(/\s+/g, ' ').trim();
    
    // Remove potentially dangerous characters
    text = text.replace(/[<>]/g, '');
    
    return text;
  },

  /**
   * Validate player name
   * @param {string} name - Player name to validate
   * @returns {boolean} True if valid
   */
  isValidPlayerName: function(name) {
    if (!name) return false;
    
    const normalized = name.toLowerCase();
    
    // Check length
    if (name.length > this.PLAYER_RULES.maxLength || 
        name.length < this.PLAYER_RULES.minLength) {
      return false;
    }
    
    // Check banned names
    if (this.PLAYER_RULES.bannedNames.has(normalized)) {
      return false;
    }
    
    // Check invalid patterns
    for (const pattern of this.PLAYER_RULES.invalidPatterns) {
      if (pattern.test(name)) {
        return false;
      }
    }
    
    return true;
  },

  /**
   * Normalize text for comparison
   * @param {string} text - Text to normalize
   * @returns {string} Normalized text
   */
  normalize: function(text) {
    let t = this.sanitizeText(text).toLowerCase();
    
    // Strip one leading [TAG] or (TAG)
    t = t.replace(/^[\[\(][^\]\)]+[\]\)]\s*/, '');
    
    // Additional normalization rules
    t = t.replace(/[^\w\s-]/g, ''); // Remove special characters
    t = t.replace(/\s+/g, ' '); // Normalize spaces
    
    return t;
  }
};

// Update existing functions to use new DataValidator
function clean_(s) {
  return DataValidator.sanitizeText(s);
}

function valid_(name) {
  return DataValidator.isValidPlayerName(name);
}

function unique_(arr) {
  const seen = new Set();
  const out = [];
  for (const n of arr) {
    const k = DataValidator.normalize(n);
    if (!seen.has(k)) {
      seen.add(k);
      out.push(n);
    }
  }
  return out;
}

function normKey_(s) {
  return DataValidator.normalize(s);
}
function durFmt_(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), ss = s % 60;
  return (h ? (h + "h ") : "") + (m ? (m + "m ") : "") + ss + "s";
}
/**
 * Enhanced state management with validation and versioning
 */
const StateManager = {
  VERSION: '1.0',
  
  /**
   * State schemas for validation
   */
  schemas: {
    SOVR_STATE_STABLE: {
      required: ['map', 'players', 'joinTimes'],
      types: {
        map: 'string',
        players: 'array',
        joinTimes: 'object'
      }
    },
    SOVR_PENDING_MAP: {
      required: ['pendingMap', 'seenCount'],
      types: {
        pendingMap: 'string',
        seenCount: 'number'
      }
    }
  },

  /**
   * Validate state object against schema
   * @param {string} key - State key
   * @param {Object} state - State object to validate
   * @throws {ValidationError} If validation fails
   */
  _validate: function(key, state) {
    const schema = this.schemas[key];
    if (!schema) return; // No schema defined for this key
    
    // Check required fields
    for (const field of schema.required) {
      if (!(field in state)) {
        throw new ValidationError(`Missing required field: ${field}`, field);
      }
    }
    
    // Check types
    for (const [field, type] of Object.entries(schema.types)) {
      if (field in state) {
        const actualType = Array.isArray(state[field]) ? 'array' : typeof state[field];
        if (actualType !== type) {
          throw new ValidationError(`Invalid type for ${field}: expected ${type}, got ${actualType}`, field);
        }
      }
    }
  },

  /**
   * Read state with validation
   * @param {string} key - State key
   * @returns {Object} State object
   * @throws {StateError} If state is invalid or corrupted
   */
  read: function(key) {
    try {
      const props = PropertiesService.getScriptProperties();
      const raw = props.getProperty(key);
      if (!raw) return {};
      
      const state = JSON.parse(raw);
      this._validate(key, state);
      return state;
    } catch (e) {
      if (e instanceof ValidationError) {
        // State is corrupted, reset it
        this.write(key, {});
        throw new StateError(`Invalid state for ${key}: ${e.message}`, {});
      }
      Logger.log(`State read error for ${key}: ${e.message}`);
      return {};
    }
  },

  /**
   * Write state with validation
   * @param {string} key - State key
   * @param {Object} state - State object to write
   * @throws {ValidationError} If state is invalid
   */
  write: function(key, state) {
    try {
      this._validate(key, state);
      const props = PropertiesService.getScriptProperties();
      const wrapped = {
        data: state,
        version: this.VERSION,
        timestamp: Date.now()
      };
      props.setProperty(key, JSON.stringify(wrapped));
    } catch (e) {
      throw new StateError(`Failed to write state for ${key}: ${e.message}`, state);
    }
  },

  /**
   * Update state atomically
   * @param {string} key - State key
   * @param {Function} updateFn - Function that takes old state and returns new state
   * @throws {StateError} If update fails
   */
  update: function(key, updateFn) {
    const lock = LockService.getScriptLock();
    try {
      lock.waitLock(10000);
      const oldState = this.read(key);
      const newState = updateFn(oldState);
      this.write(key, newState);
    } catch (e) {
      throw new StateError(`Failed to update state for ${key}: ${e.message}`, {});
    } finally {
      lock.releaseLock();
    }
  }
};

// Update old state functions to use new StateManager
function readState_(k) {
  return StateManager.read(k);
}

function writeState_(k, obj) {
  return StateManager.write(k, obj);
}
