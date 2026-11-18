require('dotenv').config();
const express = require('express');
const Database = require('better-sqlite3');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const AIS_WS_URL = process.env.AIS_WS_URL || 'wss://stream.aisstream.io/v0/stream';
const AIS_API_KEY = process.env.AIS_API_KEY;

// 1. Initialize SQLite Database
const db = new Database(path.join(__dirname, 'fleet.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS vessels (
    imo INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    mmsi TEXT,
    lat REAL,
    lon REAL,
    sog REAL,
    cog REAL,
    heading INTEGER,
    nav_status INTEGER,
    destination TEXT,
    draught REAL,
    last_seen TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_vessels_mmsi ON vessels(mmsi);
`);

const FLEET_SEED = [
  { name: "AJWAD", imo: 9830795 }, { name: "AMAD", imo: 9783693 },
  { name: "AMJAD", imo: 9779800 }, { name: "AMZAN", imo: 9693745 },
  { name: "ASLAF", imo: 9779824 }, { name: "BAYRAQ", imo: 9829643 },
  { name: "BURQAN", imo: 9926685 }, { name: "DALIL", imo: 9733806 },
  { name: "DIAMOND SHINE", imo: 9384239 }, { name: "DILAM", imo: 9448700 },
  { name: "DORRA", imo: 9386964 }, { name: "EMERALD SHINE", imo: 9384227 },
  { name: "FARHAH", imo: 9484742 }, { name: "GHAZAL", imo: 9387009 },
  { name: "GHINAH", imo: 9484728 }, { name: "GOLD SHINE", imo: 9519717 },
  { name: "HABARI", imo: 9386940 }, { name: "HAZM", imo: 9829631 },
  { name: "JANA", imo: 9386938 }, { name: "KAHLA", imo: 9386952 },
  { name: "KASSAB", imo: 9783710 }, { name: "KHURAIS", imo: 9783679 },
  { name: "KHUZAMA", imo: 9384215 }, { name: "LAWHAH", imo: 9783681 },
  { name: "LAYLA", imo: 9336098 }, { name: "LAYNAH", imo: 9926697 },
  { name: "MAHARAH", imo: 9779812 }, { name: "MAJRA", imo: 9830812 },
  { name: "NAFEES", imo: 9830800 }, { name: "NIBAN", imo: 9484716 },
  { name: "NISALAH", imo: 9484730 }, { name: "PLATINUM SHINE", imo: 9378541 },
  { name: "QAMRAN", imo: 9783708 }, { name: "QASBA", imo: 9853589 },
  { name: "RAWAQ", imo: 9853591 }, { name: "RAYAH", imo: 9897743 },
  { name: "RIMTHAN", imo: 9779836 }, { name: "SAHBA", imo: 9388273 },
  { name: "SALAM", imo: 9733818 }, { name: "SAMHA", imo: 9858450 },
  { name: "SANAM", imo: 9858486 }, { name: "SAPHIRE SHINE", imo: 9384198 },
  { name: "SHADEN", imo: 9779848 }, { name: "SIDR", imo: 9854715 },
  { name: "SILVER SHINE", imo: 9421415 }, { name: "TITANIUM SHINE", imo: 9519482 },
  { name: "UQAB", imo: 9830824 }, { name: "WEDYAN", imo: 9524970 },
  { name: "ZAYNAH", imo: 9854703 }
];

const insertSeed = db.prepare(`
  INSERT OR IGNORE INTO vessels (imo, name) VALUES (@imo, @name)
`);
const seedTx = db.transaction((vessels) => {
  for (const v of vessels) insertSeed.run(v);
});
seedTx(FLEET_SEED);

// Lookup maps for fast in-memory matching by IMO, MMSI, or normalized ShipName
const imoToVessel = new Map(FLEET_SEED.map(v => [v.imo, v]));
const nameToImo = new Map(FLEET_SEED.map(v => [v.name.toUpperCase(), v.imo]));
const mmsiToImo = new Map();

// Restore any previously discovered MMSIs from SQLite
db.prepare('SELECT imo, mmsi FROM vessels WHERE mmsi IS NOT NULL').all().forEach(row => {
  mmsiToImo.set(row.mmsi, row.imo);
});

const updateStaticStmt = db.prepare(`
  UPDATE vessels
  SET mmsi = COALESCE(@mmsi, mmsi),
      destination = COALESCE(NULLIF(TRIM(@destination), ''), destination),
      draught = COALESCE(@draught, draught),
      lat = COALESCE(@lat, lat),
      lon = COALESCE(@lon, lon),
      last_seen = @last_seen
  WHERE imo = @imo
`);

const updatePositionByImoStmt = db.prepare(`
  UPDATE vessels
  SET mmsi = COALESCE(@mmsi, mmsi),
      lat = @lat,
      lon = @lon,
      sog = @sog,
      cog = @cog,
      heading = @heading,
      nav_status = @nav_status,
      last_seen = @last_seen
  WHERE imo = @imo
`);

function normalizeVesselName(rawName) {
  if (!rawName) return '';
  // Trim padding/null bytes and strip common "BAHRI " prefix if transmitted
  return rawName
    .replace(/@+/g, '')
    .trim()
    .toUpperCase()
    .replace(/^BAHRI\s+/, '');
}

// 2. AIS WebSocket Stream Client
let wsStatus = 'disconnected';
let packetsReceived = 0;
let reconnectDelay = 2000;

function connectAISStream() {
  if (!AIS_API_KEY) {
    console.warn('[AIS] Missing AIS_API_KEY in .env — stream disabled.');
    wsStatus = 'missing_api_key';
    return;
  }

  wsStatus = 'connecting';
  const ws = new WebSocket(AIS_WS_URL, { perMessageDeflate: true });

  ws.on('open', () => {
    wsStatus = 'connected';
    reconnectDelay = 2000;
    console.log(`[AIS] Connected to ${AIS_WS_URL}`);

    const subscription = {
      APIKey: AIS_API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FilterMessageTypes: ['PositionReport', 'ShipStaticData', 'StandardClassBPositionReport']
    };

    ws.send(JSON.stringify(subscription));
  });

  ws.on('message', (rawData) => {
    try {
      const packet = JSON.parse(rawData.toString('utf8'));

      // Log explicit API key or subscription errors from the server
      if (packet.error) {
        console.error('[AIS] Stream Server Error:', packet.error);
        wsStatus = `error: ${packet.error}`;
        return;
      }

      packetsReceived++;
      if (packetsReceived % 1000 === 0) {
        console.log(`[AIS] Processed ${packetsReceived.toLocaleString()} packets | Matched vessels: ${mmsiToImo.size}/49`);
      }

      const meta = packet.MetaData || {};
      const mmsi = meta.MMSI ? String(meta.MMSI).padStart(9, '0') : null;
      const cleanName = normalizeVesselName(meta.ShipName);
      const now = meta.time_utc || new Date().toISOString();

      // Resolve IMO via known MMSI, Static Data IMO, or MetaData.ShipName
      let matchedImo = mmsi ? mmsiToImo.get(mmsi) : undefined;

      if (!matchedImo && packet.MessageType === 'ShipStaticData' && packet.Message?.ShipStaticData) {
        const staticImo = Number(packet.Message.ShipStaticData.ImoNumber);
        if (imoToVessel.has(staticImo)) {
          matchedImo = staticImo;
        }
      }

      if (!matchedImo && cleanName && nameToImo.has(cleanName)) {
        matchedImo = nameToImo.get(cleanName);
      }

      if (!matchedImo) return;

      // Record MMSI mapping once resolved
      if (mmsi && !mmsiToImo.has(mmsi)) {
        mmsiToImo.set(mmsi, matchedImo);
        const vessel = imoToVessel.get(matchedImo);
        console.log(`[AIS] Matched ${vessel.name} (IMO ${matchedImo}) -> MMSI ${mmsi}`);
      }

      // Update Static Data
      if (packet.MessageType === 'ShipStaticData' && packet.Message?.ShipStaticData) {
        const staticData = packet.Message.ShipStaticData;
        updateStaticStmt.run({
          imo: matchedImo,
          mmsi,
          destination: staticData.Destination || null,
          draught: staticData.MaximumStaticDraught || null,
          lat: meta.latitude ?? null,
          lon: meta.longitude ?? null,
          last_seen: now
        });
      }

      // Update Position Report
      if (packet.Message?.PositionReport) {
        const pos = packet.Message.PositionReport;
        updatePositionByImoStmt.run({
          imo: matchedImo,
          mmsi,
          lat: pos.Latitude ?? meta.latitude,
          lon: pos.Longitude ?? meta.longitude,
          sog: pos.Sog ?? null,
          cog: pos.Cog ?? null,
          heading: pos.TrueHeading === 511 ? null : pos.TrueHeading,
          nav_status: pos.NavigationalStatus ?? null,
          last_seen: now
        });
      }
    } catch (err) {
      // Ignore non-JSON frames
    }
  });

  ws.on('close', (code) => {
    wsStatus = `disconnected (${code})`;
    console.warn(`[AIS] Stream closed (${code}). Reconnecting in ${reconnectDelay / 1000}s...`);
    setTimeout(connectAISStream, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 60000);
  });

  ws.on('error', (err) => {
    console.error('[AIS] WebSocket error:', err.message);
  });
}

connectAISStream();

// 3. REST API & Static Frontend
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/api/fleet', (req, res) => {
  const vessels = db.prepare('SELECT * FROM vessels ORDER BY name ASC').all();
  const locatedCount = vessels.filter(v => v.lat !== null && v.lon !== null).length;

  res.json({
    stream_status: wsStatus,
    packets_received: packetsReceived,
    total_vessels: vessels.length,
    located_count: locatedCount,
    discovered_mmsis: mmsiToImo.size,
    server_time: new Date().toISOString(),
    vessels
  });
});

app.listen(PORT, () => {
  console.log(`Bahri Fleet Tracker running at http://localhost:${PORT}`);
});