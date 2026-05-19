require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const os = require('os');
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const mime = require('mime-types');

const {
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE,
  PORT = 3000,
} = process.env;

if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
  console.error('❌ Missing R2 env vars. Check .env.example');
  process.exit(1);
}

// ── Fast HTTP(S) agents with keep-alive ──
const httpAgent  = new http.Agent({  keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });

// ── S3/R2 client ──
const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  maxAttempts: 5,
  requestHandler: new NodeHttpHandler({
    httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 256 }),
    connectionTimeout: 15_000,
    socketTimeout:    600_000,
  }),
});

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ── Job store ──
const jobs = new Map();
function newJob() {
  const id = crypto.randomBytes(8).toString('hex');
  const job = {
    id,
    status: 'pending',
    phase: '',
    loaded: 0,
    total: 0,
    percent: 0,
    result: null,
    error: null,
    listeners: new Set(),
    createdAt: Date.now(),
    lastEmitAt: Date.now(),
  };
  jobs.set(id, job);
  setTimeout(() => jobs.delete(id), 2 * 60 * 60 * 1000);
  return job;
}

function emit(job, patch = {}) {
  Object.assign(job, patch);
  job.lastEmitAt = Date.now();
  const payload = JSON.stringify({
    status: job.status,
    phase: job.phase,
    loaded: job.loaded,
    total: job.total,
    percent: job.percent,
    result: job.result,
    error: job.error,
  });
  for (const res of job.listeners) {
    try { res.write(`data: ${payload}\n\n`); } catch (_) {}
  }
}

setInterval(() => {
  const now = Date.now();
  for (const job of jobs.values()) {
    if (job.listeners.size === 0) continue;
    for (const res of job.listeners) {
      try { res.write(`: ping ${now}\n\n`); } catch (_) {}
    }
  }
}, 15_000).unref();

function pickFilename(urlStr, fallback) {
  try {
    const u = new URL(urlStr);
    const last = decodeURIComponent(u.pathname.split('/').filter(Boolean).pop() || '');
    if (last) return last;
  } catch (_) {}
  return fallback || `file-${Date.now()}`;
}

function stripExt(name) {
  const i = name.lastIndexOf('.');
  return i > 0 ? name.slice(0, i) : name;
}

function isHlsUrl(url, contentType = '') {
  if (/\.m3u8(\?|$)/i.test(url)) return true;
  if (/mpegurl|x-mpegurl/i.test(contentType)) return true;
  return false;
}

function hasFfmpeg() {
  return new Promise((resolve) => {
    const p = spawn('ffmpeg', ['-version']);
    p.on('error', () => resolve(false));
    p.on('exit', (code) => resolve(code === 0));
  });
}

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ── Download URL to file with progress ──
function downloadToFile(url, destPath, onProgress) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      agent: url.startsWith('https') ? httpsAgent : httpAgent,
      headers: { 'User-Agent': 'r2-uploader/1.5', 'Accept': '*/*' },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = new URL(res.headers.location, url).toString();
        downloadToFile(next, destPath, onProgress).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200 && res.statusCode !== 206) {
        res.resume();
        return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
      }
      const total = Number(res.headers['content-length']) || 0;
      const contentType = res.headers['content-type'] || 'application/octet-stream';
      let loaded = 0;
      res.on('data', (chunk) => {
        loaded += chunk.length;
        if (onProgress) onProgress(loaded, total);
      });
      const ws = fs.createWriteStream(destPath);
      pipeline(res, ws)
        .then(() => resolve({ total: total || loaded, contentType }))
        .catch(reject);
    });
    req.on('error', reject);
    req.setTimeout(180_000, () => req.destroy(new Error('Download timeout')));
  });
}

// ── Download URL to text (for m3u8) ──
function fetchText(url, attempt = 1) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      agent: url.startsWith('https') ? httpsAgent : httpAgent,
      headers: { 'User-Agent': 'r2-uploader/1.5', 'Accept': '*/*' },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = new URL(res.headers.location, url).toString();
        fetchText(next).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`Fetch failed: HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        resolve({
          text: buf.toString('utf8'),
          finalUrl: url,
          contentType: res.headers['content-type'] || '',
        });
      });
      res.on('error', reject);
    });
    req.on('error', async (err) => {
      if (attempt < 3) { await sleep(800 * attempt); fetchText(url, attempt + 1).then(resolve, reject); }
      else reject(err);
    });
    req.setTimeout(60_000, () => req.destroy(new Error('Fetch timeout')));
  });
}

// ── ffmpeg: faststart remux ──
function ffmpegFaststart(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const args = ['-y', '-i', inputPath, '-c', 'copy', '-movflags', '+faststart', outputPath];
    const p = spawn('ffmpeg', args);
    p.stderr.on('data', () => {});
    p.on('error', reject);
    p.on('exit', (code) => code === 0 ? resolve(outputPath) : reject(new Error(`ffmpeg exited ${code}`)));
  });
}

// ── ffmpeg: produce HLS (master.m3u8 + .ts segments) from local file ──
function ffmpegHls(inputPath, outDir) {
  return new Promise((resolve, reject) => {
    const playlist = path.join(outDir, 'master.m3u8');
    const args = [
      '-y',
      '-i', inputPath,
      '-c', 'copy',
      '-map', '0',
      '-f', 'hls',
      '-hls_time', '6',
      '-hls_list_size', '0',
      '-hls_playlist_type', 'vod',
      '-hls_segment_filename', path.join(outDir, 'seg_%05d.ts'),
      playlist,
    ];
    const p = spawn('ffmpeg', args);
    let err = '';
    p.stderr.on('data', (d) => { err += d.toString(); });
    p.on('error', reject);
    p.on('exit', (code) => {
      if (code === 0) return resolve(playlist);
      const args2 = [
        '-y',
        '-i', inputPath,
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-f', 'hls',
        '-hls_time', '6',
        '-hls_list_size', '0',
        '-hls_playlist_type', 'vod',
        '-hls_segment_filename', path.join(outDir, 'seg_%05d.ts'),
        playlist,
      ];
      const p2 = spawn('ffmpeg', args2);
      p2.stderr.on('data', () => {});
      p2.on('error', reject);
      p2.on('exit', (c2) => c2 === 0 ? resolve(playlist) : reject(new Error(`ffmpeg HLS failed: ${err.slice(-400)}`)));
    });
  });
}

// ── ffmpeg: HLS (.m3u8 URL or local) → MP4 ──
function ffmpegHlsToMp4(inputUrlOrPath, outputPath, faststart = true) {
  return new Promise((resolve, reject) => {
    const args = [
      '-y',
      '-headers', 'User-Agent: r2-uploader/1.5\r\n',
      '-i', inputUrlOrPath,
      '-c', 'copy',
      '-bsf:a', 'aac_adtstoasc',
    ];
    if (faststart) args.push('-movflags', '+faststart');
    args.push(outputPath);
    const p = spawn('ffmpeg', args);
    let err = '';
    p.stderr.on('data', (d) => { err += d.toString(); });
    p.on('error', reject);
    p.on('exit', (code) => {
      if (code === 0) return resolve(outputPath);
      // Retry with re-encode
      const args2 = [
        '-y',
        '-headers', 'User-Agent: r2-uploader/1.5\r\n',
        '-i', inputUrlOrPath,
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
      ];
      if (faststart) args2.push('-movflags', '+faststart');
      args2.push(outputPath);
      const p2 = spawn('ffmpeg', args2);
      let err2 = '';
      p2.stderr.on('data', (d) => { err2 += d.toString(); });
      p2.on('error', reject);
      p2.on('exit', (c2) => c2 === 0
        ? resolve(outputPath)
        : reject(new Error(`ffmpeg HLS→MP4 failed: ${(err2 || err).slice(-400)}`)));
    });
  });
}

// ── Upload helpers with retry ──
async function uploadFileToR2(localPath, key, contentType, onProgress, attempt = 1) {
  const stat = fs.statSync(localPath);
  try {
    const body = fs.createReadStream(localPath, { highWaterMark: 1024 * 1024 });
    const uploader = new Upload({
      client: s3,
      params: {
        Bucket: R2_BUCKET,
        Key: key,
        Body: body,
        ContentType: contentType,
        ContentDisposition: `attachment; filename="${path.basename(key)}"`,
      },
      queueSize: 4,
      partSize: 8 * 1024 * 1024,
      leavePartsOnError: false,
    });
    if (onProgress) {
      uploader.on('httpUploadProgress', (p) => onProgress(p.loaded || 0, p.total || stat.size));
    }
    await uploader.done();
    return stat.size;
  } catch (err) {
    if (attempt < 3) {
      console.warn(`uploadFileToR2 retry ${attempt} for ${key}: ${err.message}`);
      await sleep(1500 * attempt);
      return uploadFileToR2(localPath, key, contentType, onProgress, attempt + 1);
    }
    throw err;
  }
}

async function uploadBufferToR2(buf, key, contentType, cacheControl) {
  const up = new Upload({
    client: s3,
    params: {
      Bucket: R2_BUCKET,
      Key: key,
      Body: buf,
      ContentType: contentType,
      ...(cacheControl ? { CacheControl: cacheControl } : {}),
    },
  });
  await up.done();
}

async function uploadDirToR2(dir, keyPrefix, onTick) {
  const entries = await fsp.readdir(dir);
  const files = entries.map((name) => ({
    name,
    full: path.join(dir, name),
    size: fs.statSync(path.join(dir, name)).size,
  }));
  const totalBytes = files.reduce((a, f) => a + f.size, 0);

  const loadedPerFile = new Map(files.map((f) => [f.name, 0]));
  const reportTick = () => {
    let done = 0;
    for (const v of loadedPerFile.values()) done += v;
    if (onTick) onTick(done, totalBytes);
  };

  const CONCURRENCY = 6;

  async function uploadOne(f, attempt = 1) {
    try {
      const ct = f.name.endsWith('.m3u8')
        ? 'application/vnd.apple.mpegurl'
        : f.name.endsWith('.ts')
          ? 'video/mp2t'
          : (mime.lookup(f.name) || 'application/octet-stream');
      const key = `${keyPrefix}/${f.name}`;
      const body = fs.createReadStream(f.full, { highWaterMark: 1024 * 1024 });
      const up = new Upload({
        client: s3,
        params: {
          Bucket: R2_BUCKET,
          Key: key,
          Body: body,
          ContentType: ct,
          CacheControl: 'public, max-age=31536000, immutable',
        },
        queueSize: 4,
        partSize: 8 * 1024 * 1024,
        leavePartsOnError: false,
      });
      up.on('httpUploadProgress', (p) => {
        loadedPerFile.set(f.name, p.loaded || 0);
        reportTick();
      });
      await up.done();
      loadedPerFile.set(f.name, f.size);
      reportTick();
    } catch (err) {
      if (attempt < 3) {
        console.warn(`segment retry ${attempt} for ${f.name}: ${err.message}`);
        await sleep(1500 * attempt);
        loadedPerFile.set(f.name, 0);
        reportTick();
        return uploadOne(f, attempt + 1);
      }
      throw err;
    }
  }

  async function worker(queue) {
    while (queue.length) {
      const f = queue.shift();
      if (!f) break;
      await uploadOne(f);
    }
  }

  const queue = files.slice();
  const workers = Array.from({ length: Math.min(CONCURRENCY, files.length) }, () => worker(queue));
  await Promise.all(workers);
  return { totalBytes, count: files.length };
}

// ═══════════════════════════════════════════════════════════════
// HLS MIRROR helpers
// ═══════════════════════════════════════════════════════════════

function isMasterPlaylist(text) {
  return /#EXT-X-STREAM-INF/i.test(text);
}

function resolveUrl(base, ref) {
  try { return new URL(ref, base).toString(); } catch { return ref; }
}

function localNameFor(url, kind, index) {
  const u = (() => { try { return new URL(url); } catch { return null; } })();
  const pathname = u ? u.pathname : url;
  const ext = (pathname.match(/\.[A-Za-z0-9]+$/) || [''])[0].toLowerCase();
  const base = path.basename(pathname).replace(/[^A-Za-z0-9._-]+/g, '_');
  const hash = crypto.createHash('sha1').update(url).digest('hex').slice(0, 8);
  if (kind === 'segment') {
    const safeExt = ext || '.ts';
    return `seg_${String(index).padStart(5, '0')}_${hash}${safeExt}`;
  }
  if (kind === 'key') return `key_${hash}${ext || '.key'}`;
  if (kind === 'map') return `map_${hash}${ext || '.mp4'}`;
  if (kind === 'variant') return `variant_${hash}.m3u8`;
  return `${base || hash}${ext}`;
}

function guessContentType(name) {
  if (name.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
  if (name.endsWith('.ts'))   return 'video/mp2t';
  if (name.endsWith('.m4s'))  return 'video/iso.segment';
  if (name.endsWith('.mp4'))  return 'video/mp4';
  if (name.endsWith('.aac'))  return 'audio/aac';
  if (name.endsWith('.vtt'))  return 'text/vtt';
  if (name.endsWith('.key'))  return 'application/octet-stream';
  return mime.lookup(name) || 'application/octet-stream';
}

function downloadBuffer(url, attempt = 1) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      agent: url.startsWith('https') ? httpsAgent : httpAgent,
      headers: { 'User-Agent': 'r2-uploader/1.5', 'Accept': '*/*' },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = new URL(res.headers.location, url).toString();
        downloadBuffer(next).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200 && res.statusCode !== 206) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      let size = 0;
      res.on('data', (c) => { chunks.push(c); size += c.length; });
      res.on('end', () => resolve({ buffer: Buffer.concat(chunks), size }));
      res.on('error', reject);
    });
    req.on('error', async (err) => {
      if (attempt < 3) { await sleep(800 * attempt); downloadBuffer(url, attempt + 1).then(resolve, reject); }
      else reject(err);
    });
    req.setTimeout(180_000, () => req.destroy(new Error('Download timeout')));
  });
}

async function mirrorHlsToR2(rootUrl, keyPrefix, onTick) {
  const visited = new Map();
  let segmentCount = 0;
  let totalBytes = 0;
  const uploadedRes = new Map();

  let approxBytes = 0;
  let approxTotal = 0;
  let resourcesSeen = 0;
  let resourcesDone = 0;

  const tick = () => {
    if (!onTick) return;
    const denom = approxTotal > 0 ? approxTotal : Math.max(resourcesSeen, 1);
    const num   = approxTotal > 0 ? approxBytes : resourcesDone;
    onTick(num, denom);
  };

  async function processPlaylist(playlistUrl, isRoot) {
    if (visited.has(playlistUrl)) return visited.get(playlistUrl);

    const localName = isRoot ? 'master.m3u8' : localNameFor(playlistUrl, 'variant', 0);
    visited.set(playlistUrl, localName);

    const { text, finalUrl } = await fetchText(playlistUrl);
    const baseUrl = finalUrl || playlistUrl;
    const lines = text.split(/\r?\n/);
    const rewritten = [];

    const tasks = [];
    const isMaster = isMasterPlaylist(text);

    for (let i = 0; i < lines.length; i++) {
      const raw = lines[i];
      const line = raw.trim();

      if (!line) { rewritten.push(raw); continue; }

      if (line.startsWith('#')) {
        let newLine = raw;
        const tagMatches = [
          /#EXT-X-KEY:.*?URI="([^"]+)"/i,
          /#EXT-X-MAP:.*?URI="([^"]+)"/i,
          /#EXT-X-MEDIA:.*?URI="([^"]+)"/i,
          /#EXT-X-I-FRAME-STREAM-INF:.*?URI="([^"]+)"/i,
        ];
        for (const re of tagMatches) {
          const m = newLine.match(re);
          if (m) {
            const ref = m[1];
            const absUrl = resolveUrl(baseUrl, ref);
            const kind = /EXT-X-KEY/i.test(newLine) ? 'key'
                        : /EXT-X-MAP/i.test(newLine) ? 'map'
                        : 'variant';
            resourcesSeen++;
            if (kind === 'variant' && /\.m3u8/i.test(ref)) {
              tasks.push({
                kind: 'subplaylist',
                absUrl,
                replace: async () => processPlaylist(absUrl, false),
              });
              newLine = newLine.replace(re, (full, g1) => full.replace(`"${g1}"`, `"__SUB_${tasks.length - 1}__"`));
            } else {
              tasks.push({ kind, absUrl, isText: false });
              newLine = newLine.replace(re, (full, g1) => full.replace(`"${g1}"`, `"__RES_${tasks.length - 1}__"`));
            }
          }
        }
        rewritten.push(newLine);
        continue;
      }

      const absUrl = resolveUrl(baseUrl, line);
      resourcesSeen++;
      if (isMaster || /\.m3u8(\?|$)/i.test(line)) {
        tasks.push({
          kind: 'subplaylist',
          absUrl,
          replace: async () => processPlaylist(absUrl, false),
        });
        rewritten.push(`__SUB_${tasks.length - 1}__`);
      } else {
        tasks.push({ kind: 'segment', absUrl, isText: false });
        rewritten.push(`__RES_${tasks.length - 1}__`);
      }
    }

    const CONCURRENCY = 6;
    const resolved = new Array(tasks.length);

    let nextIdx = 0;
    async function workerLoop() {
      while (true) {
        const myIdx = nextIdx++;
        if (myIdx >= tasks.length) break;
        const t = tasks[myIdx];
        try {
          if (t.kind === 'subplaylist') {
            const subName = await t.replace();
            resolved[myIdx] = subName;
            resourcesDone++;
            tick();
          } else {
            if (uploadedRes.has(t.absUrl)) {
              resolved[myIdx] = uploadedRes.get(t.absUrl);
              resourcesDone++;
              tick();
              continue;
            }
            const idx = t.kind === 'segment' ? ++segmentCount : 0;
            const lname = localNameFor(t.absUrl, t.kind, idx);
            const { buffer, size } = await downloadBuffer(t.absUrl);
            totalBytes += size;
            approxTotal += size;
            const r2Key = `${keyPrefix}/${lname}`;
            await uploadBufferToR2(buffer, r2Key, guessContentType(lname),
              t.kind === 'segment' || t.kind === 'map' ? 'public, max-age=31536000, immutable' : 'public, max-age=300');
            approxBytes += size;
            uploadedRes.set(t.absUrl, lname);
            resolved[myIdx] = lname;
            resourcesDone++;
            tick();
          }
        } catch (err) {
          throw new Error(`Failed on ${t.absUrl}: ${err.message}`);
        }
      }
    }

    const workers = Array.from({ length: Math.min(CONCURRENCY, tasks.length || 1) }, workerLoop);
    await Promise.all(workers);

    const finalLines = rewritten.map((ln) => {
      let out = ln;
      out = out.replace(/__SUB_(\d+)__/g, (_, n) => resolved[Number(n)] || '');
      out = out.replace(/__RES_(\d+)__/g, (_, n) => resolved[Number(n)] || '');
      return out;
    });
    const finalText = finalLines.join('\n');

    await uploadBufferToR2(
      Buffer.from(finalText, 'utf8'),
      `${keyPrefix}/${localName}`,
      'application/vnd.apple.mpegurl',
      'public, max-age=60',
    );

    return localName;
  }

  const masterLocalName = await processPlaylist(rootUrl, true);
  return { masterKey: `${keyPrefix}/${masterLocalName}`, segmentCount, totalBytes };
}

// ═══════════════════════════════════════════════════════════════
// Main job runner
// ═══════════════════════════════════════════════════════════════
async function runUpload(job, { url, key, faststart, outMp4, outHls }) {
  const tmpRoot = path.join(os.tmpdir(), `r2-${job.id}`);
  await fsp.mkdir(tmpRoot, { recursive: true });
  const tmpIn   = path.join(tmpRoot, 'input');
  const tmpFast = path.join(tmpRoot, 'fast.mp4');
  const tmpMp4  = path.join(tmpRoot, 'converted.mp4');
  const hlsDir  = path.join(tmpRoot, 'hls');

  // Default: at least produce MP4 if user picked nothing
  if (!outMp4 && !outHls) outMp4 = true;

  try {
    const objectKey = (key && key.trim()) || pickFilename(url);
    const guessType = mime.lookup(objectKey) || 'application/octet-stream';
    const ffmpegAvailable = await hasFfmpeg();

    // Detect source type
    const sourceIsHls = isHlsUrl(url);

    const result = {
      success: true,
      bucket: R2_BUCKET,
      key: objectKey,
      sourceType: sourceIsHls ? 'hls' : 'file',
      mp4: false,
      hls: false,
      faststart: false,
      mp4Requested: !!outMp4,
      hlsRequested: !!outHls,
      faststartRequested: !!faststart,
    };

    // ════════════════════════════════════════════════════════════
    // BRANCH A: Source is HLS (.m3u8)
    // ════════════════════════════════════════════════════════════
    if (sourceIsHls) {
      // Progress band split
      let band;
      if (outMp4 && outHls)      band = [0, 50, 100];
      else                        band = [0, 100];

      // ── HLS mirror ──
      if (outHls) {
        const hlsPrefix = stripExt(objectKey) + '_hls';
        emit(job, { status: 'processing', phase: 'mirroring HLS playlist + segments to R2', percent: 0, loaded: 0, total: 0 });
        const span = outMp4 ? band[1] - band[0] : 100;
        const startPct = 0;
        const { segmentCount, totalBytes } = await mirrorHlsToR2(url, hlsPrefix, (done, total) => {
          const pct = total ? Math.floor(startPct + (done / total) * span) : startPct;
          emit(job, { loaded: done, total, percent: Math.min(pct, startPct + span - 1) });
        });
        result.hls = true;
        result.hlsPrefix = hlsPrefix;
        result.hlsSegmentCount = segmentCount;
        result.hlsTotalBytes = totalBytes;
        if (R2_PUBLIC_BASE) {
          result.hlsPlaylistUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${hlsPrefix}/master.m3u8`;
        }
        emit(job, { percent: outMp4 ? band[1] : 99 });
      }

      // ── HLS → MP4 ──
      if (outMp4) {
        if (!ffmpegAvailable) {
          if (outHls) {
            // HLS already done; just skip MP4 with reason
            result.skippedReason = 'ffmpeg not available for HLS→MP4 conversion';
          } else {
            throw new Error('HLS → MP4 အတွက် ffmpeg မရှိပါ။ HLS checkbox ကိုသာ ဖွင့်ပြီး mirror လုပ်နိုင်ပါတယ်။');
          }
        } else {
          const startPct = outHls ? band[1] : band[0];
          const endPct   = outHls ? band[2] : band[1];

          emit(job, { status: 'processing', phase: 'converting HLS → MP4 (ffmpeg)', percent: startPct, loaded: 0, total: 0 });
          await ffmpegHlsToMp4(url, tmpMp4, !!faststart);

          const mp4Key = /\.(mp4|m4v|mov)$/i.test(objectKey) ? objectKey : (stripExt(objectKey) + '.mp4');
          const mp4Stat = fs.statSync(tmpMp4);

          emit(job, { status: 'uploading', phase: 'uploading MP4 to R2', percent: startPct, loaded: 0, total: mp4Stat.size });
          const span = Math.max(1, endPct - startPct);
          await uploadFileToR2(tmpMp4, mp4Key, 'video/mp4', (loaded, total) => {
            const pct = total ? Math.floor(startPct + (loaded / total) * span) : startPct;
            emit(job, { loaded, total, percent: Math.min(pct, endPct - 1) });
          });
          result.mp4 = true;
          result.mp4Key = mp4Key;
          result.contentType = 'video/mp4';
          result.contentLength = mp4Stat.size;
          result.faststart = !!faststart;
          if (R2_PUBLIC_BASE) {
            result.publicUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${encodeURIComponent(mp4Key)}`;
          }
          result.key = mp4Key;
        }
      }

      emit(job, { status: 'done', phase: 'completed', percent: 100, result });
      return;
    }

    // ════════════════════════════════════════════════════════════
    // BRANCH B: Source is regular file (mp4/etc.)
    // ════════════════════════════════════════════════════════════
    const isMp4Like = /\.(mp4|m4v|mov)$/i.test(objectKey) || /mp4|quicktime|m4v/i.test(guessType);
    const wantFast = !!faststart && outMp4 && isMp4Like && ffmpegAvailable;
    const wantHls  = !!outHls && ffmpegAvailable;
    const needLocalFile = wantFast || wantHls;

    // If user wants neither MP4 nor any local processing and no HLS, just stream-upload as raw MP4.
    // If user wants only HLS but ffmpeg not available, we still must download to do nothing useful — error out.
    if (outHls && !ffmpegAvailable && !outMp4) {
      throw new Error('HLS output အတွက် ffmpeg လိုပါတယ်။ Server မှာ ffmpeg မရှိပါ။');
    }

    // Progress band design:
    // - download : 0 .. D
    // - process  : D .. P
    // - upload mp4 : P .. (P + Mspan)
    // - upload hls : (P + Mspan) .. 100
    let band;
    if (wantHls && wantFast)            band = [0, 30, 60, 100];
    else if (wantHls)                   band = [0, 35, 65, 100];
    else if (wantFast)                  band = [0, 40, 60, 100];
    else                                band = [0, 0,  0,  100];

    let realContentType = guessType;

    if (needLocalFile) {
      emit(job, { status: 'downloading', phase: 'downloading', percent: band[0] });
      const { contentType } = await downloadToFile(url, tmpIn, (loaded, total) => {
        const span = band[1] - band[0];
        const pct = total ? Math.floor(band[0] + (loaded / total) * span) : band[0];
        emit(job, { loaded, total, percent: Math.min(pct, band[1]) });
      });
      realContentType = contentType || guessType;

      emit(job, { status: 'processing', phase: wantHls ? 'transcoding to HLS' : 'faststart (ffmpeg)', percent: band[1] });

      // Produce faststart MP4 if needed (used for both MP4 upload AND HLS source)
      if (wantFast) {
        await ffmpegFaststart(tmpIn, tmpFast);
      }
      if (wantHls) {
        await fsp.mkdir(hlsDir, { recursive: true });
        await ffmpegHls(wantFast ? tmpFast : tmpIn, hlsDir);
      }
      emit(job, { percent: band[2] });

      // ── Upload MP4 (if requested) ──
      const uploadSpan = band[3] - band[2];
      let mp4Span = 0, hlsSpan = uploadSpan;
      if (outMp4 && wantHls) { mp4Span = Math.floor(uploadSpan * 0.4); hlsSpan = uploadSpan - mp4Span; }
      else if (outMp4 && !wantHls) { mp4Span = uploadSpan; hlsSpan = 0; }
      else if (!outMp4 && wantHls) { mp4Span = 0; hlsSpan = uploadSpan; }

      if (outMp4) {
        const mp4Source = wantFast ? tmpFast : tmpIn;
        const mp4Stat = fs.statSync(mp4Source);
        emit(job, { status: 'uploading', phase: 'uploading MP4 to R2', percent: band[2], loaded: 0, total: mp4Stat.size });
        await uploadFileToR2(mp4Source, objectKey, realContentType, (loaded, total) => {
          const pct = total ? Math.floor(band[2] + (loaded / total) * mp4Span) : band[2];
          emit(job, { loaded, total, percent: Math.min(pct, band[2] + mp4Span) });
        });
        result.mp4 = true;
        result.mp4Key = objectKey;
        result.contentType = realContentType;
        result.contentLength = mp4Stat.size;
        result.faststart = wantFast;
        if (R2_PUBLIC_BASE) {
          result.publicUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${encodeURIComponent(objectKey)}`;
        }
      }

      // ── Upload HLS (if requested) ──
      if (wantHls) {
        const hlsPrefix = stripExt(objectKey) + '_hls';
        emit(job, { phase: 'uploading HLS segments to R2', percent: band[2] + mp4Span });
        const { totalBytes, count } = await uploadDirToR2(hlsDir, hlsPrefix, (done, total) => {
          const pct = total ? Math.floor((band[2] + mp4Span) + (done / total) * hlsSpan) : (band[2] + mp4Span);
          emit(job, { loaded: done, total, percent: Math.min(pct, 99) });
        });
        result.hls = true;
        result.hlsPrefix = hlsPrefix;
        result.hlsSegmentCount = Math.max(0, count - 1);
        result.hlsTotalBytes = totalBytes;
        if (R2_PUBLIC_BASE) {
          result.hlsPlaylistUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${hlsPrefix}/master.m3u8`;
        }
      } else if (outHls && !ffmpegAvailable) {
        result.skippedReason = 'ffmpeg not available';
      }

      emit(job, { status: 'done', phase: 'completed', percent: 100, result });
    } else {
      // Pure stream upload (MP4 only, no faststart, no HLS)
      emit(job, { status: 'downloading', phase: 'streaming → R2', percent: 0 });

      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);
      const contentType = resp.headers.get('content-type') || guessType;
      const contentLength = Number(resp.headers.get('content-length')) || 0;

      // If server says it's actually an m3u8, redirect to HLS branch logic.
      if (isHlsUrl(url, contentType)) {
        // Re-run as HLS source
        return runUpload(job, { url, key, faststart, outMp4, outHls });
      }

      emit(job, { status: 'uploading', phase: 'uploading to R2', total: contentLength, loaded: 0, percent: 0 });

      const uploader = new Upload({
        client: s3,
        params: {
          Bucket: R2_BUCKET,
          Key: objectKey,
          Body: resp.body,
          ContentType: contentType,
          ContentDisposition: `attachment; filename="${objectKey}"`,
        },
        queueSize: 4,
        partSize: 8 * 1024 * 1024,
        leavePartsOnError: false,
      });
      uploader.on('httpUploadProgress', (p) => {
        const loaded = p.loaded || 0;
        const total = p.total || contentLength || 0;
        const pct = total ? Math.floor((loaded / total) * 100) : 0;
        emit(job, { loaded, total, percent: Math.min(pct, 99) });
      });
      await uploader.done();

      result.mp4 = true;
      result.mp4Key = objectKey;
      result.contentType = contentType;
      result.contentLength = contentLength || null;
      result.faststart = false;
      if (R2_PUBLIC_BASE) {
        result.publicUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${encodeURIComponent(objectKey)}`;
      }
      if (outHls && !ffmpegAvailable) result.skippedReason = 'ffmpeg not available';

      emit(job, { status: 'done', phase: 'completed', percent: 100, result });
    }
  } catch (err) {
    console.error(err);
    emit(job, { status: 'error', error: err.message || 'Upload failed' });
  } finally {
    for (const res of job.listeners) {
      try { res.end(); } catch (_) {}
    }
    job.listeners.clear();
    fsp.rm(tmpRoot, { recursive: true, force: true }).catch(() => {});
  }
}

// ── Routes ──
app.post('/api/upload', (req, res) => {
  const { url, key, faststart, outMp4, outHls, hls } = req.body || {};
  if (!url) return res.status(400).json({ error: 'url is required' });
  // Backward-compat: old clients send `hls` only.
  const finalOutMp4 = (typeof outMp4 === 'boolean') ? outMp4 : true;
  const finalOutHls = (typeof outHls === 'boolean') ? outHls : !!hls;
  const job = newJob();
  runUpload(job, {
    url,
    key,
    faststart: !!faststart,
    outMp4: finalOutMp4,
    outHls: finalOutHls,
  }).catch(() => {});
  res.json({ jobId: job.id });
});

app.get('/api/progress/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).end();
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders?.();
  res.write('retry: 3000\n\n');
  res.write(`data: ${JSON.stringify({
    status: job.status, phase: job.phase, loaded: job.loaded,
    total: job.total, percent: job.percent, result: job.result, error: job.error,
  })}\n\n`);
  if (job.status === 'done' || job.status === 'error') return res.end();
  job.listeners.add(res);
  req.on('close', () => job.listeners.delete(res));
});

app.get('/api/status/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'not found' });
  res.json({
    status: job.status, phase: job.phase, loaded: job.loaded,
    total: job.total, percent: job.percent, result: job.result, error: job.error,
  });
});

app.get('/healthz', (_, res) => res.send('ok'));

app.listen(PORT, () => {
  console.log(`🚀 Server on :${PORT}`);
});
