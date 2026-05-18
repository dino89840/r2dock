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

// ── Fast HTTP(S) agents with keep-alive (for download speed) ──
const httpAgent  = new http.Agent({  keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });

// ── S3/R2 client with tuned HTTP handler (for upload speed) ──
const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  requestHandler: new NodeHttpHandler({
    httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 128 }),
    connectionTimeout: 10_000,
    socketTimeout:    120_000,
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
  };
  jobs.set(id, job);
  setTimeout(() => jobs.delete(id), 60 * 60 * 1000);
  return job;
}
function emit(job, patch = {}) {
  Object.assign(job, patch);
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

function hasFfmpeg() {
  return new Promise((resolve) => {
    const p = spawn('ffmpeg', ['-version']);
    p.on('error', () => resolve(false));
    p.on('exit', (code) => resolve(code === 0));
  });
}

// ── Fast streamed download with progress (uses keep-alive agent) ──
function downloadToFile(url, destPath, onProgress) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      agent: url.startsWith('https') ? httpsAgent : httpAgent,
      headers: { 'User-Agent': 'r2-uploader/1.2', 'Accept': '*/*' },
    }, (res) => {
      // follow redirects
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
    req.setTimeout(120_000, () => req.destroy(new Error('Download timeout')));
  });
}

// ── ffmpeg: faststart remux ──
function ffmpegFaststart(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const args = ['-y', '-i', inputPath, '-c', 'copy', '-movflags', '+faststart', outputPath];
    const p = spawn('ffmpeg', args);
    p.stderr.on('data', () => {}); // discard
    p.on('error', reject);
    p.on('exit', (code) => code === 0 ? resolve(outputPath) : reject(new Error(`ffmpeg exited ${code}`)));
  });
}

// ── ffmpeg: produce HLS (master.m3u8 + .ts segments) ──
// Stream copy (no re-encode) → very fast. Falls back to re-encode only if needed.
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
      // Retry with re-encode if codec copy fails (e.g., non-AAC audio)
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

// ── Upload helpers ──
async function uploadFileToR2(localPath, key, contentType, onProgress) {
  const stat = fs.statSync(localPath);
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
  });
  if (onProgress) {
    uploader.on('httpUploadProgress', (p) => onProgress(p.loaded || 0, p.total || stat.size));
  }
  await uploader.done();
  return stat.size;
}

// Upload many files concurrently (for HLS segments). 6 parallel uploads.
async function uploadDirToR2(dir, keyPrefix, onTick) {
  const entries = await fsp.readdir(dir);
  const files = entries.map((name) => ({
    name,
    full: path.join(dir, name),
    size: fs.statSync(path.join(dir, name)).size,
  }));
  const totalBytes = files.reduce((a, f) => a + f.size, 0);
  let doneBytes = 0;
  const CONCURRENCY = 6;

  async function worker(queue) {
    while (queue.length) {
      const f = queue.shift();
      if (!f) break;
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
          // For HLS files we want inline (so players can stream them)
          CacheControl: 'public, max-age=31536000, immutable',
        },
        queueSize: 4,
        partSize: 8 * 1024 * 1024,
      });
      await up.done();
      doneBytes += f.size;
      if (onTick) onTick(doneBytes, totalBytes);
    }
  }

  const queue = files.slice();
  const workers = Array.from({ length: Math.min(CONCURRENCY, files.length) }, () => worker(queue));
  await Promise.all(workers);
  return { totalBytes, count: files.length };
}

// ── Main job runner ──
async function runUpload(job, { url, key, faststart, hls }) {
  const tmpRoot = path.join(os.tmpdir(), `r2-${job.id}`);
  await fsp.mkdir(tmpRoot, { recursive: true });
  const tmpIn = path.join(tmpRoot, 'input');
  const tmpFast = path.join(tmpRoot, 'fast.mp4');
  const hlsDir = path.join(tmpRoot, 'hls');

  try {
    const objectKey = (key && key.trim()) || pickFilename(url);
    const guessType = mime.lookup(objectKey) || 'application/octet-stream';
    const isMp4Like = /\.(mp4|m4v|mov)$/i.test(objectKey) || /mp4|quicktime|m4v/i.test(guessType);

    const ffmpegAvailable = await hasFfmpeg();
    const wantFast = !!faststart && isMp4Like && ffmpegAvailable;
    const wantHls  = !!hls && ffmpegAvailable;
    const needLocalFile = wantFast || wantHls;

    // Allocate progress bands based on what we'll do
    // download : process : upload
    let band;
    if (wantHls && wantFast)      band = [0, 30, 60, 100];
    else if (wantHls)             band = [0, 35, 65, 100];
    else if (wantFast)            band = [0, 40, 60, 100];
    else                          band = [0, 0,  0,  100]; // pure stream upload

    let realContentType = guessType;
    let inputSize = 0;

    if (needLocalFile) {
      // ── Download to disk first ──
      emit(job, { status: 'downloading', phase: 'downloading', percent: band[0] });
      const { total, contentType } = await downloadToFile(url, tmpIn, (loaded, total) => {
        const span = band[1] - band[0];
        const pct = total ? Math.floor(band[0] + (loaded / total) * span) : band[0];
        emit(job, { loaded, total, percent: Math.min(pct, band[1]) });
      });
      realContentType = contentType || guessType;
      inputSize = total;

      // ── Process (ffmpeg) ──
      emit(job, { status: 'processing', phase: wantHls ? 'transcoding to HLS' : 'faststart (ffmpeg)', percent: band[1] });

      if (wantFast && !wantHls) {
        await ffmpegFaststart(tmpIn, tmpFast);
      } else if (wantHls && !wantFast) {
        await fsp.mkdir(hlsDir, { recursive: true });
        await ffmpegHls(tmpIn, hlsDir);
      } else if (wantHls && wantFast) {
        // both: faststart first → then HLS from the faststart mp4
        await ffmpegFaststart(tmpIn, tmpFast);
        await fsp.mkdir(hlsDir, { recursive: true });
        await ffmpegHls(tmpFast, hlsDir);
      }
      emit(job, { percent: band[2] });

      // ── Upload ──
      const result = {
        success: true,
        bucket: R2_BUCKET,
        key: objectKey,
        faststart: wantFast,
        hls: wantHls,
      };

      // MP4 upload (skip if HLS-only requested without faststart? — we still upload MP4 as primary)
      const mp4Source = wantFast ? tmpFast : tmpIn;
      const mp4Stat = fs.statSync(mp4Source);

      emit(job, { status: 'uploading', phase: 'uploading MP4 to R2', percent: band[2], loaded: 0, total: mp4Stat.size });

      // If we're also doing HLS, MP4 upload takes the first half of the upload band
      const uploadSpan = band[3] - band[2];
      const mp4Span = wantHls ? Math.floor(uploadSpan * 0.4) : uploadSpan;
      const hlsSpan = uploadSpan - mp4Span;

      await uploadFileToR2(mp4Source, objectKey, realContentType, (loaded, total) => {
        const pct = total ? Math.floor(band[2] + (loaded / total) * mp4Span) : band[2];
        emit(job, { loaded, total, percent: Math.min(pct, band[2] + mp4Span) });
      });

      result.mp4Key = objectKey;
      result.contentType = realContentType;
      result.contentLength = mp4Stat.size;
      if (R2_PUBLIC_BASE) {
        result.publicUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${encodeURIComponent(objectKey)}`;
      }

      // HLS upload
      if (wantHls) {
        const hlsPrefix = stripExt(objectKey) + '_hls';
        emit(job, { phase: 'uploading HLS segments to R2', percent: band[2] + mp4Span });
        const { totalBytes, count } = await uploadDirToR2(hlsDir, hlsPrefix, (done, total) => {
          const pct = total ? Math.floor((band[2] + mp4Span) + (done / total) * hlsSpan) : (band[2] + mp4Span);
          emit(job, { loaded: done, total, percent: Math.min(pct, 99) });
        });
        result.hlsPrefix = hlsPrefix;
        result.hlsSegmentCount = count - 1; // minus the .m3u8 itself
        result.hlsTotalBytes = totalBytes;
        if (R2_PUBLIC_BASE) {
          result.hlsPlaylistUrl = `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${hlsPrefix}/master.m3u8`;
        }
      }

      emit(job, { status: 'done', phase: 'completed', percent: 100, result });
    } else {
      // ── Pure stream upload (no ffmpeg) — fastest path ──
      emit(job, { status: 'downloading', phase: 'streaming → R2', percent: 0 });

      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);
      const contentType = resp.headers.get('content-type') || guessType;
      const contentLength = Number(resp.headers.get('content-length')) || 0;

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
      });
      uploader.on('httpUploadProgress', (p) => {
        const loaded = p.loaded || 0;
        const total = p.total || contentLength || 0;
        const pct = total ? Math.floor((loaded / total) * 100) : 0;
        emit(job, { loaded, total, percent: Math.min(pct, 99) });
      });
      await uploader.done();

      const publicUrl = R2_PUBLIC_BASE
        ? `${R2_PUBLIC_BASE.replace(/\/$/, '')}/${encodeURIComponent(objectKey)}`
        : null;

      emit(job, {
        status: 'done',
        phase: 'completed',
        percent: 100,
        result: {
          success: true,
          bucket: R2_BUCKET,
          key: objectKey,
          mp4Key: objectKey,
          contentType,
          contentLength: contentLength || null,
          publicUrl,
          faststart: false,
          hls: false,
          faststartRequested: !!faststart,
          hlsRequested: !!hls,
          skippedReason: (faststart || hls)
            ? (ffmpegAvailable ? 'not an mp4/mov file' : 'ffmpeg not available')
            : undefined,
        },
      });
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
  const { url, key, faststart, hls } = req.body || {};
  if (!url) return res.status(400).json({ error: 'url is required' });
  const job = newJob();
  runUpload(job, { url, key, faststart, hls }).catch(() => {});
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
  res.write(`data: ${JSON.stringify({
    status: job.status, phase: job.phase, loaded: job.loaded,
    total: job.total, percent: job.percent, result: job.result, error: job.error,
  })}\n\n`);

  if (job.status === 'done' || job.status === 'error') return res.end();
  job.listeners.add(res);
  req.on('close', () => job.listeners.delete(res));
});

app.get('/healthz', (_, res) => res.send('ok'));

app.listen(PORT, () => {
  console.log(`🚀 Server on :${PORT}`);
});
