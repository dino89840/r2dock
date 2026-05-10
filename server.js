require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

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

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// In-memory job store (single-instance only)
const jobs = new Map();
function newJob() {
  const id = crypto.randomBytes(8).toString('hex');
  const job = {
    id,
    status: 'pending',     // pending | downloading | processing | uploading | done | error
    phase: '',
    loaded: 0,
    total: 0,
    percent: 0,
    result: null,
    error: null,
    listeners: new Set(),  // SSE res objects
    createdAt: Date.now(),
  };
  jobs.set(id, job);
  // GC after 1 hour
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

function hasFfmpeg() {
  return new Promise((resolve) => {
    const p = spawn('ffmpeg', ['-version']);
    p.on('error', () => resolve(false));
    p.on('exit', (code) => resolve(code === 0));
  });
}

// Run ffmpeg -movflags +faststart on a downloaded file → returns new path
function ffmpegFaststart(inputPath, outputPath, onProgress) {
  return new Promise((resolve, reject) => {
    // copy codecs, just remux to move moov atom to start
    const args = [
      '-y',
      '-i', inputPath,
      '-c', 'copy',
      '-movflags', '+faststart',
      outputPath,
    ];
    const p = spawn('ffmpeg', args);
    p.stderr.on('data', (d) => {
      if (onProgress) onProgress(d.toString());
    });
    p.on('error', reject);
    p.on('exit', (code) => {
      if (code === 0) resolve(outputPath);
      else reject(new Error(`ffmpeg exited ${code}`));
    });
  });
}

async function runUpload(job, { url, key, faststart }) {
  let tmpIn = null, tmpOut = null;
  try {
    // 1) Fetch headers / start download
    emit(job, { status: 'downloading', phase: 'connecting' });
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);

    const contentType = resp.headers.get('content-type') || 'application/octet-stream';
    const contentLength = Number(resp.headers.get('content-length')) || 0;
    const objectKey = (key && key.trim()) || pickFilename(url);
    const isMp4 = /mp4|quicktime|m4v/i.test(contentType) || /\.(mp4|m4v|mov)$/i.test(objectKey);

    // Decide path: faststart needs full file on disk first
    const wantFaststart = !!faststart && isMp4 && (await hasFfmpeg());

    if (wantFaststart) {
      // ── Path A: download → ffmpeg faststart → upload ──
      tmpIn = path.join(os.tmpdir(), `r2-in-${job.id}`);
      tmpOut = path.join(os.tmpdir(), `r2-out-${job.id}.mp4`);

      emit(job, { phase: 'downloading', total: contentLength, loaded: 0, percent: 0 });

      // Download with progress
      const fileStream = fs.createWriteStream(tmpIn);
      const reader = resp.body.getReader();
      let received = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        fileStream.write(Buffer.from(value));
        received += value.length;
        const pct = contentLength ? Math.floor((received / contentLength) * 40) : 0; // download = 0–40%
        emit(job, { loaded: received, total: contentLength, percent: pct });
      }
      await new Promise((r) => fileStream.end(r));

      // ffmpeg phase = 40–60%
      emit(job, { status: 'processing', phase: 'faststart (ffmpeg)', percent: 45 });
      await ffmpegFaststart(tmpIn, tmpOut);
      emit(job, { percent: 60 });

      // Upload phase = 60–100%
      const stat = fs.statSync(tmpOut);
      const uploadStream = fs.createReadStream(tmpOut);
      emit(job, { status: 'uploading', phase: 'uploading to R2', loaded: 0, total: stat.size, percent: 60 });

      const uploader = new Upload({
        client: s3,
        params: {
          Bucket: R2_BUCKET,
          Key: objectKey,
          Body: uploadStream,
          ContentType: contentType,
        },
        queueSize: 4,
        partSize: 8 * 1024 * 1024,
      });
      uploader.on('httpUploadProgress', (p) => {
        const loaded = p.loaded || 0;
        const total = p.total || stat.size;
        const pct = 60 + Math.floor((loaded / total) * 40);
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
          contentType,
          contentLength: stat.size,
          publicUrl,
          faststart: true,
        },
      });
    } else {
      // ── Path B: direct stream upload (fast, low memory) ──
      emit(job, { status: 'uploading', phase: 'uploading to R2', total: contentLength, loaded: 0, percent: 0 });

      const uploader = new Upload({
        client: s3,
        params: {
          Bucket: R2_BUCKET,
          Key: objectKey,
          Body: resp.body,
          ContentType: contentType,
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
          contentType,
          contentLength: contentLength || null,
          publicUrl,
          faststart: false,
          faststartRequested: !!faststart,
          faststartSkippedReason: faststart
            ? (isMp4 ? 'ffmpeg not available' : 'not an mp4/mov file')
            : undefined,
        },
      });
    }
  } catch (err) {
    console.error(err);
    emit(job, { status: 'error', error: err.message || 'Upload failed' });
  } finally {
    // Close SSE listeners
    for (const res of job.listeners) {
      try { res.end(); } catch (_) {}
    }
    job.listeners.clear();
    // cleanup tmp
    if (tmpIn && fs.existsSync(tmpIn)) fs.unlink(tmpIn, () => {});
    if (tmpOut && fs.existsSync(tmpOut)) fs.unlink(tmpOut, () => {});
  }
}

// Start a job → returns jobId immediately
app.post('/api/upload', (req, res) => {
  const { url, key, faststart } = req.body || {};
  if (!url) return res.status(400).json({ error: 'url is required' });
  const job = newJob();
  // Run async
  runUpload(job, { url, key, faststart }).catch(() => {});
  res.json({ jobId: job.id });
});

// SSE progress stream
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
  // send current state right away
  res.write(`data: ${JSON.stringify({
    status: job.status, phase: job.phase, loaded: job.loaded,
    total: job.total, percent: job.percent, result: job.result, error: job.error,
  })}\n\n`);

  if (job.status === 'done' || job.status === 'error') {
    return res.end();
  }
  job.listeners.add(res);
  req.on('close', () => job.listeners.delete(res));
});

app.get('/healthz', (_, res) => res.send('ok'));

app.listen(PORT, () => {
  console.log(`🚀 Server on :${PORT}`);
});
