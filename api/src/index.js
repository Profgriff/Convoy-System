require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const { createBullBoard } = require('@bull-board/api');
const { ExpressAdapter } = require('@bull-board/express');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');

const app = express();
app.use(express.json());
app.use(helmet());
app.use(rateLimit({ windowMs: 60000, max: 100 }));

// Redis connection
const connection = new IORedis(process.env.REDIS_URL || "redis://redis:6379");

// Queue (must match worker)
const convoyQueue = new Queue('convoyQueue', {
  connection,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 3000
    },
    removeOnComplete: false,
    removeOnFail: false
  }
});

// Bull Board dashboard
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');
createBullBoard({
  queues: [new BullMQAdapter(convoyQueue)],
  serverAdapter
});
app.use('/admin/queues', serverAdapter.getRouter());

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'convoy-api' });
});

app.get('/ready', async (req, res) => {
  try {
    await connection.ping();
    res.json({ status: 'ready' });
  } catch (err) {
    res.status(500).json({ status: 'not ready' });
  }
});

app.get('/job/:id', async (req, res) => {
  try {
    const job = await convoyQueue.getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Not found' });

    const state = await job.getState();
    res.json({ id: job.id, state });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/metrics', async (req, res) => {
  try {
    const counts = await convoyQueue.getJobCounts();
    res.json({ processedJobs: counts.completed, uptime: process.uptime() });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/dispatch', async (req, res) => {
  try {
    const job = await convoyQueue.add('convoy-job', req.body);
    console.log('📤 Job sent:', job.id);

    res.json({ success: true, jobId: job.id });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
