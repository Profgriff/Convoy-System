require('dotenv').config();
const pino = require('pino');
const { Pool } = require('pg');
const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const nodemailer = require('nodemailer');

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const db = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://convoy:convoy@postgres:5432/convoydb'
});

const transporter = process.env.ALERT_EMAIL && process.env.SMTP_HOST ? nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: Number(process.env.SMTP_PORT || 587),
  secure: process.env.SMTP_SECURE === 'true',
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  }
}) : null;

// Redis connection
const connection = new IORedis(process.env.REDIS_URL || "redis://redis:6379", {
  maxRetriesPerRequest: null
});

// Queue name MUST match API side
const QUEUE_NAME = 'convoyQueue';
let processedJobs = 0;

async function initWorker() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      type TEXT,
      status TEXT,
      payload JSONB,
      result JSONB,
      error TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
      logger.info({ jobId: job.id, data: job.data }, 'Processing job');

      switch (job.data.type) {
        case 'test':
          logger.info({ message: job.data.message }, '🧪 Test job');
          break;

        case 'alert':
          logger.info({ message: job.data.message }, '🚨 ALERT');
          break;

        case 'report':
          logger.info({ jobId: job.id }, '📄 Generating report');
          break;

        default:
          logger.warn({ jobId: job.id, type: job.data.type }, '⚠️ Unknown job type');
      }

      // simulate processing
      await new Promise(res => setTimeout(res, 1500));

      const result = { success: true };
      logger.info({ jobId: job.id }, '✅ Job done');
      return result;
    },
    { connection, concurrency: 5 }
  );

  worker.on('completed', async (job) => {
    processedJobs += 1;
    logger.info({ jobId: job.id }, '🎉 Completed job');

    try {
      await db.query(
        `INSERT INTO jobs (id, type, status, payload, result)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET
           status = EXCLUDED.status,
           result = EXCLUDED.result,
           error = NULL,
           updated_at = NOW()` ,
        [job.id, job.name, 'completed', JSON.stringify(job.data), JSON.stringify({ success: true })]
      );
    } catch (err) {
      logger.error({ err, jobId: job.id }, 'Failed to log completed job to database');
    }
  });

  worker.on('failed', async (job, err) => {
    logger.error({ jobId: job.id, err }, 'Job failed');
    console.log(`🚨 ALERT: Job ${job.id} failed`);

    if (process.env.ALERT_EMAIL && transporter) {
      try {
        await transporter.sendMail({
          from: process.env.ALERT_FROM || process.env.ALERT_EMAIL,
          to: process.env.ALERT_EMAIL,
          subject: `Job ${job.id} failed`,
          text: `Job ${job.id} failed with error: ${err.message}`
        });
      } catch (emailErr) {
        logger.error({ emailErr }, 'Failed to send failure alert email');
      }
    }

    try {
      await db.query(
        `INSERT INTO jobs (id, type, status, payload, error)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET
           status = EXCLUDED.status,
           error = EXCLUDED.error,
           updated_at = NOW()` ,
        [job.id, job.name, 'failed', JSON.stringify(job.data), err.message]
      );
    } catch (dbErr) {
      logger.error({ dbErr, jobId: job.id }, 'Failed to log failed job to database');
    }
  });

  process.on('SIGINT', async () => {
    logger.info('SIGINT received, shutting down worker...');
    await worker.close();
    await db.end();
    process.exit(0);
  });

  logger.info('🚀 Convoy Worker running...');
}

initWorker().catch((err) => {
  logger.error(err);
  process.exit(1);
});