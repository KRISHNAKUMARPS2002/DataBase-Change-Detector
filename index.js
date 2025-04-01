// app.js - Optimized version
require("dotenv").config();
const cron = require("node-cron");
const express = require("express");
const helmet = require("helmet");
const compression = require("compression");
const logger = require("./config/logger");
const { connectWebDB, closeConnections } = require("./services/database");
const { syncProcess } = require("./services/sync");
const { purgeOldSnapshots } = require("./services/snapshot");
const apiRoutes = require("./routes/api");

// Initialize the app
const app = express();
const PORT = process.env.PORT || 5000;

// Security and performance middleware
app.use(helmet());
app.use(compression());
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: false, limit: "1mb" }));

// Apply routes
app.use("/", apiRoutes);

// Track whether sync is running to avoid overlaps
let isSyncRunning = false;

// Initialize database connections
connectWebDB()
  .then(() => {
    logger.info("Started database connections");

    // Schedule the sync process with protection against overlapping executions
    const syncIntervalSeconds = process.env.SYNC_INTERVAL || 10;
    logger.info(
      `Scheduling sync process to run every ${syncIntervalSeconds} seconds`
    );

    const syncJob = cron.schedule(
      `*/${syncIntervalSeconds} * * * * *`,
      async () => {
        if (isSyncRunning) {
          logger.warn("Previous sync still running, skipping this run");
          return;
        }

        isSyncRunning = true;
        try {
          await syncProcess();
        } catch (err) {
          logger.error(`Unhandled error in sync process: ${err.message}`);
        } finally {
          isSyncRunning = false;
        }
      }
    );

    // Schedule snapshot cleanup once per day
    const cleanupJob = cron.schedule("0 0 * * *", () => {
      const snapshotRetentionDays = parseInt(
        process.env.SNAPSHOT_RETENTION_DAYS || "30"
      );
      purgeOldSnapshots(snapshotRetentionDays);
    });

    // Start the server
    app.listen(PORT, () => {
      logger.info(`Server is running on port ${PORT}`);
      logger.info("Sync service started successfully");
    });

    // Graceful shutdown
    process.on("SIGTERM", () => shutdown(syncJob, cleanupJob));
    process.on("SIGINT", () => shutdown(syncJob, cleanupJob));
  })
  .catch((err) => {
    logger.error(`Failed to start service: ${err.message}`);
    process.exit(1);
  });

// Shutdown function
function shutdown(syncJob, cleanupJob) {
  logger.info("Shutting down sync service...");

  if (syncJob) {
    syncJob.stop();
  }

  if (cleanupJob) {
    cleanupJob.stop();
  }

  closeConnections()
    .then(() => {
      logger.info("Database connections closed");
      logger.info("Sync service stopped");
      process.exit(0);
    })
    .catch((err) => {
      logger.error(`Error closing database connections: ${err.message}`);
      process.exit(1);
    });
}
