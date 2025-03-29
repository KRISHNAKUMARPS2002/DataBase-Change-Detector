require("dotenv").config();
const cron = require("node-cron");
const express = require("express");
const logger = require("./config/logger");
const { connectWebDB, closeConnections } = require("./services/database");
const { syncProcess } = require("./services/sync");
const apiRoutes = require("./routes/api");

// Initialize the app
const app = express();
const PORT = process.env.PORT || 5000;

// Apply middleware
app.use(express.json());

// Apply routes
app.use("/", apiRoutes);

// Initialize database connections
connectWebDB()
  .then(() => {
    logger.info("Started database connections");

    // Schedule the sync process
    const syncIntervalSeconds = process.env.SYNC_INTERVAL || 10;
    logger.info(
      `Scheduling sync process to run every ${syncIntervalSeconds} seconds`
    );

    const syncJob = cron.schedule(`*/${syncIntervalSeconds} * * * * *`, () => {
      syncProcess().catch((err) =>
        logger.error(`Unhandled error in sync process: ${err.message}`)
      );
    });

    // Start the server
    app.listen(PORT, () => {
      logger.info(`Server is running on port ${PORT}`);
      logger.info("Sync service started successfully");
    });

    // Graceful shutdown
    process.on("SIGTERM", () => shutdown(syncJob));
    process.on("SIGINT", () => shutdown(syncJob));
  })
  .catch((err) => {
    logger.error(`Failed to start service: ${err.message}`);
    process.exit(1);
  });

// Shutdown function
function shutdown(syncJob) {
  logger.info("Shutting down sync service...");

  if (syncJob) {
    syncJob.stop();
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
