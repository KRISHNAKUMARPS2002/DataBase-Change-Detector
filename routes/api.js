const express = require("express");
const router = express.Router();
const logger = require("../config/logger");
const { syncProcess, getSyncStats } = require("../services/sync");

// Basic health check endpoint
router.get("/health", (req, res) => {
  res.json({
    status: "running",
    uptime: process.uptime(),
    syncStats: getSyncStats(),
  });
});

// Detailed stats endpoint
router.get("/stats", (req, res) => {
  const syncStats = getSyncStats();

  res.json({
    syncStats,
    lastSync: {
      time: syncStats.lastSyncTime,
      status:
        syncStats.lastErrorTime === syncStats.lastSyncTime
          ? "failed"
          : "success",
      error: syncStats.lastError,
    },
    config: {
      syncInterval: process.env.SYNC_INTERVAL || 10,
      webDbHost: process.env.WEB_DB_HOST,
      localDSN: process.env.LOCAL_DSN,
    },
  });
});

// Manual sync trigger endpoint
router.post("/sync", async (req, res) => {
  logger.info("Manual sync triggered via API");

  try {
    await syncProcess();
    res.json({ status: "success", message: "Sync completed" });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

module.exports = router;
