const fs = require("fs");
const logger = require("../config/logger");

// File paths
const snapshotFilePath = "./snapshot.json";
const snapshotBackupPath = "./snapshot.backup.json";

// Load the previous snapshot
function loadSnapshot() {
  logger.info("Loading previous database snapshot");

  try {
    if (fs.existsSync(snapshotFilePath)) {
      const data = fs.readFileSync(snapshotFilePath, "utf8");
      const snapshot = JSON.parse(data);
      logger.info("Snapshot loaded successfully");
      return snapshot;
    }

    // If no snapshot exists, start with empty arrays
    logger.info("No previous snapshot found, starting with empty data");
    return { acc_master: [], acc_users: [] };
  } catch (error) {
    logger.error(`Error loading snapshot: ${error.message}`);

    // Try to load from backup if main snapshot is corrupted
    if (fs.existsSync(snapshotBackupPath)) {
      logger.info("Attempting to load from backup snapshot");
      try {
        const backupData = fs.readFileSync(snapshotBackupPath, "utf8");
        return JSON.parse(backupData);
      } catch (backupError) {
        logger.error(`Backup snapshot also corrupted: ${backupError.message}`);
      }
    }

    // Return empty data if all attempts fail
    return { acc_master: [], acc_users: [] };
  }
}

// Save the current snapshot
function saveSnapshot(snapshot) {
  logger.info("Saving current database snapshot");

  try {
    // First backup the current snapshot if it exists
    if (fs.existsSync(snapshotFilePath)) {
      fs.copyFileSync(snapshotFilePath, snapshotBackupPath);
      logger.info("Previous snapshot backed up successfully");
    }

    // Now save the new snapshot
    fs.writeFileSync(snapshotFilePath, JSON.stringify(snapshot, null, 2));
    logger.info("Snapshot saved successfully");
  } catch (error) {
    logger.error(`Error saving snapshot: ${error.message}`);
    throw error;
  }
}

module.exports = {
  loadSnapshot,
  saveSnapshot,
};
