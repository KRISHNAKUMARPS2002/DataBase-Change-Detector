// services/snapshot.js - Optimized version
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const logger = require("../config/logger");

// File paths with configurable directory
const SNAPSHOT_DIR = process.env.SNAPSHOT_DIR || "./data";
const snapshotFilePath = path.join(SNAPSHOT_DIR, "snapshot.json.gz");
const snapshotBackupPath = path.join(SNAPSHOT_DIR, "snapshot.backup.json.gz");

// Create snapshot directory if it doesn't exist
function ensureDirectoryExists() {
  if (!fs.existsSync(SNAPSHOT_DIR)) {
    fs.mkdirSync(SNAPSHOT_DIR, { recursive: true });
    logger.info(`Created snapshot directory: ${SNAPSHOT_DIR}`);
  }
}

// Load the previous snapshot with compression
function loadSnapshot() {
  logger.info("Loading previous database snapshot");
  ensureDirectoryExists();

  try {
    if (fs.existsSync(snapshotFilePath)) {
      // Read and decompress the gzipped snapshot
      const compressedData = fs.readFileSync(snapshotFilePath);
      const data = zlib.gunzipSync(compressedData).toString("utf8");
      const snapshot = JSON.parse(data);
      logger.info("Snapshot loaded and decompressed successfully");
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
        const compressedBackup = fs.readFileSync(snapshotBackupPath);
        const backupData = zlib.gunzipSync(compressedBackup).toString("utf8");
        return JSON.parse(backupData);
      } catch (backupError) {
        logger.error(`Backup snapshot also corrupted: ${backupError.message}`);
      }
    }

    // Return empty data if all attempts fail
    return { acc_master: [], acc_users: [] };
  }
}

// Save the current snapshot with compression
function saveSnapshot(snapshot) {
  logger.info("Saving current database snapshot");
  ensureDirectoryExists();

  try {
    // First backup the current snapshot if it exists
    if (fs.existsSync(snapshotFilePath)) {
      fs.copyFileSync(snapshotFilePath, snapshotBackupPath);
      logger.info("Previous snapshot backed up successfully");
    }

    // Compress and save the new snapshot
    const jsonData = JSON.stringify(snapshot);
    const compressedData = zlib.gzipSync(jsonData);
    fs.writeFileSync(snapshotFilePath, compressedData);

    logger.info(
      `Snapshot compressed and saved successfully (${compressedData.length} bytes)`
    );
  } catch (error) {
    logger.error(`Error saving snapshot: ${error.message}`);
    throw error;
  }
}

// Purge old snapshots (could be called periodically to clean up space)
function purgeOldSnapshots(maxAge = 30) {
  const backupDir = path.join(SNAPSHOT_DIR, "archives");
  ensureDirectoryExists();

  try {
    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }

    // Read all files in the backup directory
    const files = fs.readdirSync(backupDir);

    const now = new Date();
    let deletedCount = 0;

    for (const file of files) {
      if (file.endsWith(".json.gz")) {
        const filePath = path.join(backupDir, file);
        const stats = fs.statSync(filePath);

        // Calculate file age in days
        const ageInDays = (now - stats.mtime) / (1000 * 60 * 60 * 24);

        if (ageInDays > maxAge) {
          fs.unlinkSync(filePath);
          deletedCount++;
        }
      }
    }

    if (deletedCount > 0) {
      logger.info(`Purged ${deletedCount} old snapshot archives`);
    }
  } catch (error) {
    logger.error(`Error purging old snapshots: ${error.message}`);
  }
}

module.exports = {
  loadSnapshot,
  saveSnapshot,
  purgeOldSnapshots,
};
