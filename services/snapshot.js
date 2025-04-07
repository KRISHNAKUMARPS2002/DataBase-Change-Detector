const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const logger = require("../config/logger");

// File paths with configurable directory
const SNAPSHOT_DIR = process.env.SNAPSHOT_DIR || "./data";
const SNAPSHOT_RETENTION_DAYS = parseInt(
  process.env.SNAPSHOT_RETENTION_DAYS || "30",
  10
);

// Create a more flexible structure for managing snapshots of different databases
class SnapshotManager {
  constructor() {
    this.snapshotDir = SNAPSHOT_DIR;
    this.ensureDirectoryExists();
  }

  // Create snapshot directory if it doesn't exist
  ensureDirectoryExists() {
    if (!fs.existsSync(this.snapshotDir)) {
      fs.mkdirSync(this.snapshotDir, { recursive: true });
      logger.info(`Created snapshot directory: ${this.snapshotDir}`);
    }

    // Create archives directory
    const archiveDir = path.join(this.snapshotDir, "archives");
    if (!fs.existsSync(archiveDir)) {
      fs.mkdirSync(archiveDir, { recursive: true });
    }
  }

  // Get file paths for a specific database snapshot
  getSnapshotPaths(dbName) {
    return {
      main: path.join(this.snapshotDir, `${dbName}.snapshot.json.gz`),
      backup: path.join(this.snapshotDir, `${dbName}.backup.json.gz`),
      archive: (timestamp) =>
        path.join(
          this.snapshotDir,
          "archives",
          `${dbName}.snapshot.${timestamp}.json.gz`
        ),
    };
  }

  // Load snapshot for a specific database
  loadSnapshot(dbName) {
    logger.info(`Loading previous database snapshot for ${dbName}`);
    const paths = this.getSnapshotPaths(dbName);

    try {
      if (fs.existsSync(paths.main)) {
        // Read and decompress the gzipped snapshot
        const compressedData = fs.readFileSync(paths.main);
        const data = zlib.gunzipSync(compressedData).toString("utf8");
        const snapshot = JSON.parse(data);
        logger.info(
          `Snapshot for ${dbName} loaded and decompressed successfully`
        );
        return snapshot;
      }

      // If no snapshot exists, start with empty object
      logger.info(
        `No previous snapshot found for ${dbName}, starting with empty data`
      );
      return {};
    } catch (error) {
      logger.error(`Error loading snapshot for ${dbName}: ${error.message}`);

      // Try to load from backup if main snapshot is corrupted
      if (fs.existsSync(paths.backup)) {
        logger.info(`Attempting to load from backup snapshot for ${dbName}`);
        try {
          const compressedBackup = fs.readFileSync(paths.backup);
          const backupData = zlib.gunzipSync(compressedBackup).toString("utf8");
          return JSON.parse(backupData);
        } catch (backupError) {
          logger.error(
            `Backup snapshot for ${dbName} also corrupted: ${backupError.message}`
          );
        }
      }

      // Return empty object if all attempts fail
      return {};
    }
  }

  // Save snapshot for a specific database with compression
  saveSnapshot(dbName, snapshot) {
    logger.info(`Saving current database snapshot for ${dbName}`);
    const paths = this.getSnapshotPaths(dbName);

    try {
      // First backup the current snapshot if it exists
      if (fs.existsSync(paths.main)) {
        fs.copyFileSync(paths.main, paths.backup);

        // Also create a timestamped archive copy
        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
        const archivePath = paths.archive(timestamp);
        fs.copyFileSync(paths.main, archivePath);
        logger.info(
          `Previous snapshot for ${dbName} backed up and archived successfully`
        );
      }

      // Compress and save the new snapshot
      const jsonData = JSON.stringify(snapshot);
      const compressedData = zlib.gzipSync(jsonData);
      fs.writeFileSync(paths.main, compressedData);

      logger.info(
        `Snapshot for ${dbName} compressed and saved successfully (${compressedData.length} bytes)`
      );
      return true;
    } catch (error) {
      logger.error(`Error saving snapshot for ${dbName}: ${error.message}`);
      throw error;
    }
  }

  // Purge old snapshots for all databases
  purgeOldSnapshots(maxAgeInDays = SNAPSHOT_RETENTION_DAYS) {
    const archiveDir = path.join(this.snapshotDir, "archives");

    try {
      // Read all files in the archive directory
      const files = fs.readdirSync(archiveDir);
      const now = new Date();
      let deletedCount = 0;

      for (const file of files) {
        if (file.endsWith(".json.gz")) {
          const filePath = path.join(archiveDir, file);
          const stats = fs.statSync(filePath);

          // Calculate file age in days
          const ageInDays = (now - stats.mtime) / (1000 * 60 * 60 * 24);

          if (ageInDays > maxAgeInDays) {
            fs.unlinkSync(filePath);
            deletedCount++;
          }
        }
      }

      if (deletedCount > 0) {
        logger.info(`Purged ${deletedCount} old snapshot archives`);
      }
      return deletedCount;
    } catch (error) {
      logger.error(`Error purging old snapshots: ${error.message}`);
      return 0;
    }
  }

  // Merge new data into existing snapshot
  mergeSnapshot(dbName, newData, tables) {
    // Load existing snapshot
    const existingSnapshot = this.loadSnapshot(dbName);

    // Initialize tables that don't exist in the snapshot
    tables.forEach((table) => {
      if (!existingSnapshot[table] && newData[table]) {
        existingSnapshot[table] = [];
      }
    });

    // Merge new data into snapshot
    tables.forEach((table) => {
      if (newData[table] && newData[table].length > 0) {
        existingSnapshot[table] = newData[table];
        logger.info(
          `Updated ${table} in ${dbName} snapshot with ${newData[table].length} records`
        );
      }
    });

    // Save updated snapshot
    return this.saveSnapshot(dbName, existingSnapshot);
  }
}

// Create a singleton instance
const snapshotManager = new SnapshotManager();

module.exports = snapshotManager;
