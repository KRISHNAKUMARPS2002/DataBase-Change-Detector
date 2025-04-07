const logger = require("../config/logger");
const dbService = require("./database");
const snapshotManager = require("./snapshot");
const { computeDiff } = require("../utils/diff");

// Stats for tracking sync performance per database
const syncStats = {
  web: {
    totalSyncs: 0,
    successfulSyncs: 0,
    failedSyncs: 0,
    lastSyncTime: null,
    lastSuccessTime: null,
    lastErrorTime: null,
    lastError: null,
    averageDuration: 0,
    totalDuration: 0,
  },
  remote: {
    totalSyncs: 0,
    successfulSyncs: 0,
    failedSyncs: 0,
    lastSyncTime: null,
    lastSuccessTime: null,
    lastErrorTime: null,
    lastError: null,
    averageDuration: 0,
    totalDuration: 0,
  },
  // Add more database stats as needed
};

/**
 * Sync data to the web database
 * @param {Object} diffData - Object containing differences to sync
 * @param {Object} client - Database client with transaction
 */
async function syncToWebDB(diffData, client) {
  if (!client) {
    throw new Error("Database client is required");
  }

  try {
    // Process rrc_clients changes with batch operations
    if (diffData.rrc_clients?.inserts?.length > 0) {
      const insertQuery = `INSERT INTO rrc_clients (code, name, address, branch)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (code) DO UPDATE SET
           name = EXCLUDED.name,
           address = EXCLUDED.address,
           branch = EXCLUDED.branch
         `;
      const insertPromises = diffData.rrc_clients.inserts.map((row) =>
        client.query(insertQuery, [row.code, row.name, row.address, row.branch])
      );
      await Promise.all(insertPromises);
      logger.info(
        `Processed ${diffData.rrc_clients.inserts.length} rrc_clients inserts`
      );
    }

    if (diffData.rrc_clients?.updates?.length > 0) {
      const updateQuery = `
        UPDATE rrc_clients
        SET name = $2, address = $3, branch = $4
        WHERE code = $1
      `;
      const updatePromises = diffData.rrc_clients.updates.map((row) =>
        client.query(updateQuery, [row.code, row.name, row.address, row.branch])
      );
      await Promise.all(updatePromises);
      logger.info(
        `Processed ${diffData.rrc_clients.updates.length} rrc_clients updates`
      );
    }

    if (diffData.rrc_clients?.deletes?.length > 0) {
      const deletePromises = diffData.rrc_clients.deletes.map((row) =>
        client.query(`DELETE FROM rrc_clients WHERE code = $1`, [row.code])
      );
      await Promise.all(deletePromises);
      logger.info(
        `Processed ${diffData.rrc_clients.deletes.length} rrc_clients deletes`
      );
    }

    // Process acc_users changes efficiently
    const diffUsers = diffData.acc_users;

    // Process inserts with batch operations
    if (diffUsers?.inserts?.length > 0) {
      // Get columns from the first row
      const sampleRow = diffUsers.inserts[0];
      const columns = Object.keys(sampleRow);

      // First check if each record exists
      const insertPromises = diffUsers.inserts.map(async (row) => {
        // Check if record exists
        const checkResult = await client.query(
          `SELECT id FROM acc_users WHERE id = $1`,
          [row.id]
        );

        if (checkResult.rows.length > 0) {
          // Record exists, do UPDATE
          const updateColumns = columns.filter((col) => col !== "id");
          const setClause = updateColumns
            .map((col, i) => `${col} = $${i + 2}`)
            .join(", ");

          const updateQuery = `
            UPDATE acc_users
            SET ${setClause}
            WHERE id = $1
          `;

          const values = [row.id, ...updateColumns.map((col) => row[col])];
          return client.query(updateQuery, values);
        } else {
          // Record doesn't exist, do INSERT
          const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
          const insertQuery = `
            INSERT INTO acc_users (${columns.join(", ")})
            VALUES (${placeholders})
          `;

          const values = columns.map((col) => row[col]);
          return client.query(insertQuery, values);
        }
      });

      await Promise.all(insertPromises);
      logger.info(`Processed ${diffUsers.inserts.length} acc_users inserts`);
    }

    // Process updates with batch operations
    if (diffUsers?.updates?.length > 0) {
      // Get columns from the first row
      const sampleRow = diffUsers.updates[0];
      const columns = Object.keys(sampleRow).filter((col) => col !== "id");

      // Create a dynamic update query
      const updateQuery = `
        UPDATE acc_users
        SET ${columns.map((col, i) => `${col} = $${i + 2}`).join(", ")}
        WHERE id = $1
      `;

      const updatePromises = diffUsers.updates.map((row) => {
        const values = [row.id, ...columns.map((col) => row[col])];
        return client.query(updateQuery, values);
      });

      await Promise.all(updatePromises);
      logger.info(`Processed ${diffUsers.updates.length} acc_users updates`);
    }

    // Process deletions with batch operations
    if (diffUsers?.deletes?.length > 0) {
      // Use IN clause for more efficient deletes when there are many
      if (diffUsers.deletes.length > 10) {
        const ids = diffUsers.deletes.map((row) => row.id);
        await client.query(`DELETE FROM acc_users WHERE id = ANY($1)`, [ids]);
      } else {
        const deletePromises = diffUsers.deletes.map((row) =>
          client.query(`DELETE FROM acc_users WHERE id = $1`, [row.id])
        );
        await Promise.all(deletePromises);
      }
      logger.info(`Processed ${diffUsers.deletes.length} acc_users deletes`);
    }
  } catch (err) {
    // Add more specific error handling
    if (err.code === "23505") {
      // PostgreSQL unique violation code
      logger.error(`Duplicate key violation: ${err.detail || err.message}`);
      // Extract the conflicting key for debugging
      if (err.detail) {
        const match = err.detail.match(/Key \((\w+)\)=\(([^)]+)\)/);
        if (match) {
          const [, column, value] = match;
          logger.error(`Conflict on ${column} with value ${value}`);
        }
      }
    } else {
      logger.error(`Error in syncToWebDB: ${err.message}`);
    }
    throw err;
  }
}

/**
 * Create a snapshot from the current web database
 * @returns {Promise<Object>} The snapshot data
 */
async function createSnapshotFromWebDB() {
  const webDB = await dbService.getDBConnection("web");
  const client = await webDB.connect();
  try {
    logger.info("Creating snapshot from current web database state...");
    const rrc_clients = (await client.query("SELECT * FROM rrc_clients")).rows;
    const accUsers = (await client.query("SELECT * FROM acc_users")).rows;
    const webData = {
      rrc_clients,
      acc_users: accUsers,
    };
    snapshotManager.saveSnapshot("web", webData);
    logger.info(
      `Created snapshot with ${rrc_clients.length} rrc_clients records and ${accUsers.length} acc_users records`
    );
    return webData;
  } catch (err) {
    logger.error(`Failed to create snapshot from web database: ${err.message}`);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Main sync process for remote to web database
 * @returns {Promise<void>}
 */
async function syncRemoteToWeb() {
  const startTime = Date.now();
  logger.info("Starting remote to web sync process...");
  syncStats.remote.totalSyncs++;

  const webDB = await dbService.getDBConnection("web");
  const client = await webDB.connect();

  try {
    // Begin transaction
    await client.query("BEGIN");

    // Fetch data from remote database
    const newData = await dbService.fetchRemoteData();

    // Load previous snapshot for remote database
    let oldSnapshot = snapshotManager.loadSnapshot("remote");

    // Check if web database has data but we don't have a snapshot
    if (!oldSnapshot || Object.keys(oldSnapshot).length === 0) {
      logger.info(
        "No valid previous snapshot found for remote, checking web database..."
      );

      // Check if web database already has data
      const checkExistingData = await client.query(
        "SELECT COUNT(*) FROM rrc_clients"
      );
      const hasExistingData = parseInt(checkExistingData.rows[0].count) > 0;

      if (hasExistingData) {
        logger.info(
          "Web database already contains data but no valid snapshot exists for remote."
        );
        oldSnapshot = await createSnapshotFromWebDB();
        logger.info("Snapshot created from web database.");

        // Set default empty arrays for tables that don't exist in the snapshot
        ["rrc_clients", "acc_users"].forEach((table) => {
          if (!oldSnapshot[table]) {
            oldSnapshot[table] = [];
          }
        });

        // Save this as the remote snapshot too for future comparisons
        snapshotManager.saveSnapshot("remote", oldSnapshot);
      } else {
        logger.info(
          "No data in web database and no previous snapshot for remote. Starting with empty data."
        );
        oldSnapshot = { rrc_clients: [], acc_users: [] };
      }
    }

    // Compute differences efficiently
    const diffRrcClients = computeDiff(
      oldSnapshot.rrc_clients || [],
      newData.rrc_clients || [],
      "code"
    );

    const diffAccUsers = computeDiff(
      oldSnapshot.acc_users || [],
      newData.acc_users || [],
      "id"
    );

    const diffData = {
      rrc_clients: diffRrcClients,
      acc_users: diffAccUsers,
    };

    // Check if there are any changes
    const totalChanges =
      diffRrcClients.inserts.length +
      diffRrcClients.updates.length +
      diffRrcClients.deletes.length +
      diffAccUsers.inserts.length +
      diffAccUsers.updates.length +
      diffAccUsers.deletes.length;

    if (totalChanges === 0) {
      logger.info(
        "No changes detected in remote data, skipping database update"
      );
    } else {
      logger.info(
        `Detected ${totalChanges} changes from remote, updating web database`
      );
      // Update the web DB with detected changes
      await syncToWebDB(diffData, client);

      // Save the new snapshot ONLY after successful sync
      snapshotManager.saveSnapshot("remote", newData);
      logger.info("New remote snapshot saved successfully");
    }

    // Commit the transaction
    await client.query("COMMIT");

    // Update stats
    syncStats.remote.successfulSyncs++;
    syncStats.remote.lastSuccessTime = new Date();

    const duration = Date.now() - startTime;
    syncStats.remote.totalDuration += duration;
    syncStats.remote.averageDuration =
      syncStats.remote.totalDuration / syncStats.remote.successfulSyncs;
    logger.info(`Remote to web sync process complete. Duration: ${duration}ms`);
  } catch (err) {
    // Rollback on error
    await client.query("ROLLBACK");

    syncStats.remote.failedSyncs++;
    syncStats.remote.lastErrorTime = new Date();
    syncStats.remote.lastError = err.message;
    logger.error(`Remote to web sync process failed: ${err.message}`);
    throw err;
  } finally {
    client.release();
    syncStats.remote.lastSyncTime = new Date();
  }
}

/**
 * Generic function to sync data between any two databases
 * @param {string} sourceDbKey - Source database key
 * @param {string} targetDbKey - Target database key
 * @param {Object} options - Options for sync (tables, etc.)
 * @returns {Promise<Object>} - Sync results
 */
async function syncBetweenDatabases(sourceDbKey, targetDbKey, options = {}) {
  const startTime = Date.now();
  const tables = options.tables || [];

  logger.info(`Starting sync from ${sourceDbKey} to ${targetDbKey}...`);

  // Initialize stats for this source if they don't exist
  if (!syncStats[sourceDbKey]) {
    syncStats[sourceDbKey] = {
      totalSyncs: 0,
      successfulSyncs: 0,
      failedSyncs: 0,
      lastSyncTime: null,
      lastSuccessTime: null,
      lastErrorTime: null,
      lastError: null,
      averageDuration: 0,
      totalDuration: 0,
    };
  }

  syncStats[sourceDbKey].totalSyncs++;

  try {
    // For now, we only support syncing TO web database
    // This could be extended to sync between any databases in the future
    if (targetDbKey !== "web") {
      throw new Error(
        `Syncing to ${targetDbKey} is not supported yet. Currently only web is supported as target.`
      );
    }

    // Set up source data fetching based on the source database key
    let sourceData;
    let fetchFunc;

    if (sourceDbKey === "remote") {
      fetchFunc = dbService.fetchRemoteData;
    } else {
      // Set up a custom fetch function for other databases
      fetchFunc = async () => {
        throw new Error(`Fetching from ${sourceDbKey} is not implemented yet.`);
      };
    }

    // Fetch the source data
    sourceData = await fetchFunc();

    // Get target database connection
    const targetDB = await dbService.getDBConnection(targetDbKey);
    const client = await targetDB.connect();

    try {
      // Begin transaction
      await client.query("BEGIN");

      // Load previous snapshot for source database
      let oldSnapshot = snapshotManager.loadSnapshot(sourceDbKey);

      // If no snapshot exists, check if target DB has data
      if (!oldSnapshot || Object.keys(oldSnapshot).length === 0) {
        logger.info(
          `No valid previous snapshot found for ${sourceDbKey}, checking ${targetDbKey} database...`
        );

        // Set default empty data for tables
        const emptySnapshot = {};
        tables.forEach((table) => {
          emptySnapshot[table] = [];
        });

        // If target is web, we can create snapshot from it
        if (targetDbKey === "web") {
          // Check if web has data
          const checkExistingData = await client.query(
            "SELECT COUNT(*) FROM rrc_clients"
          );
          const hasExistingData = parseInt(checkExistingData.rows[0].count) > 0;

          if (hasExistingData) {
            logger.info(
              `${targetDbKey} database contains data but no valid snapshot exists for ${sourceDbKey}.`
            );
            // For web, we can create snapshot
            oldSnapshot = await createSnapshotFromWebDB();
            logger.info(`Snapshot created from ${targetDbKey} database.`);

            // Save this as source snapshot for future comparisons
            snapshotManager.saveSnapshot(sourceDbKey, oldSnapshot);
          } else {
            logger.info(
              `No data in ${targetDbKey} database and no previous snapshot for ${sourceDbKey}. Starting with empty data.`
            );
            oldSnapshot = emptySnapshot;
          }
        } else {
          // For other dbs, just use empty data
          logger.info(
            `No previous snapshot found for ${sourceDbKey}. Starting with empty data.`
          );
          oldSnapshot = emptySnapshot;
        }
      }

      // Compute differences for each table
      const diffData = {};
      let totalChanges = 0;

      tables.forEach((table) => {
        // Initialize with empty arrays if missing in either snapshot
        const oldTable = oldSnapshot[table] || [];
        const newTable = sourceData[table] || [];

        // Determine primary key for the table
        let primaryKey = "id";
        if (table === "rrc_clients") primaryKey = "code";

        // Compute differences
        const tableDiff = computeDiff(oldTable, newTable, primaryKey);
        diffData[table] = tableDiff;

        // Add to total changes count
        totalChanges +=
          tableDiff.inserts.length +
          tableDiff.updates.length +
          tableDiff.deletes.length;
      });

      // Check if there are any changes
      if (totalChanges === 0) {
        logger.info(
          `No changes detected from ${sourceDbKey}, skipping database update`
        );
      } else {
        logger.info(
          `Detected ${totalChanges} changes from ${sourceDbKey}, updating ${targetDbKey} database`
        );

        // For web target, use syncToWebDB
        if (targetDbKey === "web") {
          await syncToWebDB(diffData, client);
        }

        // Save the new snapshot ONLY after successful sync
        snapshotManager.saveSnapshot(sourceDbKey, sourceData);
        logger.info(`New ${sourceDbKey} snapshot saved successfully`);
      }

      // Commit the transaction
      await client.query("COMMIT");

      // Update stats
      syncStats[sourceDbKey].successfulSyncs++;
      syncStats[sourceDbKey].lastSuccessTime = new Date();

      const duration = Date.now() - startTime;
      syncStats[sourceDbKey].totalDuration += duration;
      syncStats[sourceDbKey].averageDuration =
        syncStats[sourceDbKey].totalDuration /
        syncStats[sourceDbKey].successfulSyncs;

      logger.info(
        `${sourceDbKey} to ${targetDbKey} sync process complete. Duration: ${duration}ms`
      );

      return {
        success: true,
        message: `Sync completed successfully in ${duration}ms`,
        totalChanges,
        duration,
      };
    } catch (err) {
      // Rollback on error
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    syncStats[sourceDbKey].failedSyncs++;
    syncStats[sourceDbKey].lastErrorTime = new Date();
    syncStats[sourceDbKey].lastError = err.message;
    logger.error(
      `${sourceDbKey} to ${targetDbKey} sync process failed: ${err.message}`
    );

    return {
      success: false,
      message: `Sync failed: ${err.message}`,
      error: err.message,
    };
  } finally {
    syncStats[sourceDbKey].lastSyncTime = new Date();
  }
}

/**
 * Force the creation of a snapshot from the web database
 * @returns {Promise<Object>} Result of snapshot creation
 */
async function forceCreateSnapshot(dbKey = "web") {
  try {
    if (dbKey === "web") {
      await createSnapshotFromWebDB();
    } else if (dbKey === "remote") {
      const remoteData = await dbService.fetchRemoteData();
      snapshotManager.saveSnapshot("remote", remoteData);
    } else {
      throw new Error(`Creating snapshot for ${dbKey} is not supported yet`);
    }

    return {
      success: true,
      message: `Snapshot created successfully for ${dbKey} database`,
    };
  } catch (err) {
    return {
      success: false,
      message: `Failed to create snapshot for ${dbKey}: ${err.message}`,
    };
  }
}

/**
 * Get sync statistics for all databases
 * @returns {Object} Statistics for all databases
 */
function getSyncStats() {
  const formattedStats = {};

  for (const [dbKey, stats] of Object.entries(syncStats)) {
    formattedStats[dbKey] = {
      ...stats,
      averageDuration: `${Math.round(stats.averageDuration)}ms`,
    };
  }

  return formattedStats;
}

module.exports = {
  syncRemoteToWeb,
  syncBetweenDatabases,
  getSyncStats,
  forceCreateSnapshot,
  createSnapshotFromWebDB,
};
