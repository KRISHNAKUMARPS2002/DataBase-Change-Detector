const logger = require("../config/logger");
const { getWebDB, fetchLocalData } = require("./database");
const { loadSnapshot, saveSnapshot } = require("./snapshot");
const { computeDiff } = require("../utils/diff");

// Stats for tracking sync performance
const syncStats = {
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

// Sync data to the web database
async function syncToWebDB(diffData, client) {
  if (!client) {
    throw new Error("Database client is required");
  }

  try {
    // Process acc_master changes with batch operations
    if (diffData.acc_master.inserts.length > 0) {
      const insertQuery = `INSERT INTO acc_master (code, name, place, address, phone)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (code) DO UPDATE SET
           name = EXCLUDED.name,
           place = EXCLUDED.place,
           address = EXCLUDED.address,
           phone = EXCLUDED.phone
         `;
      const insertPromises = diffData.acc_master.inserts.map((row) =>
        client.query(insertQuery, [
          row.code,
          row.name,
          row.place,
          row.address,
          row.phone,
        ])
      );
      await Promise.all(insertPromises);
      logger.info(
        `Processed ${diffData.acc_master.inserts.length} acc_master inserts`
      );
    }

    if (diffData.acc_master.updates.length > 0) {
      const updateQuery = `
        UPDATE acc_master
        SET name = $1, place = $2, address = $3, phone = $4
        WHERE code = $5
      `;
      const updatePromises = diffData.acc_master.updates.map((row) =>
        client.query(updateQuery, [
          row.name,
          row.place,
          row.address,
          row.phone,
          row.code,
        ])
      );
      await Promise.all(updatePromises);
      logger.info(
        `Processed ${diffData.acc_master.updates.length} acc_master updates`
      );
    }

    if (diffData.acc_master.deletes.length > 0) {
      const deletePromises = diffData.acc_master.deletes.map((row) =>
        client.query(`DELETE FROM acc_master WHERE code = $1`, [row.code])
      );
      await Promise.all(deletePromises);
      logger.info(
        `Processed ${diffData.acc_master.deletes.length} acc_master deletes`
      );
    }

    // Process acc_users changes efficiently
    const diffUsers = diffData.acc_users;

    // Process inserts with batch operations
    if (diffUsers.inserts.length > 0) {
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
    if (diffUsers.updates.length > 0) {
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
    if (diffUsers.deletes.length > 0) {
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

// Create a snapshot from the current web database
async function createSnapshotFromWebDB() {
  const webDB = getWebDB();
  const client = await webDB.connect();
  try {
    logger.info("Creating snapshot from current web database state...");
    const accMaster = (await client.query("SELECT * FROM acc_master")).rows;
    const accUsers = (await client.query("SELECT * FROM acc_users")).rows;
    const webData = {
      acc_master: accMaster,
      acc_users: accUsers,
    };
    saveSnapshot(webData);
    logger.info(
      `Created snapshot with ${accMaster.length} acc_master records and ${accUsers.length} acc_users records`
    );
    return webData;
  } catch (err) {
    logger.error(`Failed to create snapshot from web database: ${err.message}`);
    throw err;
  } finally {
    client.release();
  }
}

// Main sync process
async function syncProcess() {
  const startTime = Date.now();
  logger.info("Starting sync process...");
  syncStats.totalSyncs++;

  const webDB = getWebDB();
  const client = await webDB.connect();

  try {
    // Begin transaction
    await client.query("BEGIN");

    // Use memory-efficient streaming for fetching local data
    const newData = await fetchLocalData();

    // Load the previous snapshot (consider incremental loading for large datasets)
    let oldSnapshot = loadSnapshot();

    // Check if web database has data but we don't have a snapshot
    if (!oldSnapshot || !oldSnapshot.acc_master || !oldSnapshot.acc_users) {
      logger.info("No valid previous snapshot found, checking web database...");

      // Check if web database already has data
      const checkExistingData = await client.query(
        "SELECT COUNT(*) FROM acc_master"
      );
      const hasExistingData = parseInt(checkExistingData.rows[0].count) > 0;

      if (hasExistingData) {
        logger.info(
          "Web database already contains data but no valid snapshot exists."
        );
        oldSnapshot = await createSnapshotFromWebDB();
        logger.info("Snapshot created from web database.");

        // Compute differences after creating snapshot
        const diffAccMaster = computeDiff(
          oldSnapshot.acc_master,
          newData.acc_master,
          "code"
        );

        const diffAccUsers = computeDiff(
          oldSnapshot.acc_users,
          newData.acc_users,
          "id"
        );

        const diffData = {
          acc_master: diffAccMaster,
          acc_users: diffAccUsers,
        };

        // Check if there are any real changes after creating snapshot
        const totalChanges =
          diffAccMaster.inserts.length +
          diffAccMaster.updates.length +
          diffAccMaster.deletes.length +
          diffAccUsers.inserts.length +
          diffAccUsers.updates.length +
          diffAccUsers.deletes.length;

        if (totalChanges === 0) {
          logger.info(
            "No changes detected after creating snapshot, skipping database update"
          );
          await client.query("COMMIT");

          // Update stats for successful run
          syncStats.successfulSyncs++;
          syncStats.lastSuccessTime = new Date();

          const duration = Date.now() - startTime;
          syncStats.totalDuration += duration;
          syncStats.averageDuration =
            syncStats.totalDuration / syncStats.successfulSyncs;
          logger.info(
            `Sync process complete (snapshot only). Duration: ${duration}ms`
          );

          client.release();
          syncStats.lastSyncTime = new Date();
          return;
        } else {
          logger.info(
            `Detected ${totalChanges} changes after creating snapshot, updating web database`
          );
        }
      } else {
        logger.info(
          "No data in web database and no previous snapshot. Starting with empty data."
        );
        oldSnapshot = { acc_master: [], acc_users: [] };
      }
    }

    // Compute differences efficiently
    const diffAccMaster = computeDiff(
      oldSnapshot.acc_master,
      newData.acc_master,
      "code"
    );

    const diffAccUsers = computeDiff(
      oldSnapshot.acc_users,
      newData.acc_users,
      "id"
    );

    const diffData = {
      acc_master: diffAccMaster,
      acc_users: diffAccUsers,
    };

    // Check if there are any changes
    const totalChanges =
      diffAccMaster.inserts.length +
      diffAccMaster.updates.length +
      diffAccMaster.deletes.length +
      diffAccUsers.inserts.length +
      diffAccUsers.updates.length +
      diffAccUsers.deletes.length;

    if (totalChanges === 0) {
      logger.info("No changes detected, skipping database update");
    } else {
      logger.info(`Detected ${totalChanges} changes, updating web database`);
      // Update the web DB with detected changes
      await syncToWebDB(diffData, client);

      // Save the new snapshot ONLY after successful sync
      saveSnapshot(newData);
      logger.info("New snapshot saved successfully");
    }

    // Commit the transaction
    await client.query("COMMIT");

    // Update stats
    syncStats.successfulSyncs++;
    syncStats.lastSuccessTime = new Date();

    const duration = Date.now() - startTime;
    syncStats.totalDuration += duration;
    syncStats.averageDuration =
      syncStats.totalDuration / syncStats.successfulSyncs;
    logger.info(`Sync process complete. Duration: ${duration}ms`);
  } catch (err) {
    // Rollback on error
    await client.query("ROLLBACK");

    syncStats.failedSyncs++;
    syncStats.lastErrorTime = new Date();
    syncStats.lastError = err.message;
    logger.error(`Sync process failed: ${err.message}`);
    throw err;
  } finally {
    client.release();
    syncStats.lastSyncTime = new Date();
  }
}

// Force the creation of a snapshot from the web database
async function forceCreateSnapshot() {
  try {
    await createSnapshotFromWebDB();
    return {
      success: true,
      message: "Snapshot created successfully from web database",
    };
  } catch (err) {
    return {
      success: false,
      message: `Failed to create snapshot: ${err.message}`,
    };
  }
}

// Get sync statistics
function getSyncStats() {
  return {
    ...syncStats,
    averageDuration: `${Math.round(syncStats.averageDuration)}ms`,
  };
}

module.exports = {
  syncProcess,
  getSyncStats,
  forceCreateSnapshot,
  createSnapshotFromWebDB,
};
