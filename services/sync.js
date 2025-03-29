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
};

// Sync data to the web database
async function syncToWebDB(diffData) {
  logger.info(
    "Received data to sync:",
    JSON.stringify(diffData.acc_users, null, 2)
  );

  const webDB = getWebDB();
  const client = await webDB.connect();

  try {
    // Start a transaction
    await client.query("BEGIN");

    // Process acc_master changes
    logger.info(
      `Processing ${diffData.acc_master.inserts.length} acc_master inserts`
    );
    for (const row of diffData.acc_master.inserts) {
      await client.query(
        `INSERT INTO acc_master (code, name, place, address, phone)
         VALUES ($1, $2, $3, $4, $5)`,
        [row.code, row.name, row.place, row.address, row.phone]
      );
    }

    logger.info(
      `Processing ${diffData.acc_master.updates.length} acc_master updates`
    );
    for (const row of diffData.acc_master.updates) {
      await client.query(
        `UPDATE acc_master
         SET name = $1, place = $2, address = $3, phone = $4
         WHERE code = $5`,
        [row.name, row.place, row.address, row.phone, row.code]
      );
    }

    logger.info(
      `Processing ${diffData.acc_master.deletes.length} acc_master deletes`
    );
    for (const row of diffData.acc_master.deletes) {
      await client.query(`DELETE FROM acc_master WHERE code = $1`, [row.code]);
    }

    // Process acc_users changes (optimized for many columns)
    const diffUsers = diffData.acc_users;

    // Process inserts and updates using upsert logic
    if (diffUsers.inserts.length > 0 || diffUsers.updates.length > 0) {
      logger.info(
        `Processing ${diffUsers.inserts.length} acc_users inserts and ${diffUsers.updates.length} updates`
      );

      // Use the first row to get column names (no filtering)
      const sampleRow =
        diffUsers.inserts.length > 0
          ? diffUsers.inserts[0]
          : diffUsers.updates[0];
      const columns = Object.keys(sampleRow);
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
      const updateSet = columns
        .filter((col) => col !== "id")
        .map((col) => `${col} = EXCLUDED.${col}`)
        .join(", ");

      const upsertQuery = `
    INSERT INTO acc_users (${columns.join(", ")})
    VALUES (${placeholders})
    ON CONFLICT (id) DO UPDATE SET ${updateSet}
  `;

      // Process all changes (inserts and updates)
      const allChanges = [...diffUsers.inserts, ...diffUsers.updates];
      for (const row of allChanges) {
        const values = columns.map((col) => row[col]);
        await client.query(upsertQuery, values);
      }
    }

    // Process deletions for acc_users
    logger.info(`Processing ${diffUsers.deletes.length} acc_users deletes`);
    for (const row of diffUsers.deletes) {
      await client.query(`DELETE FROM acc_users WHERE id = $1`, [row.id]);
    }

    // Commit the transaction
    await client.query("COMMIT");
    logger.info("Web database sync completed successfully");
  } catch (err) {
    // Rollback on error
    await client.query("ROLLBACK");
    logger.error(`Error syncing to web DB: ${err.message}`);
    throw err;
  } finally {
    // Release the client
    client.release();
  }
}

// Main sync process
async function syncProcess() {
  const startTime = Date.now();
  logger.info("Starting sync process...");
  syncStats.totalSyncs++;

  try {
    // Fetch current data from local database
    const newData = await fetchLocalData();

    // Load the previous snapshot
    const oldSnapshot = loadSnapshot();

    // Compute differences
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

    console.log(
      "Computed diff for acc_users:",
      JSON.stringify(diffAccUsers, null, 2)
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
      await syncToWebDB(diffData);
    }

    // Save the new snapshot
    saveSnapshot(newData);

    // Update stats
    syncStats.successfulSyncs++;
    syncStats.lastSuccessTime = new Date();

    const duration = Date.now() - startTime;
    logger.info(`Sync process complete. Duration: ${duration}ms`);
  } catch (err) {
    syncStats.failedSyncs++;
    syncStats.lastErrorTime = new Date();
    syncStats.lastError = err.message;
    logger.error(`Sync process failed: ${err.message}`);
    throw err;
  } finally {
    syncStats.lastSyncTime = new Date();
  }
}

// Get sync statistics
function getSyncStats() {
  return syncStats;
}

module.exports = {
  syncProcess,
  getSyncStats,
};
