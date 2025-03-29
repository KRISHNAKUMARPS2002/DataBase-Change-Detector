const crypto = require("crypto");
const logger = require("../config/logger");

// Calculate hash for a row
function computeRowHash(row) {
  return crypto.createHash("md5").update(JSON.stringify(row)).digest("hex");
}

// Compute differences between old and new data
function computeDiff(oldData, newData, keyField) {
  logger.info(`Computing diff using key field: ${keyField}`);

  // Create maps for faster lookups
  const oldMap = {};
  for (const row of oldData) {
    oldMap[row[keyField]] = { row, hash: computeRowHash(row) };
  }

  const newMap = {};
  for (const row of newData) {
    newMap[row[keyField]] = { row, hash: computeRowHash(row) };
  }

  // Find insertions, updates, and deletions
  const inserts = [];
  const updates = [];
  const deletes = [];

  // Check for new or updated rows
  for (const key in newMap) {
    if (!oldMap[key]) {
      inserts.push(newMap[key].row);
    } else if (newMap[key].hash !== oldMap[key].hash) {
      updates.push(newMap[key].row);
    }
  }

  // Check for deletions
  for (const key in oldMap) {
    if (!newMap[key]) {
      deletes.push(oldMap[key].row);
    }
  }

  logger.info(
    `Diff results: ${inserts.length} inserts, ${updates.length} updates, ${deletes.length} deletes`
  );

  return { inserts, updates, deletes };
}

module.exports = {
  computeDiff,
};
