const crypto = require("crypto");
const logger = require("../config/logger");

// Cache for row hashes to avoid recalculating
const hashCache = new Map();

// Calculate hash for a row with caching
function computeRowHash(row) {
  const rowString = JSON.stringify(row);

  // Check if we already calculated this hash
  if (hashCache.has(rowString)) {
    return hashCache.get(rowString);
  }

  // Calculate and cache the hash
  const hash = crypto.createHash("md5").update(rowString).digest("hex");

  // Store in cache (limit cache size to avoid memory issues)
  if (hashCache.size > 10000) {
    // Clear cache if it gets too large
    hashCache.clear();
  }

  hashCache.set(rowString, hash);
  return hash;
}

// Compute differences between old and new data with optimized algorithm
function computeDiff(oldData, newData, keyField) {
  // Create maps for faster lookups
  const oldMap = new Map();
  const newMap = new Map();

  // Pre-process old data
  for (const row of oldData) {
    const key = row[keyField];
    if (key) {
      // Skip rows with undefined keys
      oldMap.set(key, {
        row,
        hash: computeRowHash(row),
      });
    }
  }

  // Pre-process new data
  for (const row of newData) {
    const key = row[keyField];
    if (key) {
      // Skip rows with undefined keys
      newMap.set(key, {
        row,
        hash: computeRowHash(row),
      });
    }
  }

  // Find changes efficiently
  const inserts = [];
  const updates = [];
  const deletes = [];

  // Process new data first to find inserts and updates
  for (const [key, newItem] of newMap.entries()) {
    const oldItem = oldMap.get(key);

    if (!oldItem) {
      inserts.push(newItem.row);
    } else if (newItem.hash !== oldItem.hash) {
      updates.push(newItem.row);
    }
  }

  // Find deleted items
  for (const [key, oldItem] of oldMap.entries()) {
    if (!newMap.has(key)) {
      deletes.push(oldItem.row);
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
