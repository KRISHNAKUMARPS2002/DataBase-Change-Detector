const crypto = require("crypto");
const logger = require("../config/logger");

// Cache for row hashes to avoid recalculating
const hashCache = new Map();

/**
 * Calculate hash for a row with caching
 * @param {Object} row - Row data to hash
 * @returns {string} MD5 hash of the row
 */
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
    logger.debug("Row hash cache cleared due to size limit");
  }

  hashCache.set(rowString, hash);
  return hash;
}

/**
 * Compute differences between old and new datasets
 * @param {Array} oldData - Previous dataset
 * @param {Array} newData - Current dataset
 * @param {string} keyField - Primary key field name
 * @returns {Object} Object with inserts, updates, and deletes arrays
 */
function computeDiff(oldData, newData, keyField) {
  // Handle null/undefined inputs gracefully
  if (!Array.isArray(oldData)) oldData = [];
  if (!Array.isArray(newData)) newData = [];

  // Create maps for faster lookups
  const oldMap = new Map();
  const newMap = new Map();

  // Pre-process old data
  for (const row of oldData) {
    const key = row[keyField];
    if (key !== undefined && key !== null) {
      // Skip rows with undefined/null keys
      oldMap.set(key, {
        row,
        hash: computeRowHash(row),
      });
    } else {
      logger.warn(`Skipping row in oldData with missing ${keyField}`);
    }
  }

  // Pre-process new data
  for (const row of newData) {
    const key = row[keyField];
    if (key !== undefined && key !== null) {
      // Skip rows with undefined/null keys
      newMap.set(key, {
        row,
        hash: computeRowHash(row),
      });
    } else {
      logger.warn(`Skipping row in newData with missing ${keyField}`);
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

/**
 * Clear the hash cache
 * Useful when memory usage needs to be reduced
 */
function clearHashCache() {
  const size = hashCache.size;
  hashCache.clear();
  logger.debug(`Hash cache cleared, removed ${size} entries`);
  return size;
}

module.exports = {
  computeDiff,
  clearHashCache,
};
