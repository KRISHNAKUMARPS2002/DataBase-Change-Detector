const { Pool } = require("pg");
const odbc = require("odbc");
const logger = require("../config/logger");

// Store database connections with their respective configurations
const dbConnections = {
  web: {
    pool: null,
    connected: false,
    config: {
      type: "postgres",
      params: {
        host: process.env.WEB_DB_HOST,
        port: process.env.WEB_DB_PORT,
        user: process.env.WEB_DB_USER,
        password: process.env.WEB_DB_PASS,
        database: process.env.WEB_DB_NAME,
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
      },
    },
  },
  remote: {
    pool: [],
    connected: false,
    config: {
      type: "odbc",
      params: {
        connectionString: () => {
          return (
            `DSN=${process.env.REMOTE_DSN};` +
            `UID=${process.env.REMOTE_DB_USER};` +
            `PWD=${process.env.REMOTE_DB_PASS};` +
            `DatabaseName=${process.env.REMOTE_DB_NAME};` +
            `Server=${process.env.REMOTE_DB_SERVER};` +
            `Port=${process.env.REMOTE_DB_PORT || 1433};`
          );
        },
        maxPoolSize: 5,
      },
    },
  },
  // You can add more databases here in the future
  // example: {
  //   pool: null,
  //   connected: false,
  //   config: { ... }
  // }
};

/**
 * Connect to a database based on its key
 * @param {string} dbKey - The key of the database to connect to
 * @returns {Promise<Object>} - The database connection
 */
async function connectToDatabase(dbKey) {
  const db = dbConnections[dbKey];

  if (!db) {
    throw new Error(`Database configuration for '${dbKey}' not found`);
  }

  if (db.config.type === "postgres") {
    if (!db.pool) {
      try {
        db.pool = new Pool(db.config.params);
        await db.pool.query("SELECT NOW()");
        db.connected = true;
        logger.info(`${dbKey} database connected successfully`);
      } catch (error) {
        logger.error(
          `Failed to initialize ${dbKey} database pool: ${error.message}`
        );
        db.connected = false;
        throw error;
      }
    }
    return db.pool;
  } else if (db.config.type === "odbc") {
    try {
      const connectionString = db.config.params.connectionString();
      logger.info(`Attempting connection to ${dbKey} DB: ${connectionString}`);

      // Implement retry logic for remote connections
      let retries = 3;
      let connection;

      while (retries > 0) {
        try {
          connection = await odbc.connect(connectionString);
          break;
        } catch (err) {
          retries--;
          if (retries === 0) throw err;
          logger.warn(
            `Connection attempt failed, retrying... (${retries} attempts left)`
          );
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      logger.info(`${dbKey} database connected successfully`);
      return connection;
    } catch (err) {
      logger.error(`ODBC Connection Error for ${dbKey}: ${err.message}`);
      throw err;
    }
  } else {
    throw new Error(`Unsupported database type: ${db.config.type}`);
  }
}

/**
 * Get a connection for the specified database
 * @param {string} dbKey - The key of the database
 * @returns {Promise<Object>} - The database connection
 */
async function getDBConnection(dbKey) {
  if (dbKey === "web") {
    if (!dbConnections.web.connected) {
      await connectToDatabase("web");
    }
    return dbConnections.web.pool;
  }
  // For ODBC connections, create a new connection or get one from the pool
  else if (dbConnections[dbKey]?.config.type === "odbc") {
    const pool = dbConnections[dbKey].pool;
    if (pool.length > 0) {
      return pool.pop();
    }
    return connectToDatabase(dbKey);
  }

  throw new Error(`Unknown database key: ${dbKey}`);
}

/**
 * Release an ODBC connection back to its pool
 * @param {string} dbKey - The key of the database
 * @param {Object} connection - The connection to release
 */
function releaseConnection(dbKey, connection) {
  const db = dbConnections[dbKey];

  if (!db) {
    logger.warn(
      `Attempted to release connection for unknown database: ${dbKey}`
    );
    return;
  }

  if (db.config.type === "odbc") {
    if (db.pool.length < db.config.params.maxPoolSize) {
      db.pool.push(connection);
    } else {
      connection.close().catch((err) => {
        logger.warn(
          `Error closing ${dbKey} database connection: ${err.message}`
        );
      });
    }
  }
  // PostgreSQL pool manages its own connections
}

/**
 * Fetch data from a remote ODBC database
 * @param {string} dbKey - The key of the database to query
 * @param {Object} queries - Object containing query names and SQL strings
 * @returns {Promise<Object>} - Results of all queries
 */
async function fetchData(dbKey, queries) {
  let connection;
  const results = {};

  try {
    connection = await getDBConnection(dbKey);

    // Execute all queries in parallel for better performance
    const queryPromises = [];
    const queryNames = [];

    for (const [name, query] of Object.entries(queries)) {
      queryPromises.push(connection.query(query));
      queryNames.push(name);
    }

    const queryResults = await Promise.all(queryPromises);

    // Map results to their respective names
    queryNames.forEach((name, index) => {
      results[name] = queryResults[index];
    });

    logger.info(
      `Successfully executed ${queryPromises.length} queries on ${dbKey} database`
    );

    return results;
  } catch (err) {
    logger.error(`Error fetching data from ${dbKey}: ${err.message}`);
    throw err;
  } finally {
    if (connection && dbConnections[dbKey]?.config.type === "odbc") {
      releaseConnection(dbKey, connection);
    }
  }
}

/**
 * Close all database connections
 * @returns {Promise<boolean>} - True if all connections closed successfully
 */
async function closeConnections() {
  const closePromises = [];

  for (const [key, db] of Object.entries(dbConnections)) {
    if (db.config.type === "postgres" && db.pool) {
      closePromises.push(
        db.pool.end().catch((err) => {
          logger.warn(
            `Error closing ${key} database connection: ${err.message}`
          );
        })
      );
      db.connected = false;
    } else if (db.config.type === "odbc" && db.pool.length > 0) {
      const poolClosePromises = db.pool.map((conn) => {
        return conn.close().catch((err) => {
          logger.warn(`Error closing ${key} DB connection: ${err.message}`);
        });
      });
      closePromises.push(...poolClosePromises);
      db.pool = [];
    }
  }

  await Promise.all(closePromises);
  logger.info("All database connections closed");

  return true;
}

// Example of how to fetch data from the remote database
async function fetchRemoteData() {
  const queries = {
    rrc_clients: `
      SELECT code, name, address, branch
      FROM rrc_clients
      WHERE branch IN ('RITS Wayanad', 'IMC', 'IMC Mukkam');
    `,
    acc_users: `SELECT * FROM acc_users`,
  };

  return fetchData("remote", queries);
}

module.exports = {
  connectToDatabase,
  getDBConnection,
  releaseConnection,
  fetchData,
  fetchRemoteData,
  closeConnections,
};
