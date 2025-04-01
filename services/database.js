const { Pool } = require("pg");
const odbc = require("odbc");
const logger = require("../config/logger");

// PostgreSQL connection pool with optimized settings
let webDB;

// Connect to PostgreSQL with connection pooling
async function connectWebDB() {
  if (!webDB) {
    try {
      webDB = new Pool({
        host: process.env.WEB_DB_HOST,
        port: process.env.WEB_DB_PORT,
        user: process.env.WEB_DB_USER,
        password: process.env.WEB_DB_PASS,
        database: process.env.WEB_DB_NAME,
        // Optimize connection pool settings
        max: 20, // Maximum number of clients
        idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
        connectionTimeoutMillis: 5000, // Return an error after 5 seconds if connection not established
      });

      // Test the connection
      await webDB.query("SELECT NOW()");
      logger.info("Web database connected successfully");
    } catch (error) {
      logger.error(`Failed to initialize web database pool: ${error.message}`);
      throw error;
    }
  }
  return webDB;
}

// Get web database connection
function getWebDB() {
  return webDB;
}

// Connection pool for local database
let localConnectionPool = [];
const MAX_LOCAL_CONNECTIONS = 5;

// Get or create a connection to the local database
async function getLocalDBConnection() {
  const connectionString = `DSN=${process.env.LOCAL_DSN};UID=${process.env.LOCAL_DB_USER};PWD=${process.env.LOCAL_DB_PASS};DatabaseName=${process.env.LOCAL_DB_NAME};`;

  try {
    logger.info(`Attempting connection to local DB: ${connectionString}`);
    const connection = await odbc.connect(connectionString);
    logger.info("Local database connected successfully");
    return connection;
  } catch (err) {
    logger.error(`ODBC Connection Error: ${err.message}`);
    throw err;
  }
}

// Return a connection to the pool
function releaseLocalConnection(connection) {
  if (localConnectionPool.length < MAX_LOCAL_CONNECTIONS) {
    localConnectionPool.push(connection);
  } else {
    // Close the connection if the pool is full
    connection.close().catch((err) => {
      logger.warn(`Error closing local database connection: ${err.message}`);
    });
  }
}

// Fetch data from local database with optimized queries
async function fetchLocalData() {
  let connection;

  try {
    connection = await getLocalDBConnection();

    // Use more efficient queries with specific columns and optimized WHERE clauses
    const accMasterQuery = `
      SELECT code, name, place, address, phone 
      FROM acc_master
      WHERE super_code = 'SUNCR'
    `;

    const accUsersQuery = `SELECT * FROM acc_users`;

    // Execute queries in parallel for better performance
    const [accMasterResult, accUsersResult] = await Promise.all([
      connection.query(accMasterQuery),
      connection.query(accUsersQuery),
    ]);

    logger.info(
      `Retrieved ${accMasterResult.length} records from acc_master, ${accUsersResult.length} from acc_users`
    );

    return {
      acc_master: accMasterResult,
      acc_users: accUsersResult,
    };
  } catch (err) {
    logger.error(`Error fetching local data: ${err.message}`);
    logger.error(`SQL Query: ${accUsersQuery} and ${accMasterQuery}`); // Logs the exact query for debugging
    throw err;
  } finally {
    if (connection) {
      releaseLocalConnection(connection);
    }
  }
}

// Close all database connections
async function closeConnections() {
  // Close all local connections in the pool
  const closePromises = localConnectionPool.map((conn) => {
    return conn.close().catch((err) => {
      logger.warn(`Error closing local DB connection: ${err.message}`);
    });
  });

  // Clear the pool
  localConnectionPool = [];

  // Close the web DB pool
  if (webDB) {
    closePromises.push(
      webDB.end().catch((err) => {
        logger.warn(`Error closing web database connection: ${err.message}`);
      })
    );
  }

  await Promise.all(closePromises);
  logger.info("All database connections closed");

  return true;
}

module.exports = {
  connectWebDB,
  getWebDB,
  fetchLocalData,
  closeConnections,
};
