const { Pool } = require("pg");
const odbc = require("odbc");
const logger = require("../config/logger");

// PostgreSQL connection pool
let webDB;

// Connect to PostgreSQL
async function connectWebDB() {
  try {
    webDB = new Pool({
      host: process.env.WEB_DB_HOST,
      port: process.env.WEB_DB_PORT,
      user: process.env.WEB_DB_USER,
      password: process.env.WEB_DB_PASS,
      database: process.env.WEB_DB_NAME,
    });

    // Test the connection
    const result = await webDB.query("SELECT NOW()");
    logger.info("Web database connected successfully");
    return webDB;
  } catch (error) {
    logger.error(`Failed to initialize web database pool: ${error.message}`);
    throw error;
  }
}

// Get web database connection
function getWebDB() {
  return webDB;
}

// Connect to local SQL Anywhere database
async function connectLocalDB() {
  const localDSN = process.env.LOCAL_DSN;
  const connectionString = `DSN=${localDSN};UID=${process.env.LOCAL_DB_USER};PWD=${process.env.LOCAL_DB_PASS};DatabaseName=${process.env.LOCAL_DB_NAME};`;

  logger.info("Attempting to connect to local database...");

  try {
    const connection = await odbc.connect(connectionString);
    logger.info("Local database connected successfully");
    return connection;
  } catch (err) {
    logger.error(`Error connecting to local DB: ${err.message}`);
    throw err;
  }
}

// Fetch data from local database
async function fetchLocalData() {
  let connection;

  try {
    connection = await connectLocalDB();

    // Fetch from acc_master where super_code = 'SUNCR'
    const accMasterQuery = `
      SELECT code, name, place, address, phone 
      FROM acc_master 
      WHERE super_code = 'SUNCR'
    `;
    logger.info("Executing acc_master query");
    const accMasterResult = await connection.query(accMasterQuery);
    logger.info(`Retrieved ${accMasterResult.length} records from acc_master`);

    // Fetch from acc_users (all columns)
    const accUsersQuery = `SELECT * FROM acc_users`;
    logger.info("Executing acc_users query");
    const accUsersResult = await connection.query(accUsersQuery);
    logger.info(`Retrieved ${accUsersResult.length} records from acc_users`);

    return {
      acc_master: accMasterResult,
      acc_users: accUsersResult,
    };
  } catch (err) {
    logger.error(`Error fetching local data: ${err.message}`);
    throw err;
  } finally {
    if (connection) {
      await connection.close();
      logger.info("Local database connection closed");
    }
  }
}

// Close all database connections
async function closeConnections() {
  if (webDB) {
    await webDB.end();
  }
  return true;
}

module.exports = {
  connectWebDB,
  getWebDB,
  fetchLocalData,
  closeConnections,
};
