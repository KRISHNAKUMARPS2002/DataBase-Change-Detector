require("dotenv").config();
const { Pool } = require("pg");
const cron = require("node-cron");

// Create connection pool for the local database
const localDB = new Pool({
  host: process.env.LOCAL_DB_HOST,
  port: process.env.LOCAL_DB_PORT,
  user: process.env.LOCAL_DB_USER,
  password: process.env.LOCAL_DB_PASS,
  database: process.env.LOCAL_DB_NAME,
});

// Create connection pool for the web database
const webDB = new Pool({
  host: process.env.WEB_DB_HOST,
  port: process.env.WEB_DB_PORT,
  user: process.env.WEB_DB_USER,
  password: process.env.WEB_DB_PASS,
  database: process.env.WEB_DB_NAME,
});

// Function to process sync log events
async function processSyncLogs() {
  console.log("Checking for new changes in sync_logs...");

  try {
    // Fetch unprocessed sync log events ordered by created_at
    const { rows: events } = await localDB.query(
      `SELECT id, user_id, action FROM sync_logs WHERE processed = false ORDER BY created_at ASC`
    );

    for (const event of events) {
      console.log(
        `Processing event: ${event.action} for user_id ${event.user_id}`
      );
      if (event.action === "insert" || event.action === "update") {
        // For insert or update, fetch current user data from local DB
        const { rows: userRows } = await localDB.query(
          `SELECT id, name, email, created_at FROM users WHERE id = $1`,
          [event.user_id]
        );
        if (userRows.length > 0) {
          const user = userRows[0];

          // Upsert user in web DB (using ON CONFLICT clause for PostgreSQL)
          await webDB.query(
            `INSERT INTO users (id, name, email, created_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE SET 
               name = EXCLUDED.name,
               email = EXCLUDED.email,
               created_at = EXCLUDED.created_at`,
            [user.id, user.name, user.email, user.created_at]
          );
          console.log(`Upserted user ${user.id} in web DB`);
        } else {
          console.warn(`User with id ${event.user_id} not found in local DB.`);
        }
      } else if (event.action === "delete") {
        // For delete, remove the record from the web DB
        await webDB.query(`DELETE FROM users WHERE id = $1`, [event.user_id]);
        console.log(`Deleted user ${event.user_id} from web DB`);
      }

      // Mark the event as processed in the local sync_logs table
      await localDB.query(
        `UPDATE sync_logs SET processed = true WHERE id = $1`,
        [event.id]
      );
    }
  } catch (error) {
    console.error("Error processing sync logs:", error);
  }
}

// Schedule the sync process using node-cron
// This example runs the process every SYNC_INTERVAL seconds
const interval = process.env.SYNC_INTERVAL || 10;
cron.schedule(`*/${interval} * * * * *`, () => {
  processSyncLogs();
});

// Optionally, add a basic HTTP server for a health-check endpoint
const express = require("express");
const app = express();
const PORT = process.env.PORT || 3000;

app.get("/health", (req, res) => res.send("Sync Service Running"));

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
