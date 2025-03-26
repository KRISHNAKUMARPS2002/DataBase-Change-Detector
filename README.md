# DB Change Detector

A Node.js-based synchronization service that monitors a local PostgreSQL database for changes and automatically updates a remote (web) PostgreSQL database. The project uses PostgreSQL triggers (configured separately) to log changes (inserts, updates, and deletes) into a `sync_logs` table, and a Node.js service periodically processes these logs to keep the remote database in sync.

## Features

- **Real-Time Sync:** Automatically synchronizes changes from the local database to the remote database.
- **Change Detection:** Uses PostgreSQL triggers to log changes into a `sync_logs` table.
- **Automated Processing:** A Node.js service (using `node-cron`) periodically processes logged changes.
- **Health Check Endpoint:** An Express-based endpoint to verify that the service is running.

## Prerequisites

- Node.js (v12 or higher)
- PostgreSQL (running on both local and remote servers)
- npm (for dependency management)

> **Note:** Database tables and triggers should be set up separately as per your project requirements.

## Setup and Installation

### 1. Clone the Repository

### npm install pg dotenv node-cron express

### Create a .env

# Local Database Settings
LOCAL_DB_HOST=yours
LOCAL_DB_PORT=yours
LOCAL_DB_USER=yours
LOCAL_DB_PASS=yours
LOCAL_DB_NAME=yours

# Web (Remote) Database Settings
WEB_DB_HOST=yours
WEB_DB_PORT=yours
WEB_DB_USER=yours
WEB_DB_PASS=yours
WEB_DB_NAME=yours

# Sync Interval (in seconds)
SYNC_INTERVAL=your_wish

# Express Server Port for Health Check
PORT=your_wish


