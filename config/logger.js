const winston = require("winston");
const fs = require("fs");

// Create logs directory if it doesn't exist
if (!fs.existsSync("./logs")) {
  fs.mkdirSync("./logs");
}

// Configure logger
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `${timestamp} ${level}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "./logs/sync-service.log" }),
  ],
});

module.exports = logger;
