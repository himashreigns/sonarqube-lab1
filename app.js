const mysql = require('mysql2/promise');
const fetch = require('node-fetch');

/* -------------------- Config -------------------- */

function getDbConfig() {
  if (!process.env.DB_USER || !process.env.DB_PASSWORD) {
    throw new Error('Database credentials must be set via environment variables');
  }

  return {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: Number(process.env.DB_PORT || 3306)
  };
}

function getApiConfig() {
  if (!process.env.API_URL) {
    throw new Error('API_URL environment variable is required');
  }

  return {
    url: process.env.API_URL,
    apiKey: process.env.API_KEY
  };
}

/* -------------------- Database -------------------- */

async function fetchUsers(dbConfig) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const [rows] = await connection.execute(
      'SELECT id, name, email, created_at FROM users'
    );
    return rows;
  } finally {
    await connection.end();
  }
}

/* -------------------- Data Mapping -------------------- */

function mapUsers(rows) {
  return rows.map(user => ({
    id: user.id,
    name: user.name,
    email: user.email,
    createdAt: user.created_at
  }));
}

/* -------------------- API -------------------- */

async function sendToApi(apiConfig, users) {
  const headers = { 'Content-Type': 'application/json' };

  if (apiConfig.apiKey) {
    headers.Authorization = `Bearer ${apiConfig.apiKey}`;
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 5000);

  try {
    const response = await fetch(apiConfig.url, {
      method: 'POST',
      headers,
      body: JSON.stringify({ users }),
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`API Error: ${response.status} ${response.statusText}`);
    }
  } finally {
    clearTimeout(timeoutId);
  }
}

/* -------------------- Main -------------------- */

async function main() {
  const dbConfig = getDbConfig();
  const apiConfig = getApiConfig();

  const rows = await fetchUsers(dbConfig);
  console.log(`Retrieved ${rows.length} user records.`);

  const users = mapUsers(rows);
  await sendToApi(apiConfig, users);

  console.log('Data successfully sent to third-party API.');
}

main().catch(error => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
