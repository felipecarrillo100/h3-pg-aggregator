const { Client } = require('pg');
require('dotenv').config();

const dbConfig = {
    user: process.env.H3_DB_USER,
    host: process.env.H3_DB_HOST,
    database: process.env.H3_DB_NAME,
    password: process.env.H3_DB_PASSWORD,
    port: parseInt(process.env.H3_DB_PORT || '5432'),
};

async function getClient() {
    const client = new Client(dbConfig);
    await client.connect();
    return client;
}

/**
 * Creates or refreshes the table based on mapping.
 * @param {Client} client 
 * @param {string} tableName 
 * @param {object} mapping 
 */
async function setupTable(client, tableName, mapping) {
    console.log(`Setting up table: ${tableName}`);
    
    // Drop if exists (as requested: "If the table exist it will drop all rows and fill it again" - usually means TRUNCATE or DROP/CREATE)
    // The user said: "Can you drop the table and make the required changes?" and later "If the table exist it will drop all rows and fill it again".
    // I will use DROP and CREATE to ensure the schema matches the mapping.
    await client.query(`DROP TABLE IF EXISTS ${tableName} CASCADE`);

    const columns = [
        'id TEXT PRIMARY KEY',
        'geom GEOMETRY(POLYGON, 4326)',
        'resolution INTEGER'
    ];

    for (const [key, config] of Object.entries(mapping)) {
        columns.push(`${config.column} ${config.type}`);
    }

    const createQuery = `
        CREATE TABLE ${tableName} (
            ${columns.join(',\n            ')}
        )
    `;

    await client.query(createQuery);
    
    // Indexes
    await client.query(`CREATE INDEX ${tableName}_geom_idx ON ${tableName} USING GIST (geom)`);
    await client.query(`CREATE INDEX ${tableName}_res_idx ON ${tableName} (resolution)`);
    
    console.log(`Table ${tableName} created successfully.`);
}

module.exports = {
    getClient,
    setupTable,
    dbConfig
};
