const { Client } = require('pg');

const dbConfig = {
    user: 'h3expert',
    host: 'localhost',
    database: 'h3dbtest',
    password: 'h3password',
    port: 5432,
};

async function truncateTable() {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        console.log('Connected to PostgreSQL.');
        await client.query('TRUNCATE TABLE h3_features;');
        console.log('Table h3_features truncated successfully.');
    } catch (err) {
        console.error('Error truncating table:', err);
    } finally {
        await client.end();
    }
}

truncateTable();
