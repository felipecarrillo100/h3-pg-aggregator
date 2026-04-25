const fs = require('fs');
const readline = require('readline');
const { Client } = require('pg');
const h3 = require('h3-js');

/**
 * Uploads H3 JSON data to PostGIS.
 * @param {string} filePath - Path to the optimized JSON file.
 * @param {object} dbConfig - Postgres connection configuration.
 */
async function uploadToPostgres(filePath, dbConfig) {
    const client = new Client(dbConfig);
    await client.connect();
    console.log('Connected to PostgreSQL.');

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let count = 0;
    const batchSize = 1000;
    let batch = [];
    const startTime = Date.now();

    console.log(`--- H3 Postgres Uploader Started ---`);

    for await (const line of rl) {
        // Skip JSON array brackets and handle commas
        let cleanLine = line.trim();
        if (cleanLine === '[' || cleanLine === ']') continue;
        if (cleanLine.endsWith(',')) cleanLine = cleanLine.slice(0, -1);
        
        if (!cleanLine) continue;

        try {
            const entry = JSON.parse(cleanLine);
            const h3Index = entry.i;
            const color = entry.c;

            // 1. Convert H3 to WKT Polygon
            // H3 returns [[lat, lng], ...]
            // WKT requires POLYGON((lng lat, lng lat, ...))
            const boundary = h3.cellToBoundary(h3Index);
            // Close the loop and swap to [lng, lat]
            const points = [...boundary, boundary[0]]
                .map(p => `${p[1]} ${p[0]}`)
                .join(', ');
            
            const wkt = `POLYGON((${points}))`;
            const resolution = h3.getResolution(h3Index);

            batch.push([h3Index, wkt, color, resolution]);

            if (batch.length >= batchSize) {
                await insertBatch(client, batch);
                count += batch.length;
                batch = [];
                const elapsed = (Date.now() - startTime) / 1000;
                console.log(`  Uploaded: ${count.toLocaleString()} cells... (${Math.round(count / elapsed)} cells/sec)`);
            }
        } catch (err) {
            console.warn('Skipping invalid line:', line);
        }
    }

    // Final batch
    if (batch.length > 0) {
        await insertBatch(client, batch);
        count += batch.length;
    }

    console.log(`--- Upload Complete ---`);
    console.log(`Total rows uploaded: ${count}`);
    console.log(`Total time: ${((Date.now() - startTime) / 1000).toFixed(2)}s`);
    await client.end();
}

/**
 * Performs a batched insertion into Postgres.
 */
async function insertBatch(client, rows) {
    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const row of rows) {
        values.push(`($${paramIndex++}, ST_GeomFromText($${paramIndex++}, 4326), $${paramIndex++}, $${paramIndex++})`);
        params.push(row[0], row[1], row[2], row[3]);
    }

    const query = `INSERT INTO h3_features (id, geom, color, resolution) VALUES ${values.join(', ')} ON CONFLICT (id) DO UPDATE SET color = EXCLUDED.color, geom = EXCLUDED.geom, resolution = EXCLUDED.resolution;`;
    await client.query(query, params);
}

// Configuration
const dbConfig = {
    user: 'h3expert',
    host: 'localhost',
    database: 'h3dbtest',
    password: 'h3password',
    port: 5432,
};

const inputPath = 'flintstones_res11_optimized.json';

uploadToPostgres(inputPath, dbConfig)
    .catch(err => console.error('Fatal Upload Error:', err));
