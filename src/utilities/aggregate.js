const { Client } = require('pg');
const h3 = require('h3-js');

/**
 * H3 Hierarchical Aggregator for Thematic Grids
 * Aggregates hexagons from Level N to N-1 using Majority Rule (Mode).
 */

const dbConfig = {
    user: 'h3expert',
    host: 'localhost',
    database: 'h3dbtest',
    password: 'h3password',
    port: 5432,
};

/**
 * Aggregates a single level to its parent level.
 * @param {Client} client - Postgres client.
 * @param {number} sourceLevel - The resolution level to aggregate from.
 */
async function aggregateLevel(client, sourceLevel) {
    const targetLevel = sourceLevel - 1;
    console.log(`\n--- Aggregating Level ${sourceLevel} -> Level ${targetLevel} ---`);

    // 1. Fetch all data for the source level
    const { rows } = await client.query(
        'SELECT id, color FROM h3_features WHERE resolution = $1',
        [sourceLevel]
    );

    if (rows.length === 0) {
        console.warn(`No data found for Level ${sourceLevel}. Skipping.`);
        return;
    }

    console.log(`  Fetched ${rows.length.toLocaleString()} cells.`);

    // 2. Group by Parent and count color occurrences
    // parentCounts = { parentId: { color: count } }
    const parentCounts = new Map();

    for (const row of rows) {
        const parentId = h3.cellToParent(row.id, targetLevel);
        if (!parentCounts.has(parentId)) {
            parentCounts.set(parentId, new Map());
        }
        const counts = parentCounts.get(parentId);
        counts.set(row.color, (counts.get(row.color) || 0) + 1);
    }

    console.log(`  Found ${parentCounts.size.toLocaleString()} unique parents.`);

    // 3. Prepare parent data (Majority Rule)
    const batchSize = 1000;
    let batch = [];
    let count = 0;

    for (const [parentId, counts] of parentCounts.entries()) {
        // Find the mode color
        let modeColor = null;
        let maxCount = -1;
        for (const [color, count] of counts.entries()) {
            if (count > maxCount) {
                maxCount = count;
                modeColor = color;
            }
        }

        // Generate WKT for the parent
        const boundary = h3.cellToBoundary(parentId);
        const points = [...boundary, boundary[0]]
            .map(p => `${p[1]} ${p[0]}`)
            .join(', ');
        const wkt = `POLYGON((${points}))`;

        batch.push([parentId, wkt, modeColor, targetLevel]);

        if (batch.length >= batchSize) {
            await insertBatch(client, batch);
            count += batch.length;
            process.stdout.write(`  Processed ${count.toLocaleString()} parents...\r`);
            batch = [];
        }
    }

    // Final batch
    if (batch.length > 0) {
        await insertBatch(client, batch);
        count += batch.length;
    }

    console.log(`\n  Completed Level ${targetLevel}: ${count.toLocaleString()} rows upserted.`);
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

    const query = `
        INSERT INTO h3_features (id, geom, color, resolution) 
        VALUES ${values.join(', ')} 
        ON CONFLICT (id) 
        DO UPDATE SET 
            color = EXCLUDED.color, 
            geom = EXCLUDED.geom, 
            resolution = EXCLUDED.resolution;
    `;
    await client.query(query, params);
}

async function main() {
    const client = new Client(dbConfig);
    await client.connect();
    console.log('Connected to PostgreSQL.');

    try {
        // Start from Level 11 and go down to Level 7
        // (11->10, 10->9, 9->8, 8->7)
        for (let level = 11; level >= 8; level--) {
            await aggregateLevel(client, level);
        }
        console.log('\n--- All Aggregations Complete ---');
    } catch (err) {
        console.error('Error during aggregation:', err);
    } finally {
        await client.end();
    }
}

main();
