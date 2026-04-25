const fs = require('fs');
const h3 = require('h3-js');
const cliProgress = require('cli-progress');
const colors = require('colors');
const StatsCollector = require('./stats_collector');

/**
 * Processes H3 data using streaming
 */
async function processH3Data(client, dataStream, tableName, mapping, aggregateLevels, aggregateTo, totalCells = 0) {
    const stats = new StatsCollector(mapping);
    const globalStartTime = Date.now();
    let count = 0;
    const resolutions = new Set();
    let batch = [];

    // --- PHASE 1: INGESTION ---
    console.log(colors.cyan('\n--- Starting Ingestion Pipeline ---'));
    const ingestMulti = new cliProgress.MultiBar({
        format: ' {bar} | {percentage}% | {value}/{total} | {status} | {speed}c/s',
        hideCursor: true,
        clearOnComplete: false,
    }, cliProgress.Presets.shades_grey);

    const uploadBar = ingestMulti.create(totalCells || 1, 0, { status: 'Uploading', speed: 0 });
    const ingestionStartTime = Date.now();

    try {
        for await (const entry of dataStream) {
            const rowData = entry.value || entry;
            const h3Index = rowData.i;
            if (!h3Index) continue;

            const resolution = h3.getResolution(h3Index);
            resolutions.add(resolution);
            stats.collect(h3Index, rowData);

            const boundary = h3.cellToBoundary(h3Index);
            const points = [...boundary, boundary[0]].map(p => `${p[1]} ${p[0]}`).join(', ');
            const wkt = `POLYGON((${points}))`;

            const row = [h3Index, wkt, resolution];
            for (const [key, config] of Object.entries(mapping)) {
                row.push(rowData[key] ?? rowData[config.column] ?? null);
            }

            batch.push(row);
            count++;

            if (batch.length >= 1000) {
                await insertBatch(client, tableName, mapping, batch);
                batch.length = 0;
                const elapsed = (Date.now() - ingestionStartTime) / 1000;
                uploadBar.update(count, { speed: Math.round(count / (elapsed || 1)) });
            }
        }

        if (batch.length > 0) {
            await insertBatch(client, tableName, mapping, batch);
        }

        const ingestionDuration = ((Date.now() - ingestionStartTime) / 1000).toFixed(1);
        uploadBar.update(totalCells || count, { status: `Complete (${ingestionDuration}s)`, speed: Math.round(count / (ingestionDuration || 1)) });
        ingestMulti.stop();
        
        console.log(colors.green(`\nSuccessfully processed ${count.toLocaleString()} cells.`));
        stats.finalize(Array.from(resolutions));

        // --- PHASE 2: AGGREGATION ---
        if (aggregateLevels > 0 || aggregateTo !== null) {
            console.log(colors.cyan('\n--- Starting Hierarchical Aggregation ---'));
            const aggMulti = new cliProgress.MultiBar({
                format: ' {bar} | {percentage}% | {value}/{total} | {status} | {speed}c/s',
                hideCursor: true,
                clearOnComplete: false,
            }, cliProgress.Presets.shades_grey);

            const sortedRes = Array.from(resolutions).sort((a, b) => b - a);
            for (const res of sortedRes) {
                let currentRes = res;
                const levels = aggregateTo !== null ? (res - aggregateTo) : aggregateLevels;
                if (levels <= 0) continue;

                for (let i = 0; i < levels; i++) {
                    const targetRes = currentRes - 1;
                    const aggBar = aggMulti.create(100, 0, { status: `Aggregating ${currentRes}->${targetRes}`, speed: 0 });
                    
                    const aggStartTime = Date.now();
                    const actualCount = await aggregateLevel(client, tableName, mapping, currentRes, aggBar);
                    const aggDuration = ((Date.now() - aggStartTime) / 1000).toFixed(1);

                    aggBar.setTotal(actualCount || 1);
                    aggBar.update(actualCount || 1, { status: `Res ${targetRes} Done (${aggDuration}s)`, speed: Math.round(actualCount / (aggDuration || 1)) });
                    aggBar.stop();
                    
                    currentRes--;
                    if (currentRes < 0) break;
                }
            }
            aggMulti.stop();
        }
        
        return stats;
    } catch (err) {
        if (ingestMulti) ingestMulti.stop();
        throw err;
    }
}

async function insertBatch(client, tableName, mapping, rows) {
    const colNames = ['id', 'geom', 'resolution', ...Object.values(mapping).map(m => m.column)];
    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const row of rows) {
        const placeholders = [];
        placeholders.push(`$${paramIndex++}`);
        params.push(row[0]);
        placeholders.push(`ST_GeomFromText($${paramIndex++}, 4326)`);
        params.push(row[1]);
        placeholders.push(`$${paramIndex++}`);
        params.push(row[2]);
        for (let i = 3; i < row.length; i++) {
            placeholders.push(`$${paramIndex++}`);
            params.push(row[i]);
        }
        values.push(`(${placeholders.join(', ')})`);
    }

    const setClause = colNames.map(col => `${col} = EXCLUDED.${col}`).join(', ');
    const query = `
        INSERT INTO ${tableName} (${colNames.join(', ')})
        VALUES ${values.join(', ')}
        ON CONFLICT (id) DO UPDATE SET ${setClause};
    `;
    await client.query(query, params);
}

async function aggregateLevel(client, tableName, mapping, sourceLevel, aggBar) {
    const targetLevel = sourceLevel - 1;
    const countRes = await client.query(`SELECT COUNT(*) FROM ${tableName} WHERE resolution = $1`, [sourceLevel]);
    const totalSourceRows = parseInt(countRes.rows[0].count);
    if (totalSourceRows === 0) return 0;

    aggBar.setTotal(totalSourceRows);
    let processedRows = 0;
    const startTime = Date.now();
    const colConfigs = Object.values(mapping);
    const colNames = colConfigs.map(m => m.column);
    
    const query = `SELECT id, ${colNames.join(', ')} FROM ${tableName} WHERE resolution = $1`;
    const { rows } = await client.query(query, [sourceLevel]);
    if (rows.length === 0) return 0;

    const parentGroups = new Map();
    for (const row of rows) {
        processedRows++;
        if (processedRows % 100 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            aggBar.update(processedRows, { speed: Math.round(processedRows / (elapsed || 1)) });
        }
        const parentId = h3.cellToParent(row.id, targetLevel);
        if (!parentGroups.has(parentId)) {
            parentGroups.set(parentId, {});
            for (const col of colNames) parentGroups.get(parentId)[col] = [];
        }
        const group = parentGroups.get(parentId);
        for (const col of colNames) {
            group[col].push(row[col]);
        }
    }

    const batch = [];
    for (const [parentId, data] of parentGroups.entries()) {
        const boundary = h3.cellToBoundary(parentId);
        const points = [...boundary, boundary[0]].map(p => `${p[1]} ${p[0]}`).join(', ');
        const wkt = `POLYGON((${points}))`;

        const row = [parentId, wkt, targetLevel];
        for (const config of colConfigs) {
            const values = data[config.column];
            const method = (config.method || 'MODE').toUpperCase();
            row.push(aggregateValues(values, method));
        }
        batch.push(row);
        if (batch.length >= 1000) {
            await insertBatch(client, tableName, mapping, batch);
            batch.length = 0;
        }
    }
    if (batch.length > 0) await insertBatch(client, tableName, mapping, batch);
    return totalSourceRows;
}

function aggregateValues(values, method) {
    if (!values || values.length === 0) return null;
    switch (method) {
        case 'SUM': return values.reduce((a, b) => a + (Number(b) || 0), 0);
        case 'AVG': return values.reduce((a, b) => a + (Number(b) || 0), 0) / values.length;
        case 'MIN': return Math.min(...values.filter(v => v !== null));
        case 'MAX': return Math.max(...values.filter(v => v !== null));
        case 'MODE':
        default:
            const counts = {};
            let mode = values[0];
            let maxCount = 0;
            for (const val of values) {
                counts[val] = (counts[val] || 0) + 1;
                if (counts[val] > maxCount) { maxCount = counts[val]; mode = val; }
            }
            return mode;
    }
}

module.exports = {
    processH3Data,
    aggregateLevel
};
