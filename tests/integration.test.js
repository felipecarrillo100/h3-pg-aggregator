const { getClient, setupTable } = require('../src/db');
const { processH3Data } = require('../src/processor');
const { createJSONStream, createCSVStream, createParquetGenerator } = require('../src/parsers');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

describe('Multi-Format Integration Tests', () => {
    let client;
    const filePath = path.resolve(__dirname, '../sampleData/sample.json');
    const mappingPath = path.resolve(__dirname, '../sampleData/sample_mapping.json');
    let mapping;

    beforeAll(async () => {
        mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
        client = await getClient();
    });

    afterAll(async () => {
        if (client) await client.end();
    });

    const formats = [
        { name: 'JSON', file: 'sample.json', stream: createJSONStream },
        { name: 'CSV', file: 'sample.csv', stream: createCSVStream },
        { name: 'Parquet', file: 'sample.parquet', stream: createParquetGenerator }
    ];

    formats.forEach(format => {
        test(`should upload ${format.name} data and verify integrity`, async () => {
            const tableName = `test_${format.name.toLowerCase()}`;
            const filePath = path.resolve(__dirname, `../sampleData/${format.file}`);

            // 1. Setup Table
            await setupTable(client, tableName, mapping);

            // 2. Start Ingestion
            const dataStream = format.stream(filePath);
            
            // Mock progress bar
            const mockBar = { 
                update: () => {}, 
                stop: () => {}, 
                create: () => ({ update: () => {}, stop: () => {} }),
                setTotal: () => {}
            };

            await processH3Data(client, dataStream, tableName, mapping, 1, null);

            // 3. Verify Row Count (217 children + parents)
            const countRes = await client.query(`SELECT COUNT(*) FROM ${tableName}`);
            const totalRows = parseInt(countRes.rows[0].count);
            
            console.log(`Verified: ${totalRows} rows in ${tableName}`);
            expect(totalRows).toBeGreaterThan(217); // Children (217) + aggregated parents

            // 4. Verify Resolution Mix
            const resRes = await client.query(`SELECT DISTINCT resolution FROM ${tableName} ORDER BY resolution`);
            const resolutions = resRes.rows.map(r => r.resolution);
            expect(resolutions).toContain(11); // Original
            expect(resolutions).toContain(10); // Aggregated
        }, 30000);
    });
});
