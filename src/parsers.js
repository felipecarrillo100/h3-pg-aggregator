const fs = require('fs');
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/stream-array.js');
const { parse: csvParser } = require('csv-parse');
const parquet = require('parquetjs-lite');

/**
 * Creates a stream that yields { i: h3Index, ...attributes }
 */
function createJSONStream(filePath) {
    const sourceStream = fs.createReadStream(filePath);
    const pipeline = chain([
        sourceStream,
        parser(),
        streamArray(),
        (data) => data.value
    ]);
    pipeline.sourceStream = sourceStream; // Attach to track bytes
    return pipeline;
}

/**
 * Creates a stream for CSV files
 */
function createCSVStream(filePath) {
    const sourceStream = fs.createReadStream(filePath);
    const pipeline = chain([
        sourceStream,
        csvParser({
            columns: true,
            skip_empty_lines: true,
            cast: true
        })
    ]);
    pipeline.sourceStream = sourceStream; // Attach to track bytes
    return pipeline;
}

/**
 * Helper to wrap Parquet reader in a stream-like interface
 */
async function* createParquetGenerator(filePath) {
    const reader = await parquet.ParquetReader.openFile(filePath);
    const cursor = reader.getCursor();
    let record = null;
    while (record = await cursor.next()) {
        yield record;
    }
    await reader.close();
}

/**
 * High-speed streaming scan to count H3 cells in a file before processing.
 * This ensures the progress bar total is 100% accurate.
 */
async function countCells(filePath, format) {
    let count = 0;
    if (format === 'json') {
        const pipeline = createJSONStream(filePath);
        for await (const _ of pipeline) count++;
    } else if (format === 'csv') {
        const pipeline = createCSVStream(filePath);
        for await (const _ of pipeline) count++;
    } else if (format === 'parquet') {
        const reader = await parquet.ParquetReader.openFile(filePath);
        count = Number(reader.getRowCount());
        await reader.close();
    }
    return count;
}

module.exports = {
    createJSONStream,
    createCSVStream,
    createParquetGenerator,
    countCells
};
