const { createJSONStream, createCSVStream } = require('../src/parsers');
const path = require('path');
const fs = require('fs');

describe('Data Parsers', () => {
    const testCsvPath = path.resolve(__dirname, 'test.csv');
    const testJsonPath = path.resolve(__dirname, 'test.json');

    beforeAll(() => {
        fs.writeFileSync(testCsvPath, 'i,c,p\n8b8af68ee9aefff,15761189,100.5');
        fs.writeFileSync(testJsonPath, JSON.stringify([{ i: '8b8af68ee9aefff', c: 15761189, p: 100.5 }]));
    });

    afterAll(() => {
        if (fs.existsSync(testCsvPath)) fs.unlinkSync(testCsvPath);
        if (fs.existsSync(testJsonPath)) fs.unlinkSync(testJsonPath);
    });

    test('CSV Parser should yield correct objects', async () => {
        const stream = createCSVStream(testCsvPath);
        const results = [];
        for await (const row of stream) {
            results.push(row);
        }
        expect(results).toHaveLength(1);
        expect(results[0].i).toBe('8b8af68ee9aefff');
        // csv-parse with cast:true returns numbers for numeric strings
        expect(Number(results[0].c)).toBe(15761189);
    });

    test('JSON Parser should yield correct objects', async () => {
        const stream = createJSONStream(testJsonPath);
        const results = [];
        for await (const row of stream) {
            // results[0] will be the raw object because createJSONStream flattens it
            results.push(row);
        }
        expect(results).toHaveLength(1);
        expect(results[0].i).toBe('8b8af68ee9aefff');
    });
});
