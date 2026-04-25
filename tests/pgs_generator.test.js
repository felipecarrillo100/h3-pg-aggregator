const { generatePgs } = require('../src/pgs_generator');
const fs = require('fs');
const path = require('path');

describe('PGS Generator', () => {
    const outputDir = path.resolve(__dirname, 'output_test');
    const tableName = 'test_table';
    const config = { host: 'localhost', user: 'user', database: 'db', port: 5432 };
    const mapping = {
        c: { column: 'color', type: 'INTEGER' },
        p: { column: 'population', type: 'NUMERIC' }
    };

    beforeAll(() => {
        if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
    });

    afterAll(() => {
        if (fs.existsSync(path.join(outputDir, `${tableName}.pgs`))) {
            fs.unlinkSync(path.join(outputDir, `${tableName}.pgs`));
        }
        fs.rmdirSync(outputDir);
    });

    test('should generate a valid .pgs file content', () => {
        generatePgs(outputDir, tableName, config, mapping);
        const pgsPath = path.join(outputDir, `${tableName}.pgs`);
        
        expect(fs.existsSync(pgsPath)).toBe(true);
        const content = fs.readFileSync(pgsPath, 'utf8');
        
        expect(content).toMatch(/table\s*=\s*test_table/);
        expect(content).toMatch(/featureDataTypes\.2\s*=\s*INTEGER/);
        expect(content).toMatch(/featureDataTypes\.3\s*=\s*NUMERIC/);
        expect(content).toMatch(/url\s*=\s*jdbc:postgresql:\/\/localhost/);
    });
});
