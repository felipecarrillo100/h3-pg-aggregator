const { setupTable } = require('../src/db');

describe('Database Utilities', () => {
    let mockClient;

    beforeEach(() => {
        mockClient = {
            query: jest.fn().mockResolvedValue({ rows: [] })
        };
    });

    test('setupTable should generate correct SQL with mapping', async () => {
        const tableName = 'test_h3';
        const mapping = {
            c: { column: 'color', type: 'INTEGER' },
            p: { column: 'pop', type: 'NUMERIC(10,2)' }
        };

        await setupTable(mockClient, tableName, mapping);

        // Check if query was called
        expect(mockClient.query).toHaveBeenCalled();
        
        // The first call is DROP TABLE
        const dropSql = mockClient.query.mock.calls[0][0];
        expect(dropSql).toContain('DROP TABLE IF EXISTS test_h3 CASCADE');

        // The second call is CREATE TABLE
        const createSql = mockClient.query.mock.calls[1][0];
        expect(createSql).toContain('CREATE TABLE test_h3');
        expect(createSql).toContain('color INTEGER');
        expect(createSql).toContain('pop NUMERIC(10,2)');
        expect(createSql).toContain('id TEXT PRIMARY KEY');
    });
});
