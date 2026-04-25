const { aggregateLevel } = require('../src/processor');
const h3 = require('h3-js');

describe('Aggregation Logic (Many-to-One)', () => {
    let mockClient;
    let mockBar;

    beforeEach(() => {
        mockClient = {
            query: jest.fn()
        };
        mockBar = {
            setTotal: jest.fn(),
            update: jest.fn(),
            stop: jest.fn()
        };
    });

    test('should collapse multiple children into 1 parent with SUMmed values', async () => {
        const sourceLevel = 11;
        const targetLevel = 10;
        const tableName = 'test_table';
        const mapping = {
            p: { column: 'population', type: 'NUMERIC', method: 'SUM' }
        };

        // Pick a parent and get its children
        const parentId = '8a8af68ee9affff';
        const children = h3.cellToChildren(parentId, sourceLevel);
        
        // Take 3 children
        const childA = children[0];
        const childB = children[1];
        const childC = children[2];

        // Mock 1: Initial count query
        mockClient.query.mockResolvedValueOnce({ rows: [{ count: '3' }] });
        
        // Mock 2: Data selection query
        mockClient.query.mockResolvedValueOnce({
            rows: [
                { id: childA, population: 100 },
                { id: childB, population: 200 },
                { id: childC, population: 50.5 }
            ]
        });

        // Mock 3: Insertion query
        mockClient.query.mockResolvedValueOnce({ rows: [] });

        await aggregateLevel(mockClient, tableName, mapping, sourceLevel, mockBar);

        // Verify the insertion call
        const insertCall = mockClient.query.mock.calls[2];
        const insertParams = insertCall[1];

        // Now we expect only 4 parameters (id, geom, resolution, sum) 
        // because all children collapsed into 1 parent
        expect(insertParams).toHaveLength(4);
        expect(insertParams[0]).toBe(parentId);
        expect(insertParams[2]).toBe(targetLevel);
        expect(insertParams[3]).toBe(350.5);
    });
});
