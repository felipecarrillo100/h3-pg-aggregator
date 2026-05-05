/**
 * Base class for Database Providers
 */
class BaseProvider {
    constructor(config, mapping) {
        this.config = config;
        this.mapping = mapping;
    }

    /**
     * Connect to the database
     */
    async connect() {
        throw new Error('Method connect() must be implemented');
    }

    /**
     * Setup the table (Drop and Create)
     */
    async setupTable(tableName) {
        throw new Error('Method setupTable() must be implemented');
    }

    /**
     * Insert a batch of rows
     */
    async insertBatch(tableName, rows) {
        throw new Error('Method insertBatch() must be implemented');
    }

    /**
     * Fetch rows for a specific resolution
     */
    async fetchRows(tableName, resolution) {
        throw new Error('Method fetchRows() must be implemented');
    }

    /**
     * Generate the Luciad connection descriptor
     */
    async generateDescriptor(outputDir, tableName) {
        throw new Error('Method generateDescriptor() must be implemented');
    }

    /**
     * Close the connection
     */
    async close() {
        throw new Error('Method close() must be implemented');
    }
}

module.exports = BaseProvider;
