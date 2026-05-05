const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const BaseProvider = require('./BaseProvider');

class PostgresProvider extends BaseProvider {
    async connect() {
        this.client = new Client({
            user: this.config.user,
            host: this.config.host,
            database: this.config.database,
            password: this.config.password,
            port: parseInt(this.config.port || '5432'),
        });
        await this.client.connect();
        return this.client;
    }

    async setupTable(tableName) {
        console.log(`Setting up Postgres table: ${tableName}`);
        await this.client.query(`DROP TABLE IF EXISTS ${tableName} CASCADE`);

        const columns = [
            'id TEXT PRIMARY KEY',
            'geom GEOMETRY(POLYGON, 4326)',
            'resolution INTEGER'
        ];

        for (const [key, config] of Object.entries(this.mapping)) {
            columns.push(`${config.column} ${config.type}`);
        }

        const createQuery = `
            CREATE TABLE ${tableName} (
                ${columns.join(',\n            ')}
            )
        `;

        await this.client.query(createQuery);
        console.log(`Table ${tableName} created successfully.`);
    }

    async createIndexes(tableName) {
        console.log(`Creating indexes for ${tableName}...`);
        await this.client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_geom_idx ON ${tableName} USING GIST (geom)`);
        await this.client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_res_idx ON ${tableName} (resolution)`);
        
        for (const [key, config] of Object.entries(this.mapping)) {
            const idxName = `${tableName}_${config.column}_idx`;
            await this.client.query(`CREATE INDEX IF NOT EXISTS ${idxName} ON ${tableName} (${config.column})`);
        }
    }

    async insertBatch(tableName, rows) {
        if (rows.length === 0) return;

        const colConfigs = Object.values(this.mapping);
        const colNames = ['id', 'geom', 'resolution', ...colConfigs.map(m => m.column)];
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
        await this.client.query(query, params);
    }

    async fetchRows(tableName, resolution) {
        const colNames = Object.values(this.mapping).map(m => m.column);
        const query = `SELECT id, ${colNames.join(', ')} FROM ${tableName} WHERE resolution = $1`;
        const { rows } = await this.client.query(query, [resolution]);
        return rows;
    }

    async countRows(tableName, resolution) {
        const { rows } = await this.client.query(`SELECT COUNT(*) FROM ${tableName} WHERE resolution = $1`, [resolution]);
        return parseInt(rows[0].count);
    }

    async generateDescriptor(outputDir, tableName) {
        const pgsPath = path.join(outputDir, `${tableName}.pgs`);
        let content = `# LuciadFusion Connection Descriptor for ${tableName} (PostgreSQL)\n`;
        content += `driver   = org.postgresql.Driver\n`;
        content += `url      = jdbc:postgresql://${this.config.host}:${this.config.port}/${this.config.database}\n`;
        content += `user     = ${this.config.user}\n`;
        content += `password = ${this.config.password}\n\n`;
        
        content += `table         = ${tableName}\n`;
        content += `spatialColumn = geom\n`;
        content += `SRID          = 4326\n\n`;
        
        content += `featureNames.0 = id\n`;
        content += `featureNames.1 = resolution\n`;
        
        let index = 2;
        for (const [key, config] of Object.entries(this.mapping)) {
            content += `featureNames.${index} = ${config.column}\n`;
            index++;
        }
        
        content += `\nfeatureDisplayNames.0 = H3 Index\n`;
        content += `featureDisplayNames.1 = Resolution\n`;
        
        index = 2;
        for (const [key, config] of Object.entries(this.mapping)) {
            content += `featureDisplayNames.${index} = ${config.displayName || config.column}\n`;
            index++;
        }
        
        content += `\nprimaryFeatureIndex = 0\n`;
        content += `primaryFeatureAutoGenerated = false\n\n`;
        
        content += `featureDataTypes.0 = TEXT\n`;
        content += `featureDataTypes.1 = INTEGER\n`;
        
        index = 2;
        for (const [key, config] of Object.entries(this.mapping)) {
            content += `featureDataTypes.${index} = ${config.type}\n`;
            index++;
        }
        
        content += `\nmaxCacheSize = 1000\n`;

        if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
        fs.writeFileSync(pgsPath, content);
    }

    async close() {
        if (this.client) await this.client.end();
    }
}

module.exports = PostgresProvider;
