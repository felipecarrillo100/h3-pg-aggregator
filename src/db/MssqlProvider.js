const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const BaseProvider = require('./BaseProvider');

class MssqlProvider extends BaseProvider {
    async connect() {
        const config = {
            user: this.config.user,
            password: this.config.password,
            server: this.config.host,
            database: this.config.database,
            port: parseInt(this.config.port || '1433'),
            options: {
                encrypt: false,
                trustServerCertificate: true,
            },
            pool: {
                max: 10,
                min: 0,
                idleTimeoutMillis: 30000
            }
        };
        this.pool = await sql.connect(config);
        return this.pool;
    }

    async setupTable(tableName) {
        console.log(`Setting up MSSQL table: ${tableName}`);

        // Drop table if exists
        await this.pool.request().query(`DROP TABLE IF EXISTS [${tableName}]`);

        const columns = [
            'id NVARCHAR(100) PRIMARY KEY',
            'geom GEOGRAPHY',
            'resolution INT'
        ];

        for (const [key, config] of Object.entries(this.mapping)) {
            // Map common PG types to MSSQL if needed, though most are compatible
            let type = config.type.toUpperCase();
            if (type === 'TEXT') type = 'NVARCHAR(MAX)';
            if (type === 'INTEGER') type = 'INT';
            if (type.includes('NUMERIC')) type = type.replace('NUMERIC', 'DECIMAL');

            columns.push(`${config.column} ${type}`);
        }

        const createQuery = `
            CREATE TABLE [${tableName}] (
                ${columns.join(',\n            ')}
            );
        `;

        await this.pool.request().query(createQuery);
        console.log(`Table [${tableName}] created successfully.`);
    }

    async createIndexes(tableName) {
        console.log(`Creating indexes for [${tableName}]...`);
        
        // 1. Spatial Index (Critical for BBOX queries)
        try {
            const request = this.pool.request();
            request.timeout = 300000; // 5 minutes for large spatial indexing
            await request.query(`
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '${tableName}_geom_idx')
                CREATE SPATIAL INDEX [${tableName}_geom_idx] ON [${tableName}](geom);
            `);
        } catch (e) {
            console.warn(`Warning: Could not create spatial index: ${e.message}`);
        }

        // 2. Resolution Index
        await this.pool.request().query(`
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '${tableName}_res_idx')
            CREATE INDEX [${tableName}_res_idx] ON [${tableName}](resolution)
        `);

        // 3. Mapping Column Indexes (Speed up "per column" queries)
        for (const [key, config] of Object.entries(this.mapping)) {
            const idxName = `${tableName}_${config.column}_idx`;
            try {
                await this.pool.request().query(`
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '${idxName}')
                    CREATE INDEX [${idxName}] ON [${tableName}]([${config.column}])
                `);
            } catch (e) {
                console.warn(`Could not create index for ${config.column}: ${e.message}`);
            }
        }
        console.log('Indexes created successfully.');
    }

    async insertBatch(tableName, rows) {
        if (rows.length === 0) return;

        // JSON Speed Boost: Send data as a single JSON string and let MSSQL parse it.
        // This is significantly faster than row-by-row inserts for spatial data.
        const colConfigs = Object.values(this.mapping);
        const colNames = colConfigs.map(m => m.column);
        
        // Prepare the JSON structure
        const jsonData = rows.map(row => {
            const entry = {
                id: row[0],
                wkt: row[1],
                res: row[2]
            };
            colNames.forEach((col, idx) => {
                entry[col] = row[idx + 3];
            });
            return entry;
        });

        // Map column types for OPENJSON
        const jsonSchema = [
            'id NVARCHAR(450)',
            'wkt NVARCHAR(MAX)',
            'res INT'
        ];
        colNames.forEach((col, idx) => {
            let type = colConfigs[idx].type.toUpperCase();
            if (type === 'TEXT') type = 'NVARCHAR(MAX)';
            if (type === 'INTEGER') type = 'INT';
            if (type.includes('NUMERIC')) type = type.replace('NUMERIC', 'DECIMAL');
            jsonSchema.push(`${col} ${type}`);
        });

        const updateSet = colNames.map(col => `target.${col} = source.${col}`).join(', ');
        const insertCols = `id, geom, resolution, ${colNames.join(', ')}`;
        const insertVals = `source.id, geography::STGeomFromText(source.wkt, 4326), source.res, ${colNames.map(col => `source.${col}`).join(', ')}`;

        const query = `
            MERGE INTO [${tableName}] AS target
            USING (
                SELECT * FROM OPENJSON(@json)
                WITH (${jsonSchema.join(', ')})
            ) AS source
            ON (target.id = source.id)
            WHEN MATCHED THEN
                UPDATE SET 
                    target.geom = geography::STGeomFromText(source.wkt, 4326),
                    target.resolution = source.res,
                    ${updateSet}
            WHEN NOT MATCHED THEN
                INSERT (${insertCols}) VALUES (${insertVals});
        `;

        const request = this.pool.request();
        request.input('json', sql.NVarChar(sql.MAX), JSON.stringify(jsonData));
        await request.query(query);
    }

    async fetchRows(tableName, resolution) {
        const colNames = Object.values(this.mapping).map(m => m.column);
        const query = `SELECT id, ${colNames.join(', ')} FROM [${tableName}] WHERE resolution = @res`;
        const result = await this.pool.request()
            .input('res', sql.Int, resolution)
            .query(query);
        return result.recordset;
    }

    async countRows(tableName, resolution) {
        const result = await this.pool.request()
            .input('res', sql.Int, resolution)
            .query(`SELECT COUNT(*) as count FROM [${tableName}] WHERE resolution = @res`);
        return result.recordset[0].count;
    }

    async generateDescriptor(outputDir, tableName) {
        const mssPath = path.join(outputDir, `${tableName}.mss`);
        let content = `# LuciadFusion Connection Descriptor for ${tableName} (MSSQL)\n`;
        content += `driver   = com.microsoft.sqlserver.jdbc.SQLServerDriver\n`;
        content += `url      = jdbc:sqlserver://${this.config.host}:${this.config.port};databaseName=${this.config.database};encrypt=true;trustServerCertificate=true\n`;
        content += `user     = ${this.config.user}\n`;
        content += `password = ${this.config.password}\n\n`;

        content += `table         = ${tableName}\n`;
        content += `spatialColumn = geom\n`;
        content += `SRID          = 4326\n`;
        content += `geometryType  = GEOGRAPHY\n\n`;

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

        content += `featureDataTypes.0 = NVARCHAR(100)\n`;
        content += `featureDataTypes.1 = INTEGER\n`;

        index = 2;
        for (const [key, config] of Object.entries(this.mapping)) {
            let type = config.type.toUpperCase();
            if (type === 'TEXT') type = 'NVARCHAR(MAX)';
            if (type === 'INTEGER') type = 'INT';
            content += `featureDataTypes.${index} = ${type}\n`;
            index++;
        }

        content += `\nmaxCacheSize = 1000\n`;

        if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
        fs.writeFileSync(mssPath, content);
    }

    async close() {
        if (this.pool) await this.pool.close();
    }
}

module.exports = MssqlProvider;
