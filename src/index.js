#!/usr/bin/env node
const colors = require('colors');
const { Command } = require('commander');
const path = require('path');
const fs = require('fs');
const { getClient, setupTable, dbConfig } = require('./db');
const { processH3Data } = require('./processor');
const { generatePgs } = require('./pgs_generator');
require('dotenv').config();

const { createJSONStream, createCSVStream, createParquetGenerator, countCells } = require('./parsers');

const program = new Command();

program
    .name('h3-pg-aggregator')
    .description('Production-ready H3 Image Digitalizer and PostGIS Uploader')
    .version('1.0.0')
    .argument('<input-file>', 'JSON, CSV, or Parquet file containing H3 cells')
    .option('-t, --table <name>', 'Table name to store the data', 'h3_features')
    .option('-o, --output <folder>', 'Output folder for .pgs file', './output')
    .option('-a, --aggregate <levels>', 'Number of levels to aggregate back', '3')
    .option('--aggregateTo <res>', 'Target resolution to aggregate down to', '8')
    .option('-m, --mapping <path>', 'Path to the column mapping JSON file', './mapping.json')
    .option('-f, --format <type>', 'Input format: json, csv, parquet (auto-detected by default)')
    .option('-s, --statistics', 'Generate detailed statistics report (report.json and summary.md)', false)
    .action(async (inputFile, options) => {
        const absoluteInputPath = path.resolve(inputFile);
        const absoluteOutputPath = path.resolve(options.output);
        const tableName = options.table;
        const aggregateLevels = parseInt(options.aggregate);
        const aggregateTo = options.aggregateTo ? parseInt(options.aggregateTo) : null;

        // 1. Environment Validation
        const requiredEnv = ['H3_DB_USER', 'H3_DB_HOST', 'H3_DB_NAME', 'H3_DB_PASSWORD'];
        const missingEnv = requiredEnv.filter(key => !process.env[key]);

        if (missingEnv.length > 0) {
            console.error('\x1b[31mError: Missing required environment variables:\x1b[0m', missingEnv.join(', '));
            console.log('\nPlease set these in your shell or create a .env file:');
            console.log('----------------------------------------------------');
            console.log('H3_DB_USER=h3expert\nH3_DB_HOST=localhost\nH3_DB_NAME=h3dbtest\nH3_DB_PASSWORD=h3password\nH3_DB_PORT=5432');
            console.log('----------------------------------------------------');
            process.exit(1);
        }

        // 2. Load mapping
        const mappingPath = path.resolve(options.mapping);
        if (!fs.existsSync(mappingPath)) {
            console.error(`\x1b[31mError: Mapping file not found at ${mappingPath}\x1b[0m`);
            console.log('\nPlease ensure your mapping file exists or specify it with -m <path>.');
            process.exit(1);
        }
        const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));

        // 3. Detect Format and Create Stream
        const ext = path.extname(absoluteInputPath).toLowerCase();
        const format = options.format || (ext === '.json' ? 'json' : ext === '.csv' ? 'csv' : ext === '.parquet' ? 'parquet' : null);

        if (!format) {
            console.error(`\x1b[31mError: Could not detect format for ${absoluteInputPath}. Use -f to specify.\x1b[0m`);
            process.exit(1);
        }

        console.log(`--- H3 PG Aggregator CLI ---`);
        console.log(`Input: ${absoluteInputPath} (${format.toUpperCase()})`);
        console.log(`Mapping: ${mappingPath}`);
        console.log(`Table: ${tableName}`);
        console.log(`Output Folder: ${absoluteOutputPath}`);
        console.log(`Aggregate Levels: ${aggregateLevels}`);
        if (aggregateTo !== null) console.log(`Aggregate To: ${aggregateTo} (Priority)`);
        console.log(`----------------------------\n`);

        let client;
        try {
            client = await getClient();

            // 1. Setup Table
            await setupTable(client, tableName, mapping);

            // 2. Create Data Stream
            let dataStream;
            if (format === 'json') {
                dataStream = createJSONStream(absoluteInputPath);
            } else if (format === 'csv') {
                dataStream = createCSVStream(absoluteInputPath);
            } else if (format === 'parquet') {
                dataStream = createParquetGenerator(absoluteInputPath);
            }

            // 3. Analyze & Process
            const globalStartTime = Date.now();
            console.log(colors.yellow('Analyzing data for accurate progress tracking...'));
            const totalCells = await countCells(absoluteInputPath, format);
            
            const stats = await processH3Data(client, dataStream, tableName, mapping, aggregateLevels, aggregateTo, totalCells, options.statistics);

            // 4. Generate PGS
            console.log(colors.cyan('\n--- PGS Generation ---'));
            generatePgs(absoluteOutputPath, tableName, dbConfig, mapping);
            console.log(`.pgs file generated at: ${path.join(absoluteOutputPath, tableName + '.pgs')}`);

            // 5. Generate Statistics Report
            if (options.statistics && stats) {
                console.log(colors.cyan('\n--- Statistics Report ---'));
                const report = stats.generateReport();
                const markdown = stats.generateMarkdown(tableName);

                if (!fs.existsSync(absoluteOutputPath)) fs.mkdirSync(absoluteOutputPath, { recursive: true });
                fs.writeFileSync(path.join(absoluteOutputPath, 'report.json'), JSON.stringify(report, null, 2));
                fs.writeFileSync(path.join(absoluteOutputPath, 'summary.md'), markdown);
                console.log(`Statistics report saved to: ${path.join(absoluteOutputPath, 'summary.md')}`);
            }

            const totalDuration = ((Date.now() - globalStartTime) / 1000).toFixed(1);
            console.log(colors.green(`\nTotal Execution Time: ${totalDuration}s`));
        } catch (err) {
            console.error(`Fatal Error:`, err);
            process.exit(1);
        } finally {
            if (client) await client.end();
        }
    });

program.parse(process.argv);
