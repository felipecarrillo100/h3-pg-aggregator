# H3 DB Aggregator

A high-performance, production-ready tool to digitalize images into H3 hexagonal grids, upload them to **PostGIS** or **Microsoft SQL Server (MSSQL)**, and perform hierarchical spatial aggregation. Designed for seamless integration with **LuciadFusion** and **LuciadRIA**.

## 🚀 Key Features

- **Multi-Database Support**: Native connectors for **PostgreSQL (PostGIS)** and **MSSQL (Spatial)**.
- **Multi-Format Support**: Seamlessly ingest data from **JSON**, **CSV**, and **Apache Parquet**.
- **High-Speed Ingestion**: Optimized streaming pipeline capable of **~17,000 cells/sec**.
- **Hierarchical Aggregation**: Automatically collapses children into parents (e.g., Res 11 → 10 → 9 → 8) using configurable math (`SUM`, `AVG`, `MODE`).
- **Professional Dashboard**: Real-time progress monitoring via pre-scan analysis.
- **Luciad Integration**: Generates `.pgs` (Postgres) or `.mss` (MSSQL) connection descriptors for instant layer deployment.

## 🛠️ Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd h3-pg-aggregator

# Install dependencies (includes pg and mssql drivers)
npm install

# Build the production bundle
npm run build
```

## ⚙️ Configuration

1. **Environment Variables**: Copy `.env.sample` to `.env` and configure your credentials. The tool uses a unified set of environment variables for simplicity:
   ```env
   H3_DB_HOST=localhost
   H3_DB_NAME=h3dbtest
   H3_DB_USER=h3expert
   H3_DB_PASSWORD=H3password!
   H3_DB_PORT=5432  # Use 1433 for MSSQL
   ```

2. **Column Mapping**: Define how your data maps to the database in `mapping.json`:
   ```json
   {
     "p": {
       "column": "population",
       "type": "NUMERIC(11,3)",
       "method": "SUM",
       "displayName": "Population Density"
     }
   }
   ```

## 📖 Usage

### PostGIS Ingestion
```bash
# Defaults to postgres
h3-pg-aggregator data.json --table city_grid --aggregate 3
```

### MSSQL Ingestion
```bash
# Specify MSSQL via the -d flag
h3-pg-aggregator data.json --db mssql --table city_grid_ms --aggregate 3
```

### Advanced Options
```bash
h3-pg-aggregator data.parquet --db mssql --table world_data --aggregateTo 8 --statistics
```

## 📊 CLI Options

| Option | Description | Default |
| :--- | :--- | :--- |
| `-d, --db` | Database type: `postgres` or `mssql` | `postgres` |
| `-t, --table` | Target table name | `h3_features` |
| `-o, --output` | Folder for descriptors (.pgs/.mss) and reports | `./output` |
| `-a, --aggregate` | Number of levels to aggregate up | `3` |
| `--aggregateTo` | Target resolution to aggregate down to | `8` |
| `-m, --mapping` | Path to column mapping JSON file | `./mapping.json` |
| `-s, --statistics` | Generate detailed reports (`summary.md`) | `false` |
| `-f, --format` | Force format (json, csv, parquet) | Auto-detect |

## 🧪 Development & Testing

- **Run Tests**: `npm test`
- **Dev Mode**: `npm run dev -- <args>`
- **Build**: `npm run build`

---
Built with ❤️ for High-Performance Geospatial Analytics.
