# H3 PG Aggregator

A high-performance, production-ready tool to digitalize images into H3 hexagonal grids, upload them to PostGIS, and perform hierarchical spatial aggregation. Designed for seamless integration with **LuciadFusion** and **LuciadRIA**.

## 🚀 Key Features

- **Multi-Format Support**: Seamlessly ingest data from **JSON**, **CSV**, and **Apache Parquet**.
- **High-Speed Ingestion**: Optimized streaming pipeline capable of **~17,000 cells/sec**.
- **Hierarchical Aggregation**: Automatically collapses children into parents (e.g., Res 11 → 10 → 9 → 8) using configurable math (`SUM`, `AVG`, `MODE`).
- **Professional Dashboard**: Real-time progress monitoring with per-phase timing and 100% accurate percentage tracking via pre-scan analysis.
- **Data Analytics (Optional)**: Generate detailed `summary.md` and `report.json` insights using the `--statistics` flag.
- **Luciad Integration**: Generates `.pgs` connection descriptors for instant layer deployment in Luciad systems.

## 🛠️ Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd h3-pg-aggregator

# Install dependencies
npm install

# Build the production bundle
npm run build
```

## ⚙️ Configuration

1. **Environment Variables**: Copy `.env.sample` to `.env` and configure your PostGIS credentials:
   ```env
   H3_DB_HOST=localhost
   H3_DB_NAME=h3db
   H3_DB_USER=h3expert
   H3_DB_PASSWORD=yourpassword
   ```

2. **Column Mapping**: Define how your data maps to Postgres in `mapping.json`:
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

### Basic Ingestion
```bash
h3-pg-aggregator data.json --table city_grid aggregateTo
```

### Advanced Aggregation (3 levels)
```bash
h3-pg-aggregator data.parquet --table world_data --aggregate 3
```

### With Professional Statistics
```bash
h3-pg-aggregator data.csv --table region_stats --statistics --output ./reports
```

## 📊 CLI Options

| Option | Description                            | Default |
| :--- |:---------------------------------------| :--- |
| `-t, --table` | Target PostGIS table name              | `h3_features` |
| `-o, --output` | Folder for .pgs and reports            | `./output` |
| `-a, --aggregate` | Number of levels to aggregate up       | `3` |
| `--aggregateTo` | Target resolution to aggregate down to | `8` |
| `-m, --mapping` | Path to column mapping JSON file       | `./mapping.json` |
| `-s, --statistics` | Generate detailed reports              | `false` |
| `-f, --format` | Force format (json, csv, parquet)      | Auto-detect |

## 🧪 Development & Testing

- **Run Tests**: `npm test`
- **Dev Mode**: `npm run dev -- <args>` (Run directly from source)
- **Build**: `npm run build` (Minifies project into `dist/`)

---
Built with ❤️ for High-Performance Geospatial Analytics.
