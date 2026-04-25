# H3 PG Aggregator CLI

A high-performance, production-ready Node.js CLI tool for processing large-scale H3 hexagonal data. It streamlines the ingestion of spatial data into PostGIS, performs hierarchical aggregation, and generates LuciadFusion/LuciadRIA connection descriptors (.pgs).

---

## 🚀 Features

-   **Multi-Format Ingestion**: Native streaming support for **JSON**, **CSV**, and **Apache Parquet**.
-   **Intelligent Aggregation**: Hierarchically aggregate data across H3 resolutions using multiple methods:
    -   `SUM`: Total value of children (e.g., Population).
    -   `AVG`: Average value (e.g., Temperature).
    -   `MIN` / `MAX`: Extrema values.
    -   `MODE`: Most frequent value (default, e.g., Land Use Type).
-   **PostGIS Optimization**: Automatically manages tables, spatial indexes, and batch inserts for maximum throughput (~10k+ cells/sec).
-   **LuciadFusion Ready**: Automatically generates `.pgs` connection descriptors for instant integration with the Luciad ecosystem.
-   **Visual Dashboard**: Real-time progress bars and performance metrics powered by `cli-progress`.
-   **Namespaced Config**: Safe environment variable management with `H3_` prefixing.

---

## 📦 Installation

### Prerequisites
-   **Node.js**: v18 or higher.
-   **PostgreSQL**: v15 or higher with **PostGIS** extension installed.

### Setup
1.  Clone the repository and install dependencies:
    ```bash
    npm install
    ```
2.  (Optional) Install globally to use the command anywhere:
    ```bash
    npm install -g .
    ```

---

## ⚙️ Configuration

### Environment Variables
Create a `.env` file in the root or set these in your shell:

```env
H3_DB_USER=your_user
H3_DB_HOST=localhost
H3_DB_NAME=your_db
H3_DB_PASSWORD=your_password
H3_DB_PORT=5432
```

### Column Mapping (`column_mapping.json`)
Define how your input data maps to PostGIS columns:

```json
{
  "population_count": {
    "column": "population",
    "type": "NUMERIC(11,3)",
    "method": "SUM",
    "displayName": "Total Population"
  },
  "color_hex": {
    "column": "color",
    "type": "INTEGER",
    "method": "MODE",
    "displayName": "Representative Color"
  }
}
```

---

## 🛠️ Usage

### Basic Ingestion
The tool automatically detects format by file extension:
```bash
h3-pg-aggregator data.csv --table climate_data
```

### Aggregation
Aggregate data up 3 levels from the source resolution:
```bash
h3-pg-aggregator data.parquet --table regional_data --aggregate 3
```

Aggregate specifically to a target resolution (e.g., Resolution 8):
```bash
h3-pg-aggregator data.json --aggregateTo 8
```

### Full Options
| Flag | Description | Default |
| :--- | :--- | :--- |
| `-t, --table` | Database table name | `h3_features` |
| `-o, --output` | Folder for `.pgs` file | `./output` |
| `-a, --aggregate` | Levels to aggregate back | `3` |
| `--aggregateTo` | Target resolution (overrides `-a`) | `null` |
| `-m, --mapping` | Path to mapping file | `./column_mapping.json` |
| `-f, --format` | Force format: `json`, `csv`, `parquet` | *Auto* |

---

## 🧪 Testing

The project includes a comprehensive test suite covering parsers, database logic, and aggregation math.

```bash
# Run all tests (Unit + Integration)
npm test

# Run only logic tests
npx jest tests/aggregation_logic.test.js

# Run full PostGIS integration test
npx jest tests/integration.test.js
```

---

## 📄 License
ISC License. Built for high-performance spatial engineering.
