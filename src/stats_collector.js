const colors = require('colors');

class StatsCollector {
    constructor(mapping) {
        this.mapping = mapping;
        this.startTime = Date.now();
        this.totalProcessed = 0;
        this.resolutions = new Set();
        
        // Detailed metrics per column
        this.metrics = {};
        this.uniqueValues = {}; // Maps: col -> Map(value -> count)
        
        for (const [key, config] of Object.entries(mapping)) {
            const col = config.column;
            this.metrics[col] = {
                min: Infinity,
                max: -Infinity,
                sum: 0,
                count: 0,
                type: config.type || 'INTEGER'
            };
            this.uniqueValues[col] = new Map();
        }
    }

    collect(h3Index, rowData) {
        this.totalProcessed++;
        
        for (const [key, config] of Object.entries(this.mapping)) {
            const col = config.column;
            const val = rowData[key] ?? rowData[col];
            
            if (val === undefined || val === null) continue;

            const num = Number(val);
            if (!isNaN(num)) {
                if (num < this.metrics[col].min) this.metrics[col].min = num;
                if (num > this.metrics[col].max) this.metrics[col].max = num;
                this.metrics[col].sum += num;
            }
            this.metrics[col].count++;

            // Track unique values (internal Map is efficient)
            if (this.uniqueValues[col].size <= 100) {
                const currentCount = this.uniqueValues[col].get(val) || 0;
                this.uniqueValues[col].set(val, currentCount + 1);
            }
        }
    }

    finalize(resolutions) {
        this.durationMs = Date.now() - this.startTime;
        this.resolutions = resolutions;
    }

    _sortValues(values, type) {
        const isNumeric = type.includes('INT') || type.includes('NUM') || type.includes('FLOAT') || type.includes('DOUBLE');
        return [...values].sort((a, b) => {
            if (isNumeric) return Number(a) - Number(b);
            return String(a).localeCompare(String(b));
        });
    }

    generateReport() {
        const report = {
            execution: {
                timestamp: new Date().toISOString(),
                duration_sec: (this.durationMs / 1000).toFixed(2),
                total_cells: this.totalProcessed,
                throughput_cells_per_sec: Math.round(this.totalProcessed / (this.durationMs / 1000 || 1))
            },
            data: {
                resolutions: this.resolutions,
                columns: {}
            }
        };

        for (const col in this.metrics) {
            const m = this.metrics[col];
            const rawUniques = Array.from(this.uniqueValues[col].keys());
            const sortedUniques = rawUniques.length <= 36 ? this._sortValues(rawUniques, m.type) : null;
            
            report.data.columns[col] = {
                type: m.type,
                processed_count: m.count,
                min: m.min === Infinity ? null : m.min,
                max: m.max === -Infinity ? null : m.max,
                avg: m.count > 0 ? (m.sum / m.count) : 0,
                sum: m.sum,
                unique_count: rawUniques.length,
                sample_values: sortedUniques
            };
        }

        return report;
    }

    generateMarkdown(tableName) {
        const r = this.generateReport();
        let md = `# H3 Data Summary: ${tableName}\n\n`;
        
        md += `## ⏱️ Execution\n`;
        md += `- **Date**: ${r.execution.timestamp}\n`;
        md += `- **Duration**: ${r.execution.duration_sec}s\n`;
        md += `- **Speed**: ${r.execution.throughput_cells_per_sec.toLocaleString()} cells/sec\n\n`;
        
        md += `## 📊 Data Overview\n`;
        md += `- **Total Cells**: ${r.execution.total_cells.toLocaleString()}\n`;
        md += `- **Resolutions**: ${r.data.resolutions.join(', ')}\n\n`;
        
        md += `## 📑 Column Analysis\n\n`;
        md += `| Column | Type | Values | Uniques | Range / Sum |\n`;
        md += `| :--- | :--- | :--- | :--- | :--- |\n`;
        
        for (const col in r.data.columns) {
            const c = r.data.columns[col];
            const displayName = Object.values(this.mapping).find(m => m.column === col)?.displayName || col;
            const counts = this.uniqueValues[col];
            
            let uniqueStr = '';
            if (c.sample_values) {
                // Include counts only in Markdown, following the sorted order
                uniqueStr = c.sample_values
                    .map(val => `${val} (${(counts.get(val) || 0).toLocaleString()})`)
                    .join(', ');
            } else {
                uniqueStr = `Too many to list (>${c.unique_count})`;
            }

            const rangeStr = c.min !== null ? `[${c.min.toLocaleString()} to ${c.max.toLocaleString()}]<br>Σ: ${c.sum.toLocaleString()}` : 'N/A';
            
            md += `| **${displayName}** | ${c.type} | ${c.processed_count.toLocaleString()} | ${uniqueStr} | ${rangeStr} |\n`;
        }
        
        return md;
    }
}

module.exports = StatsCollector;
