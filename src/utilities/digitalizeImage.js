const h3 = require('h3-js');
const sharp = require('sharp');
const fs = require('fs');

/**
 * Digitalizes an image into H3 cells using Super-Sampling and generates a color palette.
 */
async function digitalizeToH3Streaming(imagePath, startCell, outputPath, radiusCells = 400) {
    const res = h3.getResolution(startCell);
    const startCoords = h3.cellToLatLng(startCell);

    console.log(`--- H3 Digitalizer Phase 1: Pre-processing & Analysis ---`);
    const image = sharp(imagePath);
    const metadata = await image.metadata();
    
    // Calculate global stats for the 16-color gradient
    const stats = await image.stats();
    const minColor = [stats.channels[0].min, stats.channels[1].min, stats.channels[2].min];
    const maxColor = [stats.channels[0].max, stats.channels[1].max, stats.channels[2].max];

    // Resize for efficient sampling
    const processingWidth = Math.min(metadata.width, 2000);
    const resizedImage = image.resize(processingWidth);
    const { data, info } = await resizedImage.raw().toBuffer({ resolveWithObject: true });
    
    console.log(`Image analysis complete. Extracting 16 dominant colors (K-Means)...`);
    const palette = extractPalette(data, info, 16);
    fs.writeFileSync('palette.json', JSON.stringify({
        description: "16 dominant colors extracted via K-Means",
        gradient: palette
    }, null, 2));

    // Initialize Streaming Output
    const writeStream = fs.createWriteStream(outputPath);
    writeStream.write('[\n');

    console.log(`--- H3 Digitalizer Phase 2: Traversal & Super-Sampling ---`);
    const allCells = h3.gridDisk(startCell, radiusCells);
    console.log(`Processing ${allCells.length.toLocaleString()} cells at Res ${res}...`);

    const clusterCoords = allCells.map(c => h3.cellToLatLng(c));
    let minLat = Infinity, maxLat = -Infinity, minLng = Infinity, maxLng = -Infinity;
    for (const [lat, lng] of clusterCoords) {
        if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat;
        if (lng < minLng) minLng = lng; if (lng > maxLng) maxLng = lng;
    }

    const latRange = maxLat - minLat;
    const lngRange = maxLng - minLng;
    const startTime = Date.now();

    for (let i = 0; i < allCells.length; i++) {
        const [lat, lng] = clusterCoords[i];
        const normX = (lng - minLng) / lngRange;
        const normY = 1 - ((lat - minLat) / latRange);

        const centerX = normX * (info.width - 1);
        const centerY = normY * (info.height - 1);

        // Super-Sampling: Average color in a 3x3 grid
        let rSum = 0, gSum = 0, bSum = 0, count = 0;
        for (let dy = -1; dy <= 1; dy++) {
            for (let dx = -1; dx <= 1; dx++) {
                const px = Math.min(Math.max(Math.round(centerX + dx), 0), info.width - 1);
                const py = Math.min(Math.max(Math.round(centerY + dy), 0), info.height - 1);
                const idx = (py * info.width + px) * info.channels;
                rSum += data[idx];
                gSum += data[idx + 1];
                bSum += data[idx + 2];
                count++;
            }
        }

        const r = Math.round(rSum / count);
        const g = Math.round(gSum / count);
        const b = Math.round(bSum / count);
        const avgColor = (r << 16) + (g << 8) + b;

        // Map to nearest palette color
        let closestColor = palette[0];
        let minDist = Infinity;
        for (const pColor of palette) {
            const pr = (pColor >> 16) & 0xFF;
            const pg = (pColor >> 8) & 0xFF;
            const pb = pColor & 0xFF;
            const dist = Math.pow(r - pr, 2) + Math.pow(g - pg, 2) + Math.pow(b - pb, 2);
            if (dist < minDist) {
                minDist = dist;
                closestColor = pColor;
            }
        }

        const entry = { i: allCells[i], c: closestColor };
        const isLast = i === allCells.length - 1;
        writeStream.write('  ' + JSON.stringify(entry) + (isLast ? '' : ',') + '\n');

        if ((i + 1) % 50000 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            console.log(`  Progress: ${(i + 1).toLocaleString()} cells... (${Math.round((i + 1) / elapsed)} cells/sec)`);
        }
    }

    writeStream.write(']');
    return new Promise((resolve, reject) => {
        writeStream.on('finish', () => {
            console.log(`--- H3 Digitalizer Complete ---`);
            console.log(`Output: ${outputPath} | Palette: palette.json`);
            resolve();
        });
        writeStream.on('error', reject);
        writeStream.end();
    });
}

/**
 * Extracts the top N dominant colors using a simple K-Means implementation.
 */
function extractPalette(data, info, count) {
    const samples = [];
    const sampleCount = 2000;
    
    // 1. Sample pixels randomly for clustering
    for (let i = 0; i < sampleCount; i++) {
        const idx = Math.floor(Math.random() * (data.length / info.channels)) * info.channels;
        samples.push([data[idx], data[idx + 1], data[idx + 2]]);
    }

    // 2. Initialize Centroids (randomly from samples)
    let centroids = samples.slice(0, count).map(s => [...s]);
    
    // 3. K-Means Iterations
    for (let iter = 0; iter < 10; iter++) {
        const clusters = Array.from({ length: count }, () => []);
        
        // Assignment Step
        for (const s of samples) {
            let minDist = Infinity;
            let closest = 0;
            for (let c = 0; c < count; c++) {
                const d = Math.pow(s[0] - centroids[c][0], 2) + 
                          Math.pow(s[1] - centroids[c][1], 2) + 
                          Math.pow(s[2] - centroids[c][2], 2);
                if (d < minDist) {
                    minDist = d;
                    closest = c;
                }
            }
            clusters[closest].push(s);
        }

        // Update Step
        for (let c = 0; c < count; c++) {
            if (clusters[c].length > 0) {
                const avg = clusters[c].reduce((acc, val) => [acc[0] + val[0], acc[1] + val[1], acc[2] + val[2]], [0, 0, 0]);
                centroids[c] = [
                    Math.round(avg[0] / clusters[c].length),
                    Math.round(avg[1] / clusters[c].length),
                    Math.round(avg[2] / clusters[c].length)
                ];
            }
        }
    }

    // Convert centroids to numeric format
    return centroids.map(c => (c[0] << 16) + (c[1] << 8) + c[2]);
}

// Example Usage:
const startHex = '8b8af68ee9aefff'; // Brazil Jungle
digitalizeToH3Streaming('the-main-cast-of-the-flintstones.avif', startHex, 'flintstones_res11_optimized.json', 400)
    .catch(err => console.error('Fatal Error:', err));