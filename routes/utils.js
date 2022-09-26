const needle = require("needle");
const fs = require('fs');
const path = require('path');
const {logger} = require("../middleware/logger");
const gMapUrl = `https://maps.googleapis.com/maps/api/js`

const arrayToCsv = (results, parseDate = true, dateColumns = 'Date', replaceNull = null) => {
    if (results.length === 0) {
        return {headers: [], values: []};
    }
    // const headers = results.columns.map(col => col.name);
    const headers = Object.keys(results[0]);

    if (parseDate && typeof dateColumns === 'string') {
        results.forEach(d => {
            d[dateColumns] = d[dateColumns].toISOString().split('T')[0]
        })
    }

    if (parseDate && Array.isArray(dateColumns)) {
        dateColumns.forEach(dateColumn => {
            results.forEach(d => {
                if (d[dateColumn] === null) {
                    return null;
                }
                d[dateColumn] = d[dateColumn].toISOString().split('T')[0]
            })
        });
    }
    if (replaceNull) {
        results.forEach(d => {
            Object.keys(d).forEach(key => {
                if (d[key] === null) {
                    d[key] = replaceNull
                }
            })
        })
    }
    const values = results.map(d => Object.values(d).join('\t'));
    return {headers, values};
};

function gMap() {
    return async (req, res, next) => {
        try {
            const params = new URLSearchParams({
                key: process.env.GOOGLE_KEY,
                libraries: 'places,visualization,geometries',
                v: 'quarterly'
            })

            const apiRes = await needle('get', `${gMapUrl}?${params}`)
            const data = apiRes.body

            // Log the request to the public API
            if (process.env.NODE_ENV !== 'production') {
                console.log(`REQUEST: ${gMapUrl}?${params}`)
            }

            res.status(200).send(data)
        } catch (error) {
            next(error)
        }
    };
}


const getFolderContents = (pathToFolder) => {
    // Get folder contents and return filename and size in Megabytes and date modified in json format
    const files = fs.readdirSync(pathToFolder);
    return files.map(file => {
        const stats = fs.statSync(path.join(pathToFolder, file));
        return {
            name: file,
            size: stats.size / 1000000.0,
            date: stats.mtime.toLocaleString(),
            parentFolder: pathToFolder.split(path.sep).pop(),
        }
    });
}

const downloadZipFile = (operator, folderName) => (req, res) => {
    // Download zip file from folder
    const {fileName} = req.params;
    const zipPath = `/home/eproject/${operator}/${folderName}/${fileName}`;
    res.download(zipPath, fileName, (err) => {
        if (err) {
            console.log(err);
            logger.error(err);
        } else {
            console.log('File downloaded successfully');
        }
    })
}

module.exports = {
    arrayToCsv,
    gMap,
    getFolderContents,
    downloadZipFile
}