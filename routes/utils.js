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
        const callback = req.query.callback || 'console.log';
        try {
            const params = new URLSearchParams({
                key: process.env.GOOGLE_KEY,
                callback,
                libraries: 'places,visualization',
                v: 'quarterly'
            });

            const apiRes = await needle('get', `${gMapUrl}?${params}`);
            const data = apiRes.body;

            // Log the request to the public API
            if (process.env.NODE_ENV !== 'production') {
                console.log(`REQUEST: ${gMapUrl}?${params}`);
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
            logger.error(err);
            throw err;
        } else {
            logger.info(`File ${fileName} downloaded successfully`);
        }
    })
}

const jsonFileToResponse = (filePath) => (req, res, next) => {
    fs.readFile(filePath, 'utf8', (err, data) => {
        if (err) {
            logger.error(err);
            next(err);
        } else {
            return res.status(200).send(JSON.parse(data));
        }
    });
}

const testRunPython = (req, res) => {
    const {arg1, arg2} = req.params;
    const spawn = require("child_process").spawn;
    const process = spawn(`cd /home2/eproject/dnb && /home/eproject/anaconda3/envs/dnb/bin/python -m scripts.misc.hello_node.py ${arg1} ${arg2}`,
        {
            shell: true
        });
    process.stdout.on('data', function (data) {
        res.send({
            success: true,
            data: data.toString()
        });
    })
};

module.exports = {

    arrayToCsv,
    gMap,
    getFolderContents,
    downloadZipFile,
    jsonFileToResponse,
    testRunPython
}