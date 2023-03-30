const fs = require("fs");
const express = require('express');
const router = express.Router();
const {cache12h, cache15m, cache6h} = require("#src/middleware/redisCache");
const asyncHandler = require("#src/middleware/async");
const {logRequest} = require("#src/middleware/logger");
const {auth} = require("#src/auth");
const path = require("path");
// const bodyParser = require("body-parser");
//
// const urlencodedParser = bodyParser.urlencoded({limit: "50mb", extended: true, parameterLimit:50000});

const operator = 'dnb';
router.use(auth(operator));
router.use(logRequest);


function getPolygonFolder() {
    if (process.platform === 'win32') {
        return 'C:\\Users\\eveepha\\PycharmProjects\\q-project-progress-map\\temp\\';
    }
    return '/home2/eproject/dnb/dnb_file_repo/polygons/project-progress/';
}

function getProjectBaseFolder() {

    if (process.platform === 'win32') {
        return 'C:\\Users\\eveepha\\PycharmProjects\\q-project-progress-map\\temp\\';
    }
    return '/home2/eproject/dnb/dnb_file_repo/project-progress/';

}

const getGeoJsonFile = async (request, response) => {
    const {file} = request.query;
    const baseFolder = getPolygonFolder();
    const filePaths = {
        'clusters': baseFolder + 'cluster.geojson',
        'districts': baseFolder + 'mcmc_district.geojson',
        'local_councils': baseFolder + 'local_council.geojson',
    };
    const filePath = filePaths[file];
    fs.readFile(filePath, 'utf8', (err, data) => {
        if (err) {
            throw err;
        }
        response.send(JSON.parse(data));
    });
};

router.get('/polygons', cache12h, asyncHandler(getGeoJsonFile));

router.post('/uploadExcelReport', asyncHandler(async (req, res) => {
    if (!req.files) {
        return res.status(501).json({
            success: false,
            message: 'No file uploaded'
        });
    }
    //Use the name of the input field (i.e. "testFile") to retrieve the uploaded file
    let {excelReport} = req.files;

    //Use the mv() method to place the file in upload directory (i.e. "uploads")
    excelReport.mv(getProjectBaseFolder() + excelReport.name);

    //send response
    return res.json({
        success: true,
        message: 'File is uploaded',
        data: {
            name: excelReport.name,
            mimetype: excelReport.mimetype,
            size: excelReport.size
        }
    });
}));

router.post('/uploadExcelData', asyncHandler(async (req, res) => {
    const data = req.body;
    console.log(data);
    // create a folder to store the data as json file
    const baseFolder = getProjectBaseFolder();
    if (!fs.existsSync(baseFolder)) {
        fs.mkdirSync(baseFolder);
    }
    const filePath = baseFolder + '/' + data.metaData.fileName.split('.')[0].toLowerCase().replace(' ', '_') + '.json';

    // write the data to json file
    fs.writeFile(filePath, JSON.stringify(data), (err) => {
            if (err) {
                throw err;
            }
            console.log(`JSON data is saved to ${filePath}`);
        }
    );

    return res.json({
            success: true,
            message: 'Data is uploaded',
        }
    );

}));


router.get('/getAvailableData', asyncHandler(async (req, res) => {
    const baseFolder = getProjectBaseFolder();

    const getSortedFiles = async (dir) => {
        const files = fs.readdirSync(dir, {withFileTypes: true}).filter(f => f.isFile());
        return files
            .map(file => ({
                name: file.name,
                time: fs.statSync(`${dir}/${file.name}`).mtime.getTime(),
            }))
            .sort((a, b) => a.time - b.time)
            .map(file => file.name.split('.')[0].toLowerCase().replace(' ', '_'));
    };

    const fileNames = await getSortedFiles(baseFolder);

    return res.json({
        success: true,
        data: fileNames
    });
}));

router.get('/getProgressData', asyncHandler(async (req, res) => {
    const {selectedDataFile} = req.query;
    const baseFolder = getProjectBaseFolder();
    // read json file and send as response
    // const data = require(baseFolder + selectedDataFile + '.json');
    const rawData = fs.readFileSync(baseFolder + selectedDataFile + '.json');
    const data = JSON.parse(rawData);
    res.header("Content-Type", 'application/json');
    return res.json({
        success: true,
        data: data
    });
}));


module.exports = router;