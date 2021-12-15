const dataToCSV = (dataList, headers) => {

    let allObjects = [];
    allObjects.push(headers);

    dataList.forEach(function (object) {
        let arr = [];
        arr.push(object.id);
        arr.push(object.term);
        arr.push(object.Date);
        allObjects.push(arr)
    });

    let csvContent = "";
    allObjects.forEach(function (infoArray, index) {
        let dataString = infoArray.join(",");
        csvContent += index < allObjects.length ? dataString + "\n" : dataString;
    });

    return csvContent;
};

const getCookies = request => {
    let cookies = {};
    request.headers && request.headers.cookie && request.headers.cookie.split(';').forEach(function (cookie) {
        let parts = cookie.match(/(.*?)=(.*)$/)
        cookies[parts[1].trim()] = (parts[2] || '').trim();
    });
    return cookies;
};

const createListener = function (pgClient, eventName, callBack = null) {
    pgClient.connect();
    pgClient.query(`LISTEN "${eventName}"`);
    pgClient.on('notification', function (data) {
        if (callBack) callBack(data);
    });
};


const isObject = (v) => typeof v === 'object' && v !== null;

module.exports = {
    dataToCSV,
    getCookies,
    createListener,
    isObject
}