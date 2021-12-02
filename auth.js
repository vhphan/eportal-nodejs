const MySQLBackend = require("./db/MySQLBackend");

const get_cookies = request => {
    let cookies = {};
    request.headers && request.headers.cookie && request.headers.cookie.split(';').forEach(function (cookie) {
        let parts = cookie.match(/(.*?)=(.*)$/)
        cookies[parts[1].trim()] = (parts[2] || '').trim();
    });
    return cookies;
};

const checkApi = async (apiKey, operator) => {
    let mySQLBackend;
    let sqlQuery;
    mySQLBackend = new MySQLBackend(operator);

    switch (operator) {
        case 'dnb':
            sqlQuery = "SELECT * FROM eproject_dnb.tbluser WHERE API_token = ?";
            break;
        case 'celcom':
            sqlQuery =  "SELECT * FROM eproject_cm.tbluser WHERE API_token = ?";
            break;
    }
    let numRows = await mySQLBackend.numRows(sqlQuery, [apiKey]);
    return !!numRows;
};

const auth = (operator) => {
    return async (req, res, next) => {
        const apiKey = req.headers['API'] || req.headers['api'] || get_cookies(req)['API'];
        console.log(req.headers)
        console.log(apiKey);
        console.log(get_cookies(req));
        if (!apiKey) {
            const err = new Error('No api key! You are not authenticated! Please login!')
            return res.status(401).json({
                error: err.message
            });
        }
        if (await checkApi(apiKey, operator)) {
            return next();
        } else {
            const err = new Error('You are not authenticated! Please login!')
            // err.status = 401
            // return next(err)
            return res.status(401).json({
                error: err.message
            });
        }
    };
}

module.exports = auth;