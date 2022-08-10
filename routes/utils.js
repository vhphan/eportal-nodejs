const needle = require("needle");

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

module.exports = {
    arrayToCsv,
    gMap
}