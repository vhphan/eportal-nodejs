const arrayToCsv = (results, parseDate = true) => {
    const headers = results.columns.map(col => col.name);
    if (results.length === 0) {
        return {headers, values: []};
    }
    if (parseDate) {
        results.forEach(d => {
            d['Date'] = d['Date'].toISOString().split('T')[0]
        })
    }
    const values = results.map(d => Object.values(d).join('\t'));
    return {headers, values};
};

module.exports = {
    arrayToCsv
}