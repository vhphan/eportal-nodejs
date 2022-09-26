async function getSqlResults(db, sqlQuery, sqlParams=[]) {
    const [rows, fields] = await db.query(
        sqlQuery, sqlParams
    );
    return ({
        success: true,
        data: rows,
        fields: fields.map(field => field.name),
    });
}

module.exports = {
    getSqlResults,
}