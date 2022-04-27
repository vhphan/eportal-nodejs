const sql = require('./PgJsBackend');

const testQuery = async (request, response) => {
    const results = await sql`SELECT *
                   FROM dnb.public.sites_on_air
                   ORDER BY random()
                   LIMIT 10`;
    response.status(200).json(results);
}

const getNbrRelation = async (request, response) => {
    const {cell, relationType} = request.query;
    let {tech} = request.query;
    if (!tech) {
        if (['_N7_', '_N3_'].some(v => cell.includes(v))) {
            tech = 'NR';
        } else if (['_L7_'].some(v => cell.includes(v))) {
            tech = 'LTE';
        }
    }
    if (!tech || !cell) {
        response.status(400).json({
            error: 'Missing parameters'
        });
        return;
    }
    if (tech === 'NR' && relationType === 'intraTech') {
        const results = await sql`SELECT *, NRL."NBRCELL"
                                    FROM dnb.nbr."NRCellRelation"
                                    INNER JOIN dnb.nbr."NRLookup" NRL on "NRCellRelation"."NRCellRelation" = NRL."NRCellRelation"
                                    WHERE "NRCellRelation"."NRCellCU" = ${cell}`;
        response.status(200).json(results);
    }
    if (tech === 'NR' && relationType === 'interTech') {
        response.status(200).json([]);
    }
    if (tech === 'LTE' && relationType === 'intraTech') {
        const results = await sql`SELECT *, EUL."NBRCELL"
                                    FROM dnb.nbr."EUtranCellRelation"
                                    INNER JOIN dnb.nbr."EUtranLookup" EUL on "EUtranCellRelation"."EUtranCellRelation" = EUL."EUtranCellRelation"
                                    WHERE "EUtranCellRelation"."EUtranCellFDD" = ${cell}`;
        response.status(200).json(results);
    }
    if (tech === 'LTE' && relationType === 'interTech') {
        const results = await sql`SELECT *, GUL."NBRCELL"
                                    FROM dnb.nbr."GUtranCellRelation"
                                    INNER JOIN dnb.nbr."GUtranLookup" GUL on "GUtranCellRelation"."GUtranCellRelation" = GUL."GUtranCellRelation"
                                    WHERE "GUtranCellRelation"."EUtranCellFDD" = ${cell}`;
        response.status(200).json(results);
    }

    response.status(400).json({
        error: 'Invalid parameters'
    });


}

module.exports = {
    testQuery,
    getNbrRelation
};