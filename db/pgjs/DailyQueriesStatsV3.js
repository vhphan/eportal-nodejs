const sql = require('./PgJsBackend');
const {arrayToCsv} = require("../../routes/utils");
const {logger} = require("../../middleware/logger");
const {networkKpiList, plmnKpiList} = require("../constants");
const {response} = require("express");
const {getClusterId, getCellId} = require("./utils");


const resultsAsExcelFormat = (results, {parseDate = false, dateColumns = []}) => async (request, response) => {

    const {headers, values} = arrayToCsv(results, parseDate, dateColumns, '#N/A');

    response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: values.join('\n'),
        page: 1,
        size: results.length,
        total_pages: 1,
    });
};

function sendResults(request, response, results, opts = {}) {
    if (request.query.format === 'csv') {
        return resultsAsExcelFormat(results, opts)(request, response);
    }
    response.status(200).json(results);
}

const networkDailyStatsLTE = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time,
                            'Network' as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
        WHERE t1."Region" = 'All'
        ORDER BY time
    `;
    return sendResults(request, response, results);
};

const regionDailyStatsLTE = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time,
                            t1."Region" as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
        WHERE t1."Region" <> 'All'
          and t1."Cluster_ID" = 'All'
        ORDER BY t1."Region", t1.date_id
    `;
    return sendResults(request, response, results);
};

const clusterDailyStatsLTE = async (request, response) => {
    logger.info('clusterDailyStatsLTE');
    const clusterId = request.query.clusterId || request.params.clusterId || '%';
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time, 
                            t1."Cluster_ID" as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
        WHERE t1."Cluster_ID" <> 'All'
          AND t1."Cluster_ID" LIKE ${clusterId}
        ORDER BY t1."Cluster_ID", t1.date_id;`;

    return sendResults(request, response, results);
};

const cellDailyStatsLTE = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId || request.query.object || request.params.object;
    if (!cellId) {
        response.status(400).json({
            success: false, message: "cellId is required"
        });
        return;
    }
    const siteId = cellId.split('_')[0];
    const startTime = new Date();
    const results = await sql`
        with t1 AS (SELECT * from dnb.stats_v3.tbl_cell_eutrancellfdd_std_kpi where eutrancellfdd = ${cellId}),
             t2 AS (SELECT * from dnb.stats_v3.tbl_cell_eutrancellfdd_v_std_kpi where eutrancellfdd = ${cellId}),
             t3 AS (SELECT * from dnb.stats_v3.tbl_cell_eutrancellfddflex_std_kpi where eutrancellfdd = ${cellId}),
             t4 AS (SELECT * from dnb.stats_v3.tbl_cell_eutrancellrelation_std_kpi where eutrancellfdd = ${cellId}),
             d6 AS (SELECT * FROM dnb.rfdb.df_dpm WHERE site_id = ${siteId} LIMIT 1)
        
        SELECT t1.date_id::varchar(10) as time,
               t1.eutrancellfdd        as object,
               on_board_date,
               ${sql(networkKpiList.LTE)}
        FROM t1
                 LEFT JOIN t2
                           on t1.date_id=t2.date_id
                 LEFT JOIN t3
                           on t1.date_id=t3.date_id
                 LEFT JOIN t4
                           on t1.date_id=t4.date_id
                 CROSS JOIN d6
        ORDER BY t1.date_id
        
        `;
        const endTime = new Date();
        logger.info(`cellDailyStatsLTE query took: ${endTime - startTime}ms`);
    return sendResults(request, response, results, {parseDate: true, dateColumns: ['on_board_date']});
};

const networkDailyStatsNR = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time,
                            'Network' as object, ${sql(networkKpiList.NR)}
        FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "Cluster_ID")

        WHERE t1."Region" = 'All'
        ORDER BY time

    `;
    return sendResults(request, response, results);
};

const regionDailyStatsNR = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time,
                            t1."Region" as object, ${sql(networkKpiList.NR)}
        FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "Cluster_ID")
        WHERE t1."Region" <> 'All'
          and t1."Cluster_ID" = 'All'
        ORDER BY t1."Region", t1.date_id
    `;
    return sendResults(request, response, results);
};

const clusterDailyStatsNR = async (request, response) => {
    const clusterId = request.query.clusterId || request.params.clusterId || '%';
    const results = await sql`
        SELECT t1.date_id::varchar(10) as time, 
                            t1."Cluster_ID" as object, ${sql(networkKpiList.NR)}
        FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "Cluster_ID")
            LEFT JOIN dnb.stats_v3.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "Cluster_ID")
        WHERE t1."Cluster_ID" <> 'All'
          AND t1."Cluster_ID" LIKE ${clusterId}
        ORDER BY t1."Cluster_ID", t1.date_id;`;
    return sendResults(request, response, results);
};

const cellDailyStatsNR = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId || request.query.object || request.params.object;
    if (!cellId) {
        response.status(400).json({
            success: false, message: "cellId is required"
        });
        return;
    }
    const siteId = cellId.split('_')[0];
    const siteIdLike = siteId + '%';

    const results = await sql`
                    with t1 AS (SELECT * from dnb.stats_v3.tbl_cell_nrcelldu_std_kpi where nrcelldu = ${cellId}),
                     t2 AS (SELECT * from dnb.stats_v3.tbl_cell_nrcellcu_std_kpi where nrcellcu = ${cellId}),
                     t3 AS (SELECT * from dnb.stats_v3.tbl_cell_nrcelldu_v_std_kpi where nrcelldu = ${cellId}),
                     t4 AS (SELECT * from dnb.stats_v3.tbl_cell_rpuserplanelink_v_std_kpi where ne_name like ${siteIdLike}),
                     t5 AS (SELECT * from dnb.stats_v3.tbl_cell_mpprocessingresource_v_std_kpi where erbs like ${siteIdLike}),
                     d6 AS (SELECT * FROM dnb.rfdb.df_dpm WHERE site_id = ${siteId} LIMIT 1)
                
                SELECT t1.date_id::varchar(10) as time,
                       t1.nrcelldu             as object,
                       d6.on_board_date
                        ,
                       ${sql(networkKpiList.NR)}
                FROM t1
                         LEFT JOIN t2
                                   on t1.date_id = t2.date_id
                         LEFT JOIN t3
                                   on t1.date_id = t3.date_id
                         LEFT JOIN t4
                                   on t1.date_id = t4.date_id
                         LEFT JOIN t5
                                   on t1.date_id = t5.date_id
                         CROSS JOIN d6
                ORDER BY t1.date_id;
`;

    return sendResults(request, response, results, {parseDate: true, dateColumns: ['on_board_date']});
};

const cellsList = async (request, response) => {
    const rand = request.query.rand;
    if (rand) {
        const results = await sql`
        SELECT "Cellname",
               "Region",
               "Cluster_ID",
               "SITEID",
               "SystemID",
               "Sitename",
               t2.on_board_date::varchar(10) as on_board_date
        FROM dnb.rfdb.cell_mapping as t1
                 LEFT JOIN dnb.rfdb.df_dpm as t2 on t1."SITEID" = t2.site_id
                 WHERE on_board_date is not  null;
    `;
        return sendResults(request, response, results);
    }
    const results = await sql`
        SELECT "Cellname",
               "Region",
               "Cluster_ID",
               "SITEID",
               "SystemID",
               "Sitename",
               t2.on_board_date::varchar(10) as on_board_date
        FROM dnb.rfdb.cell_mapping as t1
                 LEFT JOIN dnb.rfdb.df_dpm as t2 on t1."SITEID" = t2.site_id;
    `;
    return sendResults(request, response, results);
};

const cellsListNR = async (request, response) => {
    const results = await sql`
        SELECT nrcellcu as object, on_board_date:: varchar(10)
        FROM stats_v3."NRCELLCU" t1
                 LEFT JOIN rfdb.df_dpm t2 on left (t1.nrcellcu, 9) = t2.site_id
        where t1.date_id in (select max (date_id) from stats_v3."NRCELLCU")
        GROUP BY nrcellcu, on_board_date
        order by nrcellcu;
    `;
    return response.status(200).json(results);
};

const cellsListLTE = async (request, response) => {
    const results = await sql`
        SELECT eutrancellfdd as object, on_board_date:: varchar(10)
        FROM stats_v3."EUTRANCELLFDD" t1
                 LEFT JOIN rfdb.df_dpm t2 on left (t1.eutrancellfdd, 9) = t2.site_id
        where t1.date_id in (select max (date_id) from stats_v3."EUTRANCELLFDD")
        GROUP BY eutrancellfdd, on_board_date
        order by eutrancellfdd;
    `;
    return response.status(200).json(results);
};

const customCellListStatsNR = async (request, response) => {

    const cells = request.query.cells;
    const results = await sql`
        with counters as (SELECT dt1.date_id,
        --<editor-fold desc="nrcellcu">
            sum(gnbcucpfunction) AS gnbcucpfunction,
            sum(pmendcpscellchangeattintersgnb) AS pmendcpscellchangeattintersgnb,
            sum(pmendcpscellchangeattintrasgnb) AS pmendcpscellchangeattintrasgnb,
            sum(pmendcpscellchangesuccintersgnb) AS pmendcpscellchangesuccintersgnb,
            sum(pmendcpscellchangesuccintrasgnb) AS pmendcpscellchangesuccintrasgnb,
            sum(pmendcrelueabnormalmenb) AS pmendcrelueabnormalmenb,
            sum(pmendcrelueabnormalsgnb) AS pmendcrelueabnormalsgnb,
            sum(pmendcrelueabnormalsgnbact) AS pmendcrelueabnormalsgnbact,
            sum(pmendcreluenormal) AS pmendcreluenormal,
            sum(pmendcsetupueatt) AS pmendcsetupueatt,
            sum(pmendcsetupuesucc) AS pmendcsetupuesucc,
            sum(pmrrcconnestabattmos) AS pmrrcconnestabattmos,
            sum(pmrrcconnestabattreattmos) AS pmrrcconnestabattreattmos,
            sum(pmrrcconnestabsuccmos) AS pmrrcconnestabsuccmos,
            sum(pmrrcconnlevelmaxendc) AS pmrrcconnlevelmaxendc,
            --</editor-fold>

        --<editor-fold desc="nrcelldu">
            sum(dt2.period_duration) AS period_duration,
            sum(pmactiveuedlmax) AS pmactiveuedlmax,
            sum(pmmacharqdlack16qam) AS pmmacharqdlack16qam,
            sum(pmmacharqdlack256qam) AS pmmacharqdlack256qam,
            sum(pmmacharqdlack64qam) AS pmmacharqdlack64qam,
            sum(pmmacharqdlackqpsk) AS pmmacharqdlackqpsk,
            sum(pmmacharqdldtx16qam) AS pmmacharqdldtx16qam,
            sum(pmmacharqdldtx256qam) AS pmmacharqdldtx256qam,
            sum(pmmacharqdldtx64qam) AS pmmacharqdldtx64qam,
            sum(pmmacharqdldtxqpsk) AS pmmacharqdldtxqpsk,
            sum(pmmacharqdlnack16qam) AS pmmacharqdlnack16qam,
            sum(pmmacharqdlnack256qam) AS pmmacharqdlnack256qam,
            sum(pmmacharqdlnack64qam) AS pmmacharqdlnack64qam,
            sum(pmmacharqdlnackqpsk) AS pmmacharqdlnackqpsk,
            sum(pmmacharqulack16qam) AS pmmacharqulack16qam,
            sum(pmmacharqulack256qam) AS pmmacharqulack256qam,
            sum(pmmacharqulack64qam) AS pmmacharqulack64qam,
            sum(pmmacharqulackqpsk) AS pmmacharqulackqpsk,
            sum(pmmacharquldtx16qam) AS pmmacharquldtx16qam,
            sum(pmmacharquldtx256qam) AS pmmacharquldtx256qam,
            sum(pmmacharquldtx64qam) AS pmmacharquldtx64qam,
            sum(pmmacharquldtxqpsk) AS pmmacharquldtxqpsk,
            sum(pmmacharqulnack16qam) AS pmmacharqulnack16qam,
            sum(pmmacharqulnack256qam) AS pmmacharqulnack256qam,
            sum(pmmacharqulnack64qam) AS pmmacharqulnack64qam,
            sum(pmmacharqulnackqpsk) AS pmmacharqulnackqpsk,
            sum(pmmacpdcchblockingpdschoccasions) AS pmmacpdcchblockingpdschoccasions,
            sum(pmmacpdcchblockingpuschoccasions) AS pmmacpdcchblockingpuschoccasions,
            sum(pmmacrbsymavaildl) AS pmmacrbsymavaildl,
            sum(pmmacrbsymavailul) AS pmmacrbsymavailul,
            sum(pmmacrbsymcsirs) AS pmmacrbsymcsirs,
            sum(pmmacrbsymusedpdcchtypea) AS pmmacrbsymusedpdcchtypea,
            sum(pmmacrbsymusedpdcchtypeb) AS pmmacrbsymusedpdcchtypeb,
            sum(pmmacrbsymusedpdschtypea) AS pmmacrbsymusedpdschtypea,
            sum(pmmacrbsymusedpdschtypeabroadcasting) AS pmmacrbsymusedpdschtypeabroadcasting,
            sum(pmmacrbsymusedpuschtypea) AS pmmacrbsymusedpuschtypea,
            sum(pmmacrbsymusedpuschtypeb) AS pmmacrbsymusedpuschtypeb,
            sum(pmmactimedldrb) AS pmmactimedldrb,
            sum(pmmactimeulresue) AS pmmactimeulresue,
            sum(pmmacvoldl) AS pmmacvoldl,
            sum(pmmacvoldldrb) AS pmmacvoldldrb,
            sum(pmmacvolul) AS pmmacvolul,
            sum(pmmacvolulresue) AS pmmacvolulresue,
            sum(pmpdschschedactivity) AS pmpdschschedactivity,
            sum(pmpuschschedactivity) AS pmpuschschedactivity,
            sum(dt2.pmcelldowntimeauto) AS pmcelldowntimeauto,
            sum(dt2.pmcelldowntimeman) AS pmcelldowntimeman,
            --</editor-fold>

        --<editor-fold desc="nrcelldu-v">
            sum(pmradiouerepcqi256qamrank1distr_0) AS pmradiouerepcqi256qamrank1distr_0,
            sum(pmradiouerepcqi256qamrank1distr_1) AS pmradiouerepcqi256qamrank1distr_1,
            sum(pmradiouerepcqi256qamrank1distr_2) AS pmradiouerepcqi256qamrank1distr_2,
            sum(pmradiouerepcqi256qamrank1distr_3) AS pmradiouerepcqi256qamrank1distr_3,
            sum(pmradiouerepcqi256qamrank1distr_4) AS pmradiouerepcqi256qamrank1distr_4,
            sum(pmradiouerepcqi256qamrank1distr_5) AS pmradiouerepcqi256qamrank1distr_5,
            sum(pmradiouerepcqi256qamrank1distr_6) AS pmradiouerepcqi256qamrank1distr_6,
            sum(pmradiouerepcqi256qamrank1distr_7) AS pmradiouerepcqi256qamrank1distr_7,
            sum(pmradiouerepcqi256qamrank1distr_8) AS pmradiouerepcqi256qamrank1distr_8,
            sum(pmradiouerepcqi256qamrank1distr_9) AS pmradiouerepcqi256qamrank1distr_9,
            sum(pmradiouerepcqi256qamrank1distr_10) AS pmradiouerepcqi256qamrank1distr_10,
            sum(pmradiouerepcqi256qamrank1distr_11) AS pmradiouerepcqi256qamrank1distr_11,
            sum(pmradiouerepcqi256qamrank1distr_12) AS pmradiouerepcqi256qamrank1distr_12,
            sum(pmradiouerepcqi256qamrank1distr_13) AS pmradiouerepcqi256qamrank1distr_13,
            sum(pmradiouerepcqi256qamrank1distr_14) AS pmradiouerepcqi256qamrank1distr_14,
            sum(pmradiouerepcqi256qamrank1distr_15) AS pmradiouerepcqi256qamrank1distr_15,
            sum(pmradiouerepcqi256qamrank2distr_0) AS pmradiouerepcqi256qamrank2distr_0,
            sum(pmradiouerepcqi256qamrank2distr_1) AS pmradiouerepcqi256qamrank2distr_1,
            sum(pmradiouerepcqi256qamrank2distr_2) AS pmradiouerepcqi256qamrank2distr_2,
            sum(pmradiouerepcqi256qamrank2distr_3) AS pmradiouerepcqi256qamrank2distr_3,
            sum(pmradiouerepcqi256qamrank2distr_4) AS pmradiouerepcqi256qamrank2distr_4,
            sum(pmradiouerepcqi256qamrank2distr_5) AS pmradiouerepcqi256qamrank2distr_5,
            sum(pmradiouerepcqi256qamrank2distr_6) AS pmradiouerepcqi256qamrank2distr_6,
            sum(pmradiouerepcqi256qamrank2distr_7) AS pmradiouerepcqi256qamrank2distr_7,
            sum(pmradiouerepcqi256qamrank2distr_8) AS pmradiouerepcqi256qamrank2distr_8,
            sum(pmradiouerepcqi256qamrank2distr_9) AS pmradiouerepcqi256qamrank2distr_9,
            sum(pmradiouerepcqi256qamrank2distr_10) AS pmradiouerepcqi256qamrank2distr_10,
            sum(pmradiouerepcqi256qamrank2distr_11) AS pmradiouerepcqi256qamrank2distr_11,
            sum(pmradiouerepcqi256qamrank2distr_12) AS pmradiouerepcqi256qamrank2distr_12,
            sum(pmradiouerepcqi256qamrank2distr_13) AS pmradiouerepcqi256qamrank2distr_13,
            sum(pmradiouerepcqi256qamrank2distr_14) AS pmradiouerepcqi256qamrank2distr_14,
            sum(pmradiouerepcqi256qamrank2distr_15) AS pmradiouerepcqi256qamrank2distr_15,
            sum(pmradiouerepcqi256qamrank3distr_0) AS pmradiouerepcqi256qamrank3distr_0,
            sum(pmradiouerepcqi256qamrank3distr_1) AS pmradiouerepcqi256qamrank3distr_1,
            sum(pmradiouerepcqi256qamrank3distr_2) AS pmradiouerepcqi256qamrank3distr_2,
            sum(pmradiouerepcqi256qamrank3distr_3) AS pmradiouerepcqi256qamrank3distr_3,
            sum(pmradiouerepcqi256qamrank3distr_4) AS pmradiouerepcqi256qamrank3distr_4,
            sum(pmradiouerepcqi256qamrank3distr_5) AS pmradiouerepcqi256qamrank3distr_5,
            sum(pmradiouerepcqi256qamrank3distr_6) AS pmradiouerepcqi256qamrank3distr_6,
            sum(pmradiouerepcqi256qamrank3distr_7) AS pmradiouerepcqi256qamrank3distr_7,
            sum(pmradiouerepcqi256qamrank3distr_8) AS pmradiouerepcqi256qamrank3distr_8,
            sum(pmradiouerepcqi256qamrank3distr_9) AS pmradiouerepcqi256qamrank3distr_9,
            sum(pmradiouerepcqi256qamrank3distr_10) AS pmradiouerepcqi256qamrank3distr_10,
            sum(pmradiouerepcqi256qamrank3distr_11) AS pmradiouerepcqi256qamrank3distr_11,
            sum(pmradiouerepcqi256qamrank3distr_12) AS pmradiouerepcqi256qamrank3distr_12,
            sum(pmradiouerepcqi256qamrank3distr_13) AS pmradiouerepcqi256qamrank3distr_13,
            sum(pmradiouerepcqi256qamrank3distr_14) AS pmradiouerepcqi256qamrank3distr_14,
            sum(pmradiouerepcqi256qamrank3distr_15) AS pmradiouerepcqi256qamrank3distr_15,
            sum(pmradiouerepcqi256qamrank4distr_0) AS pmradiouerepcqi256qamrank4distr_0,
            sum(pmradiouerepcqi256qamrank4distr_1) AS pmradiouerepcqi256qamrank4distr_1,
            sum(pmradiouerepcqi256qamrank4distr_2) AS pmradiouerepcqi256qamrank4distr_2,
            sum(pmradiouerepcqi256qamrank4distr_3) AS pmradiouerepcqi256qamrank4distr_3,
            sum(pmradiouerepcqi256qamrank4distr_4) AS pmradiouerepcqi256qamrank4distr_4,
            sum(pmradiouerepcqi256qamrank4distr_5) AS pmradiouerepcqi256qamrank4distr_5,
            sum(pmradiouerepcqi256qamrank4distr_6) AS pmradiouerepcqi256qamrank4distr_6,
            sum(pmradiouerepcqi256qamrank4distr_7) AS pmradiouerepcqi256qamrank4distr_7,
            sum(pmradiouerepcqi256qamrank4distr_8) AS pmradiouerepcqi256qamrank4distr_8,
            sum(pmradiouerepcqi256qamrank4distr_9) AS pmradiouerepcqi256qamrank4distr_9,
            sum(pmradiouerepcqi256qamrank4distr_10) AS pmradiouerepcqi256qamrank4distr_10,
            sum(pmradiouerepcqi256qamrank4distr_11) AS pmradiouerepcqi256qamrank4distr_11,
            sum(pmradiouerepcqi256qamrank4distr_12) AS pmradiouerepcqi256qamrank4distr_12,
            sum(pmradiouerepcqi256qamrank4distr_13) AS pmradiouerepcqi256qamrank4distr_13,
            sum(pmradiouerepcqi256qamrank4distr_14) AS pmradiouerepcqi256qamrank4distr_14,
            sum(pmradiouerepcqi256qamrank4distr_15) AS pmradiouerepcqi256qamrank4distr_15,
            sum(pmradiorecinterferencepwrdistr_0) AS pmradiorecinterferencepwrdistr_0,
            sum(pmradiorecinterferencepwrdistr_1) AS pmradiorecinterferencepwrdistr_1,
            sum(pmradiorecinterferencepwrdistr_2) AS pmradiorecinterferencepwrdistr_2,
            sum(pmradiorecinterferencepwrdistr_3) AS pmradiorecinterferencepwrdistr_3,
            sum(pmradiorecinterferencepwrdistr_4) AS pmradiorecinterferencepwrdistr_4,
            sum(pmradiorecinterferencepwrdistr_5) AS pmradiorecinterferencepwrdistr_5,
            sum(pmradiorecinterferencepwrdistr_6) AS pmradiorecinterferencepwrdistr_6,
            sum(pmradiorecinterferencepwrdistr_7) AS pmradiorecinterferencepwrdistr_7,
            sum(pmradiorecinterferencepwrdistr_8) AS pmradiorecinterferencepwrdistr_8,
            sum(pmradiorecinterferencepwrdistr_9) AS pmradiorecinterferencepwrdistr_9,
            sum(pmradiorecinterferencepwrdistr_10) AS pmradiorecinterferencepwrdistr_10,
            sum(pmradiorecinterferencepwrdistr_11) AS pmradiorecinterferencepwrdistr_11,
            sum(pmradiorecinterferencepwrdistr_12) AS pmradiorecinterferencepwrdistr_12,
            sum(pmradiorecinterferencepwrdistr_13) AS pmradiorecinterferencepwrdistr_13,
            sum(pmradiorecinterferencepwrdistr_14) AS pmradiorecinterferencepwrdistr_14,
            sum(pmradiorecinterferencepwrdistr_15) AS pmradiorecinterferencepwrdistr_15,

            sum(pmradioraatttadistr_0) AS pmradioraatttadistr_0,
            sum(pmradioraatttadistr_1) AS pmradioraatttadistr_1,
            sum(pmradioraatttadistr_2) AS pmradioraatttadistr_2,
            sum(pmradioraatttadistr_3) AS pmradioraatttadistr_3,
            sum(pmradioraatttadistr_4) AS pmradioraatttadistr_4,
            sum(pmradioraatttadistr_5) AS pmradioraatttadistr_5,
            sum(pmradioraatttadistr_6) AS pmradioraatttadistr_6,
            sum(pmradioraatttadistr_7) AS pmradioraatttadistr_7,
            sum(pmradioraatttadistr_8) AS pmradioraatttadistr_8,
            sum(pmradioraatttadistr_9) AS pmradioraatttadistr_9,
            sum(pmradioraatttadistr_10) AS pmradioraatttadistr_10,
            sum(pmradioraatttadistr_11) AS pmradioraatttadistr_11,
            sum(rssi_nr_dnom) AS rssi_nr_dnom,
            sum(rssi_nr_nom) AS rssi_nr_nom,
            sum(pmmaclattimedldrxsyncqos) AS pmmaclattimedldrxsyncqos,
            sum(pmmaclattimedlnodrxsyncqos) AS pmmaclattimedlnodrxsyncqos,
            sum(pmmaclattimedldrxsyncsampqos) AS pmmaclattimedldrxsyncsampqos,
            sum(pmmaclattimedlnodrxsyncsampqos) AS pmmaclattimedlnodrxsyncsampqos
            --</editor-fold>

        FROM stats_v3."NRCELLCU" as dt1
        LEFT JOIN stats_v3."NRCELLDU" as dt2 on dt1.date_id = dt2.date_id and
        dt1.nrcellcu = dt2.nrcelldu
        LEFT JOIN stats_v3.tbl_agg_nrcelldu_v_vectors as dt3 on dt1.date_id = dt3.date_id and
        dt1.nrcellcu = dt3.nrcelldu
        LEFT JOIN stats_v3.tbl_agg_nrcelldu_v_vectors2 as dt4
        on dt1.date_id = dt4.date_id and
        dt1.nrcellcu = dt4.nrcelldu
        WHERE nrcellcu IN ${sql(cells)}
        GROUP BY dt1.date_id)
        SELECT "date_id"::varchar(10) as    time,
        --<editor-fold desc="kpi nrcellcu">
            100 * (pmendcsetupuesucc || pmendcsetupueatt) AS "ENDC SR",
            100 * pmendcrelueabnormalsgnbact ||
            (pmendcreluenormal + pmendcrelueabnormalmenb + pmendcrelueabnormalsgnb) AS "Erab Drop Call rate (sgNB)",
            100 * pmendcpscellchangesuccintrasgnb || pmendcpscellchangeattintrasgnb AS "Intra-SgNB Pscell Change Success
            Rate",
            100 * (pmendcpscellchangesuccintersgnb || pmendcpscellchangeattintersgnb) AS "Inter-SgNB PSCell Change
            Success Rate",
            pmrrcconnlevelmaxendc AS "Max of RRC Connected User (ENDC)",
            100 * pmrrcconnestabsuccmos ||
            (pmrrcconnestabattmos - pmrrcconnestabattreattmos) AS "RRC Setup Success Rate (Signaling) (%)",
            --</editor-fold>
        --<editor-fold desc="kpi nrcelldu">
            100 * ((60 * (period_duration)) - ((pmcelldowntimeauto + (pmcelldowntimeman))) ||
            (60 * (period_duration)::double precision)) AS "Cell Availability",
            100 * (pmmacpdcchblockingpdschoccasions + pmmacpdcchblockingpuschoccasions) ||
            (pmmacrbsymusedpdcchtypea + pmmacrbsymusedpdcchtypeb) AS "E-RAB Block Rate",
            100 * (pmmacrbsymusedpdschtypea + pmmacrbsymusedpdschtypeabroadcasting + pmmacrbsymcsirs) ||
            (pmmacrbsymavaildl) AS "Resource Block Utilizing Rate (DL)",
            100 * (pmmacrbsymusedpuschtypea + pmmacrbsymusedpuschtypeb) ||
            (pmmacrbsymavailul) AS "Resource Block Utilizing Rate (UL)",
            100 * (pmmacharqulnackqpsk + pmmacharqdlnack16qam + pmmacharqdlnack64qam) ||
            (pmmacharqulackqpsk + pmmacharqulack16qam + pmmacharqulack64qam + pmmacharqulnackqpsk + pmmacharqulnack16qam
            +
            pmmacharqulnack64qam) AS "UL BLER",
            64 * (pmmacvoldldrb || pmmactimedldrb) / 1000 AS "DL User Throughput",
            64 * (pmmacvolulresue || pmmactimeulresue) / 1000 AS "UL User Throughput",
            64 * (pmmacvoldl || pmpdschschedactivity) / 1000 AS "DL Cell Throughput",
            64 * pmmacvolul || pmpuschschedactivity / 1000 AS "UL Cell Throughput",
            pmmacvoldl / 1024 / 1024 / 1024 AS "DL Data Volume",
            pmmacvolul / 1024 / 1024 / 1024 AS "UL Data Volume",
            pmactiveuedlmax AS "Max of Active User",
            (pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
            100 AS "DL QPSK %",
            (pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
            100 AS "DL 16QAM%",
            (pmmacharqdlack64qam + pmmacharqdlnack64qam + pmmacharqdldtx64qam) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
            100 AS "DL 64QAM%",
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
            100 AS "DL 256QAM%",
            (pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) ||
            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam +
            pmmacharquldtx16qam +
            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
            100 AS "UL QPSK %",
            (pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam) ||
            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam +
            pmmacharquldtx16qam +
            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
            100 AS "UL 16QAM%",
            (pmmacharqulack64qam + pmmacharqulnack64qam + pmmacharquldtx64qam) ||
            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam +
            pmmacharquldtx16qam +
            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
            100 AS "UL 64QAM%",
            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam) ||
            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam +
            pmmacharquldtx16qam +
            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
            100 AS "UL 256QAM%",
            --</editor-fold>
        --<editor-fold desc="nrcelldu-v">
            (1 * pmradiouerepcqi256qamrank1distr_1 + 2 * pmradiouerepcqi256qamrank1distr_2 +
            3 * pmradiouerepcqi256qamrank1distr_3 + 4 * pmradiouerepcqi256qamrank1distr_4 +
            5 * pmradiouerepcqi256qamrank1distr_5 + 6 * pmradiouerepcqi256qamrank1distr_6 +
            7 * pmradiouerepcqi256qamrank1distr_7 + 8 * pmradiouerepcqi256qamrank1distr_8 +
            9 * pmradiouerepcqi256qamrank1distr_9 + 10 * pmradiouerepcqi256qamrank1distr_10 +
            11 * pmradiouerepcqi256qamrank1distr_11 + 12 * pmradiouerepcqi256qamrank1distr_12 +
            13 * pmradiouerepcqi256qamrank1distr_13 + 14 * pmradiouerepcqi256qamrank1distr_14 +
            15 * pmradiouerepcqi256qamrank1distr_15 + 1 * pmradiouerepcqi256qamrank2distr_1 +
            2 * pmradiouerepcqi256qamrank2distr_2 + 3 * pmradiouerepcqi256qamrank2distr_3 +
            4 * pmradiouerepcqi256qamrank2distr_4 + 5 * pmradiouerepcqi256qamrank2distr_5 +
            6 * pmradiouerepcqi256qamrank2distr_6 + 7 * pmradiouerepcqi256qamrank2distr_7 +
            8 * pmradiouerepcqi256qamrank2distr_8 + 9 * pmradiouerepcqi256qamrank2distr_9 +
            10 * pmradiouerepcqi256qamrank2distr_10 + 11 * pmradiouerepcqi256qamrank2distr_11 +
            12 * pmradiouerepcqi256qamrank2distr_12 + 13 * pmradiouerepcqi256qamrank2distr_13 +
            14 * pmradiouerepcqi256qamrank2distr_14 + 15 * pmradiouerepcqi256qamrank2distr_15 +
            1 * pmradiouerepcqi256qamrank3distr_1 + 2 * pmradiouerepcqi256qamrank3distr_2 +
            3 * pmradiouerepcqi256qamrank3distr_3 + 4 * pmradiouerepcqi256qamrank3distr_4 +
            5 * pmradiouerepcqi256qamrank3distr_5 + 6 * pmradiouerepcqi256qamrank3distr_6 +
            7 * pmradiouerepcqi256qamrank3distr_7 + 8 * pmradiouerepcqi256qamrank3distr_8 +
            9 * pmradiouerepcqi256qamrank3distr_9 + 10 * pmradiouerepcqi256qamrank3distr_10 +
            11 * pmradiouerepcqi256qamrank3distr_11 + 12 * pmradiouerepcqi256qamrank3distr_12 +
            13 * pmradiouerepcqi256qamrank3distr_13 + 14 * pmradiouerepcqi256qamrank3distr_14 +
            15 * pmradiouerepcqi256qamrank3distr_15 + 1 * pmradiouerepcqi256qamrank4distr_1 +
            2 * pmradiouerepcqi256qamrank4distr_2 + 3 * pmradiouerepcqi256qamrank4distr_3 +
            4 * pmradiouerepcqi256qamrank4distr_4 + 5 * pmradiouerepcqi256qamrank4distr_5 +
            6 * pmradiouerepcqi256qamrank4distr_6 + 7 * pmradiouerepcqi256qamrank4distr_7 +
            8 * pmradiouerepcqi256qamrank4distr_8 + 9 * pmradiouerepcqi256qamrank4distr_9 +
            10 * pmradiouerepcqi256qamrank4distr_10 + 11 * pmradiouerepcqi256qamrank4distr_11 +
            12 * pmradiouerepcqi256qamrank4distr_12 + 13 * pmradiouerepcqi256qamrank4distr_13 +
            14 * pmradiouerepcqi256qamrank4distr_14 + 15 * pmradiouerepcqi256qamrank4distr_15) ||
            (pmradiouerepcqi256qamrank1distr_1 + pmradiouerepcqi256qamrank1distr_2 + pmradiouerepcqi256qamrank1distr_3 +
            pmradiouerepcqi256qamrank1distr_4 + pmradiouerepcqi256qamrank1distr_5 + pmradiouerepcqi256qamrank1distr_6 +
            pmradiouerepcqi256qamrank1distr_7 + pmradiouerepcqi256qamrank1distr_8 + pmradiouerepcqi256qamrank1distr_9 +
            pmradiouerepcqi256qamrank1distr_10 + pmradiouerepcqi256qamrank1distr_11 + pmradiouerepcqi256qamrank1distr_12
            +
            pmradiouerepcqi256qamrank1distr_13 + pmradiouerepcqi256qamrank1distr_14 + pmradiouerepcqi256qamrank1distr_15
            +
            pmradiouerepcqi256qamrank2distr_1 + pmradiouerepcqi256qamrank2distr_2 + pmradiouerepcqi256qamrank2distr_3 +
            pmradiouerepcqi256qamrank2distr_4 + pmradiouerepcqi256qamrank2distr_5 + pmradiouerepcqi256qamrank2distr_6 +
            pmradiouerepcqi256qamrank2distr_7 + pmradiouerepcqi256qamrank2distr_8 + pmradiouerepcqi256qamrank2distr_9 +
            pmradiouerepcqi256qamrank2distr_10 + pmradiouerepcqi256qamrank2distr_11 + pmradiouerepcqi256qamrank2distr_12
            +
            pmradiouerepcqi256qamrank2distr_13 + pmradiouerepcqi256qamrank2distr_14 + pmradiouerepcqi256qamrank2distr_15
            +
            pmradiouerepcqi256qamrank3distr_1 + pmradiouerepcqi256qamrank3distr_2 + pmradiouerepcqi256qamrank3distr_3 +
            pmradiouerepcqi256qamrank3distr_4 + pmradiouerepcqi256qamrank3distr_5 + pmradiouerepcqi256qamrank3distr_6 +
            pmradiouerepcqi256qamrank3distr_7 + pmradiouerepcqi256qamrank3distr_8 + pmradiouerepcqi256qamrank3distr_9 +
            pmradiouerepcqi256qamrank3distr_10 + pmradiouerepcqi256qamrank3distr_11 + pmradiouerepcqi256qamrank3distr_12
            +
            pmradiouerepcqi256qamrank3distr_13 + pmradiouerepcqi256qamrank3distr_14 + pmradiouerepcqi256qamrank3distr_15
            +
            pmradiouerepcqi256qamrank4distr_1 + pmradiouerepcqi256qamrank4distr_2 + pmradiouerepcqi256qamrank4distr_3 +
            pmradiouerepcqi256qamrank4distr_4 + pmradiouerepcqi256qamrank4distr_5 + pmradiouerepcqi256qamrank4distr_6 +
            pmradiouerepcqi256qamrank4distr_7 + pmradiouerepcqi256qamrank4distr_8 + pmradiouerepcqi256qamrank4distr_9 +
            pmradiouerepcqi256qamrank4distr_10 + pmradiouerepcqi256qamrank4distr_11 + pmradiouerepcqi256qamrank4distr_12
            +
            pmradiouerepcqi256qamrank4distr_13 + pmradiouerepcqi256qamrank4distr_14 +
            pmradiouerepcqi256qamrank4distr_15) AS "Average CQI",
            -- NR_Avg_TA_Meters = WeightedAverage(msrbs_NRCellDU.pmRadioRaAttTaDistr, [0, 10, 20, 30, 40, 50, 60, 70,
--             80, 90, 100, 110] )*5000*0.001
            5 * (pmradioraatttadistr_0 * 0 + pmradioraatttadistr_1 * 10 + pmradioraatttadistr_2 * 20 +
            pmradioraatttadistr_3 * 30 +
            pmradioraatttadistr_4 * 40 + pmradioraatttadistr_5 * 50 + pmradioraatttadistr_6 * 60 +
            pmradioraatttadistr_7 * 70 +
            pmradioraatttadistr_8 * 80 + pmradioraatttadistr_9 * 90 + pmradioraatttadistr_10 * 100 +
            pmradioraatttadistr_11 * 110) ||
            (pmradioraatttadistr_0 + pmradioraatttadistr_1 + pmradioraatttadistr_2 +
            pmradioraatttadistr_3 +
            pmradioraatttadistr_4 + pmradioraatttadistr_5 + pmradioraatttadistr_6 +
            pmradioraatttadistr_7 +
            pmradioraatttadistr_8 + pmradioraatttadistr_9 + pmradioraatttadistr_10 +
            pmradioraatttadistr_11) as "NR_Avg_TA_Meters",
            rssi_nr_nom || rssi_nr_dnom as "Avg PUSCH UL RSSI",
            (pmmaclattimedldrxsyncqos + pmmaclattimedlnodrxsyncqos) ||
            (8 * pmmaclattimedldrxsyncsampqos + pmmaclattimedlnodrxsyncsampqos) AS "Latency (only Radio interface)"
            --</editor-fold>
        FROM counters
        ORDER BY time
        ;
    `;
    return response.status(200).json(results);
};

const customCellListStatsNR2 = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
        WITH counters1 AS (SELECT dt1.date_id,
                                  dt1.ne_name,
                                  SUM(pmpdcppkttransdldiscqos)    AS pmpdcppkttransdldiscqos,
                                  SUM(pmpdcppkttransdldiscaqmqos) AS pmpdcppkttransdldiscaqmqos,
                                  SUM(pmpdcppkttransdlqos)        AS pmpdcppkttransdlqos,
                                  SUM(pmpdcppktlossulqos)         AS pmpdcppktlossulqos,
                                  SUM(pmpdcppktreculoooqos)       AS pmpdcppktreculoooqos,
                                  SUM(pmpdcppktreculqos)          AS pmpdcppktreculqos,
                                  SUM(pmpdcppktlossultoqos)       AS pmpdcppktlossultoqos,
                                  SUM(pmpdcppktlossultodiscqos)   AS pmpdcppktlossultodiscqos
                           FROM stats_v3."RPUSERPLANELINK-V" as dt1
                           WHERE ne_name IN (SELECT DISTINCT "Sitename"
                                             FROM rfdb.cell_mapping
                                             WHERE "Cellname" IN ${sql(cells)})
                           GROUP BY dt1.date_id, dt1.ne_name),
             counters2 AS (SELECT date_id,
                                  erbs,
                                  Sum(10 * pmprocessorloadlcdistr_0 +
                                      25 * pmprocessorloadlcdistr_1 +
                                      35 * pmprocessorloadlcdistr_2 +
                                      45 * pmprocessorloadlcdistr_3 +
                                      55 * pmprocessorloadlcdistr_4 +
                                      65 * pmprocessorloadlcdistr_5 +
                                      75 * pmprocessorloadlcdistr_6 +
                                      82.5 * pmprocessorloadlcdistr_7 +
                                      87.5 * pmprocessorloadlcdistr_8 +
                                      92.5 * pmprocessorloadlcdistr_9 +
                                      97.5 * pmprocessorloadlcdistr_10) as nom_cpu,
                                  Sum(pmprocessorloadlcdistr_0 +
                                      pmprocessorloadlcdistr_1 +
                                      pmprocessorloadlcdistr_2 +
                                      pmprocessorloadlcdistr_3 +
                                      pmprocessorloadlcdistr_4 +
                                      pmprocessorloadlcdistr_5 +
                                      pmprocessorloadlcdistr_6 +
                                      pmprocessorloadlcdistr_7 +
                                      pmprocessorloadlcdistr_8 +
                                      pmprocessorloadlcdistr_9 +
                                      pmprocessorloadlcdistr_10)        as denom_cpu
                           FROM stats_v3."tbl_agg_mpprocessingresource_v_vectors" as dt
                           WHERE erbs IN (SELECT DISTINCT "Sitename"
                                          FROM rfdb.cell_mapping
                                          WHERE "Cellname" IN ${sql(cells)})
                           group by date_id, erbs)
        SELECT counters1.date_id::varchar(10) as time,
                        100 * (pmpdcppkttransdldiscqos - pmpdcppkttransdldiscaqmqos) || pmpdcppkttransdlqos AS "Packet Loss (DL)",
                        100 * (pmpdcppktlossulqos - pmpdcppktreculoooqos) ||
                        (pmpdcppktreculqos + pmpdcppktlossultoqos - pmpdcppktlossultodiscqos -
                        pmpdcppktreculoooqos)                                                             AS "Packet Loss (UL)",
                        nom_cpu || denom_cpu                                                               as "gNobeB CPU Load"
        FROM counters1
            LEFT JOIN counters2 using (date_id)
        ORDER BY time
        ;
    `;
    return response.status(200).json(results);
};

const customCellListStatsLTE = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
        WITH counters1 AS (SELECT "date_id",
        --<editor-fold desc="Counters">
            sum(period_duration) AS period_duration,
            sum(pmactiveuedlmax) AS pmactiveuedlmax,
            sum(pmcelldowntimeauto) AS pmcelldowntimeauto,
            sum(pmcelldowntimeman) AS pmcelldowntimeman,
            sum(pmerabestabattadded) AS pmerabestabattadded,
            sum(pmerabestabattaddedhoongoing) AS pmerabestabattaddedhoongoing,
            sum(pmerabestabattinit) AS pmerabestabattinit,
            sum(pmerabestabsuccadded) AS pmerabestabsuccadded,
            sum(pmerabestabsuccinit) AS pmerabestabsuccinit,
            sum(pmerabrelabnormalenb) AS pmerabrelabnormalenb,
            sum(pmerabrelabnormalenbact) AS pmerabrelabnormalenbact,
            sum(pmerabrelabnormalmmeact) AS pmerabrelabnormalmmeact,
            sum(pmerabrelmme) AS pmerabrelmme,
            sum(pmerabrelnormalenb) AS pmerabrelnormalenb,
            sum(pmmacharqdlack16qam) AS pmmacharqdlack16qam,
            sum(pmmacharqdlack256qam) AS pmmacharqdlack256qam,
            sum(pmmacharqdlack64qam) AS pmmacharqdlack64qam,
            sum(pmmacharqdlackqpsk) AS pmmacharqdlackqpsk,
            sum(pmmacharqdldtx16qam) AS pmmacharqdldtx16qam,
            sum(pmmacharqdldtx256qam) AS pmmacharqdldtx256qam,
            sum(pmmacharqdldtx64qam) AS pmmacharqdldtx64qam,
            sum(pmmacharqdldtxqpsk) AS pmmacharqdldtxqpsk,
            sum(pmmacharqdlnack16qam) AS pmmacharqdlnack16qam,
            sum(pmmacharqdlnack256qam) AS pmmacharqdlnack256qam,
            sum(pmmacharqdlnack64qam) AS pmmacharqdlnack64qam,
            sum(pmmacharqdlnackqpsk) AS pmmacharqdlnackqpsk,
            sum(pmmacharqulfail16qam) AS pmmacharqulfail16qam,
            sum(pmmacharqulfail256qam) AS pmmacharqulfail256qam,
            sum(pmmacharqulfail64qam) AS pmmacharqulfail64qam,
            sum(pmmacharqulfailqpsk) AS pmmacharqulfailqpsk,
            sum(pmmacharqulsucc16qam) AS pmmacharqulsucc16qam,
            sum(pmmacharqulsucc256qam) AS pmmacharqulsucc256qam,
            sum(pmmacharqulsucc64qam) AS pmmacharqulsucc64qam,
            sum(pmmacharqulsuccqpsk) AS pmmacharqulsuccqpsk,
            sum(pmpdcplatpkttransdl) AS pmpdcplatpkttransdl,
            sum(pmpdcplattimedl) AS pmpdcplattimedl,
            sum(pmpdcppktdiscdlho) AS pmpdcppktdiscdlho,
            sum(pmpdcppktdiscdlpelr) AS pmpdcppktdiscdlpelr,
            sum(pmpdcppktdiscdlpelruu) AS pmpdcppktdiscdlpelruu,
            sum(pmpdcppktfwddl) AS pmpdcppktfwddl,
            sum(pmpdcppktlostul) AS pmpdcppktlostul,
            sum(pmpdcppktreceiveddl) AS pmpdcppktreceiveddl,
            sum(pmpdcppktreceivedul) AS pmpdcppktreceivedul,
            sum(pmpdcpvoldldrb) AS pmpdcpvoldldrb,
            sum(pmpdcpvoldldrblasttti) AS pmpdcpvoldldrblasttti,
            sum(pmpdcpvoluldrb) AS pmpdcpvoluldrb,
            sum(pmrrcconnestabatt) AS pmrrcconnestabatt,
            sum(pmrrcconnestabattmos) AS pmrrcconnestabattmos,
            sum(pmrrcconnestabattreatt) AS pmrrcconnestabattreatt,
            sum(pmrrcconnestabattreattmos) AS pmrrcconnestabattreattmos,
            sum(pmrrcconnestabfailmmeovlmod) AS pmrrcconnestabfailmmeovlmod,
            sum(pmrrcconnestabfailmmeovlmos) AS pmrrcconnestabfailmmeovlmos,
            sum(pmrrcconnestabsucc) AS pmrrcconnestabsucc,
            sum(pmrrcconnestabsuccmos) AS pmrrcconnestabsuccmos,
            sum(pmrrcconnmax) AS pmrrcconnmax,
            sum(pmrrcconnmaxplmn0) AS pmrrcconnmaxplmn0,
            sum(pmrrcconnmaxplmn1) AS pmrrcconnmaxplmn1,
            sum(pmrrcconnmaxplmn2) AS pmrrcconnmaxplmn2,
            sum(pmrrcconnmaxplmn3) AS pmrrcconnmaxplmn3,
            sum(pmrrcconnmaxplmn4) AS pmrrcconnmaxplmn4,
            sum(pmrrcconnmaxplmn5) AS pmrrcconnmaxplmn5,
            sum(pmrrcconnmaxplmn6) AS pmrrcconnmaxplmn6,
            sum(pms1sigconnestabatt) AS pms1sigconnestabatt,
            sum(pms1sigconnestabfailmmeovlmos) AS pms1sigconnestabfailmmeovlmos,
            sum(pms1sigconnestabsucc) AS pms1sigconnestabsucc,
            sum(pmschedactivitycelldl) AS pmschedactivitycelldl,
            sum(pmschedactivitycellul) AS pmschedactivitycellul,
            sum(pmuectxtfetchattintraenbhoin) AS pmuectxtfetchattintraenbhoin,
            sum(pmuectxtfetchattx2hoin) AS pmuectxtfetchattx2hoin,
            sum(pmuectxtfetchsuccintraenbhoin) AS pmuectxtfetchsuccintraenbhoin,
            sum(pmuectxtfetchsuccx2hoin) AS pmuectxtfetchsuccx2hoin,
            sum(pmuethptimedl) AS pmuethptimedl,
            sum(pmuethptimeul) AS pmuethptimeul,
            sum(pmuethpvolul) AS pmuethpvolul,
            sum(pmflexerabestabsuccinit_endc2to99) as pmflexerabestabsuccinit_endc2to99,
            sum(pmflexerabestabsuccadded_endc2to99) as pmflexerabestabsuccadded_endc2to99,
            sum(pmflexerabestabattinit_endc2to99) as pmflexerabestabattinit_endc2to99,
            sum(pmflexerabestabattadded_endc2to99) as pmflexerabestabattadded_endc2to99
            --</editor-fold>
        FROM dnb.stats_v3."EUTRANCELLFDD" as dt1
        LEFT JOIN (SELECT date_id,
        erbs,
        eutrancellfdd,
        sum(pmflexerabestabsuccinit)  as pmflexerabestabsuccinit_endc2to99,
        sum(pmflexerabestabsuccadded) as pmflexerabestabsuccadded_endc2to99,
        sum(pmflexerabestabattinit)   as pmflexerabestabattinit_endc2to99,
        sum(pmflexerabestabattadded)  as pmflexerabestabattadded_endc2to99
        FROM dnb.stats_v3."EUTRANCELLFDD_FLEX"
        WHERE eutrancellfdd IN
        ${sql(cells)}
        AND flex_filtername ilike 'Plmn%endc2to99'
        group by date_id, erbs, eutrancellfdd) as dt2
        using (date_id, erbs, eutrancellfdd)
        WHERE dt1.eutrancellfdd IN ${sql(cells)}
        group by "date_id"),
        counters2 AS (SELECT "date_id",
        --<editor-fold desc="Counters">
            sum(pmflexcellhoexeattlteintraf) AS pmflexcellhoexeattlteintraf,
            sum(pmflexcellhoexesucclteintraf) AS pmflexcellhoexesucclteintraf,
            sum(pmflexerabestabattadded) AS pmflexerabestabattadded,
            sum(pmflexerabestabattaddedhoongoing) AS pmflexerabestabattaddedhoongoing,
            sum(pmflexerabestabattinit) AS pmflexerabestabattinit,
            sum(pmflexerabestabsuccadded) AS pmflexerabestabsuccadded,
            sum(pmflexerabestabsuccinit) AS pmflexerabestabsuccinit,
            sum(pmflexerabrelabnormalenb) AS pmflexerabrelabnormalenb,
            sum(pmflexerabrelabnormalenbact) AS pmflexerabrelabnormalenbact,
            sum(pmflexerabrelabnormalmmeact) AS pmflexerabrelabnormalmmeact,
            sum(pmflexerabrelmme) AS pmflexerabrelmme,
            sum(pmflexerabrelnormalenb) AS pmflexerabrelnormalenb,
            sum(pmflexmacharqdlack16qam) AS pmflexmacharqdlack16qam,
            sum(pmflexmacharqdlack256qam) AS pmflexmacharqdlack256qam,
            sum(pmflexmacharqdlack64qam) AS pmflexmacharqdlack64qam,
            sum(pmflexmacharqdlackqpsk) AS pmflexmacharqdlackqpsk,
            sum(pmflexmacharqdldtx16qam) AS pmflexmacharqdldtx16qam,
            sum(pmflexmacharqdldtx256qam) AS pmflexmacharqdldtx256qam,
            sum(pmflexmacharqdldtx64qam) AS pmflexmacharqdldtx64qam,
            sum(pmflexmacharqdldtxqpsk) AS pmflexmacharqdldtxqpsk,
            sum(pmflexmacharqdlnack16qam) AS pmflexmacharqdlnack16qam,
            sum(pmflexmacharqdlnack256qam) AS pmflexmacharqdlnack256qam,
            sum(pmflexmacharqdlnack64qam) AS pmflexmacharqdlnack64qam,
            sum(pmflexmacharqdlnackqpsk) AS pmflexmacharqdlnackqpsk,
            sum(pmflexmacharqulfail16qam) AS pmflexmacharqulfail16qam,
            sum(pmflexmacharqulfail256qam) AS pmflexmacharqulfail256qam,
            sum(pmflexmacharqulfail64qam) AS pmflexmacharqulfail64qam,
            sum(pmflexmacharqulfailqpsk) AS pmflexmacharqulfailqpsk,
            sum(pmflexmacharqulsucc16qam) AS pmflexmacharqulsucc16qam,
            sum(pmflexmacharqulsucc256qam) AS pmflexmacharqulsucc256qam,
            sum(pmflexmacharqulsucc64qam) AS pmflexmacharqulsucc64qam,
            sum(pmflexmacharqulsuccqpsk) AS pmflexmacharqulsuccqpsk,
            sum(pmflexpdcplatpkttransdl) AS pmflexpdcplatpkttransdl,
            sum(pmflexpdcplattimedl) AS pmflexpdcplattimedl,
            sum(pmflexpdcppktdiscdlho) AS pmflexpdcppktdiscdlho,
            sum(pmflexpdcppktdiscdlpelr) AS pmflexpdcppktdiscdlpelr,
            sum(pmflexpdcppktdiscdlpelruu) AS pmflexpdcppktdiscdlpelruu,
            sum(pmflexpdcppktfwddl) AS pmflexpdcppktfwddl,
            sum(pmflexpdcppktlostul) AS pmflexpdcppktlostul,
            sum(pmflexpdcppktreceiveddl) AS pmflexpdcppktreceiveddl,
            sum(pmflexpdcppktreceivedul) AS pmflexpdcppktreceivedul,
            sum(pmflexpdcpvoldldrb) AS pmflexpdcpvoldldrb,
            sum(pmflexpdcpvoldldrblasttti) AS pmflexpdcpvoldldrblasttti,
            sum(pmflexpdcpvoluldrb) AS pmflexpdcpvoluldrb,
            sum(pmflexschedactivitycelldl) AS pmflexschedactivitycelldl,
            sum(pmflexschedactivitycellul) AS pmflexschedactivitycellul,
            sum(pmflexuethptimedl) AS pmflexuethptimedl,
            sum(pmflexuethptimeul) AS pmflexuethptimeul,
            sum(pmflexuethpvolul) AS pmflexuethpvolul
            --</editor-fold>
        FROM dnb.stats_v3."EUTRANCELLFDD_FLEX" as dt1
        WHERE dt1.eutrancellfdd IN ('DWKUL0670_L7_0020', 'DJJBR0518_L7_0030', 'DWKUL0155_L7_0010')
        AND flex_filtername ilike 'Plmn%endc2to99'
        group by "date_id"),
        counters3 AS (SELECT dt1."date_id"
        --<editor-fold desc="counters">
            , SUM(pmprbutildl_0) AS pmprbutildl_0
            , SUM(pmprbutildl_1) AS pmprbutildl_1
            , SUM(pmprbutildl_2) AS pmprbutildl_2
            , SUM(pmprbutildl_3) AS pmprbutildl_3
            , SUM(pmprbutildl_4) AS pmprbutildl_4
            , SUM(pmprbutildl_5) AS pmprbutildl_5
            , SUM(pmprbutildl_6) AS pmprbutildl_6
            , SUM(pmprbutildl_7) AS pmprbutildl_7
            , SUM(pmprbutildl_8) AS pmprbutildl_8
            , SUM(pmprbutildl_9) AS pmprbutildl_9
            , SUM(pmradiouerepcqidistr_0) AS pmradiouerepcqidistr_0
            , SUM(pmradiouerepcqidistr_1) AS pmradiouerepcqidistr_1
            , SUM(pmradiouerepcqidistr_2) AS pmradiouerepcqidistr_2
            , SUM(pmradiouerepcqidistr_3) AS pmradiouerepcqidistr_3
            , SUM(pmradiouerepcqidistr_4) AS pmradiouerepcqidistr_4
            , SUM(pmradiouerepcqidistr_5) AS pmradiouerepcqidistr_5
            , SUM(pmradiouerepcqidistr_6) AS pmradiouerepcqidistr_6
            , SUM(pmradiouerepcqidistr_7) AS pmradiouerepcqidistr_7
            , SUM(pmradiouerepcqidistr_8) AS pmradiouerepcqidistr_8
            , SUM(pmradiouerepcqidistr_9) AS pmradiouerepcqidistr_9
            , SUM(pmradiouerepcqidistr_10) AS pmradiouerepcqidistr_10
            , SUM(pmradiouerepcqidistr_11) AS pmradiouerepcqidistr_11
            , SUM(pmradiouerepcqidistr_12) AS pmradiouerepcqidistr_12
            , SUM(pmradiouerepcqidistr_13) AS pmradiouerepcqidistr_13
            , SUM(pmradiouerepcqidistr_14) AS pmradiouerepcqidistr_14
            , SUM(pmradiouerepcqidistr_15) AS pmradiouerepcqidistr_15
            , SUM(nom_rssi_pucch) AS nom_rssi_pucch
            , SUM(nom_rssi_pusch) AS nom_rssi_pusch
            , SUM(dnom_rssi_pucch) AS dnom_rssi_pucch
            , SUM(dnom_rssi_pusch) AS dnom_rssi_pusch
            , sum(ta.pmtainit2distr_0) as pmtainit2distr_0
            , sum(ta.pmtainit2distr_1) as pmtainit2distr_1
            , sum(ta.pmtainit2distr_2) as pmtainit2distr_2
            , sum(ta.pmtainit2distr_3) as pmtainit2distr_3
            , sum(ta.pmtainit2distr_4) as pmtainit2distr_4
            , sum(ta.pmtainit2distr_5) as pmtainit2distr_5
            , sum(ta.pmtainit2distr_6) as pmtainit2distr_6
            , sum(ta.pmtainit2distr_7) as pmtainit2distr_7
            , sum(ta.pmtainit2distr_8) as pmtainit2distr_8
            , sum(ta.pmtainit2distr_9) as pmtainit2distr_9
            , sum(ta.pmtainit2distr_10) as pmtainit2distr_10
            , sum(ta.pmtainit2distr_11) as pmtainit2distr_11
            , sum(ta.pmtainit2distr_12) as pmtainit2distr_12
            , sum(ta.pmtainit2distr_13) as pmtainit2distr_13
            , sum(ta.pmtainit2distr_14) as pmtainit2distr_14
            , sum(ta.pmtainit2distr_15) as pmtainit2distr_15
            , sum(ta.pmtainit2distr_16) as pmtainit2distr_16
            , sum(ta.pmtainit2distr_17) as pmtainit2distr_17
            , sum(ta.pmtainit2distr_18) as pmtainit2distr_18
            , sum(ta.pmtainit2distr_19) as pmtainit2distr_19
            , sum(ta.pmtainit2distr_20) as pmtainit2distr_20
            , sum(ta.pmtainit2distr_21) as pmtainit2distr_21
            , sum(ta.pmtainit2distr_22) as pmtainit2distr_22
            , sum(ta.pmtainit2distr_23) as pmtainit2distr_23
            , sum(ta.pmtainit2distr_24) as pmtainit2distr_24
            , sum(ta.pmtainit2distr_25) as pmtainit2distr_25
            , sum(ta.pmtainit2distr_26) as pmtainit2distr_26
            , sum(ta.pmtainit2distr_27) as pmtainit2distr_27
            , sum(ta.pmtainit2distr_28) as pmtainit2distr_28
            , sum(ta.pmtainit2distr_29) as pmtainit2distr_29
            , sum(ta.pmtainit2distr_30) as pmtainit2distr_30
            , sum(ta.pmtainit2distr_31) as pmtainit2distr_31
            , sum(ta.pmtainit2distr_32) as pmtainit2distr_32
            , sum(ta.pmtainit2distr_33) as pmtainit2distr_33
            , sum(ta.pmtainit2distr_34) as pmtainit2distr_34
            , sum(ta.pmtainit2distr_sum) as pmtainit2distr_sum
            --</editor-fold>
        FROM (SELECT erbs,
        eutrancellfdd,
        date_id,
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 0),
        0)                                                     AS "pmradiouerepcqidistr_0",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 1),
        0)                                                     AS "pmradiouerepcqidistr_1",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 2),
        0)                                                     AS "pmradiouerepcqidistr_2",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 3),
        0)                                                     AS "pmradiouerepcqidistr_3",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 4),
        0)                                                     AS "pmradiouerepcqidistr_4",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 5),
        0)                                                     AS "pmradiouerepcqidistr_5",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 6),
        0)                                                     AS "pmradiouerepcqidistr_6",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 7),
        0)                                                     AS "pmradiouerepcqidistr_7",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 8),
        0)                                                     AS "pmradiouerepcqidistr_8",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 9),
        0)                                                     AS "pmradiouerepcqidistr_9",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 10),
        0)                                                     AS "pmradiouerepcqidistr_10",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 11),
        0)                                                     AS "pmradiouerepcqidistr_11",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 12),
        0)                                                     AS "pmradiouerepcqidistr_12",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 13),
        0)                                                     AS "pmradiouerepcqidistr_13",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 14),
        0)                                                     AS "pmradiouerepcqidistr_14",
        coalesce(avg(pmradiouerepcqidistr) FILTER (WHERE dcvector_index = 15),
        0)                                                     AS "pmradiouerepcqidistr_15",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 0), 0) AS "pmprbutildl_0",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 1), 0) AS "pmprbutildl_1",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 2), 0) AS "pmprbutildl_2",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 3), 0) AS "pmprbutildl_3",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 4), 0) AS "pmprbutildl_4",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 5), 0) AS "pmprbutildl_5",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 6), 0) AS "pmprbutildl_6",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 7), 0) AS "pmprbutildl_7",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 8), 0) AS "pmprbutildl_8",
        coalesce(avg(pmprbutildl) FILTER (WHERE dcvector_index = 9), 0) AS "pmprbutildl_9",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 0), 0) AS "pmprbutilul_0",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 1), 0) AS "pmprbutilul_1",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 2), 0) AS "pmprbutilul_2",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 3), 0) AS "pmprbutilul_3",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 4), 0) AS "pmprbutilul_4",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 5), 0) AS "pmprbutilul_5",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 6), 0) AS "pmprbutilul_6",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 7), 0) AS "pmprbutilul_7",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 8), 0) AS "pmprbutilul_8",
        coalesce(avg(pmprbutilul) FILTER (WHERE dcvector_index = 9), 0) AS "pmprbutilul_9"
        FROM dnb.stats_v3."EUTRANCELLFDD_V"
        WHERE eutrancellfdd IN ${sql(cells)}
        GROUP BY erbs, eutrancellfdd, date_id) as dt1
        LEFT JOIN (SELECT date_id,
        erbs,
        eutrancellfdd,
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 0), 0)  AS "pmtainit2distr_0",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 1), 0)  AS "pmtainit2distr_1",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 2), 0)  AS "pmtainit2distr_2",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 3), 0)  AS "pmtainit2distr_3",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 4), 0)  AS "pmtainit2distr_4",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 5), 0)  AS "pmtainit2distr_5",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 6), 0)  AS "pmtainit2distr_6",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 7), 0)  AS "pmtainit2distr_7",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 8), 0)  AS "pmtainit2distr_8",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 9), 0)  AS "pmtainit2distr_9",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 10), 0) AS "pmtainit2distr_10",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 11), 0) AS "pmtainit2distr_11",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 12), 0) AS "pmtainit2distr_12",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 13), 0) AS "pmtainit2distr_13",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 14), 0) AS "pmtainit2distr_14",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 15), 0) AS "pmtainit2distr_15",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 16), 0) AS "pmtainit2distr_16",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 17), 0) AS "pmtainit2distr_17",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 18), 0) AS "pmtainit2distr_18",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 19), 0) AS "pmtainit2distr_19",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 20), 0) AS "pmtainit2distr_20",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 21), 0) AS "pmtainit2distr_21",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 22), 0) AS "pmtainit2distr_22",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 23), 0) AS "pmtainit2distr_23",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 24), 0) AS "pmtainit2distr_24",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 25), 0) AS "pmtainit2distr_25",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 26), 0) AS "pmtainit2distr_26",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 27), 0) AS "pmtainit2distr_27",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 28), 0) AS "pmtainit2distr_28",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 29), 0) AS "pmtainit2distr_29",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 30), 0) AS "pmtainit2distr_30",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 31), 0) AS "pmtainit2distr_31",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 32), 0) AS "pmtainit2distr_32",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 33), 0) AS "pmtainit2distr_33",
        coalesce(avg(pmtainit2distr) FILTER (WHERE dcvector_index = 34), 0) AS "pmtainit2distr_34",
        sum(pmtainit2distr)                                                 AS "pmtainit2distr_sum"
        FROM dnb.stats_v3."EUTRANCELLFDD_V"
        WHERE eutrancellfdd IN
        ${sql(cells)}
        group by date_id, erbs, eutrancellfdd) as ta
        USING (date_id, erbs, eutrancellfdd)
        LEFT JOIN (SELECT erbs,
        eutrancellfdd,
        date_id,
        sum((case
        when "dcvector_index" < 10
        then d1."pmradiorecinterferencepwrpucch" * (-121.5 + dcvector_index)
        else 0 end) + (case
        when dcvector_index > 9 then
        d1."pmradiorecinterferencepwrpucch" *
        (-110 + (4 * (dcvector_index - 10)))
        else 0 end)) as nom_rssi_pucch,
        sum(d1."pmradiorecinterferencepwrpucch") as dnom_rssi_pucch,
        sum((case
        when "dcvector_index" < 10
        then d1."pmradiorecinterferencepwr" * (-121.5 + dcvector_index)
        else 0 end) + (case
        when dcvector_index > 9
        then d1."pmradiorecinterferencepwr" * (-110 + (4 * (dcvector_index - 10)))
        else 0 end)) as nom_rssi_pusch,
        sum(d1."pmradiorecinterferencepwr")      as dnom_rssi_pusch
        FROM dnb.stats_v3."EUTRANCELLFDD_V" as d1
        WHERE d1.eutrancellfdd IN
        ${sql(cells)}
        GROUP BY erbs, eutrancellfdd, date_id) as dt2
        USING (erbs, eutrancellfdd, date_id)

        group by "date_id"),
        counters4 AS (SELECT dt1."date_id",
        sum(pmhoexeattlteinterf)  as pmhoexeattlteinterf,
        sum(pmhoexeattltespifho)  as pmhoexeattltespifho,
        sum(pmhoexesucclteinterf) as pmhoexesucclteinterf,
        sum(pmhoexesuccltespifho) as pmhoexesuccltespifho
        FROM dnb.stats_v3."EUTRANCELLRELATION_PER_CELL" as dt1
        WHERE dt1.eutrancellfdd IN ${sql(cells)}
        group by "date_id")
        SELECT counters1.date_id:: varchar(10)                                           as "time",
        --<editor-fold desc="kpis1"> 
        100 * ((60 * (period_duration)) - ((pmcelldowntimeauto) + (pmcelldowntimeman))) ||
            (60 * (period_duration)) ::double precision AS "Cell Availability",
            100 *
            (pmrrcconnestabsucc || (pmrrcconnestabatt - pmrrcconnestabattreatt - pmrrcconnestabfailmmeovlmos -
            pmrrcconnestabfailmmeovlmod)) *
            (pms1sigconnestabsucc || (pms1sigconnestabatt - pms1sigconnestabfailmmeovlmos)) *
            (pmflexerabestabsuccinit_endc2to99 + pmflexerabestabsuccadded_endc2to99) ||
            (pmflexerabestabattinit_endc2to99 + pmflexerabestabattadded_endc2to99) AS "Call Setup Success Rate",

            100 * (pmflexerabestabsuccinit_endc2to99 + pmflexerabestabsuccadded_endc2to99) ||
            (pmflexerabestabattinit_endc2to99 + pmflexerabestabattadded_endc2to99) AS "E-RAB Setup Success Rate_non-GBR
            (%)",

            100 * (pmrrcconnestabsucc || (pmrrcconnestabatt - pmrrcconnestabattreatt - pmrrcconnestabfailmmeovlmos -
            pmrrcconnestabfailmmeovlmod)) AS "RRC Setup Success Rate (Service) (%)",
            100 * pmrrcconnestabsuccmos ||
            (pmrrcconnestabattmos - pmrrcconnestabattreattmos) AS "RRC Setup Success Rate (Signaling) (%)",
            100 * (pmerabestabsuccinit + pmerabestabsuccadded) ||
            (pmerabestabattinit + pmerabestabattadded - pmerabestabattaddedhoongoing) AS "E-RAB Setup Success Rate (%)",
            100 * (pmerabrelabnormalenbact + pmerabrelabnormalmmeact) ||
            (pmerabrelabnormalenb + pmerabrelnormalenb + pmerabrelmme) AS "Erab Drop Call rate",
            100 * (pmuectxtfetchsuccx2hoin + pmuectxtfetchsuccintraenbhoin) ||
            (pmuectxtfetchattx2hoin + pmuectxtfetchattintraenbhoin) AS "Handover In Success Rate",
            100 * ((pmmacharqulfailqpsk + pmmacharqulfail16qam + pmmacharqulfail64qam + pmmacharqulfail256qam) ||
            (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam + pmmacharqulsucc256qam +
            pmmacharqulfailqpsk + pmmacharqulfail16qam + pmmacharqulfail64qam +
            pmmacharqulfail256qam)) AS "UL BLER",
            (pmpdcpvoldldrb - pmpdcpvoldldrblasttti) || pmuethptimedl AS "DL User Throughput",
            pmuethpvolul || pmuethptimeul AS "UL User Throughput",
            pmpdcpvoldldrb || pmschedactivitycelldl AS "DL Cell Throughput",
            pmpdcpvoluldrb || pmschedactivitycellul AS "UL Cell Throughput",
            pmpdcpvoldldrb || (8 * 1024 * 1024) ::double precision AS "DL Data Volume",
            pmpdcpvoluldrb || (8 * 1024 * 1024)::double precision AS "UL Data Volume",
            pmrrcconnmax AS "Max of RRC Connected User",
            pmactiveuedlmax AS "Max of Active User",
            100 * (pmpdcppktdiscdlpelr + pmpdcppktdiscdlpelruu + pmpdcppktdiscdlho) ||
            (pmpdcppktreceiveddl - pmpdcppktfwddl) AS "Packet Loss (DL)",
            100 * pmpdcppktlostul || (pmpdcppktlostul + pmpdcppktreceivedul) AS "Packet Loss UL",
            pmpdcplattimedl || pmpdcplatpkttransdl AS "Latency (only Radio interface)",
            100 * (pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
            pmmacharqdldtxqpsk) AS "DL QPSK %",
            (pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam) ||
            100 * (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
            pmmacharqdldtxqpsk) AS "DL 16QAM%",
            (pmmacharqdlack64qam + pmmacharqdlnack64qam + pmmacharqdldtx64qam) ||
            100 * (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
            pmmacharqdldtxqpsk) AS "DL 64QAM%",
            100 * ((pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam) ||
            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
            pmmacharqdldtx16qam +
            pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
            pmmacharqdldtxqpsk)) AS "DL 256QAM%",
            100 * ((pmmacharqulsuccqpsk) ||
            (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
            pmmacharqulsucc256qam)) AS "UL QPSK %",
            100 * ((pmmacharqulsucc16qam) ||
            (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
            pmmacharqulsucc256qam)) AS "UL 16QAM%",
            100 * ((pmmacharqulsucc64qam) ||
            (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
            pmmacharqulsucc256qam)) AS "UL 64QAM%",
            100 * ((pmmacharqulsucc256qam) ||
            (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
            pmmacharqulsucc256qam)) AS "UL 256QAM%"
            --</editor-fold>
        ,
        100 * ((pmflexcellhoexesucclteintraf) || (pmflexcellhoexeattlteintraf))   AS "Intrafreq HOSR"
        ,
        --<editor-fold desc="kpis3">
            (5 * pmprbutildl_0 + 15 * pmprbutildl_1 + 25 * pmprbutildl_2 + 35 * pmprbutildl_3 + 45 * pmprbutildl_4 +
            55 * pmprbutildl_5 + 65 * pmprbutildl_6 + 75 * pmprbutildl_7 + 85 * pmprbutildl_8 + 95 * pmprbutildl_9) ||
            (pmprbutildl_0 + pmprbutildl_1 + pmprbutildl_2 + pmprbutildl_3 + pmprbutildl_4 + pmprbutildl_5 +
            pmprbutildl_6 + pmprbutildl_7 + pmprbutildl_8 +
            pmprbutildl_9) AS "Resource Block Utilizing Rate (DL)",

            (5 * pmprbutildl_0 + 15 * pmprbutildl_1 + 25 * pmprbutildl_2 + 35 * pmprbutildl_3 +
            45 * pmprbutildl_4 + 55 * pmprbutildl_5 + 65 * pmprbutildl_6 + 75 * pmprbutildl_7 +
            85 * pmprbutildl_8 + 95 * pmprbutildl_9) ||
            (pmprbutildl_0 + pmprbutildl_1 + pmprbutildl_2 + pmprbutildl_3 + pmprbutildl_4 + pmprbutildl_5 +
            pmprbutildl_6 + pmprbutildl_7 + pmprbutildl_8 +
            pmprbutildl_9) AS "Resource Block Utilizing Rate (UL)",

            ((pmradiouerepcqidistr_0 * 0) + (pmradiouerepcqidistr_1 * 1) + (pmradiouerepcqidistr_2 * 2) +
            (pmradiouerepcqidistr_3 * 3) + (pmradiouerepcqidistr_4 * 4) + (pmradiouerepcqidistr_5 * 5) +
            (pmradiouerepcqidistr_6 * 6) + (pmradiouerepcqidistr_7 * 7) + (pmradiouerepcqidistr_8 * 8) +
            (pmradiouerepcqidistr_9 * 9) + (pmradiouerepcqidistr_10 * 10) + (pmradiouerepcqidistr_11 * 11) +
            (pmradiouerepcqidistr_12 * 12) + (pmradiouerepcqidistr_13 * 13) + (pmradiouerepcqidistr_14 * 14) +
            (pmradiouerepcqidistr_15 * 15)) || (
            pmradiouerepcqidistr_0 + pmradiouerepcqidistr_1 + pmradiouerepcqidistr_2 + pmradiouerepcqidistr_3 +
            pmradiouerepcqidistr_4 + pmradiouerepcqidistr_5 + pmradiouerepcqidistr_6 + pmradiouerepcqidistr_7 +
            pmradiouerepcqidistr_8 + pmradiouerepcqidistr_9 + pmradiouerepcqidistr_10 + pmradiouerepcqidistr_11 +
            pmradiouerepcqidistr_12 + pmradiouerepcqidistr_13 + pmradiouerepcqidistr_14 +
            pmradiouerepcqidistr_15
            ) AS "Average CQI",
            (nom_rssi_pusch) || (dnom_rssi_pusch) AS "Avg PUSCH UL RSSI",
            ((0.04 * (pmtainit2distr_0) + 0.155 * (pmtainit2distr_1) + 0.43 * (pmtainit2distr_2) +
            0.82 * (pmtainit2distr_3) +
            1.325 * (pmtainit2distr_4) + 2.03 * (pmtainit2distr_5) + 2.89 * (pmtainit2distr_6) +
            3.905 * (pmtainit2distr_7) + 5.075 * (pmtainit2distr_8) + 6.44 * (pmtainit2distr_9) +
            7.96 * (pmtainit2distr_10) + 9.62 * (pmtainit2distr_11) + 11.5 * (pmtainit2distr_12) +
            13.55 * (pmtainit2distr_13) + 15.75 * (pmtainit2distr_14) + 18.15 * (pmtainit2distr_15) +
            20.7 * (pmtainit2distr_16) + 23.4 * (pmtainit2distr_17) + 26.35 * (pmtainit2distr_18) +
            29.5 * (pmtainit2distr_19) + 32.8 * (pmtainit2distr_20) + 36.3 * (pmtainit2distr_21) +
            39.95 * (pmtainit2distr_22) + 43.8 * (pmtainit2distr_23) + 47.9 * (pmtainit2distr_24) +
            52.15 * (pmtainit2distr_25) + 56.6 * (pmtainit2distr_26) + 61.2 * (pmtainit2distr_27) +
            66 * (pmtainit2distr_28) + 71.05 * (pmtainit2distr_29) + 76.25 * (pmtainit2distr_30) +
            81.7 * (pmtainit2distr_31) + 87.35 * (pmtainit2distr_32) + 93.5 * (pmtainit2distr_33) +
            98.08 * (pmtainit2distr_34)) || (pmtainit2distr_sum)) *
            1000 as "LTE_Avg_TA_Meters"
            --</editor-fold>
        ,
        100 * ((pmhoexesuccltespifho) || (pmhoexeattltespifho))                   AS "VoLTE Redirection Success Rate",
        100 * (pmhoexesucclteinterf || pmhoexeattlteinterf)                       AS "Interfreq HOSR"
        FROM counters1
        LEFT JOIN counters2
        USING (date_id)
        LEFT JOIN counters3
        USING (date_id)
        LEFT JOIN counters4
        USING (date_id)
        ORDER BY time;

        ;
    `;
    return response.status(200).json(results);
};

const networkDailyPlmnStatsNR = async (request, response) => {
    const results = await sql`
        SELECT tt1.date_id::varchar(10) as time,
                       tt1.mobile_operator as object,
                       ${sql(plmnKpiList.NR)}
        FROM 
        dnb.stats_v3.nrcellcu_flex_plmn_kpi_view as tt1
        LEFT JOIN dnb.stats_v3.nrcelldu_flex_plmn_kpi_view as tt2
        USING (date_id, mobile_operator, "Region", "Cluster_ID")
        WHERE tt1."Region" = 'All'
        ORDER BY time;
    `;
    return sendResults(request, response, results);
};

const regionDailyPlmnStatsNR = async (request, response) => {
    const results = await sql`
        SELECT tt1.date_id as time,
                       tt1.mobile_operator as object,
                       tt1.flex_filtername,
                       tt2.flex_filtername, ${sql(plmnKpiList.NR)}
        FROM dnb.stats_v3.nrcellcu_flex_plmn_kpi_view as tt1
        LEFT JOIN dnb.stats_v3.nrcelldu_flex_plmn_kpi_view as tt2
        USING (date_id, mobile_operator, "Region", "MCMC_State", "Cluster_ID")
        WHERE tt1."Region" <> 'All'
          and tt1."Cluster_ID" = 'All'
        ORDER BY time;
    `;
    return sendResults(request, response, results);
};

const clusterDailyPlmnStatsNR = async (request, response) => {
    const clusterId = getClusterId(request);
    const results = await sql`
        SELECT tt1.date_id as time,
                       tt1.mobile_operator as object,
                       tt1.flex_filtername,
                       tt2.flex_filtername, ${sql(plmnKpiList.NR)}
        FROM dnb.stats_v3.nrcellcu_flex_plmn_kpi_view as tt1
        LEFT JOIN dnb.stats_v3.nrcelldu_flex_plmn_kpi_view as tt2
        USING (date_id, mobile_operator, "Region", "MCMC_State", "Cluster_ID")
        WHERE tt1."Cluster_ID" <> 'All'
          AND tt1."Cluster_ID" LIKE ${clusterId}
        ORDER BY tt1."Cluster_ID", tt1.date_id;
        ORDER BY time;
    `;
    return sendResults(request, response, results);
};

const cellDailyPlmnStatsNR = async (request, response) => {
    const cellId = getCellId(request);
    if (!cellId) {
        response.status(400).json({
            success: false, message: "cellId is required"
        });
        return;
    }
    const results = await sql`
    SELECT tt1.date_id::varchar(10) as time,
       tt1.mobile_operator as object,
       ${sql(plmnKpiList.NR)}
       FROM dnb.stats_v3.tbl_cell_nrcellcu_flex_plmn_kpi as tt1
       LEFT JOIN dnb.stats_v3.tbl_cell_nrcelldu_flex_plmn_kpi as tt2
         on tt1.date_id = tt2.date_id
       and tt1.mobile_operator = tt2.mobile_operator
       and tt1.nrcellcu = tt2.nrcelldu
       WHERE tt1.nrcellcu = ${cellId}
            ORDER BY time;
    `;
    return sendResults(request, response, results);
};

const networkDailyPlmnStatsLTE = async (request, response) => {
    const results = await sql`
            SELECT tt1.date_id::varchar(10) as time,
            tt1.mobile_operator as object,
            tt1.mobile_operator, ${sql(plmnKpiList.LTE)}
            FROM dnb.stats_v3.eutrancellfddflex_plmn_kpi_view as tt1
            LEFT JOIN dnb.stats_v3.eutrancellrelation_plmn_kpi_view as tt2
            USING (date_id, mobile_operator, "Region", "Cluster_ID")
            WHERE tt1."Region" = 'All'
            ORDER BY time;
    `;
    return sendResults(request, response, results);
};

const regionDailyPlmnStatsLTE = async (request, response) => {
    const results = await sql`
            SELECT tt1.date_id::varchar(10) as time,
            tt1.mobile_operator as object,
            tt1.mobile_operator, ${sql(plmnKpiList.LTE)}
            FROM dnb.stats_v3.eutrancellfddflex_plmn_kpi_view as tt1
            LEFT JOIN dnb.stats_v3.eutrancellrelation_plmn_kpi_view as tt2
            USING (date_id, mobile_operator, "Region", "Cluster_ID")
            WHERE tt1."Region" <> 'All'
            and tt1."Cluster_ID" = 'All'
            ORDER BY tt1."Region", tt1.date_id
    `;
    return sendResults(request, response, results);
};

const clusterDailyPlmnStatsLTE = async (request, response) => {
    const clusterId = request.query.clusterId || request.params.clusterId || '%';
    const results = await sql`
        SELECT tt1.date_id::varchar(10) as time,
        tt1.mobile_operator as object,
        ${sql(plmnKpiList.LTE)}
        FROM dnb.stats_v3.eutrancellfddflex_plmn_kpi_view as tt1
        LEFT JOIN dnb.stats_v3.eutrancellrelation_plmn_kpi_view as tt2
        USING (date_id, mobile_operator, "Region", "Cluster_ID")
        WHERE tt1."Cluster_ID" <> 'All'
        and tt1."Cluster_ID" LIKE ${clusterId}
        ORDER BY tt1."Cluster_ID", tt1.date_id
    `;
    return sendResults(request, response, results);
};

const cellDailyPlmnStatsLTE = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId || request.query.object || request.params.object;
    if (!cellId) {
        response.status(400).json({
            success: false, message: "cellId is required"
        });
        return;
    }
    const notReadyKPI = ['Interfreq HOSR', 'VoLTE Redirection Success Rate'];
    const kpiList = plmnKpiList.LTE;
    // remove notReadyKPI from kpiList
    const kpiListReady = kpiList.filter(kpi => !notReadyKPI.includes(kpi));

    const results = await sql`
        SELECT tt1.date_id::varchar(10) as time,
        tt1.mobile_operator as object,
        ${sql(kpiListReady)}
        FROM dnb.stats_v3.tbl_cell_eutrancellfddflex_plmn_kpi_view as tt1
        WHERE tt1.eutrancellfdd = ${cellId}
        ORDER BY time;
    `;
    return sendResults(request, response, results, {parseDate: true, dateColumns: ['on_board_date']});
};

const clusterStatsAggregatedNR = async (request, response) => {
    const {startDate, endDate} = request.query;
    const results = await sql`
                            WITH COUNTERS AS (SELECT "Cluster_ID",
                            sum(period_duration)                      AS period_duration,
                            sum(pmactiveuedlmax)                      AS pmactiveuedlmax,
                            sum(pmmacharqdlack16qam)                  AS pmmacharqdlack16qam,
                            sum(pmmacharqdlack256qam)                 AS pmmacharqdlack256qam,
                            sum(pmmacharqdlack64qam)                  AS pmmacharqdlack64qam,
                            sum(pmmacharqdlackqpsk)                   AS pmmacharqdlackqpsk,
                            sum(pmmacharqdldtx16qam)                  AS pmmacharqdldtx16qam,
                            sum(pmmacharqdldtx256qam)                 AS pmmacharqdldtx256qam,
                            sum(pmmacharqdldtx64qam)                  AS pmmacharqdldtx64qam,
                            sum(pmmacharqdldtxqpsk)                   AS pmmacharqdldtxqpsk,
                            sum(pmmacharqdlnack16qam)                 AS pmmacharqdlnack16qam,
                            sum(pmmacharqdlnack256qam)                AS pmmacharqdlnack256qam,
                            sum(pmmacharqdlnack64qam)                 AS pmmacharqdlnack64qam,
                            sum(pmmacharqdlnackqpsk)                  AS pmmacharqdlnackqpsk,
                            sum(pmmacharqulack16qam)                  AS pmmacharqulack16qam,
                            sum(pmmacharqulack256qam)                 AS pmmacharqulack256qam,
                            sum(pmmacharqulack64qam)                  AS pmmacharqulack64qam,
                            sum(pmmacharqulackqpsk)                   AS pmmacharqulackqpsk,
                            sum(pmmacharquldtx16qam)                  AS pmmacharquldtx16qam,
                            sum(pmmacharquldtx256qam)                 AS pmmacharquldtx256qam,
                            sum(pmmacharquldtx64qam)                  AS pmmacharquldtx64qam,
                            sum(pmmacharquldtxqpsk)                   AS pmmacharquldtxqpsk,
                            sum(pmmacharqulnack16qam)                 AS pmmacharqulnack16qam,
                            sum(pmmacharqulnack256qam)                AS pmmacharqulnack256qam,
                            sum(pmmacharqulnack64qam)                 AS pmmacharqulnack64qam,
                            sum(pmmacharqulnackqpsk)                  AS pmmacharqulnackqpsk,
                            sum(pmmacpdcchblockingpdschoccasions)     AS pmmacpdcchblockingpdschoccasions,
                            sum(pmmacpdcchblockingpuschoccasions)     AS pmmacpdcchblockingpuschoccasions,
                            sum(pmmacrbsymavaildl)                    AS pmmacrbsymavaildl,
                            sum(pmmacrbsymavailul)                    AS pmmacrbsymavailul,
                            sum(pmmacrbsymcsirs)                      AS pmmacrbsymcsirs,
                            sum(pmmacrbsymusedpdcchtypea)             AS pmmacrbsymusedpdcchtypea,
                            sum(pmmacrbsymusedpdcchtypeb)             AS pmmacrbsymusedpdcchtypeb,
                            sum(pmmacrbsymusedpdschtypea)             AS pmmacrbsymusedpdschtypea,
                            sum(pmmacrbsymusedpdschtypeabroadcasting) AS pmmacrbsymusedpdschtypeabroadcasting,
                            sum(pmmacrbsymusedpuschtypea)             AS pmmacrbsymusedpuschtypea,
                            sum(pmmacrbsymusedpuschtypeb)             AS pmmacrbsymusedpuschtypeb,
                            sum(pmmactimedldrb)                       AS pmmactimedldrb,
                            sum(pmmactimeulresue)                     AS pmmactimeulresue,
                            sum(pmmacvoldl)                           AS pmmacvoldl,
                            sum(pmmacvoldldrb)                        AS pmmacvoldldrb,
                            sum(pmmacvolul)                           AS pmmacvolul,
                            sum(pmmacvolulresue)                      AS pmmacvolulresue,
                            sum(pmpdschschedactivity)                 AS pmpdschschedactivity,
                            sum(pmpuschschedactivity)                 AS pmpuschschedactivity,
                            sum(pmcelldowntimeauto)                   AS pmcelldowntimeauto,
                            sum(pmcelldowntimeman)                    AS pmcelldowntimeman
                            FROM dnb.stats_v3."NRCELLDU" as dt
                            LEFT JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
                            INNER JOIN (SELECT site_id, on_board_date::date, time::date
                                       FROM dnb.rfdb.df_dpm,
                                            generate_series(on_board_date, now(), '1 day') as time) as obs
                                      on obs.time = dt."date_id" and left(dt."nr_name", 9) like obs.site_id
                            WHERE "Cluster_ID" is not null
                            AND date_id>=${startDate} AND date_id<=${endDate}
                            GROUP BY "Cluster_ID"
                            )
                            SELECT "Cluster_ID",
                                   ${startDate} as start_date,
                                      ${endDate} as end_date,
                            100 * ((60 * (period_duration)) - ((pmcelldowntimeauto + (pmcelldowntimeman))) ||
                            (60 * (period_duration)::double precision))    AS "Cell Availability",
                            100 * (pmmacpdcchblockingpdschoccasions + pmmacpdcchblockingpuschoccasions) ||
                            (pmmacrbsymusedpdcchtypea + pmmacrbsymusedpdcchtypeb) AS "E-RAB Block Rate",
                            100 * (pmmacrbsymusedpdschtypea + pmmacrbsymusedpdschtypeabroadcasting + pmmacrbsymcsirs) ||
                            (pmmacrbsymavaildl)                                   AS "Resource Block Utilizing Rate (DL)",
                            100 * (pmmacrbsymusedpuschtypea + pmmacrbsymusedpuschtypeb) ||
                            (pmmacrbsymavailul)                                   AS "Resource Block Utilizing Rate (UL)",
                            100 * (pmmacharqulnackqpsk + pmmacharqdlnack16qam + pmmacharqdlnack64qam) ||
                            (pmmacharqulackqpsk + pmmacharqulack16qam + pmmacharqulack64qam + pmmacharqulnackqpsk + pmmacharqulnack16qam +
                            pmmacharqulnack64qam)                                AS "UL BLER",
                            64 * (pmmacvoldldrb || pmmactimedldrb) / 1000         AS "DL User Throughput",
                            64 * (pmmacvolulresue || pmmactimeulresue) / 1000     AS "UL User Throughput",
                            64 * (pmmacvoldl || pmpdschschedactivity) / 1000      AS "DL Cell Throughput",
                            64 * pmmacvolul || pmpuschschedactivity / 1000        AS "UL Cell Throughput",
                            pmmacvoldl / 1024 / 1024 / 1024                       AS "DL Data Volume",
                            pmmacvolul / 1024 / 1024 / 1024                       AS "UL Data Volume",
                            pmactiveuedlmax                                       AS "Max of Active User",
                            (pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) ||
                            ((pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk)) *
                            100                                                   AS "DL QPSK %",
                            ((pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam) ||
                            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk)) *
                            100                                                   AS "DL 16QAM%",
                            ((pmmacharqdlack64qam + pmmacharqdlnack64qam + pmmacharqdldtx64qam) ||
                            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk)) *
                            100                                                   AS "DL 64QAM%",
                            ((pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam) ||
                            (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                            pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                            pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk)) *
                            100                                                   AS "DL 256QAM%",
                            ((pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) ||
                            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk)) *
                            100                                                   AS "UL QPSK %",
                            ((pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam) ||
                            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk)) *
                            100                                                   AS "UL 16QAM%",
                            ((pmmacharqulack64qam + pmmacharqulnack64qam + pmmacharquldtx64qam) ||
                            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk)) *
                            100                                                   AS "UL 64QAM%",
                            ((pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam) ||
                            (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                            pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                            pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk)) *
                            100                                                   AS "UL 256QAM%",
                            row_number() over ()                                  as id
                            FROM COUNTERS;
    
    `;
    return sendResults(request, response, results);
};

const clusterStatsAggregatedLTE = async (request, response) => {
    const {startDate, endDate} = request.query;
    const results = await sql`
        WITH COUNTERS AS (SELECT
        "Cluster_ID",
        --<editor-fold desc="Counters">
            sum(period_duration) AS period_duration,
            sum(pmactiveuedlmax) AS pmactiveuedlmax,
            sum(pmcelldowntimeauto) AS pmcelldowntimeauto,
            sum(pmcelldowntimeman) AS pmcelldowntimeman,
            sum(pmerabestabattadded) AS pmerabestabattadded,
            sum(pmerabestabattaddedhoongoing) AS pmerabestabattaddedhoongoing,
            sum(pmerabestabattinit) AS pmerabestabattinit,
            sum(pmerabestabsuccadded) AS pmerabestabsuccadded,
            sum(pmerabestabsuccinit) AS pmerabestabsuccinit,
            sum(pmerabrelabnormalenb) AS pmerabrelabnormalenb,
            sum(pmerabrelabnormalenbact) AS pmerabrelabnormalenbact,
            sum(pmerabrelabnormalmmeact) AS pmerabrelabnormalmmeact,
            sum(pmerabrelmme) AS pmerabrelmme,
            sum(pmerabrelnormalenb) AS pmerabrelnormalenb,
            sum(pmmacharqdlack16qam) AS pmmacharqdlack16qam,
            sum(pmmacharqdlack256qam) AS pmmacharqdlack256qam,
            sum(pmmacharqdlack64qam) AS pmmacharqdlack64qam,
            sum(pmmacharqdlackqpsk) AS pmmacharqdlackqpsk,
            sum(pmmacharqdldtx16qam) AS pmmacharqdldtx16qam,
            sum(pmmacharqdldtx256qam) AS pmmacharqdldtx256qam,
            sum(pmmacharqdldtx64qam) AS pmmacharqdldtx64qam,
            sum(pmmacharqdldtxqpsk) AS pmmacharqdldtxqpsk,
            sum(pmmacharqdlnack16qam) AS pmmacharqdlnack16qam,
            sum(pmmacharqdlnack256qam) AS pmmacharqdlnack256qam,
            sum(pmmacharqdlnack64qam) AS pmmacharqdlnack64qam,
            sum(pmmacharqdlnackqpsk) AS pmmacharqdlnackqpsk,
            sum(pmmacharqulfail16qam) AS pmmacharqulfail16qam,
            sum(pmmacharqulfail256qam) AS pmmacharqulfail256qam,
            sum(pmmacharqulfail64qam) AS pmmacharqulfail64qam,
            sum(pmmacharqulfailqpsk) AS pmmacharqulfailqpsk,
            sum(pmmacharqulsucc16qam) AS pmmacharqulsucc16qam,
            sum(pmmacharqulsucc256qam) AS pmmacharqulsucc256qam,
            sum(pmmacharqulsucc64qam) AS pmmacharqulsucc64qam,
            sum(pmmacharqulsuccqpsk) AS pmmacharqulsuccqpsk,
            sum(pmpdcplatpkttransdl) AS pmpdcplatpkttransdl,
            sum(pmpdcplattimedl) AS pmpdcplattimedl,
            sum(pmpdcppktdiscdlho) AS pmpdcppktdiscdlho,
            sum(pmpdcppktdiscdlpelr) AS pmpdcppktdiscdlpelr,
            sum(pmpdcppktdiscdlpelruu) AS pmpdcppktdiscdlpelruu,
            sum(pmpdcppktfwddl) AS pmpdcppktfwddl,
            sum(pmpdcppktlostul) AS pmpdcppktlostul,
            sum(pmpdcppktreceiveddl) AS pmpdcppktreceiveddl,
            sum(pmpdcppktreceivedul) AS pmpdcppktreceivedul,
            sum(pmpdcpvoldldrb) AS pmpdcpvoldldrb,
            sum(pmpdcpvoldldrblasttti) AS pmpdcpvoldldrblasttti,
            sum(pmpdcpvoluldrb) AS pmpdcpvoluldrb,
            sum(pmrrcconnestabatt) AS pmrrcconnestabatt,
            sum(pmrrcconnestabattmos) AS pmrrcconnestabattmos,
            sum(pmrrcconnestabattreatt) AS pmrrcconnestabattreatt,
            sum(pmrrcconnestabattreattmos) AS pmrrcconnestabattreattmos,
            sum(pmrrcconnestabfailmmeovlmod) AS pmrrcconnestabfailmmeovlmod,
            sum(pmrrcconnestabfailmmeovlmos) AS pmrrcconnestabfailmmeovlmos,
            sum(pmrrcconnestabsucc) AS pmrrcconnestabsucc,
            sum(pmrrcconnestabsuccmos) AS pmrrcconnestabsuccmos,
            sum(pmrrcconnmax) AS pmrrcconnmax,
            sum(pmrrcconnmaxplmn0) AS pmrrcconnmaxplmn0,
            sum(pmrrcconnmaxplmn1) AS pmrrcconnmaxplmn1,
            sum(pmrrcconnmaxplmn2) AS pmrrcconnmaxplmn2,
            sum(pmrrcconnmaxplmn3) AS pmrrcconnmaxplmn3,
            sum(pmrrcconnmaxplmn4) AS pmrrcconnmaxplmn4,
            sum(pmrrcconnmaxplmn5) AS pmrrcconnmaxplmn5,
            sum(pmrrcconnmaxplmn6) AS pmrrcconnmaxplmn6,
            sum(pms1sigconnestabatt) AS pms1sigconnestabatt,
            sum(pms1sigconnestabfailmmeovlmos) AS pms1sigconnestabfailmmeovlmos,
            sum(pms1sigconnestabsucc) AS pms1sigconnestabsucc,
            sum(pmschedactivitycelldl) AS pmschedactivitycelldl,
            sum(pmschedactivitycellul) AS pmschedactivitycellul,
            sum(pmuectxtfetchattintraenbhoin) AS pmuectxtfetchattintraenbhoin,
            sum(pmuectxtfetchattx2hoin) AS pmuectxtfetchattx2hoin,
            sum(pmuectxtfetchsuccintraenbhoin) AS pmuectxtfetchsuccintraenbhoin,
            sum(pmuectxtfetchsuccx2hoin) AS pmuectxtfetchsuccx2hoin,
            sum(pmuethptimedl) AS pmuethptimedl,
            sum(pmuethptimeul) AS pmuethptimeul,
            sum(pmuethpvolul) AS pmuethpvolul,
            sum(pmflexerabestabsuccinit_endc2to99) as pmflexerabestabsuccinit_endc2to99,
            sum(pmflexerabestabsuccadded_endc2to99) as pmflexerabestabsuccadded_endc2to99,
            sum(pmflexerabestabattinit_endc2to99) as pmflexerabestabattinit_endc2to99,
            sum(pmflexerabestabattadded_endc2to99) as pmflexerabestabattadded_endc2to99
            
        --</editor-fold>
        FROM dnb.stats_v3."EUTRANCELLFDD" as dt

        LEFT JOIN (SELECT date_id,
        erbs,
        eutrancellfdd,
        sum(pmflexerabestabsuccinit)  as pmflexerabestabsuccinit_endc2to99,
        sum(pmflexerabestabsuccadded) as pmflexerabestabsuccadded_endc2to99,
        sum(pmflexerabestabattinit)   as pmflexerabestabattinit_endc2to99,
        sum(pmflexerabestabattadded)  as pmflexerabestabattadded_endc2to99
        FROM dnb.stats_v3."EUTRANCELLFDD_FLEX"
        WHERE flex_filtername ilike 'Plmn%endc2to99'
        group by date_id, erbs, eutrancellfdd) as dt2
        using (date_id, erbs, eutrancellfdd)

        LEFT JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."eutrancellfdd"
        INNER JOIN (SELECT site_id, on_board_date::date, time::date
        FROM dnb.rfdb.df_dpm,
        generate_series(on_board_date, now(), '1 day') as time) as obs
        on obs.time = dt."date_id" and left(dt."erbs", 9) like obs.site_id

        WHERE date_id>=${startDate} AND date_id<=${endDate}
        group by "Cluster_ID" )
        SELECT "Cluster_ID",
               ${startDate} as "start_date",
                ${endDate} as "end_date",
        100 * ((60 * (period_duration)) - ((pmcelldowntimeauto) + (pmcelldowntimeman))) ||
        (60 * (period_duration))::double precision                                                          AS "Cell Availability",
        100 * (pmrrcconnestabsucc || (pmrrcconnestabatt - pmrrcconnestabattreatt - pmrrcconnestabfailmmeovlmos -
        pmrrcconnestabfailmmeovlmod)) *
        (pms1sigconnestabsucc || (pms1sigconnestabatt - pms1sigconnestabfailmmeovlmos)) *
        (pmflexerabestabsuccinit_endc2to99 + pmflexerabestabsuccadded_endc2to99) ||
        (pmflexerabestabattinit_endc2to99 + pmflexerabestabattadded_endc2to99)                              AS "Call Setup Success Rate",

        100 * (pmflexerabestabsuccinit_endc2to99 + pmflexerabestabsuccadded_endc2to99) ||
        (pmflexerabestabattinit_endc2to99 + pmflexerabestabattadded_endc2to99)                              AS "E-RAB Setup Success Rate_non-GBR (%)",

        100 * (pmrrcconnestabsucc || (pmrrcconnestabatt - pmrrcconnestabattreatt - pmrrcconnestabfailmmeovlmos -
        pmrrcconnestabfailmmeovlmod))                                         AS "RRC Setup Success Rate (Service) (%)",
        100 * pmrrcconnestabsuccmos ||
        (pmrrcconnestabattmos - pmrrcconnestabattreattmos)                                                  AS "RRC Setup Success Rate (Signaling) (%)",
        100 * (pmerabestabsuccinit + pmerabestabsuccadded) ||
        (pmerabestabattinit + pmerabestabattadded - pmerabestabattaddedhoongoing)                           AS "E-RAB Setup Success Rate (%)",
        100 * (pmerabrelabnormalenbact + pmerabrelabnormalmmeact) ||
        (pmerabrelabnormalenb + pmerabrelnormalenb + pmerabrelmme)                                          AS "Erab Drop Call rate",
        100 * (pmuectxtfetchsuccx2hoin + pmuectxtfetchsuccintraenbhoin) ||
        (pmuectxtfetchattx2hoin + pmuectxtfetchattintraenbhoin)                                             AS "Handover In Success Rate",
        100 * ((pmmacharqulfailqpsk + pmmacharqulfail16qam + pmmacharqulfail64qam + pmmacharqulfail256qam) ||
        (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam + pmmacharqulsucc256qam +
        pmmacharqulfailqpsk + pmmacharqulfail16qam + pmmacharqulfail64qam + pmmacharqulfail256qam)) AS "UL BLER",
        (pmpdcpvoldldrb - pmpdcpvoldldrblasttti) || pmuethptimedl                                           AS "DL User Throughput",
        pmuethpvolul || pmuethptimeul                                                                       AS "UL User Throughput",
        pmpdcpvoldldrb || pmschedactivitycelldl                                                             AS "DL Cell Throughput",
        pmpdcpvoluldrb || pmschedactivitycellul                                                             AS "UL Cell Throughput",
        pmpdcpvoldldrb || (8 * 1024 * 1024)::double precision                                                      AS "DL Data Volume",
        pmpdcpvoluldrb || (8 * 1024 * 1024)::double precision                                                      AS "UL Data Volume",
        pmrrcconnmax                                                                                        AS "Max of RRC Connected User",
        pmactiveuedlmax                                                                                     AS "Max of Active User",
        100 * (pmpdcppktdiscdlpelr + pmpdcppktdiscdlpelruu + pmpdcppktdiscdlho) ||
        (pmpdcppktreceiveddl - pmpdcppktfwddl)                                                              AS "Packet Loss (DL)",
        100 * pmpdcppktlostul || (pmpdcppktlostul + pmpdcppktreceivedul)                                    AS "Packet Loss UL",
        pmpdcplattimedl || pmpdcplatpkttransdl                                                              AS "Latency (only Radio interface)",
        100 * (pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) ||
        (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
        pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
        pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
        pmmacharqdldtxqpsk)                                                                                AS "DL QPSK %",
        100 * (pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam) ||
        (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
        pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
        pmmacharqdldtx16qam +
        pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
        pmmacharqdldtxqpsk)                                                                          AS "DL 16QAM%",
        100 * (pmmacharqdlack64qam + pmmacharqdlnack64qam + pmmacharqdldtx64qam) ||
        (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
        pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
        pmmacharqdldtx16qam +
        pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
        pmmacharqdldtxqpsk)                                                                          AS "DL 64QAM%",
        100 * ((pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam) ||
        (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
        pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam +
        pmmacharqdldtx16qam +
        pmmacharqdlackqpsk + pmmacharqdlnackqpsk +
        pmmacharqdldtxqpsk))                                                                        AS "DL 256QAM%",
        100 * ((pmmacharqulsuccqpsk) ||
        (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
        pmmacharqulsucc256qam))                                                                     AS "UL QPSK %",
        100 * ((pmmacharqulsucc16qam) ||
        (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
        pmmacharqulsucc256qam))                                                                     AS "UL 16QAM%",
        100 * ((pmmacharqulsucc64qam) ||
        (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
        pmmacharqulsucc256qam))                                                                     AS "UL 64QAM%",
        100 * ((pmmacharqulsucc256qam) ||
        (pmmacharqulsuccqpsk + pmmacharqulsucc16qam + pmmacharqulsucc64qam +
        pmmacharqulsucc256qam))                                                                     AS "UL 256QAM%",
        row_number() over ()                                                                                as id
        FROM COUNTERS;
    `;
    return sendResults(request, response, results);
};


module.exports = {
    clusterStatsAggregatedNR,
    clusterStatsAggregatedLTE,
    networkDailyStatsLTE,
    regionDailyStatsLTE,
    clusterDailyStatsLTE,
    cellDailyStatsLTE,


    customCellListStatsNR,
    customCellListStatsNR2,
    customCellListStatsLTE,

    networkDailyStatsNR,
    regionDailyStatsNR,
    clusterDailyStatsNR,
    cellDailyStatsNR,

    cellsList,
    cellsListNR,
    cellsListLTE,

    networkDailyPlmnStatsNR,
    regionDailyPlmnStatsNR,
    clusterDailyPlmnStatsNR,
    cellDailyPlmnStatsNR,

    networkDailyPlmnStatsLTE,
    regionDailyPlmnStatsLTE,
    clusterDailyPlmnStatsLTE,
    cellDailyPlmnStatsLTE,


    sendResults

};