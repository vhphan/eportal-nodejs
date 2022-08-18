const sql = require('./PgJsBackend');
const {arrayToCsv} = require("../../routes/utils");
const {logger} = require("../../middleware/logger");
const {networkKpiList, plmnKpiList} = require("../constants");
const {sendResults} = require("./DailyQueriesStatsV3");


const networkHourlyStatsNR = async (req, res) => {
    const result = await sql`
    SELECT t1.date_id::varchar(19) as time,
                'Network' as object, ${sql(networkKpiList.NR)}
        FROM dnb.stats_v3_hourly.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
        WHERE t1."Region" = 'All'
        ORDER BY time;`;

    return sendResults(req, res, result);
}

const regionHourlyStatsNR = async (req, res) => {
    const result = await sql`
    SELECT t1.date_id::varchar(19) as time,
    t1."Region" as object, ${sql(networkKpiList.NR)}
    FROM dnb.stats_v3_hourly.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
    WHERE t1."Region" <> 'All'
          and t1."MCMC_State" = 'All'
          and t1."DISTRICT" = 'All'
          and t1."Cluster_ID" = 'All'
        ORDER BY t1."Region", t1.date_id;`;
    return sendResults(req, res, result);
}

const clusterHourlyStatsNR = async (req, res) => {
    const clusterId = req.query.clusterId || req.params.clusterId || '%';
    const result = await sql`
    SELECT t1.date_id::varchar(19) as time,
    t1."Cluster_ID" as object, ${sql(networkKpiList.NR)}
    FROM dnb.stats_v3_hourly.nrcellcu_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.rpuserplanelink_v_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.mpprocessingresource_v_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.nrcelldu_v_std_kpi_view as t5
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
    WHERE t1."Cluster_ID" <> 'All'
          AND t1."Cluster_ID" LIKE ${clusterId}
        ORDER BY t1."Cluster_ID", t1.date_id;`;
    return sendResults(req, res, result);
}

const cellHourlyStatsNR = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId || request.query.object || request.params.object;
    if (!cellId) {
        return response.status(400).json({
            success: false,
            message: "cellId is required"
        });
    }
    const results = await sql`
        SELECT t1.date_id::varchar(19) as time, 
                            t1.nrcelldu as object, on_board_date, ${sql(networkKpiList.NR)}
        FROM dnb.stats_v3_hourly.tbl_cell_nrcelldu_std_kpi as t1
            LEFT JOIN dnb.stats_v3_hourly.tbl_cell_nrcellcu_std_kpi as t2
        on t1.date_id = t2.date_id
            and t1.nr_name = t2.nr_name
            and t1.nrcelldu = t2.nrcellcu
            LEFT JOIN dnb.stats_v3_hourly.tbl_cell_nrcelldu_v_std_kpi as t3
            on t1.date_id = t3.date_id
            and t1.nr_name = t3.nr_name
            and t1.nrcelldu = t3.nrcelldu
            LEFT JOIN dnb.stats_v3_hourly.tbl_cell_rpuserplanelink_v_std_kpi as t4
            on t1.date_id = t4.date_id
            and t1.nr_name = t4.ne_name
            LEFT JOIN dnb.stats_v3_hourly.tbl_cell_mpprocessingresource_v_std_kpi as t5
            on t1.date_id = t5.date_id
            and t1.nr_name = t5.erbs
            LEFT JOIN dnb.rfdb.df_dpm as obs on left (t1."nr_name", 9) like obs.site_id
        WHERE t1.nrcelldu LIKE ${cellId}
        ORDER BY t1.date_id;
    `;
    return sendResults(request, response, results, {parseDate: true, dateColumns: ['on_board_date']});
}

const customCellListHourlyStatsNR = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
        with counters as (SELECT dt1.date_id,
                                 -- <editor-fold desc="nrcellcu">
                                 sum(gnbcucpfunction)                      AS gnbcucpfunction,
                                 sum(pmendcpscellchangeattintersgnb)       AS pmendcpscellchangeattintersgnb,
                                 sum(pmendcpscellchangeattintrasgnb)       AS pmendcpscellchangeattintrasgnb,
                                 sum(pmendcpscellchangesuccintersgnb)      AS pmendcpscellchangesuccintersgnb,
                                 sum(pmendcpscellchangesuccintrasgnb)      AS pmendcpscellchangesuccintrasgnb,
                                 sum(pmendcrelueabnormalmenb)              AS pmendcrelueabnormalmenb,
                                 sum(pmendcrelueabnormalsgnb)              AS pmendcrelueabnormalsgnb,
                                 sum(pmendcrelueabnormalsgnbact)           AS pmendcrelueabnormalsgnbact,
                                 sum(pmendcreluenormal)                    AS pmendcreluenormal,
                                 sum(pmendcsetupueatt)                     AS pmendcsetupueatt,
                                 sum(pmendcsetupuesucc)                    AS pmendcsetupuesucc,
                                 sum(pmrrcconnestabattmos)                 AS pmrrcconnestabattmos,
                                 sum(pmrrcconnestabattreattmos)            AS pmrrcconnestabattreattmos,
                                 sum(pmrrcconnestabsuccmos)                AS pmrrcconnestabsuccmos,
                                 sum(pmrrcconnlevelmaxendc)                AS pmrrcconnlevelmaxendc,
                                 -- </editor-fold>

                                 -- <editor-fold desc="nrcelldu">
                                 sum(dt2.period_duration)                  AS period_duration,
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
                                 sum(dt2.pmcelldowntimeauto)               AS pmcelldowntimeauto,
                                 sum(dt2.pmcelldowntimeman)                AS pmcelldowntimeman,
                                 -- </editor-fold>

                                 -- <editor-fold desc="nrcelldu-v">
                                 sum(pmradiouerepcqi256qamrank1distr_0)    AS pmradiouerepcqi256qamrank1distr_0,
                                 sum(pmradiouerepcqi256qamrank1distr_1)    AS pmradiouerepcqi256qamrank1distr_1,
                                 sum(pmradiouerepcqi256qamrank1distr_2)    AS pmradiouerepcqi256qamrank1distr_2,
                                 sum(pmradiouerepcqi256qamrank1distr_3)    AS pmradiouerepcqi256qamrank1distr_3,
                                 sum(pmradiouerepcqi256qamrank1distr_4)    AS pmradiouerepcqi256qamrank1distr_4,
                                 sum(pmradiouerepcqi256qamrank1distr_5)    AS pmradiouerepcqi256qamrank1distr_5,
                                 sum(pmradiouerepcqi256qamrank1distr_6)    AS pmradiouerepcqi256qamrank1distr_6,
                                 sum(pmradiouerepcqi256qamrank1distr_7)    AS pmradiouerepcqi256qamrank1distr_7,
                                 sum(pmradiouerepcqi256qamrank1distr_8)    AS pmradiouerepcqi256qamrank1distr_8,
                                 sum(pmradiouerepcqi256qamrank1distr_9)    AS pmradiouerepcqi256qamrank1distr_9,
                                 sum(pmradiouerepcqi256qamrank1distr_10)   AS pmradiouerepcqi256qamrank1distr_10,
                                 sum(pmradiouerepcqi256qamrank1distr_11)   AS pmradiouerepcqi256qamrank1distr_11,
                                 sum(pmradiouerepcqi256qamrank1distr_12)   AS pmradiouerepcqi256qamrank1distr_12,
                                 sum(pmradiouerepcqi256qamrank1distr_13)   AS pmradiouerepcqi256qamrank1distr_13,
                                 sum(pmradiouerepcqi256qamrank1distr_14)   AS pmradiouerepcqi256qamrank1distr_14,
                                 sum(pmradiouerepcqi256qamrank1distr_15)   AS pmradiouerepcqi256qamrank1distr_15,
                                 sum(pmradiouerepcqi256qamrank2distr_0)    AS pmradiouerepcqi256qamrank2distr_0,
                                 sum(pmradiouerepcqi256qamrank2distr_1)    AS pmradiouerepcqi256qamrank2distr_1,
                                 sum(pmradiouerepcqi256qamrank2distr_2)    AS pmradiouerepcqi256qamrank2distr_2,
                                 sum(pmradiouerepcqi256qamrank2distr_3)    AS pmradiouerepcqi256qamrank2distr_3,
                                 sum(pmradiouerepcqi256qamrank2distr_4)    AS pmradiouerepcqi256qamrank2distr_4,
                                 sum(pmradiouerepcqi256qamrank2distr_5)    AS pmradiouerepcqi256qamrank2distr_5,
                                 sum(pmradiouerepcqi256qamrank2distr_6)    AS pmradiouerepcqi256qamrank2distr_6,
                                 sum(pmradiouerepcqi256qamrank2distr_7)    AS pmradiouerepcqi256qamrank2distr_7,
                                 sum(pmradiouerepcqi256qamrank2distr_8)    AS pmradiouerepcqi256qamrank2distr_8,
                                 sum(pmradiouerepcqi256qamrank2distr_9)    AS pmradiouerepcqi256qamrank2distr_9,
                                 sum(pmradiouerepcqi256qamrank2distr_10)   AS pmradiouerepcqi256qamrank2distr_10,
                                 sum(pmradiouerepcqi256qamrank2distr_11)   AS pmradiouerepcqi256qamrank2distr_11,
                                 sum(pmradiouerepcqi256qamrank2distr_12)   AS pmradiouerepcqi256qamrank2distr_12,
                                 sum(pmradiouerepcqi256qamrank2distr_13)   AS pmradiouerepcqi256qamrank2distr_13,
                                 sum(pmradiouerepcqi256qamrank2distr_14)   AS pmradiouerepcqi256qamrank2distr_14,
                                 sum(pmradiouerepcqi256qamrank2distr_15)   AS pmradiouerepcqi256qamrank2distr_15,
                                 sum(pmradiouerepcqi256qamrank3distr_0)    AS pmradiouerepcqi256qamrank3distr_0,
                                 sum(pmradiouerepcqi256qamrank3distr_1)    AS pmradiouerepcqi256qamrank3distr_1,
                                 sum(pmradiouerepcqi256qamrank3distr_2)    AS pmradiouerepcqi256qamrank3distr_2,
                                 sum(pmradiouerepcqi256qamrank3distr_3)    AS pmradiouerepcqi256qamrank3distr_3,
                                 sum(pmradiouerepcqi256qamrank3distr_4)    AS pmradiouerepcqi256qamrank3distr_4,
                                 sum(pmradiouerepcqi256qamrank3distr_5)    AS pmradiouerepcqi256qamrank3distr_5,
                                 sum(pmradiouerepcqi256qamrank3distr_6)    AS pmradiouerepcqi256qamrank3distr_6,
                                 sum(pmradiouerepcqi256qamrank3distr_7)    AS pmradiouerepcqi256qamrank3distr_7,
                                 sum(pmradiouerepcqi256qamrank3distr_8)    AS pmradiouerepcqi256qamrank3distr_8,
                                 sum(pmradiouerepcqi256qamrank3distr_9)    AS pmradiouerepcqi256qamrank3distr_9,
                                 sum(pmradiouerepcqi256qamrank3distr_10)   AS pmradiouerepcqi256qamrank3distr_10,
                                 sum(pmradiouerepcqi256qamrank3distr_11)   AS pmradiouerepcqi256qamrank3distr_11,
                                 sum(pmradiouerepcqi256qamrank3distr_12)   AS pmradiouerepcqi256qamrank3distr_12,
                                 sum(pmradiouerepcqi256qamrank3distr_13)   AS pmradiouerepcqi256qamrank3distr_13,
                                 sum(pmradiouerepcqi256qamrank3distr_14)   AS pmradiouerepcqi256qamrank3distr_14,
                                 sum(pmradiouerepcqi256qamrank3distr_15)   AS pmradiouerepcqi256qamrank3distr_15,
                                 sum(pmradiouerepcqi256qamrank4distr_0)    AS pmradiouerepcqi256qamrank4distr_0,
                                 sum(pmradiouerepcqi256qamrank4distr_1)    AS pmradiouerepcqi256qamrank4distr_1,
                                 sum(pmradiouerepcqi256qamrank4distr_2)    AS pmradiouerepcqi256qamrank4distr_2,
                                 sum(pmradiouerepcqi256qamrank4distr_3)    AS pmradiouerepcqi256qamrank4distr_3,
                                 sum(pmradiouerepcqi256qamrank4distr_4)    AS pmradiouerepcqi256qamrank4distr_4,
                                 sum(pmradiouerepcqi256qamrank4distr_5)    AS pmradiouerepcqi256qamrank4distr_5,
                                 sum(pmradiouerepcqi256qamrank4distr_6)    AS pmradiouerepcqi256qamrank4distr_6,
                                 sum(pmradiouerepcqi256qamrank4distr_7)    AS pmradiouerepcqi256qamrank4distr_7,
                                 sum(pmradiouerepcqi256qamrank4distr_8)    AS pmradiouerepcqi256qamrank4distr_8,
                                 sum(pmradiouerepcqi256qamrank4distr_9)    AS pmradiouerepcqi256qamrank4distr_9,
                                 sum(pmradiouerepcqi256qamrank4distr_10)   AS pmradiouerepcqi256qamrank4distr_10,
                                 sum(pmradiouerepcqi256qamrank4distr_11)   AS pmradiouerepcqi256qamrank4distr_11,
                                 sum(pmradiouerepcqi256qamrank4distr_12)   AS pmradiouerepcqi256qamrank4distr_12,
                                 sum(pmradiouerepcqi256qamrank4distr_13)   AS pmradiouerepcqi256qamrank4distr_13,
                                 sum(pmradiouerepcqi256qamrank4distr_14)   AS pmradiouerepcqi256qamrank4distr_14,
                                 sum(pmradiouerepcqi256qamrank4distr_15)   AS pmradiouerepcqi256qamrank4distr_15,
                                 sum(pmradiorecinterferencepwrdistr_0)     AS pmradiorecinterferencepwrdistr_0,
                                 sum(pmradiorecinterferencepwrdistr_1)     AS pmradiorecinterferencepwrdistr_1,
                                 sum(pmradiorecinterferencepwrdistr_2)     AS pmradiorecinterferencepwrdistr_2,
                                 sum(pmradiorecinterferencepwrdistr_3)     AS pmradiorecinterferencepwrdistr_3,
                                 sum(pmradiorecinterferencepwrdistr_4)     AS pmradiorecinterferencepwrdistr_4,
                                 sum(pmradiorecinterferencepwrdistr_5)     AS pmradiorecinterferencepwrdistr_5,
                                 sum(pmradiorecinterferencepwrdistr_6)     AS pmradiorecinterferencepwrdistr_6,
                                 sum(pmradiorecinterferencepwrdistr_7)     AS pmradiorecinterferencepwrdistr_7,
                                 sum(pmradiorecinterferencepwrdistr_8)     AS pmradiorecinterferencepwrdistr_8,
                                 sum(pmradiorecinterferencepwrdistr_9)     AS pmradiorecinterferencepwrdistr_9,
                                 sum(pmradiorecinterferencepwrdistr_10)    AS pmradiorecinterferencepwrdistr_10,
                                 sum(pmradiorecinterferencepwrdistr_11)    AS pmradiorecinterferencepwrdistr_11,
                                 sum(pmradiorecinterferencepwrdistr_12)    AS pmradiorecinterferencepwrdistr_12,
                                 sum(pmradiorecinterferencepwrdistr_13)    AS pmradiorecinterferencepwrdistr_13,
                                 sum(pmradiorecinterferencepwrdistr_14)    AS pmradiorecinterferencepwrdistr_14,
                                 sum(pmradiorecinterferencepwrdistr_15)    AS pmradiorecinterferencepwrdistr_15,

                                 sum(pmradioraatttadistr_0)                AS pmradioraatttadistr_0,
                                 sum(pmradioraatttadistr_1)                AS pmradioraatttadistr_1,
                                 sum(pmradioraatttadistr_2)                AS pmradioraatttadistr_2,
                                 sum(pmradioraatttadistr_3)                AS pmradioraatttadistr_3,
                                 sum(pmradioraatttadistr_4)                AS pmradioraatttadistr_4,
                                 sum(pmradioraatttadistr_5)                AS pmradioraatttadistr_5,
                                 sum(pmradioraatttadistr_6)                AS pmradioraatttadistr_6,
                                 sum(pmradioraatttadistr_7)                AS pmradioraatttadistr_7,
                                 sum(pmradioraatttadistr_8)                AS pmradioraatttadistr_8,
                                 sum(pmradioraatttadistr_9)                AS pmradioraatttadistr_9,
                                 sum(pmradioraatttadistr_10)               AS pmradioraatttadistr_10,
                                 sum(pmradioraatttadistr_11)               AS pmradioraatttadistr_11,
                                 sum(rssi_nr_dnom)                         AS rssi_nr_dnom,
                                 sum(rssi_nr_nom)                          AS rssi_nr_nom,
                                 sum(pmmaclattimedldrxsyncqos)             AS pmmaclattimedldrxsyncqos,
                                 sum(pmmaclattimedlnodrxsyncqos)           AS pmmaclattimedlnodrxsyncqos,
                                 sum(pmmaclattimedldrxsyncsampqos)         AS pmmaclattimedldrxsyncsampqos,
                                 sum(pmmaclattimedlnodrxsyncsampqos)       AS pmmaclattimedlnodrxsyncsampqos
                                 -- </editor-fold>

                          FROM stats_v3_hourly."NRCELLCU" as dt1
                                   LEFT JOIN stats_v3_hourly."NRCELLDU" as dt2 on dt1.date_id = dt2.date_id and
                                                                                  dt1.nrcellcu = dt2.nrcelldu
                                   LEFT JOIN stats_v3_hourly.tbl_agg_nrcelldu_v_vectors as dt3
                                             on dt1.date_id = dt3.date_id and
                                                dt1.nrcellcu = dt3.nrcelldu
                                   LEFT JOIN stats_v3_hourly.tbl_agg_nrcelldu_v_vectors2 as dt4
                                             on dt1.date_id = dt4.date_id and
                                                dt1.nrcellcu = dt4.nrcelldu
                          WHERE nrcellcu IN ${sql(cells)}
                          GROUP BY dt1.date_id)
        SELECT "date_id"::varchar(19) as    time,
                    -- <editor-fold desc="kpi nrcellcu">
                    100 * (pmendcsetupuesucc || pmendcsetupueatt)                             AS "ENDC SR",
                    100 * pmendcrelueabnormalsgnbact ||
                    (pmendcreluenormal + pmendcrelueabnormalmenb + pmendcrelueabnormalsgnb)   AS "Erab Drop Call rate (sgNB)",
                    100 * pmendcpscellchangesuccintrasgnb || pmendcpscellchangeattintrasgnb   AS "Intra-SgNB Pscell Change Success Rate",
                    100 * (pmendcpscellchangesuccintersgnb || pmendcpscellchangeattintersgnb) AS "Inter-SgNB PSCell Change Success Rate",
                    pmrrcconnlevelmaxendc                                                     AS "Max of RRC Connected User (ENDC)",
                    100 * pmrrcconnestabsuccmos ||
                    (pmrrcconnestabattmos - pmrrcconnestabattreattmos)                        AS "RRC Setup Success Rate (Signaling) (%)",
                    -- </editor-fold>
                    -- <editor-fold desc="kpi nrcelldu">
                    100 * ((60 * (period_duration)) - ((pmcelldowntimeauto + (pmcelldowntimeman))) ||
                      (60 * (period_duration)::double precision))                        AS "Cell Availability",
                    100 * (pmmacpdcchblockingpdschoccasions + pmmacpdcchblockingpuschoccasions) ||
                    (pmmacrbsymusedpdcchtypea + pmmacrbsymusedpdcchtypeb)                     AS "E-RAB Block Rate",
                    100 * (pmmacrbsymusedpdschtypea + pmmacrbsymusedpdschtypeabroadcasting + pmmacrbsymcsirs) ||
                    (pmmacrbsymavaildl)                                                       AS "Resource Block Utilizing Rate (DL)",
                    100 * (pmmacrbsymusedpuschtypea + pmmacrbsymusedpuschtypeb) ||
                    (pmmacrbsymavailul)                                                       AS "Resource Block Utilizing Rate (UL)",
                    100 * (pmmacharqulnackqpsk + pmmacharqdlnack16qam + pmmacharqdlnack64qam) ||
                    (pmmacharqulackqpsk + pmmacharqulack16qam + pmmacharqulack64qam + pmmacharqulnackqpsk + pmmacharqulnack16qam +
                    pmmacharqulnack64qam)                                                    AS "UL BLER",
                    64 * (pmmacvoldldrb || pmmactimedldrb) / 1000                             AS "DL User Throughput",
                    64 * (pmmacvolulresue || pmmactimeulresue) / 1000                         AS "UL User Throughput",
                    64 * (pmmacvoldl || pmpdschschedactivity) / 1000                          AS "DL Cell Throughput",
                    64 * pmmacvolul || pmpuschschedactivity / 1000                            AS "UL Cell Throughput",
                    pmmacvoldl / 1024 / 1024 / 1024                                           AS "DL Data Volume",
                    pmmacvolul / 1024 / 1024 / 1024                                           AS "UL Data Volume",
                    pmactiveuedlmax                                                           AS "Max of Active User",
                    (pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) ||
                    (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                    pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                    pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
                    100                                                                       AS "DL QPSK %",
                    (pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam) ||
                    (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                    pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                    pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
                    100                                                                       AS "DL 16QAM%",
                    (pmmacharqdlack64qam + pmmacharqdlnack64qam + pmmacharqdldtx64qam) ||
                    (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                    pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                    pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
                    100                                                                       AS "DL 64QAM%",
                    (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam) ||
                    (pmmacharqdlack256qam + pmmacharqdlnack256qam + pmmacharqdldtx256qam + pmmacharqdlack64qam +
                    pmmacharqdlnack64qam + pmmacharqdldtx64qam + pmmacharqdlack16qam + pmmacharqdlnack16qam + pmmacharqdldtx16qam +
                    pmmacharqdlackqpsk + pmmacharqdlnackqpsk + pmmacharqdldtxqpsk) *
                    100                                                                       AS "DL 256QAM%",
                    (pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) ||
                    (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                    pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                    pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
                    100                                                                       AS "UL QPSK %",
                    (pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam) ||
                    (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                    pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                    pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
                    100                                                                       AS "UL 16QAM%",
                    (pmmacharqulack64qam + pmmacharqulnack64qam + pmmacharquldtx64qam) ||
                    (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                    pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                    pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
                    100                                                                       AS "UL 64QAM%",
                    (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam) ||
                    (pmmacharqulack256qam + pmmacharqulnack256qam + pmmacharquldtx256qam + pmmacharqulack64qam +
                    pmmacharqulnack64qam + pmmacharquldtx64qam + pmmacharqulack16qam + pmmacharqulnack16qam + pmmacharquldtx16qam +
                    pmmacharqulackqpsk + pmmacharqulnackqpsk + pmmacharquldtxqpsk) *
                    100                                                                       AS "UL 256QAM%",
                    -- </editor-fold>
                    -- <editor-fold desc="nrcelldu-v">
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
                    pmradiouerepcqi256qamrank1distr_10 + pmradiouerepcqi256qamrank1distr_11 + pmradiouerepcqi256qamrank1distr_12 +
                    pmradiouerepcqi256qamrank1distr_13 + pmradiouerepcqi256qamrank1distr_14 + pmradiouerepcqi256qamrank1distr_15 +
                    pmradiouerepcqi256qamrank2distr_1 + pmradiouerepcqi256qamrank2distr_2 + pmradiouerepcqi256qamrank2distr_3 +
                    pmradiouerepcqi256qamrank2distr_4 + pmradiouerepcqi256qamrank2distr_5 + pmradiouerepcqi256qamrank2distr_6 +
                    pmradiouerepcqi256qamrank2distr_7 + pmradiouerepcqi256qamrank2distr_8 + pmradiouerepcqi256qamrank2distr_9 +
                    pmradiouerepcqi256qamrank2distr_10 + pmradiouerepcqi256qamrank2distr_11 + pmradiouerepcqi256qamrank2distr_12 +
                    pmradiouerepcqi256qamrank2distr_13 + pmradiouerepcqi256qamrank2distr_14 + pmradiouerepcqi256qamrank2distr_15 +
                    pmradiouerepcqi256qamrank3distr_1 + pmradiouerepcqi256qamrank3distr_2 + pmradiouerepcqi256qamrank3distr_3 +
                    pmradiouerepcqi256qamrank3distr_4 + pmradiouerepcqi256qamrank3distr_5 + pmradiouerepcqi256qamrank3distr_6 +
                    pmradiouerepcqi256qamrank3distr_7 + pmradiouerepcqi256qamrank3distr_8 + pmradiouerepcqi256qamrank3distr_9 +
                    pmradiouerepcqi256qamrank3distr_10 + pmradiouerepcqi256qamrank3distr_11 + pmradiouerepcqi256qamrank3distr_12 +
                    pmradiouerepcqi256qamrank3distr_13 + pmradiouerepcqi256qamrank3distr_14 + pmradiouerepcqi256qamrank3distr_15 +
                    pmradiouerepcqi256qamrank4distr_1 + pmradiouerepcqi256qamrank4distr_2 + pmradiouerepcqi256qamrank4distr_3 +
                    pmradiouerepcqi256qamrank4distr_4 + pmradiouerepcqi256qamrank4distr_5 + pmradiouerepcqi256qamrank4distr_6 +
                    pmradiouerepcqi256qamrank4distr_7 + pmradiouerepcqi256qamrank4distr_8 + pmradiouerepcqi256qamrank4distr_9 +
                    pmradiouerepcqi256qamrank4distr_10 + pmradiouerepcqi256qamrank4distr_11 + pmradiouerepcqi256qamrank4distr_12 +
                    pmradiouerepcqi256qamrank4distr_13 + pmradiouerepcqi256qamrank4distr_14 +
                    pmradiouerepcqi256qamrank4distr_15)                                      AS "Average CQI",
                    --     NR_Avg_TA_Meters = WeightedAverage(msrbs_NRCellDU.pmRadioRaAttTaDistr, [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110] )*5000*0.001
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
                    pmradioraatttadistr_11)                                                  as "NR_Avg_TA_Meters",
                    rssi_nr_nom || rssi_nr_dnom                                               as "Avg PUSCH UL RSSI",
                    (pmmaclattimedldrxsyncqos + pmmaclattimedlnodrxsyncqos) ||
                    (8 * pmmaclattimedldrxsyncsampqos + pmmaclattimedlnodrxsyncsampqos)       AS "Latency (only Radio interface)"
                    -- </editor-fold>
        FROM counters
        ORDER BY time
        ;
    `;
    return response.status(200).json(results);
}

const customCellListHourlyStatsNR2 = async (request, response) => {
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
                           FROM stats_v3_hourly."RPUSERPLANELINK-V" as dt1
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
                           FROM stats_v3_hourly."tbl_agg_mpprocessingresource_v_vectors" as dt
                           WHERE erbs IN (SELECT DISTINCT "Sitename"
                                          FROM rfdb.cell_mapping
                                          WHERE "Cellname" IN ${sql(cells)})
                           group by date_id, erbs)
        SELECT counters1.date_id::varchar(19) as time,
                        100 * (pmpdcppkttransdldiscqos - pmpdcppkttransdldiscaqmqos) / pmpdcppkttransdlqos AS "Packet Loss (DL)",
                        100 * (pmpdcppktlossulqos - pmpdcppktreculoooqos) /
                        (pmpdcppktreculqos + pmpdcppktlossultoqos - pmpdcppktlossultodiscqos -
                        pmpdcppktreculoooqos)                                                             AS "Packet Loss (UL)",
                        nom_cpu || denom_cpu                                                               as "gNobeB CPU Load"
        FROM counters1
            LEFT JOIN counters2 using (date_id)
        ORDER BY time
        ;
    `;
    return response.status(200).json(results);
}

const networkHourlyStatsLTE = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(19) as time,
                            'Network' as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3_hourly.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
        WHERE t1."Region" = 'All'
        ORDER BY time
    `;
    return sendResults(request, response, results);
};


const regionHourlyStatsLTE = async (request, response) => {
    const results = await sql`
        SELECT t1.date_id::varchar(19) as time,
                            "Region" as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3_hourly.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
        WHERE t1."Region" <> 'All'
          and t1."MCMC_State" = 'All'
          and t1."DISTRICT" = 'All'
          and t1."Cluster_ID" = 'All'
        ORDER BY t1."Region", t1.date_id;
        ORDER BY time
    `;
    return sendResults(request, response, results);
}

const clusterHourlyStatsLTE = async (req, res) => {
    const clusterId = req.query.clusterId || req.params.clusterId || '%';
    const result = await sql`
    SELECT t1.date_id::varchar(19) as time,
                            "Cluster_ID" as object, ${sql(networkKpiList.LTE)}
        FROM dnb.stats_v3_hourly.eutrancellfdd_std_kpi_view as t1
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfdd_v_std_kpi_view as t2
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellfddflex_std_kpi_view as t3
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
            LEFT JOIN dnb.stats_v3_hourly.eutrancellrelation_std_kpi_view as t4
            USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
    WHERE t1."Cluster_ID" <> 'All'
          AND t1."Cluster_ID" LIKE ${clusterId}
        ORDER BY t1."Cluster_ID", t1.date_id;`;
    return sendResults(req, res, result);
}

const cellHourlyStatsLTE = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId || request.query.object || request.params.object;
    if (!cellId) {
        return response.status(400).json({
            success: false,
            message: "cellId is required"
        });
    }
    const results = await sql`
    SELECT t1.date_id::varchar(19), ${sql(networkKpiList.LTE)}
    FROM stats_v3_hourly.tbl_cell_eutrancellfdd_std_kpi as t1
             LEFT JOIN stats_v3_hourly.tbl_cell_eutrancellfdd_v_std_kpi as t2
        USING (date_id, eutrancellfdd)
             LEFT JOIN stats_v3_hourly.tbl_cell_eutrancellfddflex_std_kpi as t3
        USING (date_id, eutrancellfdd)
             LEFT JOIN dnb.stats_v3_hourly.tbl_cell_eutrancellrelation_std_kpi as t4
        USING (date_id, eutrancellfdd)
    WHERE t1.eutrancellfdd='DBSEP1833_L7_0010'
    order by date_id;
    `;
    return sendResults(request, response, results);
}


module.exports = {
    networkHourlyStatsNR,
    regionHourlyStatsNR,
    clusterHourlyStatsNR,
    cellHourlyStatsNR,
    customCellListHourlyStatsNR,
    customCellListHourlyStatsNR2,

    networkHourlyStatsLTE,
    regionHourlyStatsLTE,
    clusterHourlyStatsLTE,
    cellHourlyStatsLTE,
};