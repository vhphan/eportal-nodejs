const sql = require('./PgJsBackend');
const {arrayToCsv} = require("../../routes/utils");
const networkKpiList = {
    "NR": [
        "Cell Availability",
        "ENDC SR",
        "E-RAB Block Rate",
        "Erab Drop Call rate (sgNB)",
        "Intra-SgNB Pscell Change Success Rate",
        "Inter-SgNB PSCell Change Success Rate",
        "Resource Block Utilizing Rate (DL)",
        "Resource Block Utilizing Rate (UL)",
        "Average CQI",
        "UL BLER",
        "Avg PUSCH UL RSSI",
        "DL User Throughput",
        "UL User Throughput",
        "DL Cell Throughput",
        "UL Cell Throughput",
        "DL Data Volume",
        "UL Data Volume",
        "Max of RRC Connected User (ENDC)",
        "Max of Active User",
        "DL QPSK %",
        "DL 16QAM%",
        "DL 64QAM%",
        "DL 256QAM%",
        "UL QPSK %",
        "UL 16QAM%",
        "UL 64QAM%",
        "UL 256QAM%",
        "RRC Setup Success Rate (Signaling) (%)",
        "gNobeB CPU Load",
        "Packet Loss (DL)",
        "Packet Loss (UL)",
        "Latency (only Radio interface)"
    ],
    "LTE": [
        "Cell Availability",
        "Call Setup Success Rate",
        "RRC Setup Success Rate (Service) (%)",
        "RRC Setup Success Rate (Signaling) (%)",
        "E-RAB Setup Success Rate_non-GBR (%)",
        "E-RAB Setup Success Rate (%)",
        "Erab Drop Call rate",
        "Handover In Success Rate",
        "UL BLER",
        "DL User Throughput",
        "UL User Throughput",
        "DL Cell Throughput",
        "UL Cell Throughput",
        "DL Data Volume",
        "UL Data Volume",
        "Max of RRC Connected User",
        "Max of Active User",
        "Packet Loss (DL)",
        "Packet Loss UL",
        "Latency (only Radio interface)",
        "DL QPSK %",
        "DL 16QAM%",
        "DL 64QAM%",
        "DL 256QAM%",
        "UL QPSK %",
        "UL 16QAM%",
        "UL 64QAM%",
        "UL 256QAM%",
        "Resource Block Utilizing Rate (DL)",
        "Resource Block Utilizing Rate (UL)",
        "Average CQI",
        "Avg PUSCH UL RSSI",
        "Intrafreq HOSR",
        "VoLTE Redirection Success Rate",
        "Interfreq HOSR"
    ]
};

const resultsAsExcelFormat = (sqlQuery) => async (request, response) => {
    let {page, size} = request.query;

    page = page === undefined ? 1 : parseInt(page);
    size = size === undefined ? 1000 : parseInt(size);
    let totalRecords = -1;
    let totalPages = -1;
    if (page === 1) {
        totalRecords = await sql`
                SELECT COUNT(*) as k FROM ${sql(sqlQuery)}
                `
        if (parseInt(totalRecords[0]['k']) === 0) {
            response.status(404).send({
                success: false,
                error: 'No records found',
                data: [],
                totalPages: 0
            });
            return;
        }
        totalPages = Math.ceil(totalRecords[0]['k'] / size)
    }
    const results = await sql`
        ${sql(sqlQuery)}
    `;

    const {headers, values} = arrayToCsv(results);

    response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: values.join('\n'),
        page,
        size,
        total_pages: totalPages,
    });


}

const networkDailyStatsLTE = async (request, response) => {
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time,
                            'Network' as object,
                            ${sql(networkKpiList.LTE)}
                            FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Region" = 'All'
    
    `;
    response.status(200).json(results);
};

const regionDailyStatsLTE = async (request, response) => {
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time,
                            t1."Region" as object,
                            ${sql(networkKpiList.LTE)}
                            FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Region" <> 'All' 
                                and t1."MCMC_State" = 'All'
                                and t1."DISTRICT" = 'All'
                                and t1."Cluster_ID" = 'All' 
                            ORDER BY t1."Region", t1.date_id    
    `;
    response.status(200).json(results);
};

const clusterDailyStatsLTE = async (request, response) => {
    const clusterId = request.query.clusterId || request.params.clusterId || '%';
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time, 
                            t1."Cluster_ID" as object,
                            ${sql(networkKpiList.LTE)}
                            FROM dnb.stats_v3.eutrancellfdd_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Cluster_ID" <> 'All' 
                            AND t1."Cluster_ID" LIKE ${clusterId}
                            ORDER BY t1."Cluster_ID", t1.date_id;`;
    response.status(200).json(results);
}

const cellDailyStatsLTE = async (request, response) => {
    const cellId = request.query.cellId || request.params.cellId;
    if (!cellId) {
        response.status(400).json({
            success: false,
            message: "cellId is required"
        });
        return;
    }
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time, 
                            t1.eutrancellfdd as object,
                            ${sql(networkKpiList.LTE)}
                            FROM dnb.stats_v3.eutrancellfdd_std_kpi_view_cell as t1
                                LEFT JOIN dnb.stats_v3.eutrancellfdd_v_std_kpi_view_cell as t2
                                USING (date_id, erbs, eutrancellfdd)
                                LEFT JOIN dnb.stats_v3.eutrancellfddflex_std_kpi_view_cell as t3
                                USING (date_id, erbs, eutrancellfdd)
                                LEFT JOIN dnb.stats_v3.eutrancellrelation_std_kpi_view_cell as t4
                                USING (date_id, erbs, eutrancellfdd)
                            WHERE t1.eutrancellfdd LIKE ${cellId}
                            ORDER BY t1.date_id;
                            `;
    response.status(200).json(results);
}

const networkDailyStatsNR = async (request, response) => {
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time,
                            'Network' as object,
                            ${sql(networkKpiList.NR)}
                            FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Region" = 'All'
    `;
    response.status(200).json(results);
};

const regionDailyStatsNR = async (request, response) => {
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time,
                            t1."Region" as object,
                            ${sql(networkKpiList.NR)}
                            FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Region" <> 'All' 
                                and t1."MCMC_State" = 'All'
                                and t1."DISTRICT" = 'All'
                                and t1."Cluster_ID" = 'All' 
                            ORDER BY t1."Region", t1.date_id    
    `;
    response.status(200).json(results);
};

const clusterDailyStatsNR= async (request, response) => {
    const clusterId = request.query.clusterId || request.params.clusterId || '%';
    const results = await sql`
                            SELECT t1.date_id::varchar(10) as time, 
                            t1."Cluster_ID" as object,
                            ${sql(networkKpiList.LTE)}
                            FROM dnb.stats_v3.nrcellcu_std_kpi_view as t1
                                LEFT JOIN dnb.stats_v3.nrcelldu_std_kpi_view as t2
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.rpuserplanelink_v_std_kpi_view as t3
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                                LEFT JOIN dnb.stats_v3.mpprocessingresource_v_std_kpi_view as t4
                                USING (date_id, "Region", "MCMC_State", "DISTRICT", "Cluster_ID")
                            WHERE t1."Cluster_ID" <> 'All' 
                            AND t1."Cluster_ID" LIKE ${clusterId}
                            ORDER BY t1."Cluster_ID", t1.date_id;`;
    response.status(200).json(results);
}


module.exports = {
    networkDailyStatsLTE,
    regionDailyStatsLTE,
    clusterDailyStatsLTE,
    cellDailyStatsLTE,
    networkDailyStatsNR,
    regionDailyStatsNR,
    clusterDailyStatsNR,
}