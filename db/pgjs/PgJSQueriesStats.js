const sql = require('./PgJsBackend');
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
        "Latency (only Radio interface)",
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
        "Packet Loss (UL)"
    ],
    "LTE": [
        "RRC Setup Success Rate (Service) (%)",
        "RRC Setup Success Rate (Signaling) (%)",
        "Erab Drop Call rate",
        "E-RAB Setup Success Rate (%)",
        "Handover In Success Rate",
        "UL BLER",
        "UL User Throughput",
        "DL Cell Throughput",
        "UL Cell Throughput",
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
        "DL User Throughput",
        "DL Data Volume",
        "UL Data Volume",
        "Cell Availability",
        "E-RAB Setup Success Rate_non-GBR (%)",
        "VoLTE Redirection Success Rate",
        "Intrafreq HOSR",
        "Interfreq HOSR",
        "Call Setup Success Rate",
        "Resource Block Utilizing Rate (DL)",
        "Resource Block Utilizing Rate (UL)",
        "Average CQI",
        "Avg PUSCH UL RSSI"
    ]
};

const testQuery = async(request, response) => {
    const results = await sql`SELECT * FROM dnb.stats."PacketLoss" LIMIT 5`;
    response.status(200).json(results);
}

const clusterDailyStatsNR = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["NR"];
    const results = await sql`
                    SELECT t1."DATE_ID", 
                            t1."Cluster_ID",
                            ${ sql(columns) }
                        FROM dnb.stats_group."DataTableClusterKPI" as t1
                        LEFT JOIN dnb.stats_group."CPULoadClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" AND t1."DATE_ID"=t2."DATE_ID"
                        LEFT JOIN dnb.stats_group."PacketLossClusterKPI" as t3 on t1."Cluster_ID"=t3."Cluster_ID" AND t1."DATE_ID"=t3."DATE_ID"
                        WHERE t1."Cluster_ID"=${clusterId}
                        `;
   response.status(200).json(results);
}

const clusterDailyStatsLTE = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["LTE"];
    const results = await sql`
                    SELECT t1."DATE_ID",
                    t1."Cluster_ID",
                    ${ sql(columns) }
                        FROM dnb.stats_group."NRCELLFDDClusterKPI" as t1
                        LEFT JOIN dnb.stats_group."PRBCQIRSSITAClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" AND t1."DATE_ID"=t2."DATE_ID"
                        WHERE t1."Cluster_ID"=${clusterId}
                        `;
   response.status(200).json(results);
}

module.exports = {
    clusterDailyStatsNR,
    clusterDailyStatsLTE,
    testQuery
}