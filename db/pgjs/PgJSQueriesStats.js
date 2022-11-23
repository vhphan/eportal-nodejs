const sql = require('./PgJsBackend');
const {getCookies} = require("../utils");
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

const testQuery = async (request, response) => {
    const results = await sql`SELECT * FROM dnb.stats."PacketLoss" LIMIT 5`;
    response.status(200).json(results);
}

const clusterDailyStatsNR = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["NR"];
    const results = await sql`
                    SELECT t1."DATE_ID"::timestamp without time zone as time, 
                            t1."Cluster_ID" as object,
                            ${sql(columns)}
                        FROM dnb.stats_group."DataTableClusterKPI" as t1
                        LEFT JOIN dnb.stats_group."CPULoadClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" AND t1."DATE_ID"=t2."DATE_ID"
                        LEFT JOIN dnb.stats_group."PacketLossClusterKPI" as t3 on t1."Cluster_ID"=t3."Cluster_ID" AND t1."DATE_ID"=t3."DATE_ID"
                        WHERE t1."Cluster_ID"=${clusterId}
                        ORDER BY t1."DATE_ID"
                        `;
    response.status(200).json(results);
}

const clusterDailyStatsLTE = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["LTE"];
    const results = await sql`
                    SELECT t1."DATE_ID"::timestamp without time zone as time,
                    t1."Cluster_ID" as object,
                    ${sql(columns)}
                        FROM dnb.stats_group."NRCELLFDDClusterKPI" as t1
                        LEFT JOIN dnb.stats_group."PRBCQIRSSITAClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" AND t1."DATE_ID"=t2."DATE_ID"
                        WHERE t1."Cluster_ID"=${clusterId}
                        ORDER BY t1."DATE_ID"
                        `;
    response.status(200).json(results);
}

const customCellListStatsNR = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
        WITH COUNTERS AS
                 (SELECT "DATE_ID"                                            as "time",
                         -- <editor-fold desc="Counters">
                         SUM("pmMacPdcchBlockingPdschOccasions")              as "pmMacPdcchBlockingPdschOccasions",
                         SUM("pmMacPdcchBlockingPuschOccasions")              as "pmMacPdcchBlockingPuschOccasions",
                         SUM("pmMacRBSymUsedPdcchTypeA")                      as "pmMacRBSymUsedPdcchTypeA",
                         SUM("pmMacRBSymUsedPdcchTypeB")                      as "pmMacRBSymUsedPdcchTypeB",
                         SUM("pmMacRBSymUsedPdschTypeABroadcasting")          as "pmMacRBSymUsedPdschTypeABroadcasting",
                         SUM("pmMacRBSymCsiRs")                               as "pmMacRBSymCsiRs",
                         SUM("pmMacRBSymAvailDl")                             as "pmMacRBSymAvailDl",
                         SUM("pmMacRBSymUsedPuschTypeA")                      as "pmMacRBSymUsedPuschTypeA",
                         SUM("pmMacRBSymUsedPuschTypeB")                      as "pmMacRBSymUsedPuschTypeB",
                         SUM("pmMacRBSymAvailUl")                             as "pmMacRBSymAvailUl",
                         SUM("pmMacVolDlDrb")                                 as "pmMacVolDlDrb",
                         SUM("pmMacTimeDlDrb")                                as "pmMacTimeDlDrb",
                         SUM("pmMacVolUlResUe")                               as "pmMacVolUlResUe",
                         SUM("pmMacTimeUlResUe")                              as "pmMacTimeUlResUe",
                         SUM("pmMacVolDl")                                    as "pmMacVolDl",
                         SUM("pmPdschSchedActivity")                          as "pmPdschSchedActivity",
                         SUM("pmMacVolUl")                                    as "pmMacVolUl",
                         SUM("pmPuschSchedActivity")                          as "pmPuschSchedActivity",
                         SUM("pmActiveUeDlMax")                               as "pmActiveUeDlMax",
                         SUM("pmMacHarqDlAck16Qam")                           as "pmMacHarqDlAck16Qam",
                         SUM("pmMacHarqDlAck16QamInit")                       as "pmMacHarqDlAck16QamInit",
                         SUM("pmMacHarqDlAck256Qam")                          as "pmMacHarqDlAck256Qam",
                         SUM("pmMacHarqDlAck256QamInit")                      as "pmMacHarqDlAck256QamInit",
                         SUM("pmMacHarqDlAck64Qam")                           as "pmMacHarqDlAck64Qam",
                         SUM("pmMacHarqDlAck64QamInit")                       as "pmMacHarqDlAck64QamInit",
                         SUM("pmMacHarqDlAckQpsk")                            as "pmMacHarqDlAckQpsk",
                         SUM("pmMacHarqDlAckQpskInit")                        as "pmMacHarqDlAckQpskInit",
                         SUM("pmMacHarqDlDtx16Qam")                           as "pmMacHarqDlDtx16Qam",
                         SUM("pmMacHarqDlDtx16QamInit")                       as "pmMacHarqDlDtx16QamInit",
                         SUM("pmMacHarqDlDtx256Qam")                          as "pmMacHarqDlDtx256Qam",
                         SUM("pmMacHarqDlDtx256QamInit")                      as "pmMacHarqDlDtx256QamInit",
                         SUM("pmMacHarqDlDtx64Qam")                           as "pmMacHarqDlDtx64Qam",
                         SUM("pmMacHarqDlDtx64QamInit")                       as "pmMacHarqDlDtx64QamInit",
                         SUM("pmMacHarqDlDtxQpsk")                            as "pmMacHarqDlDtxQpsk",
                         SUM("pmMacHarqDlDtxQpskInit")                        as "pmMacHarqDlDtxQpskInit",
                         SUM("pmMacHarqDlFail")                               as "pmMacHarqDlFail",
                         SUM("pmMacHarqDlNack16Qam")                          as "pmMacHarqDlNack16Qam",
                         SUM("pmMacHarqDlNack16QamInit")                      as "pmMacHarqDlNack16QamInit",
                         SUM("pmMacHarqDlNack256Qam")                         as "pmMacHarqDlNack256Qam",
                         SUM("pmMacHarqDlNack256QamInit")                     as "pmMacHarqDlNack256QamInit",
                         SUM("pmMacHarqDlNack64Qam")                          as "pmMacHarqDlNack64Qam",
                         SUM("pmMacHarqDlNack64QamInit")                      as "pmMacHarqDlNack64QamInit",
                         SUM("pmMacHarqDlNackQpsk")                           as "pmMacHarqDlNackQpsk",
                         SUM("pmMacHarqDlNackQpskInit")                       as "pmMacHarqDlNackQpskInit",
                         SUM("pmMacHarqUlAck16Qam")                           as "pmMacHarqUlAck16Qam",
                         SUM("pmMacHarqUlAck16QamInit")                       as "pmMacHarqUlAck16QamInit",
                         SUM("pmMacHarqUlAck256Qam")                          as "pmMacHarqUlAck256Qam",
                         SUM("pmMacHarqUlAck256QamInit")                      as "pmMacHarqUlAck256QamInit",
                         SUM("pmMacHarqUlAck64Qam")                           as "pmMacHarqUlAck64Qam",
                         SUM("pmMacHarqUlAck64QamInit")                       as "pmMacHarqUlAck64QamInit",
                         SUM("pmMacHarqUlAckQpsk")                            as "pmMacHarqUlAckQpsk",
                         SUM("pmMacHarqUlAckQpskInit")                        as "pmMacHarqUlAckQpskInit",
                         SUM("pmMacHarqUlDtx16Qam")                           as "pmMacHarqUlDtx16Qam",
                         SUM("pmMacHarqUlDtx16QamInit")                       as "pmMacHarqUlDtx16QamInit",
                         SUM("pmMacHarqUlDtx256Qam")                          as "pmMacHarqUlDtx256Qam",
                         SUM("pmMacHarqUlDtx256QamInit")                      as "pmMacHarqUlDtx256QamInit",
                         SUM("pmMacHarqUlDtx64Qam")                           as "pmMacHarqUlDtx64Qam",
                         SUM("pmMacHarqUlDtx64QamInit")                       as "pmMacHarqUlDtx64QamInit",
                         SUM("pmMacHarqUlDtxQpsk")                            as "pmMacHarqUlDtxQpsk",
                         SUM("pmMacHarqUlDtxQpskInit")                        as "pmMacHarqUlDtxQpskInit",
                         SUM("pmMacHarqUlFail")                               as "pmMacHarqUlFail",
                         SUM("pmMacHarqUlNack16Qam")                          as "pmMacHarqUlNack16Qam",
                         SUM("pmMacHarqUlNack16QamInit")                      as "pmMacHarqUlNack16QamInit",
                         SUM("pmMacHarqUlNack256Qam")                         as "pmMacHarqUlNack256Qam",
                         SUM("pmMacHarqUlNack256QamInit")                     as "pmMacHarqUlNack256QamInit",
                         SUM("pmMacHarqUlNack64Qam")                          as "pmMacHarqUlNack64Qam",
                         SUM("pmMacHarqUlNack64QamInit")                      as "pmMacHarqUlNack64QamInit",
                         SUM("pmMacHarqUlNackQpsk")                           as "pmMacHarqUlNackQpsk",
                         SUM("pmMacHarqUlNackQpskInit")                       as "pmMacHarqUlNackQpskInit",
                         SUM("Sum(pmCellDowntimeAuto) for")                   as "Sum(pmCellDowntimeAuto) for",
                         SUM("Sum(pmCellDowntimeMan) for")                    as "Sum(pmCellDowntimeMan) for",
                         SUM("Sum(PERIOD_DURATION) for")                      as "Sum(PERIOD_DURATION) for",
                         SUM("Sum(pmMacRBSymUsedPdcchTypeA) for")             as "Sum(pmMacRBSymUsedPdcchTypeA) for",
                         SUM("Sum(pmMacRBSymUsedPdschTypeABroadcasting) for") as "Sum(pmMacRBSymUsedPdschTypeABroadcasting) for",
                         SUM("Sum(pmMacRBSymCsiRs) for")                      as "Sum(pmMacRBSymCsiRs) for",
                         SUM("Sum(pmMacRBSymAvailDl) for")                    as "Sum(pmMacRBSymAvailDl) for",
                         SUM("Sum(pmMacRBSymUsedPuschTypeA) for")             as "Sum(pmMacRBSymUsedPuschTypeA) for",
                         SUM("Sum(pmMacRBSymUsedPuschTypeB) for")             as "Sum(pmMacRBSymUsedPuschTypeB) for",
                         SUM("Sum(pmMacRBSymAvailUl) for")                    as "Sum(pmMacRBSymAvailUl) for",
                         SUM("Sum(pmMacHarqUlNack16Qam) for")                 as "Sum(pmMacHarqUlNack16Qam) for",
                         SUM("Sum(pmMacHarqUlNack64Qam) for")                 as "Sum(pmMacHarqUlNack64Qam) for",
                         SUM("Sum(pmMacHarqUlNackQpsk) for")                  as "Sum(pmMacHarqUlNackQpsk) for",
                         SUM("Sum(pmMacHarqUlAckQpsk) for")                   as "Sum(pmMacHarqUlAckQpsk) for",
                         SUM("Sum(pmMacHarqUlAck64Qam) for")                  as "Sum(pmMacHarqUlAck64Qam) for",
                         SUM("Sum(pmMacHarqUlAck16Qam) for")                  as "Sum(pmMacHarqUlAck16Qam) for",
                         SUM("Sum(pmRadioPuschTable1McsDistr) for")           as "Sum(pmRadioPuschTable1McsDistr) for",
                         SUM("Sum(pmRadioPuschTable2McsDistr) for")           as "Sum(pmRadioPuschTable2McsDistr) for",
                         SUM("pmEndcSetupUeAtt")                              as "pmEndcSetupUeAtt",
                         SUM("pmEndcSetupUeSucc")                             as "pmEndcSetupUeSucc",
                         SUM("pmRrcConnEstabSucc")                            as "pmRrcConnEstabSucc",
                         SUM("pmRrcConnEstabAtt")                             as "pmRrcConnEstabAtt",
                         SUM("pmRrcConnEstabAttReatt")                        as "pmRrcConnEstabAttReatt",
                         SUM("pmRrcConnEstabSuccMos")                         as "pmRrcConnEstabSuccMos",
                         SUM("pmRrcConnEstabAttMos")                          as "pmRrcConnEstabAttMos",
                         SUM("pmRrcConnEstabAttReattMos")                     as "pmRrcConnEstabAttReattMos",
                         SUM("pmEndcRelUeAbnormalSgnbAct")                    as "pmEndcRelUeAbnormalSgnbAct",
                         SUM("pmEndcRelUeNormal")                             as "pmEndcRelUeNormal",
                         SUM("pmEndcRelUeAbnormalMenb")                       as "pmEndcRelUeAbnormalMenb",
                         SUM("pmEndcRelUeAbnormalSgnb")                       as "pmEndcRelUeAbnormalSgnb",
                         SUM("pmEndcPSCellChangeSuccIntraSgnb")               as "pmEndcPSCellChangeSuccIntraSgnb",
                         SUM("pmEndcPSCellChangeAttIntraSgnb")                as "pmEndcPSCellChangeAttIntraSgnb",
                         SUM("pmEndcPSCellChangeAttInterSgnb")                as "pmEndcPSCellChangeAttInterSgnb",
                         SUM("pmEndcPSCellChangeSuccInterSgnb")               as "pmEndcPSCellChangeSuccInterSgnb",
                         SUM("pmRrcConnLevelMaxEnDc")                         as "pmRrcConnLevelMaxEnDc",
                         SUM("Sum(pmMacBsrLcg0Distr) for")                    as "Sum(pmMacBsrLcg0Distr) for",
                         SUM("Sum(pmMacBsrLcg1Distr) for")                    as "Sum(pmMacBsrLcg1Distr) for",
                         SUM("Sum(pmMacBsrLcg6Distr) for")                    as "Sum(pmMacBsrLcg6Distr) for",
                         SUM("Sum(pmMacBsrLcg7Distr) for")                    as "Sum(pmMacBsrLcg7Distr) for",
                         SUM("Sum(pmRadioRaAttTaDistr) for 0")                as "Sum(pmRadioRaAttTaDistr) for 0",
                         SUM("Sum(pmRadioRaAttTaDistr) for 1")                as "Sum(pmRadioRaAttTaDistr) for 1",
                         SUM("Sum(pmRadioRaAttTaDistr) for 10")               as "Sum(pmRadioRaAttTaDistr) for 10",
                         SUM("Sum(pmRadioRaAttTaDistr) for 11")               as "Sum(pmRadioRaAttTaDistr) for 11",
                         SUM("Sum(pmRadioRaAttTaDistr) for 2")                as "Sum(pmRadioRaAttTaDistr) for 2",
                         SUM("Sum(pmRadioRaAttTaDistr) for 3")                as "Sum(pmRadioRaAttTaDistr) for 3",
                         SUM("Sum(pmRadioRaAttTaDistr) for 4")                as "Sum(pmRadioRaAttTaDistr) for 4",
                         SUM("Sum(pmRadioRaAttTaDistr) for 5")                as "Sum(pmRadioRaAttTaDistr) for 5",
                         SUM("Sum(pmRadioRaAttTaDistr) for 6")                as "Sum(pmRadioRaAttTaDistr) for 6",
                         SUM("Sum(pmRadioRaAttTaDistr) for 7")                as "Sum(pmRadioRaAttTaDistr) for 7",
                         SUM("Sum(pmRadioRaAttTaDistr) for 8")                as "Sum(pmRadioRaAttTaDistr) for 8",
                         SUM("Sum(pmRadioRaAttTaDistr) for 9")                as "Sum(pmRadioRaAttTaDistr) for 9",
                         SUM("Sum(pmMacRBSymUsedPdschTypeA) for")             as "Sum(pmMacRBSymUsedPdschTypeA) for"
                         -- </editor-fold>
                  FROM dnb.stats."DataTable"
                  WHERE "NRCellDU" IN ${sql(cells)}
                  group by "DATE_ID")
        SELECT "time"::varchar(10)                                                                      as "time",
               -- <editor-fold desc="KPIs">
               100 *
               ((60 * "Sum(PERIOD_DURATION) for") - ("Sum(pmCellDowntimeAuto) for" + "Sum(pmCellDowntimeMan) for")) /
               nullif(60 * "Sum(PERIOD_DURATION) for", 0)                                               as "Cell Availability",
               100 * ("pmEndcSetupUeSucc" / nullif("pmEndcSetupUeAtt", 0))                              as "ENDC SR",
               100 * ("pmMacPdcchBlockingPdschOccasions" + "pmMacPdcchBlockingPuschOccasions") /
               nullif("pmMacRBSymUsedPdcchTypeA" + "pmMacRBSymUsedPdcchTypeB", 0)                       as "E-RAB Block Rate",
               100 * "pmEndcRelUeAbnormalSgnbAct" /
               nullif("pmEndcRelUeNormal" + "pmEndcRelUeAbnormalMenb" + "pmEndcRelUeAbnormalSgnb",
                      0)                                                                                as "Erab Drop Call rate (sgNB)",
               100 * "pmEndcPSCellChangeSuccIntraSgnb" /
               nullif("pmEndcPSCellChangeAttIntraSgnb", 0)                                              as "Intra-SgNB Pscell Change Success Rate",
               100 * ("pmEndcPSCellChangeSuccInterSgnb" /
                      nullif("pmEndcPSCellChangeAttInterSgnb", 0))                                      as "Inter-SgNB PSCell Change Success Rate",
               100 * ("Sum(pmMacRBSymUsedPdschTypeA) for" +
                      "Sum(pmMacRBSymUsedPdschTypeABroadcasting) for" +
                      "Sum(pmMacRBSymCsiRs) for") /
               nullif("Sum(pmMacRBSymAvailDl) for", 0)                                                  as "Resource Block Utilizing Rate (DL)",

               100 * ("Sum(pmMacRBSymUsedPuschTypeA) for" + "Sum(pmMacRBSymUsedPuschTypeB) for") /
               nullif("Sum(pmMacRBSymAvailUl) for", 0)                                                  as "Resource Block Utilizing Rate (UL)",

               "Sum(pmMacBsrLcg0Distr) for" / nullif("Sum(pmMacBsrLcg1Distr) for", 0)                   as "Average CQI",
               100 * ("Sum(pmMacHarqUlNackQpsk) for" + "pmMacHarqDlNack16Qam" + "pmMacHarqDlNack64Qam") /
               nullif("Sum(pmMacHarqUlAckQpsk) for" + "Sum(pmMacHarqUlAck16Qam) for" + "Sum(pmMacHarqUlAck64Qam) for" +
                      "Sum(pmMacHarqUlNackQpsk) for" + "Sum(pmMacHarqUlNack16Qam) for" +
                      "Sum(pmMacHarqUlNack64Qam) for",
                      0)                                                                                as "UL BLER",
               "Sum(pmMacBsrLcg6Distr) for" / nullif("Sum(pmMacBsrLcg7Distr) for", 0)                   as "Avg PUSCH UL RSSI",
               64 * ("pmMacVolDlDrb" / nullif("pmMacTimeDlDrb", 0) / 1000)                              as "DL User Throughput",
               64 * ("pmMacVolUlResUe" / nullif("pmMacTimeUlResUe", 0) / 1000)                          as "UL User Throughput",
               64 * ("pmMacVolDl" / nullif("pmPdschSchedActivity", 0) / 1000)                           as "DL Cell Throughput",
               64 * "pmMacVolUl" / nullif("pmPuschSchedActivity", 0) / 1000                             as "UL Cell Throughput",
               "pmMacVolDl" / 1024 / 1024 / 1024                                                        as "DL Data Volume",
               "pmMacVolUl" / 1024 / 1024 / 1024                                                        as "UL Data Volume",

               "pmRrcConnLevelMaxEnDc"                                                                  as "Max of RRC Connected User (ENDC)",
               "pmActiveUeDlMax"                                                                        as "Max of Active User",

               100 * ("pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") / nullif(
                       ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" +
                        "pmMacHarqDlAck64Qam" +
                        "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" +
                        "pmMacHarqDlNack16Qam" +
                        "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk"),
                       0)                                                                               as "DL QPSK %",
               100 * ("pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" + "pmMacHarqDlDtx16Qam") / nullif(
                       ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" +
                        "pmMacHarqDlAck64Qam" +
                        "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" +
                        "pmMacHarqDlNack16Qam" +
                        "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk"),
                       0)                                                                               as "DL 16QAM%",
               100 * ("pmMacHarqDlAck64Qam" + "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam") / nullif(
                       ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" +
                        "pmMacHarqDlAck64Qam" +
                        "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" +
                        "pmMacHarqDlNack16Qam" +
                        "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk"),
                       0)                                                                               as "DL 64QAM%",
               100 * ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam") / nullif(
                       ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" +
                        "pmMacHarqDlAck64Qam" +
                        "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" +
                        "pmMacHarqDlNack16Qam" +
                        "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk"),
                       0)                                                                               as "DL 256QAM%",
               100 * ("pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") / nullif(
                       ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" +
                        "pmMacHarqUlAck64Qam" +
                        "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" +
                        "pmMacHarqUlNack16Qam" +
                        "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk"),
                       0)                                                                               as "UL QPSK %",
               100 * ("pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" + "pmMacHarqUlDtx16Qam") / nullif(
                       ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" +
                        "pmMacHarqUlAck64Qam" +
                        "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" +
                        "pmMacHarqUlNack16Qam" +
                        "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk"),
                       0)                                                                               as "UL 16QAM%",
               100 * ("pmMacHarqUlAck64Qam" + "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam") / nullif(
                       ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" +
                        "pmMacHarqUlAck64Qam" +
                        "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" +
                        "pmMacHarqUlNack16Qam" +
                        "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk"),
                       0)                                                                               as "UL 64QAM%",
               100 * ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam") / nullif(
                       ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" +
                        "pmMacHarqUlAck64Qam" +
                        "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" +
                        "pmMacHarqUlNack16Qam" +
                        "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk"),
                       0)                                                                               as "UL 256QAM%",
               100 * "pmRrcConnEstabSuccMos" /
               nullif("pmRrcConnEstabAttMos" - "pmRrcConnEstabAttReattMos", 0)                          as "RRC Setup Success Rate (Signaling) (%)",
               "Sum(pmRadioPuschTable1McsDistr) for" / nullif("Sum(pmRadioPuschTable2McsDistr) for", 0) as "Latency (only Radio interface)"
               -- </editor-fold>
        FROM COUNTERS ORDER BY "time";
    `;
    response.status(200).json(results);
}

const customCellListStatsNR2 = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
            SELECT cp."DATE_ID"::varchar(10) as "time", sum("pmDuIntensHoReqDistr") / nullif(sum("pmDuIntensHoRrcConnReqDistr"), 0) as "gNobeB CPU Load",
               100 * (sum("Sum(pmPdcpPktTransDlDiscQos) for") - sum("Sum(pmPdcpPktTransDlDiscAqmQos) for")) /
               sum("Sum(pmPdcpPktTransDlQos) for")                                               as "Packet Loss (DL)",
               100 * (sum("Sum(pmPdcpPktLossUlQos) for") - sum("Sum(pmPdcpPktRecUlOooQos) for")) /
               (sum("Sum(pmPdcpPktRecUlQos) for") + sum("Sum(pmPdcpPktLossUlToQos) for") -
                sum("Sum(pmPdcpPktLossUlToDiscQos) for") - sum("Sum(pmPdcpPktRecUlOooQos) for")) as "Packet Loss (UL)"
            from dnb.stats."CPULoad" as cp
            INNER JOIN
            dnb.stats."PacketLoss" as pl
            on cp."DATE_ID" = pl."DATE_ID"
            AND cp."ERBS" = pl."NE_NAME"
            WHERE "ERBS" IN (SELECT DISTINCT "Sitename"
                         FROM rfdb.cell_mapping
                         WHERE "Cellname" IN ${sql(cells)})
            group by cp."DATE_ID"
            order by cp."DATE_ID";
            ;
    `;
    response.status(200).json(results);
}

const customCellListStatsLTE = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
        WITH COUNTERS AS (SELECT "DATE_ID",
                                 -- <editor-fold desc="COUNTERS">
                                 SUM("pmRrcConnEstabSucc")                    AS "pmRrcConnEstabSucc",
                                 SUM("pmRrcConnEstabAtt")                     AS "pmRrcConnEstabAtt",
                                 SUM("pmRrcConnEstabAttReatt")                AS "pmRrcConnEstabAttReatt",
                                 SUM("pmRrcConnEstabFailMmeOvlMod")           AS "pmRrcConnEstabFailMmeOvlMod",
                                 SUM("pmRrcConnEstabFailMmeOvlMos")           AS "pmRrcConnEstabFailMmeOvlMos",
                                 SUM("pmS1SigConnEstabSucc")                  AS "pmS1SigConnEstabSucc",
                                 SUM("pmS1SigConnEstabAtt")                   AS "pmS1SigConnEstabAtt",
                                 SUM("pmS1SigConnEstabSuccMos")               AS "pmS1SigConnEstabSuccMos",
                                 SUM("pmRrcConnEstabSuccMos")                 AS "pmRrcConnEstabSuccMos",
                                 SUM("pmRrcConnEstabAttMos")                  AS "pmRrcConnEstabAttMos",
                                 SUM("pmRrcConnEstabAttReattMos")             AS "pmRrcConnEstabAttReattMos",
                                 SUM("pmErabEstabSuccInit")                   AS "pmErabEstabSuccInit",
                                 SUM("pmErabEstabSuccAdded")                  AS "pmErabEstabSuccAdded",
                                 SUM("pmErabEstabAttInit")                    AS "pmErabEstabAttInit",
                                 SUM("pmErabEstabAttAdded")                   AS "pmErabEstabAttAdded",
                                 SUM("pmErabEstabAttAddedHoOngoing")          AS "pmErabEstabAttAddedHoOngoing",
                                 SUM("pmErabRelAbnormalEnbAct")               AS "pmErabRelAbnormalEnbAct",
                                 SUM("pmErabRelAbnormalMmeAct")               AS "pmErabRelAbnormalMmeAct",
                                 SUM("pmErabRelAbnormalEnb")                  AS "pmErabRelAbnormalEnb",
                                 SUM("pmErabRelNormalEnb")                    AS "pmErabRelNormalEnb",
                                 SUM("pmErabRelNormalMme")                    AS "pmErabRelNormalMme",
                                 SUM("pmUeCtxtFetchSuccX2HoIn")               AS "pmUeCtxtFetchSuccX2HoIn",
                                 SUM("pmUeCtxtFetchSuccIntraEnbHoIn")         AS "pmUeCtxtFetchSuccIntraEnbHoIn",
                                 SUM("pmUeCtxtFetchAttX2HoIn")                AS "pmUeCtxtFetchAttX2HoIn",
                                 SUM("pmUeCtxtFetchAttIntraEnbHoIn")          AS "pmUeCtxtFetchAttIntraEnbHoIn",
                                 SUM("pmMacHarqDlAck")                        AS "pmMacHarqDlAck",
                                 SUM("pmMacHarqDlAck16qam")                   AS "pmMacHarqDlAck16qam",
                                 SUM("pmMacHarqDlAck256qam")                  AS "pmMacHarqDlAck256qam",
                                 SUM("pmMacHarqDlAck64qam")                   AS "pmMacHarqDlAck64qam",
                                 SUM("pmMacHarqDlAckComp")                    AS "pmMacHarqDlAckComp",
                                 SUM("pmMacHarqDlAckQpsk")                    AS "pmMacHarqDlAckQpsk",
                                 SUM("pmMacHarqDlDtx")                        AS "pmMacHarqDlDtx",
                                 SUM("pmMacHarqDlDtx16qam")                   AS "pmMacHarqDlDtx16qam",
                                 SUM("pmMacHarqDlDtx256qam")                  AS "pmMacHarqDlDtx256qam",
                                 SUM("pmMacHarqDlDtx64qam")                   AS "pmMacHarqDlDtx64qam",
                                 SUM("pmMacHarqDlDtxQpsk")                    AS "pmMacHarqDlDtxQpsk",
                                 SUM("pmMacHarqDlNack")                       AS "pmMacHarqDlNack",
                                 SUM("pmMacHarqDlNack16qam")                  AS "pmMacHarqDlNack16qam",
                                 SUM("pmMacHarqDlNack256qam")                 AS "pmMacHarqDlNack256qam",
                                 SUM("pmMacHarqDlNack64qam")                  AS "pmMacHarqDlNack64qam",
                                 SUM("pmMacHarqDlNackComp")                   AS "pmMacHarqDlNackComp",
                                 SUM("pmMacHarqDlNackQpsk")                   AS "pmMacHarqDlNackQpsk",
                                 SUM("pmMacHarqFail")                         AS "pmMacHarqFail",
                                 SUM("pmMacHarqUlAck")                        AS "pmMacHarqUlAck",
                                 SUM("pmMacHarqUlDtx")                        AS "pmMacHarqUlDtx",
                                 SUM("pmMacHarqUlDtx16qam")                   AS "pmMacHarqUlDtx16qam",
                                 SUM("pmMacHarqUlDtx256Qam")                  AS "pmMacHarqUlDtx256Qam",
                                 SUM("pmMacHarqUlDtx64Qam")                   AS "pmMacHarqUlDtx64Qam",
                                 SUM("pmMacHarqUlDtxQpsk")                    AS "pmMacHarqUlDtxQpsk",
                                 SUM("pmMacHarqUlFail16qam")                  AS "pmMacHarqUlFail16qam",
                                 SUM("pmMacHarqUlFail256Qam")                 AS "pmMacHarqUlFail256Qam",
                                 SUM("pmMacHarqUlFail64Qam")                  AS "pmMacHarqUlFail64Qam",
                                 SUM("pmMacHarqUlFailIua16qam")               AS "pmMacHarqUlFailIua16qam",
                                 SUM("pmMacHarqUlFailIuaQpsk")                AS "pmMacHarqUlFailIuaQpsk",
                                 SUM("pmMacHarqUlFailQpsk")                   AS "pmMacHarqUlFailQpsk",
                                 SUM("pmMacHarqUlNack")                       AS "pmMacHarqUlNack",
                                 SUM("pmMacHarqUlSucc16qam")                  AS "pmMacHarqUlSucc16qam",
                                 SUM("pmMacHarqUlSucc256Qam")                 AS "pmMacHarqUlSucc256Qam",
                                 SUM("pmMacHarqUlSucc64Qam")                  AS "pmMacHarqUlSucc64Qam",
                                 SUM("pmMacHarqUlSuccQpsk")                   AS "pmMacHarqUlSuccQpsk",
                                 SUM("pmPdcpVolDlDrb")                        AS "pmPdcpVolDlDrb",
                                 SUM("pmPdcpVolDlDrbLastTTI")                 AS "pmPdcpVolDlDrbLastTTI",
                                 SUM("pmUeThpVolUl")                          AS "pmUeThpVolUl",
                                 SUM("pmUeThpTimeUl")                         AS "pmUeThpTimeUl",
                                 SUM("pmSchedActivityCellDl")                 AS "pmSchedActivityCellDl",
                                 SUM("pmPdcpVolUlDrb")                        AS "pmPdcpVolUlDrb",
                                 SUM("pmSchedActivityCellUl")                 AS "pmSchedActivityCellUl",
                                 sum("pmRrcConnMax")                          AS "pmRrcConnMax",
                                 sum("pmActiveUeDlMax")                       AS "pmActiveUeDlMax",
                                 SUM("pmPdcpPktDiscDlPelr")                   AS "pmPdcpPktDiscDlPelr",
                                 SUM("pmPdcpPktDiscDlPelrUu")                 AS "pmPdcpPktDiscDlPelrUu",
                                 SUM("pmPdcpPktDiscDlHo")                     AS "pmPdcpPktDiscDlHo",
                                 SUM("pmPdcpPktReceivedDl")                   AS "pmPdcpPktReceivedDl",
                                 SUM("pmPdcpPktFwdDl")                        AS "pmPdcpPktFwdDl",
                                 SUM("pmPdcpPktLostUl")                       AS "pmPdcpPktLostUl",
                                 SUM("pmPdcpPktReceivedUl")                   AS "pmPdcpPktReceivedUl",
                                 SUM("pmPdcpLatTimeDl")                       AS "pmPdcpLatTimeDl",
                                 SUM("pmPdcpLatPktTransDl")                   AS "pmPdcpLatPktTransDl",
                                 SUM("pmErabRelMme")                          AS "pmErabRelMme",
                                 SUM("pmPdcpVolUlDrbLastTTI")                 AS "pmPdcpVolUlDrbLastTTI",
                                 SUM("pmUeThpTimeDl")                         AS "pmUeThpTimeDl",
                                 SUM("pmS1SigConnEstabFailMmeOvlMos")         AS "pmS1SigConnEstabFailMmeOvlMos",
                                 SUM("Sum(PERIOD_DURATION) for")              AS "Sum(PERIOD_DURATION) for",
                                 SUM("Sum(pmCellDowntimeAuto) for")           AS "Sum(pmCellDowntimeAuto) for",
                                 SUM("Sum(pmCellDowntimeMan) for")            AS "Sum(pmCellDowntimeMan) for",
                                 SUM("Sum(pmHoExeSuccLteSpifho) for")         AS "Sum(pmHoExeSuccLteSpifho) for",
                                 SUM("Sum(pmHoExeAttLteSpifho) for")          AS "Sum(pmHoExeAttLteSpifho) for",
                                 SUM("Sum(pmHoExeSuccLteInterF) for")         AS "Sum(pmHoExeSuccLteInterF) for",
                                 SUM("Sum(pmHoExeAttLteInterF) for")          AS "Sum(pmHoExeAttLteInterF) for",
                                 SUM("Sum(pmFlexErabEstabAttInit) for")       AS "Sum(pmFlexErabEstabAttInit) for",
                                 SUM("Sum(pmFlexErabEstabSuccAdded) for")     AS "Sum(pmFlexErabEstabSuccAdded) for",
                                 SUM("Sum(pmFlexErabEstabSuccInit) for")      AS "Sum(pmFlexErabEstabSuccInit) for",
                                 SUM("Sum(pmFlexErabEstabAttAdded) for")      AS "Sum(pmFlexErabEstabAttAdded) for",
                                 SUM("Sum(pmFlexCellHoExeSuccLteIntraF) for") AS "Sum(pmFlexCellHoExeSuccLteIntraF) for",
                                 SUM("Sum(pmFlexCellHoExeAttLteIntraF) for")  AS "Sum(pmFlexCellHoExeAttLteIntraF) for"
                                 -- </editor-fold>
                          FROM dnb.stats."NRCELLFDD"
                          WHERE "EUtranCellFDD" IN ${sql(cells)}
                          GROUP BY "DATE_ID")
        SELECT "DATE_ID"::varchar(10) AS "time",
               -- <editor-fold desc="KPIs">
               100 * "pmRrcConnEstabSuccMos" /
               nullif("pmRrcConnEstabAttMos" - "pmRrcConnEstabAttReattMos", 0)                   as "RRC Setup Success Rate (Service) (%)",

               100 *
               ("pmRrcConnEstabSucc" /
                nullif("pmRrcConnEstabAtt" - "pmRrcConnEstabAttReatt" - "pmRrcConnEstabFailMmeOvlMos" -
                       "pmRrcConnEstabFailMmeOvlMod", 0))
                                                                                                 AS "RRC Setup Success Rate (Signaling) (%)",


               100 * ("pmErabRelAbnormalEnbAct" + "pmErabRelAbnormalMmeAct") /
               nullif("pmErabRelAbnormalEnb" + "pmErabRelNormalEnb" + "pmErabRelMme",
                      0)                                                                         AS "Erab Drop Call rate",

               100 * ("pmErabEstabSuccInit" + "pmErabEstabSuccAdded") /
               nullif("pmErabEstabAttInit" + "pmErabEstabAttAdded" - "pmErabEstabAttAddedHoOngoing",
                      0)                                                                         AS "E-RAB Setup Success Rate (%)",

               100 * ("pmUeCtxtFetchSuccX2HoIn" + "pmUeCtxtFetchSuccIntraEnbHoIn") /
               nullif("pmUeCtxtFetchAttX2HoIn" + "pmUeCtxtFetchAttIntraEnbHoIn", 0)              AS "Handover In Success Rate",

               100 *
               (("pmMacHarqUlFailQpsk" + "pmMacHarqUlFail16qam" + "pmMacHarqUlFail64Qam" + "pmMacHarqUlFail256Qam") /
                nullif("pmMacHarqUlSuccQpsk" + "pmMacHarqUlSucc16qam" + "pmMacHarqUlSucc64Qam" +
                       "pmMacHarqUlSucc256Qam" +
                       "pmMacHarqUlFailQpsk" + "pmMacHarqUlFail16qam" + "pmMacHarqUlFail64Qam" +
                       "pmMacHarqUlFail256Qam",
                       0))                                                                       AS "UL BLER",

               "pmUeThpVolUl" / nullif("pmUeThpTimeUl", 0)                                       AS "UL User Throughput",

               "pmPdcpVolDlDrb" / nullif("pmSchedActivityCellDl", 0)                             AS "DL Cell Throughput",
               "pmPdcpVolUlDrb" / nullif("pmSchedActivityCellUl", 0)                             AS "UL Cell Throughput",
               "pmRrcConnMax"                                                                    AS "Max of RRC Connected User",
               "pmActiveUeDlMax"                                                                 AS "Max of Active User",
               100 * ("pmPdcpPktDiscDlPelr" + "pmPdcpPktDiscDlPelrUu" + "pmPdcpPktDiscDlHo") /
               nullif("pmPdcpPktReceivedDl" - "pmPdcpPktFwdDl", 0)                               AS "Packet Loss (DL)",
               100 * "pmPdcpPktLostUl" /
               nullif("pmPdcpPktLostUl" + "pmPdcpPktReceivedUl", 0)                              AS "Packet Loss UL",
               "pmPdcpLatTimeDl" / nullif("pmPdcpLatPktTransDl", 0)                              AS "Latency (only Radio interface)",

               ("pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") /
               nullif("pmMacHarqDlAck256qam" + "pmMacHarqDlNack256qam" + "pmMacHarqDlDtx256qam" +
                      "pmMacHarqDlAck64qam" +
                      "pmMacHarqDlNack64qam" + "pmMacHarqDlDtx64qam" + "pmMacHarqDlAck16qam" + "pmMacHarqDlNack16qam" +
                      "pmMacHarqDlDtx16qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk", 0) *
               100                                                                               AS "DL QPSK %",

               ("pmMacHarqDlAck16qam" + "pmMacHarqDlNack16qam" + "pmMacHarqDlDtx16qam") /
               nullif("pmMacHarqDlAck256qam" + "pmMacHarqDlNack256qam" + "pmMacHarqDlDtx256qam" +
                      "pmMacHarqDlAck64qam" +
                      "pmMacHarqDlNack64qam" + "pmMacHarqDlDtx64qam" + "pmMacHarqDlAck16qam" + "pmMacHarqDlNack16qam" +
                      "pmMacHarqDlDtx16qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk", 0) *
               100                                                                               AS "DL 16QAM%",

               ("pmMacHarqDlAck64qam" + "pmMacHarqDlNack64qam" + "pmMacHarqDlDtx64qam") /
               nullif("pmMacHarqDlAck256qam" + "pmMacHarqDlNack256qam" + "pmMacHarqDlDtx256qam" +
                      "pmMacHarqDlAck64qam" +
                      "pmMacHarqDlNack64qam" + "pmMacHarqDlDtx64qam" + "pmMacHarqDlAck16qam" + "pmMacHarqDlNack16qam" +
                      "pmMacHarqDlDtx16qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk", 0) *
               100                                                                               AS "DL 64QAM%",

               ("pmMacHarqDlAck256qam" + "pmMacHarqDlNack256qam" + "pmMacHarqDlDtx256qam") /
               nullif("pmMacHarqDlAck256qam" + "pmMacHarqDlNack256qam" + "pmMacHarqDlDtx256qam" +
                      "pmMacHarqDlAck64qam" +
                      "pmMacHarqDlNack64qam" + "pmMacHarqDlDtx64qam" + "pmMacHarqDlAck16qam" + "pmMacHarqDlNack16qam" +
                      "pmMacHarqDlDtx16qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk", 0) *
               100                                                                               AS "DL 256QAM%",

               "pmMacHarqUlSuccQpsk" /
               nullif("pmMacHarqUlSuccQpsk" + "pmMacHarqUlSucc16qam" + "pmMacHarqUlSucc64Qam" + "pmMacHarqUlSucc256Qam",
                      0) *
               100                                                                               AS "UL QPSK %",

               "pmMacHarqUlSucc16qam" /
               nullif("pmMacHarqUlSuccQpsk" + "pmMacHarqUlSucc16qam" + "pmMacHarqUlSucc64Qam" + "pmMacHarqUlSucc256Qam",
                      0) *
               100                                                                               AS "UL 16QAM%",

               "pmMacHarqUlSucc64Qam" /
               nullif("pmMacHarqUlSuccQpsk" + "pmMacHarqUlSucc16qam" + "pmMacHarqUlSucc64Qam" + "pmMacHarqUlSucc256Qam",
                      0) *
               100                                                                               AS "UL 64QAM%",

               "pmMacHarqUlSucc256Qam" /
               nullif("pmMacHarqUlSuccQpsk" + "pmMacHarqUlSucc16qam" + "pmMacHarqUlSucc64Qam" + "pmMacHarqUlSucc256Qam",
                      0) *
               100                                                                               AS "UL 256QAM%",

               ("pmPdcpVolDlDrb" - "pmPdcpVolDlDrbLastTTI") /
               nullif("pmUeThpTimeDl", 0)                                                        AS "DL User Throughput",

               "pmPdcpVolDlDrb" / (8 * 1024 * 1024)                                                     AS "DL Data Volume",
               "pmPdcpVolUlDrb" / (8 * 1024 * 1024)                                                     AS "UL Data Volume",

               100 *
               ((60 * "Sum(PERIOD_DURATION) for") - ("Sum(pmCellDowntimeAuto) for" + "Sum(pmCellDowntimeMan) for")) /
               nullif(60 * "Sum(PERIOD_DURATION) for", 0)                                        AS "Cell Availability",

               100 * ("Sum(pmFlexErabEstabSuccInit) for" + "Sum(pmFlexErabEstabSuccAdded) for") /
               nullif("Sum(pmFlexErabEstabAttInit) for" + "Sum(pmFlexErabEstabAttAdded) for",
                      0)                                                                         AS "E-RAB Setup Success Rate_non-GBR (%)",

               100 * ("Sum(pmHoExeSuccLteSpifho) for" /
                      nullif("Sum(pmHoExeAttLteSpifho) for", 0))                                 AS "VoLTE Redirection Success Rate",


               100 * ("Sum(pmFlexCellHoExeSuccLteIntraF) for" /
                      nullif("Sum(pmFlexCellHoExeAttLteIntraF) for", 0))                         AS "Intrafreq HOSR",

               100 * ("Sum(pmHoExeSuccLteInterF) for" /
                      nullif("Sum(pmHoExeAttLteInterF) for", 0))                                 AS "Interfreq HOSR",

               100 *
               ("pmRrcConnEstabSucc" /
                nullif("pmRrcConnEstabAtt" - "pmRrcConnEstabAttReatt" - "pmRrcConnEstabFailMmeOvlMos" -
                       "pmRrcConnEstabFailMmeOvlMod", 0)) *
               ("pmS1SigConnEstabSucc" / nullif("pmS1SigConnEstabAtt" - "pmS1SigConnEstabFailMmeOvlMos", 0)) *
               ("Sum(pmFlexErabEstabSuccInit) for" + "Sum(pmFlexErabEstabSuccAdded) for") /
               nullif("Sum(pmFlexErabEstabAttInit) for" + "Sum(pmFlexErabEstabAttAdded) for",
                      0)                                                                         AS "Call Setup Success Rate"
               -- </editor-fold>
        FROM COUNTERS
        ORDER BY "DATE_ID";
    `;
    response.status(200).json(results);
}

const customCellListStatsLTE2 = async (request, response) => {
    const cells = request.query.cells;
    const results = await sql`
                SELECT "DATE_ID"::varchar(10) AS "time",
                    SUM("pmAcBarringCsfbSpecAcPlmn1Distr") /
                    nullif(SUM("pmAcBarringCsfbSpecAcPlmn2Distr"), 0)                                        AS "Resource Block Utilizing Rate (DL)",
                    SUM("pmAcBarringCsfbSpecAcPlmn3Distr") /
                    nullif(SUM("pmAcBarringCsfbSpecialAcDistr"), 0)                                          AS "Resource Block Utilizing Rate (UL)",
                    SUM("pmAcBarringMosSpecAcPlmn1Distr") / nullif(SUM("pmAcBarringMosSpecAcPlmn2Distr"), 0) AS "Average CQI",
                    SUM("pmAcBarringModSpecAcPlmn3Distr") / nullif(SUM("pmAcBarringModSpecialAcDistr"), 0)   AS "Avg PUSCH UL RSSI"
                    FROM dnb.stats."PRBCQIRSSITA"
                    where "EUtranCellFDD" in ${sql(cells)}                  
                    group by "DATE_ID"
                    order by "DATE_ID";
                `;
    response.status(200).json(results);
}

const clusterHourlyStatsNR = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["NR"];
    const results = await sql`
                    SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", 
                            t1."Cluster_ID" as object,
                            ${sql(columns)}
                        FROM dnb.stats_group_hourly."DataTableClusterKPI" as t1
                        LEFT JOIN dnb.stats_group_hourly."CPULoadClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" 
                        AND t1."DAY"=t2."DAY"
                        AND t1."HOUR"=t2."HOUR"
                        LEFT JOIN dnb.stats_group_hourly."PacketLossClusterKPI" as t3 on t1."Cluster_ID"=t3."Cluster_ID" 
                        AND t1."DAY"=t3."DAY"
                        AND t1."HOUR"=t3."HOUR"
                        WHERE t1."Cluster_ID"=${clusterId}
                        ORDER BY t1."DAY", t1."HOUR"
                        `;
    response.status(200).json(results);
}

const clusterHourlyStatsLTE = async (request, response) => {
    const {clusterId} = request.query;
    const columns = networkKpiList["LTE"];
    const results = await sql`
                    SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", 
                            t1."Cluster_ID" as object,
                            ${sql(columns)}
                        FROM dnb.stats_group_hourly."NRCELLFDDClusterKPI" as t1
                        LEFT JOIN dnb.stats_group_hourly."PRBCQIRSSITAClusterKPI" as t2 on t1."Cluster_ID"=t2."Cluster_ID" 
                        AND t1."DAY"=t2."DAY"
                        AND t1."HOUR"=t2."HOUR"
                        WHERE t1."Cluster_ID"=${clusterId}
                        ORDER BY t1."DAY", t1."HOUR"
                        `;
    response.status(200).json(results);
}

const updateUserID = async (request, response) => {
    const userId = getCookies(request)['UserID'] || '';
    const userName = (request.headers.username || '').replace('+', ' ');
    const results = await sql`
    INSERT INTO dnb.stats_v3.dashboard_users (user_id, user_name)
    VALUES (${userId}, ${userName})
    `;
    response.status(200).json(results);
}

module.exports = {
    customCellListStatsLTE,
    customCellListStatsLTE2,
    customCellListStatsNR,
    customCellListStatsNR2,
    clusterDailyStatsNR,
    clusterDailyStatsLTE,
    clusterHourlyStatsNR,
    clusterHourlyStatsLTE,
    testQuery,
    updateUserID
}