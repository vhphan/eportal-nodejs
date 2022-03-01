const statsSqlQueries = {
    dailyNetworkNR: `SELECT "DATE_ID" as "time",
       'Network' as "object",
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
       "Latency",
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
FROM dnb.stats."DataTableDailyNetwork" t1
         LEFT JOIN dnb.stats."CPULoadDailyNetwork" t2 USING ("DATE_ID")
         LEFT JOIN dnb.stats."PacketLossDailyNetwork" t3 USING ("DATE_ID")
ORDER BY t1."DATE_ID";`,

    dailyPlmnNR: `SELECT "DATE_ID" as "time",
       "PLMN" as "object",
       "ENDC SR",
       "Erab Drop Call rate (sgNB)",
       "Intra-SgNB Pscell Change Success Rate",
       "Inter-SgNB PSCell Change Success Rate",
       "Max of RRC Connected User (ENDC)",
       "DL User Throughput",
       "UL User Throughput",
       "DL Data Volume",
       "UL Data Volume",
       "DL QPSK %",
       "DL 16QAM%",
       "DL 64QAM%",
       "DL 256QAM%",
       "UL QPSK %",
       "UL 16QAM%",
       "UL 64QAM%"
FROM stats."FlexNRCELLCUDailyPLMN" t1
         LEFT JOIN stats."FlexNRCELLDUDailyPLMN" t2 USING ("DATE_ID", "PLMN") ORDER BY "time";`,
    dailyNetworkLTE: `SELECT "DATE_ID" as "time",
        'Network' as "object",
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
       " Average CQI ",
       "Avg PUSCH UL RSSI"
FROM stats."NRCELLFDDDailyNetwork"
         FULL OUTER JOIN stats."PRBCQIRSSITADailyNetwork" USING ("DATE_ID");`,
    dailyPlmnLTE: `SELECT "DATE_ID" as "time",
       "PLMN" as "object",
       "E-RAB Setup Success Rate (%)",
       "Erab Drop Call rate",
       "Intrafreq HOSR",
       "UL BLER",
       "DL User Throughput",
       "UL User Throughput",
       "DL Cell Throughput",
       "UL Cell Throughput",
       "DL Data Volume",
       "UL Data Volume",
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
       "E-RAB Setup Success Rate_non-GBR (%)",
       "Max of RRC Connected User"

FROM stats."FexEutrancellFDDDailyPLMN"
         FULL OUTER JOIN stats."Flex_ERAB_SSR_NonGBR_DailyPLMN" USING ("DATE_ID", "PLMN")
         FULL OUTER JOIN stats."FlexMaxRRCUsersDailyPLMNPivot" USING ("DATE_ID", "PLMN") ORDER BY "time"`,
    siteList: `SELECT "ERBS"
                FROM dnb.stats."CPULoad"
                GROUP BY "ERBS" ORDER BY "ERBS";`,

    dailySiteNR: `
    WITH t1 AS (
    SELECT "DATE_ID"                          as "time",
           "ERBS"                             as "object",
           sum("pmDuIntensHoReqDistr")        as "pmDuIntensHoReqDistr",
           sum("pmDuIntensHoRrcConnReqDistr") as "pmDuIntensHoRrcConnReqDistr"
    FROM dnb.stats."CPULoad"
    group by time, object
),
     t2 AS (
         SELECT "DATE_ID"                                            as "time",
                "NR_NAME"                                            as "object",
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
                SUM("Sum(pmRadioRaAttTaDistr) for 9")                as "Sum(pmRadioRaAttTaDistr) for 9"
         FROM dnb.stats."DataTable"
         group by time, object
     ),
     t3 AS (
     SELECT "DATE_ID"                                  as "time",
            "NE_NAME"                                  as "object",
            sum("Sum(pmPdcpPktTransDlDiscQos) for")    AS "Sum(pmPdcpPktTransDlDiscQos) for",
            sum("Sum(pmPdcpPktTransDlDiscAqmQos) for") AS "Sum(pmPdcpPktTransDlDiscAqmQos) for",
            sum("Sum(pmPdcpPktTransDlQos) for")        AS "Sum(pmPdcpPktTransDlQos) for",
            sum("Sum(pmPdcpPktLossUlQos) for")         AS "Sum(pmPdcpPktLossUlQos) for",
            sum("Sum(pmPdcpPktRecUlOooQos) for")       AS "Sum(pmPdcpPktRecUlOooQos) for",
            sum("Sum(pmPdcpPktRecUlQos) for")          AS "Sum(pmPdcpPktRecUlQos) for",
            sum("Sum(pmPdcpPktLossUlToQos) for")       AS "Sum(pmPdcpPktLossUlToQos) for",
            sum("Sum(pmPdcpPktLossUlToDiscQos) for")   AS "Sum(pmPdcpPktLossUlToDiscQos) for"
     FROM dnb.stats."PacketLoss"
     group by time, object
    )
    SELECT t1.time,
    t1.object,
    
    ("pmDuIntensHoReqDistr") / nullif(("pmDuIntensHoRrcConnReqDistr"), 0)                    as "gNobeB CPU Load",
    
    100 * ((60 * "Sum(PERIOD_DURATION) for") - ("Sum(pmCellDowntimeAuto) for" + "Sum(pmCellDowntimeMan) for")) /
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
    100 * ("Sum(pmMacRBSymUsedPdschTypeABroadcasting) for" + "Sum(pmMacRBSymUsedPdschTypeABroadcasting) for" +
          "Sum(pmMacRBSymCsiRs) for") /
    nullif("Sum(pmMacRBSymAvailDl) for", 0)                                                  as "Resource Block Utilizing Rate (DL)",
    100 * ("Sum(pmMacRBSymUsedPuschTypeA) for" + "Sum(pmMacRBSymUsedPuschTypeB) for") /
    nullif("Sum(pmMacRBSymAvailUl) for", 0)                                                  as "Resource Block Utilizing Rate (UL)",
    "Sum(pmMacBsrLcg0Distr) for" / nullif("Sum(pmMacBsrLcg1Distr) for", 0)                   as "Average CQI",
    100 * ("Sum(pmMacHarqUlNackQpsk) for" + "pmMacHarqDlNack16Qam" + "pmMacHarqDlNack64Qam") /
    nullif("Sum(pmMacHarqUlAckQpsk) for" + "Sum(pmMacHarqUlAck16Qam) for" + "Sum(pmMacHarqUlAck64Qam) for" +
          "Sum(pmMacHarqUlNackQpsk) for" + "Sum(pmMacHarqUlNack16Qam) for" + "Sum(pmMacHarqUlNack64Qam) for",
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
    "Sum(pmRadioPuschTable1McsDistr) for" / nullif("Sum(pmRadioPuschTable2McsDistr) for", 0) as "Latency",
    ("pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") / nullif(
               ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" + "pmMacHarqDlAck64Qam" +
                "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" +
                "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") * 100,
               0)                                                                           as "DL QPSK %",
    ("pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" + "pmMacHarqDlDtx16Qam") / nullif(
               ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" + "pmMacHarqDlAck64Qam" +
                "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" +
                "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") * 100,
               0)                                                                           as "DL 16QAM%",
    ("pmMacHarqDlAck64Qam" + "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam") / nullif(
               ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" + "pmMacHarqDlAck64Qam" +
                "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" +
                "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") * 100,
               0)                                                                           as "DL 64QAM%",
    ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam") / nullif(
               ("pmMacHarqDlAck256Qam" + "pmMacHarqDlNack256Qam" + "pmMacHarqDlDtx256Qam" + "pmMacHarqDlAck64Qam" +
                "pmMacHarqDlNack64Qam" + "pmMacHarqDlDtx64Qam" + "pmMacHarqDlAck16Qam" + "pmMacHarqDlNack16Qam" +
                "pmMacHarqDlDtx16Qam" + "pmMacHarqDlAckQpsk" + "pmMacHarqDlNackQpsk" + "pmMacHarqDlDtxQpsk") * 100,
               0)                                                                           as "DL 256QAM%",
    ("pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") / nullif(
               ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" + "pmMacHarqUlAck64Qam" +
                "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" +
                "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") * 100,
               0)                                                                           as "UL QPSK %",
    ("pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" + "pmMacHarqUlDtx16Qam") / nullif(
               ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" + "pmMacHarqUlAck64Qam" +
                "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" +
                "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") * 100,
               0)                                                                           as "UL 16QAM%",
    ("pmMacHarqUlAck64Qam" + "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam") / nullif(
               ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" + "pmMacHarqUlAck64Qam" +
                "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" +
                "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") * 100,
               0)                                                                           as "UL 64QAM%",
    ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam") / nullif(
               ("pmMacHarqUlAck256Qam" + "pmMacHarqUlNack256Qam" + "pmMacHarqUlDtx256Qam" + "pmMacHarqUlAck64Qam" +
                "pmMacHarqUlNack64Qam" + "pmMacHarqUlDtx64Qam" + "pmMacHarqUlAck16Qam" + "pmMacHarqUlNack16Qam" +
                "pmMacHarqUlDtx16Qam" + "pmMacHarqUlAckQpsk" + "pmMacHarqUlNackQpsk" + "pmMacHarqUlDtxQpsk") * 100,
               0)                                                                           as "UL 256QAM%",
    100 * "pmRrcConnEstabSuccMos" /
    
    nullif("pmRrcConnEstabAttMos" - "pmRrcConnEstabAttReattMos", 0)                          as "RRC Setup Success Rate (Signaling) (%)",
    100 * (("Sum(pmPdcpPktTransDlDiscQos) for") - ("Sum(pmPdcpPktTransDlDiscAqmQos) for")) /
    ("Sum(pmPdcpPktTransDlQos) for")                                               as "Packet Loss (DL)",
            100 * (("Sum(pmPdcpPktLossUlQos) for") - ("Sum(pmPdcpPktRecUlOooQos) for")) /
            (("Sum(pmPdcpPktRecUlQos) for") + ("Sum(pmPdcpPktLossUlToQos) for") -
             ("Sum(pmPdcpPktLossUlToDiscQos) for") - ("Sum(pmPdcpPktRecUlOooQos) for")) as "Packet Loss (UL)"
    
    
    FROM t1
     inner join t2 USING (time, object)
     inner join t3 USING (time, object)
    
    WHERE t1.object like $1
    
    ;
    `,
    dailySiteLTE: `
    SELECT t1."DATE_ID" as "time",
       t1."ERBS" as "object",
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
    FROM stats."NRCELLFDD" t1
             INNER JOIN stats."PRBCQIRSSITA" t2
                        on t1."DATE_ID" = t2."DATE_ID" AND t1."ERBS" = t2."ERBS" AND t1."EUtranCellFDD" = t2."EUtranCellFDD"
    WHERE t1."ERBS" like $1;`,
}

module.exports = statsSqlQueries