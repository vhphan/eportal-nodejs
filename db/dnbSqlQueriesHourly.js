const statsSqlQueriesHourly = {
    hourlyNetworkLTE: `
    SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
           "Cell Availability",
           "Call Setup Success Rate",
           "RRC Setup Success Rate (Service) (%)",
           "RRC Setup Success Rate (Signaling) (%)",
           "E-RAB Setup Success Rate_non-GBR (%)",
           "E-RAB Setup Success Rate (%)",
           "VoLTE Redirection Success Rate",
           "Erab Drop Call rate",
           "Intrafreq HOSR",
           "Interfreq HOSR",
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
           "Avg PUSCH UL RSSI"
        FROM stats_hourly."NRCELLFDDHourlyNetworkKPI" t1
                 LEFT JOIN stats_hourly."PRBCQIRSSITAHourlyNetworkKPI" t2
        on t1."DAY"=t2."DAY" and t1."HOUR"=t2."HOUR" 
        ORDER BY t1."DAY",t1."HOUR";
    `,

    hourlyNetworkCellLTE: `
    WITH 
        t1 AS (
            SELECT *
            FROM stats_hourly."NRCELLFDDHourlyKPI"
            WHERE "EUtranCellFDD" = $1
                ),
         t2 AS (
             SELECT *
             FROM stats_hourly."PRBCQIRSSITAHourlyKPI"
             WHERE "EUtranCellFDD" = $1
         )
        SELECT 
               (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
               t1."ERBS",
               t1."EUtranCellFDD",
               "Cell Availability",
               "Call Setup Success Rate",
               "RRC Setup Success Rate (Service) (%)",
               "RRC Setup Success Rate (Signaling) (%)",
               "E-RAB Setup Success Rate_non-GBR (%)",
               "E-RAB Setup Success Rate (%)",
               "VoLTE Redirection Success Rate",
               "Erab Drop Call rate",
               "Intrafreq HOSR",
               "Interfreq HOSR",
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
               "Avg PUSCH UL RSSI"
        FROM t1
                 LEFT JOIN t2
                           USING ("DAY", "HOUR", "ERBS", "EUtranCellFDD")
        ORDER BY t1."DAY",t1."HOUR"
        ;
    `,

    hourlyPlmnCellLTE: `
            WITH t1 AS (
            SELECT *
            FROM stats_hourly."FexEutrancellFDDHourlyKPI"
            WHERE "EUtranCellFDD" = $1
        ),
        
             t2 AS (
                 SELECT *
                 FROM stats_hourly."FlexMaxRRCUsersHourlyKPI"
                 WHERE "EUtranCellFDD" = $1
             ),
             t3 AS (
                 SELECT *
                 FROM stats_hourly."Flex_ERAB_SSR_NonGBR_Hourly_KPI"
                 WHERE "EUtranCellFDD" = $1
             )
        
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
               t1."ERBS",
               t1."EUtranCellFDD",
               t1."PLMN" as "object",
               t1."FLEX_FILTERNAME",
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
               t3."E-RAB Setup Success Rate_non-GBR (%)",
               CASE
                   WHEN t1."PLMN" = 'YTL' THEN t2."Max of RRC Connected User (YTL)"
                   WHEN t1."PLMN" = 'DNB' THEN t2."Max of RRC Connected User (DNB)"
                   WHEN t1."PLMN" = 'Maxis' THEN t2."Max of RRC Connected User (Maxis)"
                   WHEN t1."PLMN" = 'Celcom' THEN t2."Max of RRC Connected User (Celcom)"
                   WHEN t1."PLMN" = 'UMobile' THEN t2."Max of RRC Connected User (UMobile)" END "Max of RRC Connected User"
        FROM t1
                 left join t3 USING ("DAY", "HOUR", "ERBS", "EUtranCellFDD", "FLEX_FILTERNAME", "PLMN")
                 left join t2 USING ("DAY", "HOUR", "ERBS", "EUtranCellFDD")
        ORDER BY "DAY", "HOUR";
    
    `,

    hourlyPlmnLTE:`
    
    SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
       t1."PLMN" as "object",
       t1."FLEX_FILTERNAME",
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
       t3."E-RAB Setup Success Rate_non-GBR (%)",
       CASE
           WHEN t1."PLMN" = 'YTL' THEN t2."Max of RRC Connected User (YTL)"
           WHEN t1."PLMN" = 'DNB' THEN t2."Max of RRC Connected User (DNB)"
           WHEN t1."PLMN" = 'Maxis' THEN t2."Max of RRC Connected User (Maxis)"
           WHEN t1."PLMN" = 'Celcom' THEN t2."Max of RRC Connected User (Celcom)"
           WHEN t1."PLMN" = 'UMobile' THEN t2."Max of RRC Connected User (UMobile)" END "Max of RRC Connected User"
FROM stats_hourly."FexEutrancellFDDHourlyNetworkKPI" t1
         left join stats_hourly."Flex_ERAB_SSR_NonGBR_Hourly_Network_KPI" t3 USING ("DAY", "HOUR",  "FLEX_FILTERNAME", "PLMN")
         left join stats_hourly."FlexMaxRRCUsersHourlyNetworkKPI" t2 USING ("DAY", "HOUR")
ORDER BY "DAY", "HOUR";
    
    `,

    hourlyNetworkNR:`
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
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
        FROM dnb.stats_hourly."DataTableHourlyNetworkKPI" t1
             LEFT JOIN dnb.stats_hourly."CPULoadHourlyNetwork" t2 USING ("DAY", "HOUR")
             LEFT JOIN dnb.stats_hourly."PacketLossHourlyNetwork" t3 USING ("DAY")
        ORDER BY t1."DAY", t1."HOUR";
        `,

    hourlyPlmnNR: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
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
        FROM stats_hourly."FlexNRCELLCUHourlyPLMN" t1
             LEFT JOIN stats_hourly."FlexNRCELLDUHourlyPLMN" t2 USING ("DAY", "HOUR", "PLMN") ORDER BY "time";
    `,

    hourlyNetworkCellNR: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
           t1."NR_NAME",
           t1."NRCellDU"                                            as "object",
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
        FROM dnb.stats_hourly."DataTableHourlyKPI" t1
             LEFT JOIN dnb.stats_hourly."CPULoadHourly" t2
                       on t1."DAY" = t2."DAY" and t1."HOUR" = t2."HOUR" and t1."NR_NAME" = t2."ERBS"
             LEFT JOIN dnb.stats_hourly."PacketLossHourly" t3
                       on t1."DAY" = t3."DAY" and t1."HOUR" = t3."HOUR" and t1."NR_NAME" = t3."NE_NAME"
        WHERE "NRCellDU"=$1
        ORDER BY t1."DAY", t1."HOUR";
    `,

    hourlyPlmnCellNR: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time",
           t1."NRCellCU",
           t1."PLMN"                                                as "object",
           "ENDC SR",
           "Erab Drop Call rate (sgNB)",
           "Intra-SgNB Pscell Change Success Rate",
           "Inter-SgNB PSCell Change Success Rate",
           t3."pmEbsRrcConnLevelMaxEnDc"                            as "Max of RRC Connected User (ENDC)",
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
        FROM stats_hourly."FlexNRCELLCUHourly" t1
             LEFT JOIN (SELECT * FROM stats_hourly."FlexNRCELLDUHourly" WHERE "NRCellDU" like $1) t2
                       on t1."DAY" = t2."DAY" and
                          t1."HOUR" = t2."HOUR" and
                          t1."NRCellCU" = t2."NRCellDU" and
                          t1."PLMN" = t2."PLMN" and
                          t1."NR_NAME" = t2."NR_NAME"
             LEFT JOIN (SELECT * FROM stats_hourly."FlexNRCELLCU_Patch" WHERE "NRCellCU" like $1) t3
                       on t1."DAY" = t3."DAY" and
                          t1."HOUR" = t3."HOUR" and
                          t1."NRCellCU" = t3."NRCellCU" and
                          t1."PLMN" = t3."PLMN2" and
                          t1."NR_NAME" = t3."NR_NAME"
        WHERE t1."PLMN" <> 'Overall'
        AND t1."NRCellCU" like $1
        
        ORDER BY "time";
    `,
}
module.exports = statsSqlQueriesHourly