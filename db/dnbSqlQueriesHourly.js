const statsSqlQueriesHourly = {


    hourlyNetworkLTE: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", "Cell Availability",
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
                           on t1."DAY" = t2."DAY" and t1."HOUR" = t2."HOUR"
        ORDER BY t1."DAY", t1."HOUR";
    `,



    hourlyPlmnLTE: `

        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", t1."PLMN" as "object",
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
                   WHEN t1."PLMN" = 'UMobile'
                       THEN t2."Max of RRC Connected User (UMobile)" END "Max of RRC Connected User"
        FROM stats_hourly."FexEutrancellFDDHourlyNetworkKPI" t1
                 left join stats_hourly."Flex_ERAB_SSR_NonGBR_Hourly_Network_KPI" t3
                           USING ("DAY", "HOUR", "FLEX_FILTERNAME", "PLMN")
                 left join stats_hourly."FlexMaxRRCUsersHourlyNetworkKPI" t2 USING ("DAY", "HOUR")
        ORDER BY "DAY", "HOUR";

    `,

    hourlyNetworkNR: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", 'Network' as "object",
               "Cell Availability",
               "ENDC SR",
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
                 LEFT JOIN dnb.stats_hourly."PacketLossHourlyNetwork" t3 USING ("DAY", "HOUR")
        ORDER BY t1."DAY", t1."HOUR";
    `,

    hourlyPlmnNR: `
        SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) AS "time", "PLMN" as "object",
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
                 LEFT JOIN stats_hourly."FlexNRCELLDUHourlyPLMN" t2 USING ("DAY", "HOUR", "PLMN")
        ORDER BY "time";
    `,




    hourlyNetworkCellLTE: `
        SELECT *
        FROM stats_hourly."hourlyNetworkCellLTE"
        WHERE "EUtranCellFDD" = $1
        ;
    `,

    hourlyPlmnCellLTE: `
        SELECT *
        FROM stats_hourly."hourlyPlmnCellLTE"
        WHERE "EUtranCellFDD" = $1
    `,

    hourlyNetworkCellNR: `
        SELECT *
        FROM stats_hourly."hourlyNetworkCellNR"
        WHERE "object" = $1
        ;
    `,

    hourlyPlmnCellNR: `
        SELECT *
        FROM stats_hourly."hourlyPlmnCellNR"
        WHERE "NRCellCU" = $1
        ;
    `,

}
module.exports = statsSqlQueriesHourly