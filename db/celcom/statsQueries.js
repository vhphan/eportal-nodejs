const sql = require('./PgJsBackend');
const {arrayToCsv} = require("../../routes/utils");
const {renameProps} = require("../../tools/utils");

const getAggregatedStats = (tech) => async (request, response) => {

    let {page, size, format, startDate, endDate, columns} = request.query;

    page = page === undefined ? 1 : parseInt(page);
    size = size === undefined ? 1000 : parseInt(size);
    startDate = startDate === undefined ? '2022-04-01' : startDate;
    endDate = endDate === undefined ? '2022-12-31' : endDate;
    format = format === undefined ? 'csv' : 'json';
    columns = columns !== undefined;
    let table;
    switch (tech) {
        case 'GSM':
            table = columns ? 'celcom.stats.gsm_aggregates_columns' : 'celcom.stats.gsm_aggregates';
            break;
        case 'LTE':
        default:
            table = columns ? 'celcom.stats.lte_aggregates_columns' : 'celcom.stats.lte_aggregates';
            break;
    }
    let totalRecords = -1;
    let totalPages = -1;
    if (page === 1) {
        totalRecords = await sql`
                SELECT COUNT(*) as k FROM ${sql(table)} WHERE "Date" is not null 
                and "Date">=${startDate}
                and "Date"<=${endDate}
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
    SELECT * FROM ${sql(table)} WHERE "Date" is not null
    AND "Date" >= ${startDate}
    AND "Date" <= ${endDate}
    ORDER BY "Date", "id"
    LIMIT ${size} OFFSET ${(page - 1) * size}
    `;

    const {headers, values} = arrayToCsv(results);

    response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
        page,
        size,
        total_pages: totalPages,
    });

};

const getAggregatedStatsWeek = (tech) => async (request, response) => {
    let {page, size, format, startWeek, startYear, endWeek, endYear, columns} = request.query;
    page = page === undefined ? 1 : parseInt(page);
    size = size === undefined ? 1000 : parseInt(size);
    format = format === undefined ? 'csv' : 'json';
    startYear = startYear === undefined ? 2022 : parseInt(startYear);
    endYear = endYear === undefined ? 2022 : parseInt(endYear);
    startWeek = startWeek === undefined ? 1 : parseInt(startWeek);
    endWeek = endWeek === undefined ? 52 : parseInt(endWeek);
    columns = columns !== undefined;

    let table;
    switch (tech) {
        case 'GSM':
            table = columns ? 'celcom.stats.gsm_aggregates_week_columns' : 'celcom.stats.gsm_aggregates_week';
            break;
        case 'LTE':
        default:
            table = columns ? 'celcom.stats.lte_aggregates_week_columns' : 'celcom.stats.lte_aggregates_week';
            break;
    }
    let totalPages, totalRecords;
    if (page === 1) {
        totalRecords = await sql`
                SELECT COUNT(*) as k FROM ${sql(table)} 
                WHERE "Week" is not null and "Year" is not null 
                and "Week">= ${startWeek} and "Year">=${startYear}
                and "Week"<= ${endWeek} and "Year" <=${endYear}
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
    }

    totalPages = page === 1 ? Math.ceil(totalRecords[0]['k'] / size) : -1;
    const results = await sql`
            SELECT 
             *
            FROM ${sql(table)} 
            WHERE "Week" is not null and "Year" is not null
            and "Week">= ${startWeek} and "Year">=${startYear}
            and "Week"<= ${endWeek} and "Year" <=${endYear}
            ORDER BY "id"
            LIMIT ${size} OFFSET ${(page - 1) * size}
            `

    const {headers, values} = arrayToCsv(results, false);

    response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
        page,
        size,
        total_pages: totalPages,
    });

};

const getCellStats = (tech) => async (request, response) => {

    let {cell, format, startDate, endDate} = request.query;

    format = format === undefined ? 'csv' : 'json';
    startDate = startDate === undefined ? '2022-01-01' : startDate;
    endDate = endDate === undefined ? '2025-01-01' : endDate;

    if (cell === undefined) {
        const error = new Error('No cell name provided.')
        return response.status(400).json({
            success: false,
            error: error.message || 'Server Error',
            message: error.message || 'Server Error'
        })
    }

    let results;
    if (tech === 'GSM') {
        results = await sql`
                    SELECT "Week",
                    "Date",
                    "Days (#)",
                    t2."Region",
                    "BSC Id",
                    "Cell Name",
                    "SystemID",

                    1 - (sum("G01_Availability Cell (%) Num")) /
                    nullif(sum("G01_Availability Cell (%) Denom"), 0)               AS "G01_Availability Cell (%)",
                    sum("G02_Availability TCH (%) Num") /
                    nullif(sum("G02_Availability TCH (%) Denom"), 0)                    AS "G02_Availability TCH (%)",
                    (1 - (sum("G03_CSSR SD Block Nom1") / nullif(sum("G03_CSSR SD Block Denom1"), 0))) *
                    (1 - (sum("G03_CSSR SD Drop Nom2") / nullif(sum("G03_CSSR SD Drop Denom2"), 0))) *
                    (sum("G03_CSSR TCH Assign Fail Num3") /
                    nullif(sum("G03_CSSR TCH Assign Fail Denom3"), 0))                 AS "G03_CSSR (%)",
                    sum("G04_SD Blocked Rate (%) Num") /
                    nullif(sum("G04_SD Blocked Rate (%) Denom"), 0)                     AS "G04_SD Blocked Rate (%)",
                    
                    sum("G05_SD Drop Rate (%) Num") / nullif(sum("G05_SD Drop Rate (%) Denom"), 0) AS "G05_SD Drop Rate (%)",
                    
                    --     NEW KPI
                    sum("G06_TCH Assignment SR (%) Num") /
                    nullif(sum("G06_TCH Assignment SR (%) Denom"), 0)                   AS "G06_TCH Assignment SR (%)",
                    
                    --     UPDATED KPI
                    sum("G07_TCH Blocked Rate (%) Num") /
                    nullif(sum("G07_TCH Blocked Rate (%) Denom"), 0)                    AS "G07_TCH Blocked Rate (%)",
                    --     UPDATED KPI
                    sum("G08_TCH Drop Rate (%) Num") /
                    nullif(sum("G08_TCH Drop Rate (%) Denom"), 0)                       AS "G08_TCH Drop Rate (%)",
                    --     UPDATED KPI
                    1 - sum("G09_PDCH Establishment SR (%) Num") /
                    nullif(sum("G09_PDCH Establishment SR (%) Denom"), 0)           AS "G09_PDCH Establishment SR (%)",
                    --     UPDATED KPI
                    sum("G10_PDCH Congestion Rate (%) Num") /
                    nullif(sum("G10_PDCH Congestion Rate (%) Denum"), 0)                AS "G10_PDCH Congestion Rate (%)",
                    
                    SUM("G10_PDCH Block (#)")                                           AS "G10_PDCH Block (#)",
                    
                    sum("G11_DL TBF Establishment SR (%) Num") /
                    NULLIF(sum("G11_DL TBF Establishment SR (%) Denom"), 0)             AS "G11_DL TBF Establishment SR (%)",
                    
                    sum("G12_UL TBF Establishment SR (%) Num") /
                    nullif(sum("G12_UL TBF Establishment SR (%) Denom"), 0)             AS "G12_UL TBF Establishment SR (%)",
                    
                    sum("G13_PS Drop Rate (%) Num") /
                    nullif(sum("G13_PS Drop Rate (%) Denom"), 0)                        AS "G13_PS Drop Rate (%)",
                    
                    --     NEW KPI
                    sum("G13_DL TBF Drop Rate (%) Num") /
                    nullif(sum("G13_DL TBF Drop Rate (%) Denom"), 0)                    as "G13_DL TBF Drop Rate (%)",
                    
                    sum("G14_PS Traffic (GBytes)")                                      AS "G14_PS Traffic (GBytes)",
                    SUM("G15_TCH Traffic (Erl)")                                        AS "G15_TCH Traffic (Erl)",
                    SUM("G16_SDCCH Traffic (Erl)")                                      AS "G16_SDCCH Traffic (Erl)",
                    sum("G17_Handover SR (%) Num") /
                    nullif(sum("G17_Handover SR (%) Denom"), 0)                         AS "G17_Handover SR (%)",
                    SUM("G18_ICM Band 1 (%) Num") /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G18_ICM Band 1 (%)",
                    SUM("G19_ICM Band 2 (%) Num") /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G19_ICM Band 2 (%)",
                    SUM("G20_ICM Band 3 (%) Num") /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G20_ICM Band 3 (%)",
                    SUM("G21_ICM Band 4 (%) Num") /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G21_ICM Band 4 (%)",
                    SUM("G22_ICM Band 5 (%) Num") /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G22_ICM Band 5 (%)",
                    (sum("G20_ICM Band 3 (%) Num") + sum("G21_ICM Band 4 (%) Num") + sum("G22_ICM Band 5 (%) Num")) /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                            AS "G23_Bad ICM (%)",
                    sum("G25_Rx Qual DL Good (%) Num") /
                    nullif(sum("G27_Rx Qual DL (#) Denom"), 0)                          AS "G25_Rx Qual DL Good (%)",
                    sum("G26_Rx Qual DL Bad (%) Num") /
                    nullif(sum("G27_Rx Qual DL (#) Denom"), 0)                          AS "G26_Rx Qual DL Bad (%)",
                    SUM("G28_Rx Qual UL Good (%) Num") /
                    nullif(sum("G30_RxQual UL (#) Denom"), 0)                           AS "G28_Rx Qual UL Good (%)",
                    SUM("G29_Rx Qual UL Bad (%) Num") /
                    nullif(sum("G30_RxQual UL (#) Denom"), 0)                           AS "G29_Rx Qual UL Bad (%)",
                    sum("G31_SQI Good DL (%) Num") /
                    nullif(sum("G34_SQI DL (#) Denom"), 0)                              AS "G31_SQI Good DL (%)",
                    sum("G32_SQI Accpt DL (%) Num") /
                    nullif(sum("G34_SQI DL (#) Denom"), 0)                              AS "G32_SQI Accpt DL (%)",
                    sum("G33_SQI Bad DL (%) Num") /
                    nullif(sum("G34_SQI DL (#) Denom"), 0)                              AS "G33_SQI Bad DL (%)",
                    sum("G35_SQI Good UL (%) Num") /
                    nullif(sum("G38_SQI UL (#) Denom"), 0)                              AS "G35_SQI Good UL (%)",
                    sum("G36_SQI Accpt UL (%) Num") /
                    nullif(sum("G38_SQI UL (#) Denom"), 0)                              AS "G36_SQI Accpt UL (%)",
                    sum("G37_SQI Bad UL (%) Num") /
                    nullif(sum("G38_SQI UL (#) Denom"), 0)                              AS "G37_SQI Bad UL (%)",
                    
                    --     NEW KPI
                    sum("G39_2G to 4G Fast Return (#)")                                 as "G39_2G to 4G Fast Return (#)"
                    FROM celcom.stats.gsm_oss_raw_cell as t1
                    LEFT JOIN celcom.stats.cell_mapping_gsm as t2
                           ON t1."Cell Name" = t2."CELLname"
                    WHERE t1."Cell Name"=${cell}
                    AND "Date" >=${startDate}
                    AND "Date" <=${endDate}
                    GROUP BY "Week", "Date", "Days (#)", t2."Region", "BSC Id", "Cell Name", "SystemID"
                `
    } else {
        results = await sql`
                SELECT 
                    "Date",
                    t1."EUtranCellFDD",
                   
                    SUM("L02_RRC CSSR (%) Num") / nullif(SUM("L02_RRC CSSR (%) Denom"), 0) as "L02_RRC CSSR (%)",
                    SUM("L03_RRC CSSR Serv (%) Num") /
                    nullif(SUM("L03_RRC CSSR Serv (%) Denom"), 0)                          as "L03_RRC CSSR Serv (%)",
                    SUM("L04_ERAB CSSR (%) Num") /
                    nullif(SUM("L04_ERAB CSSR (%) Denom"), 0)                              as "L04_ERAB CSSR (%)",
                    SUM("L05_ERAB DR (%) Num") /
                    nullif(SUM("L05_ERAB DR (%) Denom"), 0)                                as "L05_ERAB DR (%)",
                    SUM("L06_ERAB DR (%) Num") /
                    nullif(SUM("L06_ERAB DR (%) Denom"), 0)                                as "L06_ERAB DR (%)",
                    SUM("L07_Packet Loss Rate DL (%) Num") /
                    nullif(SUM("L07_Packet Loss Rate DL (%) Denom"), 0)                    as "L07_Packet Loss Rate DL (%)",
                    SUM("L08_Packet Loss Rate UL (%) Num") /
                    nullif(SUM("L08_Packet Loss Rate UL (%) Denom"), 0)                    as "L08_Packet Loss Rate UL (%)",
                    SUM("L09_IAF HO Prep SR (%) Num") /
                    nullif(SUM("L09_IAF HO Prep SR (%) Denom"), 0)                         as "L09_IAF HO Prep SR (%)",
                    SUM("L09_IAF HO Exe SR (%) Num") /
                    nullif(SUM("L09_IAF HO Exe SR (%) Denom"), 0)                          as "L09_IAF HO Exe SR (%)",
                    SUM("L10_IEF HO Prep SR (%) Num") /
                    nullif(SUM("L10_IEF HO Prep SR (%) Denom"), 0)                         as "L10_IEF HO Prep SR (%)",
                    SUM("L10_IEF HO Exe SR (%) Num") /
                    nullif(SUM("L10_IEF HO Exe SR (%) Denom"), 0)                          as "L10_IEF HO Exe SR (%)",
                    SUM("L11_Intra LTE HO Prep SR (%) Num") /
                    nullif(SUM("L11_Intra LTE HO Prep SR (%) Denom"), 0)                   as "L11_Intra LTE HO Prep SR (%)",
                    SUM("L11_Intra LTE HO Exe SR (%) Num") /
                    nullif(SUM("L11_Intra LTE HO Exe SR (%) Denom"), 0)                    as "L11_Intra LTE HO Exe SR (%)",
                    SUM("L12_IRAT HOSR (%) Num") /
                    nullif(SUM("L12_IRAT HOSR (%) Denom"), 0)                              as "L12_IRAT HOSR (%)",
                    SUM("L13_Cell NotAvail (%) Num") /
                    nullif(SUM("L13_Cell NotAvail (%) Denom"), 0)                          as "L13_Cell NotAvail (%)",
                    SUM("L15_Integrity DL Latency (ms) Num") /
                    nullif(SUM("L15_Integrity DL Latency (ms) Denom"), 0)                  as "L15_Integrity DL Latency (ms)",
                    SUM("L16_Avg DL Thp Cell (Mbps) Num") /
                    nullif(SUM("L16_Avg DL Thp Cell (Mbps) Denom"), 0)                     as "L16_Avg DL Thp Cell (Mbps)",
                    SUM("L17_Avg UL Thp Cell (Mbps) Num") /
                    nullif(SUM("L17_Avg UL Thp Cell (Mbps) Denom"), 0)                     as "L17_Avg UL Thp Cell (Mbps)",
                    SUM("L18_Avg DL Thp User (Mbps) Num") /
                    nullif(SUM("L18_Avg DL Thp User (Mbps) Denom"), 0)                     as "L18_Avg DL Thp User (Mbps)",
                    SUM("L19_Avg UL Thp User (Mbps) Num") /
                    nullif(SUM("L19_Avg UL Thp User (Mbps) Denom"), 0)                     as "L19_Avg UL Thp User (Mbps)",
                    
                    SUM("L20_Active UE User (#)")                                          as "L20_Active UE User (#)",
                    SUM("L21_Avg RRC User (#)")                                            as "L21_Avg RRC User (#)",
                    SUM("L21_Max RRC User (#)")                                            as "L21_Max RRC User (#)",
                    SUM("L22_Avg ERAB User (#)")                                           as "L22_Avg ERAB User (#)",
                    SUM("L22_Max ERAB User (#)")                                           as "L22_Max ERAB User (#)",
                    
                    SUM("L23_Avg UL Interference PUSCH (dBm) Num") /
                    nullif(SUM("L23_Avg UL Interference PUSCH (dBm) Denom"), 0)            as "L23_Avg UL Interference PUSCH (dBm)",
                    SUM("L23_Avg UL Interference PUCCH (dBm) Num") /
                    nullif(SUM("L23_Avg UL Interference PUCCH (dBm) Denom"), 0)            as "L23_Avg UL Interference PUCCH (dBm)",
                    SUM("L24_PRB Util DL (%) Num") /
                    nullif(SUM("L24_PRB Util DL (%) Denom"), 0)                            as "L24_PRB Util DL (%)",
                    SUM("L25_PRB Util UL (%) Num") /
                    nullif(SUM("L25_PRB Util UL (%) Denom"), 0)                            as "L25_PRB Util UL (%)",
                    
                    
                    SUM("L26_CA User (#)")                                                 as "L26_CA User (#)",
                    SUM("L27_CA Capable User (#)")                                         as "L27_CA Capable User (#)",
                    
                    
                    SUM("L29_CA Thpt (Mbps) Num") /
                    nullif(SUM("L29_CA Thpt (Mbps) Denom"), 0)                             as "L29_CA Thpt (Mbps)",
                    SUM("L30_DL BLER (%) Num") /
                    nullif(SUM("L30_DL BLER (%) Denom"), 0)                                as "L30_DL BLER (%)",
                    SUM("L31_UL BLER (%) Num") /
                    nullif(SUM("L31_UL BLER (%) Denom"), 0)                                as "L31_UL BLER (%)",
                    SUM("L33_Average CQI (#) Num") /
                    nullif(SUM("L33_Average CQI (#) Denom"), 0)                            as "L33_Average CQI (#)",
                    SUM("L34_Average RSRP (dBm) Num") /
                    nullif(SUM("L34_Average RSRP (dBm) Denom"), 0)                         as "L34_Average RSRP (dBm)",
                    SUM("L35_RSRP <-110 dBm (%) Num") /
                    nullif(SUM("L35_RSRP <-110 dBm (%) Denom"), 0)                         as "L35_RSRP <-110 dBm (%)",
                    SUM("L36_Average SINR PUSCH (dB) Num") /
                    nullif(SUM("L36_Average SINR PUSCH (dB) Denom"), 0)                    as "L36_Average SINR PUSCH (dB)",
                    SUM("L36_Average SINR PUCCH (dB) Num") /
                    nullif(SUM("L36_Average SINR PUCCH (dB) Denom"), 0)                    as "L36_Average SINR PUCCH (dB)",
                    SUM("L37_Spectral Efficiency (Bit/s/Hz) Num") /
                    nullif(SUM("L37_Spectral Efficiency (Bit/s/Hz) Denom"), 0)             as "L37_Spectral Efficiency (Bit/s/Hz)",
                    SUM("S01_Accessibility SIP QCI5 (%) Num") /
                    nullif(SUM("S01_Accessibility SIP QCI5 (%) Denom"), 0)                 as "S01_Accessibility SIP QCI5 (%)",
                    SUM("S01_Retainability SIP QCI5 (%) Num") /
                    nullif(SUM("S01_Retainability SIP QCI5 (%) Denom"), 0)                 as "S01_Retainability SIP QCI5 (%)",
                    SUM("S01_RRC Re-estab SR QCI5 (%) Num") /
                    nullif(SUM("S01_RRC Re-estab SR QCI5 (%) Denom"), 0)                   as "S01_RRC Re-estab SR QCI5 (%)",
                    SUM("V01_E-RAB Establisment SR QCI1 (%) Num") /
                    nullif(SUM("V01_E-RAB Establisment SR QCI1 (%) Denom"), 0)             as "V01_E-RAB Establisment SR QCI1 (%)",
                    SUM("V02_E-RAB Retainability QCI1 (%) Num") /
                    nullif(SUM("V02_E-RAB Retainability QCI1 (%) Denom"), 0)               as "V02_E-RAB Retainability QCI1 (%)",
                    SUM("V03_RRC Re-estab SR QCI1 (%) Num") /
                    nullif(SUM("V03_RRC Re-estab SR QCI1 (%) Denom"), 0)                   as "V03_RRC Re-estab SR QCI1 (%)",
                    SUM("V06_VoLTE Packet Loss DL (%) Num") /
                    nullif(SUM("V06_VoLTE Packet Loss DL (%) Denom"), 0)                   as "V06_VoLTE Packet Loss DL (%)",
                    SUM("V07_VoLTE Packet Loss UL (%) Num") /
                    nullif(SUM("V07_VoLTE Packet Loss UL (%) Denom"), 0)                   as "V07_VoLTE Packet Loss UL (%)",
                    SUM("V08_VoLTE IAF HO SR (%) Num") /
                    nullif(SUM("V08_VoLTE IAF HO SR (%) Denom"), 0)                        as "V08_VoLTE IAF HO SR (%)",
                    SUM("V09_VoLTE IEF HO SR (%) Num") /
                    nullif(SUM("V09_VoLTE IEF HO SR (%) Denom"), 0)                        as "V09_VoLTE IEF HO SR (%)",
                    SUM("V11_VoLTE Integrity DL Latency (ms) Num") /
                    nullif(SUM("V11_VoLTE Integrity DL Latency (ms) Denom"), 0)            as "V11_VoLTE Integrity DL Latency (ms)",
                    SUM("V12_VoLTE Integrity Cell (%) Num") /
                    nullif(SUM("V12_VoLTE Integrity Cell (%) Denom"), 0)                   as "V12_VoLTE Integrity Cell (%)",
                    SUM("V13_VoLTE Integrity UE (%) Num") /
                    nullif(SUM("V13_VoLTE Integrity UE (%) Denom"), 0)                     as "V13_VoLTE Integrity UE (%)",
                    SUM("V14_DL Silent exp per VoLTE user (ms) Num") /
                    nullif(SUM("V14_DL Silent exp per VoLTE user (ms) Denom"), 0)          as "V14_DL Silent exp per VoLTE user (ms)",
                    SUM("V15_UL Silent exp per VoLTE user (ms) Num") /
                    nullif(SUM("V15_UL Silent exp per VoLTE user (ms) Denom"), 0)          as "V15_UL Silent exp per VoLTE user (ms)",
                    SUM("V16_SRVCC HO to GERAN Prep SR (%) Num") /
                    nullif(SUM("V16_SRVCC HO to GERAN Prep SR (%) Denom"), 0)              as "V16_SRVCC HO to GERAN Prep SR (%)",
                    SUM("V16_SRVCC HO to GERAN Exe SR (%) Num") /
                    nullif(SUM("V16_SRVCC HO to GERAN Exe SR (%) Denom"), 0)               as "V16_SRVCC HO to GERAN Exe SR (%)",
                    SUM("V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0") /
                    nullif(SUM("V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0"), 0)        as "V17_SRVCC HO to GERAN Prep SR (%) PLMN0",
                    SUM("V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0") /
                    nullif(SUM("V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0"), 0)         as "V17_SRVCC HO to GERAN Exe SR (%) PLMN0",
                    SUM("V18_SRVCC HO to UTRAN Prep SR (%) Num") /
                    nullif(SUM("V18_SRVCC HO to UTRAN Prep SR (%) Denom"), 0)              as "V18_SRVCC HO to UTRAN Prep SR (%)",
                    SUM("V18_SRVCC HO to UTRAN Exe SR (%) Num") /
                    nullif(SUM("V18_SRVCC HO to UTRAN Exe SR (%) Denom"), 0)               as "V18_SRVCC HO to UTRAN Exe SR (%)",
                    SUM("L06_ERAB Drop due to Cell Down Time")                             AS "L06_ERAB Drop due to Cell Down Time",
                    SUM("L06_ERAB Drop due to Cell Down Time (PNR)")                       AS "L06_ERAB Drop due to Cell Down Time (PNR)",
                    SUM("L06_ERAB Drop due to contact with UE lost")                       AS "L06_ERAB Drop due to contact with UE lost",
                    SUM("L06_ERAB Drop due to HO Exe failure")                             AS "L06_ERAB Drop due to HO Exe failure",
                    SUM("L06_ERAB Drop due to HO Preparation")                             AS "L06_ERAB Drop due to HO Preparation",
                    SUM("L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail")                AS "L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail",
                    SUM("L06_ERAB Drop due to UE Pre-emption")                             AS "L06_ERAB Drop due to UE Pre-emption",
                    SUM("L14_PSDL Trf (GB)")                                               AS "L14_PSDL Trf (GB)",
                    SUM("L14_PSUL Trf (GB)")                                               AS "L14_PSUL Trf (GB)",
                    SUM("L32_Modulation DL QPSK (#)")                                      AS "L32_Modulation DL QPSK (#)",
                    SUM("L32_Modulation DL 16QAM (#)")                                     AS "L32_Modulation DL 16QAM (#)",
                    SUM("L32_Modulation DL 64QAM (#)")                                     AS "L32_Modulation DL 64QAM (#)",
                    SUM("L32_Modulation DL 256QAM (#)")                                    AS "L32_Modulation DL 256QAM (#)",
                    SUM("V02_VoLTE Drop due to Cell Down Time")                            AS "V02_VoLTE Drop due to Cell Down Time",
                    SUM("V02_VoLTE Drop due to contact with UE lost")                      AS "V02_VoLTE Drop due to contact with UE lost",
                    SUM("V02_VoLTE Drop due to HO Exe Failure")                            AS "V02_VoLTE Drop due to HO Exe Failure",
                    SUM("V02_VoLTE Drop due to HO Preparation")                            AS "V02_VoLTE Drop due to HO Preparation",
                    SUM("V02_VoLTE Drop due to part. ERAB path switch fail")               AS "V02_VoLTE Drop due to part. ERAB path switch fail",
                    SUM("V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail")               AS "V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail",
                    SUM("V04_VoLTE Traffic (Erlang)")                                      AS "V04_VoLTE Traffic (Erlang)",
                    SUM("V05_VoLTE User (#)")                                              AS "V05_VoLTE User (#)",
                    SUM("V10_RRC RwR CSFB L2G (#)")                                        AS "V10_RRC RwR CSFB L2G (#)",
                    SUM("V10_RRC RwR CSFB L2U (#)")                                        AS "V10_RRC RwR CSFB L2U (#)",
                    SUM("V10_CSFB Indicators Received (#)")                                AS "V10_CSFB Indicators Received (#)",
                    SUM("V10_RRC RwR SC L2G (#)")                                          AS "V10_RRC RwR SC L2G (#)",
                    SUM("V10_RRC RwR SC L2U (#)")                                          AS "V10_RRC RwR SC L2U (#)",
                    
                    
                    SUM("V19_L2G SRVCC (%) Num") /
                    nullif(SUM("V19_L2G SRVCC (%) Denom"), 0)                              as "V19_L2G SRVCC (%)",
                    SUM("V20_CSFB 2G (%) Num") /
                    nullif(SUM("V20_CSFB 2G (%) Denom"), 0)                                as "V20_CSFB 2G (%)",
                    SUM("V21_VoLTE UL Audio Gap < 6s (%) Num") /
                    nullif(SUM("V21_VoLTE UL Audio Gap < 6s (%) Denom"), 0)                as "V21_VoLTE UL Audio Gap < 6s (%)"

                   
                   
                FROM celcom.stats.lte_oss_raw_cell as t1
                     LEFT JOIN celcom.stats.cell_mapping as t2
                               ON t1."EUtranCellFDD"=t2."CELLname"
                WHERE "EUtranCellFDD" = ${cell}
                    AND "Date" >=${startDate}
                    AND "Date" <=${endDate}
                GROUP BY "Date", "EUtranCellFDD"
                `
        //</editor-fold>
    }

    if (format === 'json') {
        const cellIdColumn = tech === 'LTE' ? 'EUtranCellFDD' : 'Cell Name';
        results.forEach(d => {
            d['Date'] = d['Date'].toISOString().split('T')[0]
        });
        response.status(200).json({
            success: true,
            data: results.map(result => renameProps(result, ["Date", cellIdColumn], ['time', 'object']))
        });
        return;
    }

    const {headers, values} = arrayToCsv(results);
    const numOfCols = Object.keys(results[0]).length;
    const numOfRows = results.length;
    return response.status(200).json({
        success: true,
        headers: format === 'csv' ? headers.join('\t') : headers,
        data: format === 'csv' ? values.join('\n') : results,
        numOfRows,
        numOfCols,
    });
};

const getCellMapping = (tech) => async (request, response) => {
    let {format} = request.query;
    format = format === undefined ? 'csv' : 'json';
    let results;
    switch (tech) {
        case 'GSM':
            results = await sql`
                            SELECT * FROM celcom.stats.cell_mapping_gsm
                            `;
            break;
        case 'LTE':
        default:
            results = await sql`
                            SELECT * FROM celcom.stats.cell_mapping
                            `;
            break;
        case 'ALL':
            results = await sql`
                            SELECT *, 'GSM' as tech FROM celcom.stats.cell_mapping_gsm
                            UNION ALL
                            SELECT *, 'LTE' as tech FROM celcom.stats.cell_mapping
                            `;
            break;
    }

    const {headers, values} = arrayToCsv(results, false);
    return response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
    });

};

const getGroupedCellsStats = (tech, reqType) => async (request, response) => {
    let {cells, format, clusterName, startDate, endDate} = reqType === 'get' ? request.query : request.body;

    if (reqType === 'get') {
        cells = cells.split(',');
    } else {
        cells = cells.map(cell => cell[0]);
    }

    format = format === undefined ? 'csv' : 'json';
    startDate = startDate === undefined ? '2022-01-01' : startDate;
    endDate = endDate === undefined ? '2025-01-01' : endDate;

    if (cells === undefined || cells.length === 0) {
        const error = new Error('No cell name provided.')
        return response.status(400).json({
            success: false,
            error: error.message || 'Server Error',
            message: error.message || 'Server Error'
        })
    }
    clusterName = clusterName === undefined ? `clusterOf${cells.length}Cells` : clusterName;

    let results;
    if (tech === 'GSM') {
        results = await sql`
                    SELECT 
                    "Date",
                    1 - (sum("G01_Availability Cell (%) Num")) /
                    nullif(sum("G01_Availability Cell (%) Denom"), 0)                           AS "G01_Availability Cell (%)",
                    sum("G02_Availability TCH (%) Num") /
                    nullif(sum("G02_Availability TCH (%) Denom"), 0)                                AS "G02_Availability TCH (%)",
                    (1 - (sum("G03_CSSR SD Block Nom1") / nullif(sum("G03_CSSR SD Block Denom1"), 0))) *
                    (1 - (sum("G03_CSSR SD Drop Nom2") / nullif(sum("G03_CSSR SD Drop Denom2"), 0))) *
                    (sum("G03_CSSR TCH Assign Fail Num3") /
                    nullif(sum("G03_CSSR TCH Assign Fail Denom3"), 0))                             AS "G03_CSSR (%)",
                    sum("G04_SD Blocked Rate (%) Num") /
                    nullif(sum("G04_SD Blocked Rate (%) Denom"), 0)                                 AS "G04_SD Blocked Rate (%)",
                    
                    sum("G05_SD Drop Rate (%) Num") / nullif(sum("G05_SD Drop Rate (%) Denom"), 0)  AS "G05_SD Drop Rate (%)",
                    sum("G06_TCH Blocked Rate (%) Num") /
                    nullif(sum("G06_TCH Blocked Rate (%) Denom"), 0)                                AS "G06_TCH Blocked Rate (%)",
                    
                    Sum("G07_TCH Drop Rate (%) Num") / nullif(Sum("G07_TCH Drop Rate (%) Num"), 0)  AS "G07_TCH Drop Rate (%)",
                    (1 - (sum("G08_PDCH Establishment SR (%) Num") /
                    nullif(sum("G08_PDCH Establishment SR (%) Denom"), 0)))                   AS "G08_PDCH Establishment SR (%)",

                    sum("G09_PDCH Congestion Rate (%) Num") /
                    NULLIF(sum("G09_PDCH Congestion Rate (%) Denum"), 0)                            AS "G09_PDCH Congestion Rate (%)",
                    SUM("G10_PDCH Block (#)")                                                       AS "G10_PDCH Block (#)",
                    sum("G11_DL TBF Establishment SR (%) Num") /
                    NULLIF(sum("G11_DL TBF Establishment SR (%) Denom"), 0)                         AS "G11_DL TBF Establishment SR (%)",
                    sum("G12_UL TBF Establishment SR (%) Num") /
                    nullif(sum("G12_UL TBF Establishment SR (%) Denom"), 0)                         AS "G12_UL TBF Establishment SR (%)",
                    sum("G13_PS Drop Rate (%) Num") / nullif(sum("G13_PS Drop Rate (%) Denom"), 0)  AS "G13_PS Drop Rate (%)",
                    sum("G14_PS Traffic (GBytes)")                                                  AS "G14_PS Traffic (GBytes)",
                    SUM("G15_TCH Traffic (Erl)")                                                    AS "G15_TCH Traffic (Erl)",
                    SUM("G16_SDCCH Traffic (Erl)")                                                  AS "G16_SDCCH Traffic (Erl)",
                    sum("G17_Handover SR (%) Num") / nullif(sum("G17_Handover SR (%) Denom"), 0)    AS "G17_Handover SR (%)",
                    SUM("G18_ICM Band 1 (%) Num") / nullif(sum("G24_ICM Band (#) Denom"), 0)        AS "G18_ICM Band 1 (%)",
                    SUM("G19_ICM Band 2 (%) Num") / nullif(sum("G24_ICM Band (#) Denom"), 0)        AS "G19_ICM Band 2 (%)",
                    SUM("G20_ICM Band 3 (%) Num") / nullif(sum("G24_ICM Band (#) Denom"), 0)        AS "G20_ICM Band 3 (%)",
                    SUM("G21_ICM Band 4 (%) Num") / nullif(sum("G24_ICM Band (#) Denom"), 0)        AS "G21_ICM Band 4 (%)",
                    SUM("G22_ICM Band 5 (%) Num") / nullif(sum("G24_ICM Band (#) Denom"), 0)        AS "G22_ICM Band 5 (%)",
                    (sum("G20_ICM Band 3 (%) Num") + sum("G21_ICM Band 4 (%) Num") + sum("G22_ICM Band 5 (%) Num")) /
                    nullif(sum("G24_ICM Band (#) Denom"), 0)                                        AS "G23_Bad ICM (%)",
                    sum("G25_Rx Qual DL Good (%) Num") / nullif(sum("G27_Rx Qual DL (#) Denom"), 0) AS "G25_Rx Qual DL Good (%)",
                    sum("G26_Rx Qual DL Bad (%) Num") / nullif(sum("G27_Rx Qual DL (#) Denom"), 0)  AS "G26_Rx Qual DL Bad (%)",
                    SUM("G28_Rx Qual UL Good (%) Num") / nullif(sum("G30_RxQual UL (#) Denom"), 0)  AS "G28_Rx Qual UL Good (%)",
                    SUM("G29_Rx Qual UL Bad (%) Num") / nullif(sum("G30_RxQual UL (#) Denom"), 0)   AS "G29_Rx Qual UL Bad (%)",
                    sum("G31_SQI Good DL (%) Num") / nullif(sum("G34_SQI DL (#) Denom"), 0)         AS "G31_SQI Good DL (%)",
                    sum("G32_SQI Accpt DL (%) Num") / nullif(sum("G34_SQI DL (#) Denom"), 0)        AS "G32_SQI Accpt DL (%)",
                    sum("G33_SQI Bad DL (%) Num") / nullif(sum("G34_SQI DL (#) Denom"), 0)          AS "G33_SQI Bad DL (%)",
                    sum("G35_SQI Good UL (%) Num") / nullif(sum("G38_SQI UL (#) Denom"), 0)         AS "G35_SQI Good UL (%)",
                    sum("G36_SQI Accpt UL (%) Num") / nullif(sum("G38_SQI UL (#) Denom"), 0)        AS "G36_SQI Accpt UL (%)",
                    sum("G37_SQI Bad UL (%) Num") / nullif(sum("G38_SQI UL (#) Denom"), 0)          AS "G37_SQI Bad UL (%)"
                    FROM celcom.stats.gsm_oss_raw_cell as t1
                    LEFT JOIN celcom.stats.cell_mapping_gsm as t2
                           ON t1."Cell Name" = t2."CELLname"
                    WHERE t1."Cell Name" in ${sql(cells)}
                        AND "Date" >=${startDate}
                        AND "Date" <=${endDate}
                    GROUP BY 
                    "Date"
                ORDER BY
                    "Date"
                `
    } else {
        results = await sql`
                SELECT 
                "Date",
                    SUM("L02_RRC CSSR (%) Num") / nullif(SUM("L02_RRC CSSR (%) Denom"), 0) as "L02_RRC CSSR (%)",
                    SUM("L03_RRC CSSR Serv (%) Num") /
                    nullif(SUM("L03_RRC CSSR Serv (%) Denom"), 0)                          as "L03_RRC CSSR Serv (%)",
                    SUM("L04_ERAB CSSR (%) Num") /
                    nullif(SUM("L04_ERAB CSSR (%) Denom"), 0)                              as "L04_ERAB CSSR (%)",
                    SUM("L05_ERAB DR (%) Num") /
                    nullif(SUM("L05_ERAB DR (%) Denom"), 0)                                as "L05_ERAB DR (%)",
                    SUM("L06_ERAB DR (%) Num") /
                    nullif(SUM("L06_ERAB DR (%) Denom"), 0)                                as "L06_ERAB DR (%)",
                    SUM("L07_Packet Loss Rate DL (%) Num") /
                    nullif(SUM("L07_Packet Loss Rate DL (%) Denom"), 0)                    as "L07_Packet Loss Rate DL (%)",
                    SUM("L08_Packet Loss Rate UL (%) Num") /
                    nullif(SUM("L08_Packet Loss Rate UL (%) Denom"), 0)                    as "L08_Packet Loss Rate UL (%)",
                    SUM("L09_IAF HO Prep SR (%) Num") /
                    nullif(SUM("L09_IAF HO Prep SR (%) Denom"), 0)                         as "L09_IAF HO Prep SR (%)",
                    SUM("L09_IAF HO Exe SR (%) Num") /
                    nullif(SUM("L09_IAF HO Exe SR (%) Denom"), 0)                          as "L09_IAF HO Exe SR (%)",
                    SUM("L10_IEF HO Prep SR (%) Num") /
                    nullif(SUM("L10_IEF HO Prep SR (%) Denom"), 0)                         as "L10_IEF HO Prep SR (%)",
                    SUM("L10_IEF HO Exe SR (%) Num") /
                    nullif(SUM("L10_IEF HO Exe SR (%) Denom"), 0)                          as "L10_IEF HO Exe SR (%)",
                    SUM("L11_Intra LTE HO Prep SR (%) Num") /
                    nullif(SUM("L11_Intra LTE HO Prep SR (%) Denom"), 0)                   as "L11_Intra LTE HO Prep SR (%)",
                    SUM("L11_Intra LTE HO Exe SR (%) Num") /
                    nullif(SUM("L11_Intra LTE HO Exe SR (%) Denom"), 0)                    as "L11_Intra LTE HO Exe SR (%)",
                    SUM("L12_IRAT HOSR (%) Num") /
                    nullif(SUM("L12_IRAT HOSR (%) Denom"), 0)                              as "L12_IRAT HOSR (%)",
                    SUM("L13_Cell NotAvail (%) Num") /
                    nullif(SUM("L13_Cell NotAvail (%) Denom"), 0)                          as "L13_Cell NotAvail (%)",
                    SUM("L15_Integrity DL Latency (ms) Num") /
                    nullif(SUM("L15_Integrity DL Latency (ms) Denom"), 0)                  as "L15_Integrity DL Latency (ms)",
                    SUM("L16_Avg DL Thp Cell (Mbps) Num") /
                    nullif(SUM("L16_Avg DL Thp Cell (Mbps) Denom"), 0)                     as "L16_Avg DL Thp Cell (Mbps)",
                    SUM("L17_Avg UL Thp Cell (Mbps) Num") /
                    nullif(SUM("L17_Avg UL Thp Cell (Mbps) Denom"), 0)                     as "L17_Avg UL Thp Cell (Mbps)",
                    SUM("L18_Avg DL Thp User (Mbps) Num") /
                    nullif(SUM("L18_Avg DL Thp User (Mbps) Denom"), 0)                     as "L18_Avg DL Thp User (Mbps)",
                    SUM("L19_Avg UL Thp User (Mbps) Num") /
                    nullif(SUM("L19_Avg UL Thp User (Mbps) Denom"), 0)                     as "L19_Avg UL Thp User (Mbps)",
                    
                    SUM("L20_Active UE User (#)")                                          as "L20_Active UE User (#)",
                    SUM("L21_Avg RRC User (#)")                                            as "L21_Avg RRC User (#)",
                    SUM("L21_Max RRC User (#)")                                            as "L21_Max RRC User (#)",
                    SUM("L22_Avg ERAB User (#)")                                           as "L22_Avg ERAB User (#)",
                    SUM("L22_Max ERAB User (#)")                                           as "L22_Max ERAB User (#)",
                    
                    SUM("L23_Avg UL Interference PUSCH (dBm) Num") /
                    nullif(SUM("L23_Avg UL Interference PUSCH (dBm) Denom"), 0)            as "L23_Avg UL Interference PUSCH (dBm)",
                    SUM("L23_Avg UL Interference PUCCH (dBm) Num") /
                    nullif(SUM("L23_Avg UL Interference PUCCH (dBm) Denom"), 0)            as "L23_Avg UL Interference PUCCH (dBm)",
                    SUM("L24_PRB Util DL (%) Num") /
                    nullif(SUM("L24_PRB Util DL (%) Denom"), 0)                            as "L24_PRB Util DL (%)",
                    SUM("L25_PRB Util UL (%) Num") /
                    nullif(SUM("L25_PRB Util UL (%) Denom"), 0)                            as "L25_PRB Util UL (%)",
                    
                    
                    SUM("L26_CA User (#)")                                                 as "L26_CA User (#)",
                    SUM("L27_CA Capable User (#)")                                         as "L27_CA Capable User (#)",
                    
                    
                    SUM("L29_CA Thpt (Mbps) Num") /
                    nullif(SUM("L29_CA Thpt (Mbps) Denom"), 0)                             as "L29_CA Thpt (Mbps)",
                    SUM("L30_DL BLER (%) Num") /
                    nullif(SUM("L30_DL BLER (%) Denom"), 0)                                as "L30_DL BLER (%)",
                    SUM("L31_UL BLER (%) Num") /
                    nullif(SUM("L31_UL BLER (%) Denom"), 0)                                as "L31_UL BLER (%)",
                    SUM("L33_Average CQI (#) Num") /
                    nullif(SUM("L33_Average CQI (#) Denom"), 0)                            as "L33_Average CQI (#)",
                    SUM("L34_Average RSRP (dBm) Num") /
                    nullif(SUM("L34_Average RSRP (dBm) Denom"), 0)                         as "L34_Average RSRP (dBm)",
                    SUM("L35_RSRP <-110 dBm (%) Num") /
                    nullif(SUM("L35_RSRP <-110 dBm (%) Denom"), 0)                         as "L35_RSRP <-110 dBm (%)",
                    SUM("L36_Average SINR PUSCH (dB) Num") /
                    nullif(SUM("L36_Average SINR PUSCH (dB) Denom"), 0)                    as "L36_Average SINR PUSCH (dB)",
                    SUM("L36_Average SINR PUCCH (dB) Num") /
                    nullif(SUM("L36_Average SINR PUCCH (dB) Denom"), 0)                    as "L36_Average SINR PUCCH (dB)",
                    SUM("L37_Spectral Efficiency (Bit/s/Hz) Num") /
                    nullif(SUM("L37_Spectral Efficiency (Bit/s/Hz) Denom"), 0)             as "L37_Spectral Efficiency (Bit/s/Hz)",
                    SUM("S01_Accessibility SIP QCI5 (%) Num") /
                    nullif(SUM("S01_Accessibility SIP QCI5 (%) Denom"), 0)                 as "S01_Accessibility SIP QCI5 (%)",
                    SUM("S01_Retainability SIP QCI5 (%) Num") /
                    nullif(SUM("S01_Retainability SIP QCI5 (%) Denom"), 0)                 as "S01_Retainability SIP QCI5 (%)",
                    SUM("S01_RRC Re-estab SR QCI5 (%) Num") /
                    nullif(SUM("S01_RRC Re-estab SR QCI5 (%) Denom"), 0)                   as "S01_RRC Re-estab SR QCI5 (%)",
                    SUM("V01_E-RAB Establisment SR QCI1 (%) Num") /
                    nullif(SUM("V01_E-RAB Establisment SR QCI1 (%) Denom"), 0)             as "V01_E-RAB Establisment SR QCI1 (%)",
                    SUM("V02_E-RAB Retainability QCI1 (%) Num") /
                    nullif(SUM("V02_E-RAB Retainability QCI1 (%) Denom"), 0)               as "V02_E-RAB Retainability QCI1 (%)",
                    SUM("V03_RRC Re-estab SR QCI1 (%) Num") /
                    nullif(SUM("V03_RRC Re-estab SR QCI1 (%) Denom"), 0)                   as "V03_RRC Re-estab SR QCI1 (%)",
                    SUM("V06_VoLTE Packet Loss DL (%) Num") /
                    nullif(SUM("V06_VoLTE Packet Loss DL (%) Denom"), 0)                   as "V06_VoLTE Packet Loss DL (%)",
                    SUM("V07_VoLTE Packet Loss UL (%) Num") /
                    nullif(SUM("V07_VoLTE Packet Loss UL (%) Denom"), 0)                   as "V07_VoLTE Packet Loss UL (%)",
                    SUM("V08_VoLTE IAF HO SR (%) Num") /
                    nullif(SUM("V08_VoLTE IAF HO SR (%) Denom"), 0)                        as "V08_VoLTE IAF HO SR (%)",
                    SUM("V09_VoLTE IEF HO SR (%) Num") /
                    nullif(SUM("V09_VoLTE IEF HO SR (%) Denom"), 0)                        as "V09_VoLTE IEF HO SR (%)",
                    SUM("V11_VoLTE Integrity DL Latency (ms) Num") /
                    nullif(SUM("V11_VoLTE Integrity DL Latency (ms) Denom"), 0)            as "V11_VoLTE Integrity DL Latency (ms)",
                    SUM("V12_VoLTE Integrity Cell (%) Num") /
                    nullif(SUM("V12_VoLTE Integrity Cell (%) Denom"), 0)                   as "V12_VoLTE Integrity Cell (%)",
                    SUM("V13_VoLTE Integrity UE (%) Num") /
                    nullif(SUM("V13_VoLTE Integrity UE (%) Denom"), 0)                     as "V13_VoLTE Integrity UE (%)",
                    SUM("V14_DL Silent exp per VoLTE user (ms) Num") /
                    nullif(SUM("V14_DL Silent exp per VoLTE user (ms) Denom"), 0)          as "V14_DL Silent exp per VoLTE user (ms)",
                    SUM("V15_UL Silent exp per VoLTE user (ms) Num") /
                    nullif(SUM("V15_UL Silent exp per VoLTE user (ms) Denom"), 0)          as "V15_UL Silent exp per VoLTE user (ms)",
                    SUM("V16_SRVCC HO to GERAN Prep SR (%) Num") /
                    nullif(SUM("V16_SRVCC HO to GERAN Prep SR (%) Denom"), 0)              as "V16_SRVCC HO to GERAN Prep SR (%)",
                    SUM("V16_SRVCC HO to GERAN Exe SR (%) Num") /
                    nullif(SUM("V16_SRVCC HO to GERAN Exe SR (%) Denom"), 0)               as "V16_SRVCC HO to GERAN Exe SR (%)",
                    SUM("V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0") /
                    nullif(SUM("V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0"), 0)        as "V17_SRVCC HO to GERAN Prep SR (%) PLMN0",
                    SUM("V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0") /
                    nullif(SUM("V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0"), 0)         as "V17_SRVCC HO to GERAN Exe SR (%) PLMN0",
                    SUM("V18_SRVCC HO to UTRAN Prep SR (%) Num") /
                    nullif(SUM("V18_SRVCC HO to UTRAN Prep SR (%) Denom"), 0)              as "V18_SRVCC HO to UTRAN Prep SR (%)",
                    SUM("V18_SRVCC HO to UTRAN Exe SR (%) Num") /
                    nullif(SUM("V18_SRVCC HO to UTRAN Exe SR (%) Denom"), 0)               as "V18_SRVCC HO to UTRAN Exe SR (%)",
                    SUM("L06_ERAB Drop due to Cell Down Time")                             AS "L06_ERAB Drop due to Cell Down Time",
                    SUM("L06_ERAB Drop due to Cell Down Time (PNR)")                       AS "L06_ERAB Drop due to Cell Down Time (PNR)",
                    SUM("L06_ERAB Drop due to contact with UE lost")                       AS "L06_ERAB Drop due to contact with UE lost",
                    SUM("L06_ERAB Drop due to HO Exe failure")                             AS "L06_ERAB Drop due to HO Exe failure",
                    SUM("L06_ERAB Drop due to HO Preparation")                             AS "L06_ERAB Drop due to HO Preparation",
                    SUM("L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail")                AS "L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail",
                    SUM("L06_ERAB Drop due to UE Pre-emption")                             AS "L06_ERAB Drop due to UE Pre-emption",
                    SUM("L14_PSDL Trf (GB)")                                               AS "L14_PSDL Trf (GB)",
                    SUM("L14_PSUL Trf (GB)")                                               AS "L14_PSUL Trf (GB)",
                    SUM("L32_Modulation DL QPSK (#)")                                      AS "L32_Modulation DL QPSK (#)",
                    SUM("L32_Modulation DL 16QAM (#)")                                     AS "L32_Modulation DL 16QAM (#)",
                    SUM("L32_Modulation DL 64QAM (#)")                                     AS "L32_Modulation DL 64QAM (#)",
                    SUM("L32_Modulation DL 256QAM (#)")                                    AS "L32_Modulation DL 256QAM (#)",
                    SUM("V02_VoLTE Drop due to Cell Down Time")                            AS "V02_VoLTE Drop due to Cell Down Time",
                    SUM("V02_VoLTE Drop due to contact with UE lost")                      AS "V02_VoLTE Drop due to contact with UE lost",
                    SUM("V02_VoLTE Drop due to HO Exe Failure")                            AS "V02_VoLTE Drop due to HO Exe Failure",
                    SUM("V02_VoLTE Drop due to HO Preparation")                            AS "V02_VoLTE Drop due to HO Preparation",
                    SUM("V02_VoLTE Drop due to part. ERAB path switch fail")               AS "V02_VoLTE Drop due to part. ERAB path switch fail",
                    SUM("V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail")               AS "V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail",
                    SUM("V04_VoLTE Traffic (Erlang)")                                      AS "V04_VoLTE Traffic (Erlang)",
                    SUM("V05_VoLTE User (#)")                                              AS "V05_VoLTE User (#)",
                    SUM("V10_RRC RwR CSFB L2G (#)")                                        AS "V10_RRC RwR CSFB L2G (#)",
                    SUM("V10_RRC RwR CSFB L2U (#)")                                        AS "V10_RRC RwR CSFB L2U (#)",
                    SUM("V10_CSFB Indicators Received (#)")                                AS "V10_CSFB Indicators Received (#)",
                    SUM("V10_RRC RwR SC L2G (#)")                                          AS "V10_RRC RwR SC L2G (#)",
                    SUM("V10_RRC RwR SC L2U (#)")                                          AS "V10_RRC RwR SC L2U (#)",
                    
                    
                    SUM("V19_L2G SRVCC (%) Num") /
                    nullif(SUM("V19_L2G SRVCC (%) Denom"), 0)                              as "V19_L2G SRVCC (%)",
                    SUM("V20_CSFB 2G (%) Num") /
                    nullif(SUM("V20_CSFB 2G (%) Denom"), 0)                                as "V20_CSFB 2G (%)",
                    SUM("V21_VoLTE UL Audio Gap < 6s (%) Num") /
                    nullif(SUM("V21_VoLTE UL Audio Gap < 6s (%) Denom"), 0)                as "V21_VoLTE UL Audio Gap < 6s (%)"
                FROM celcom.stats.lte_oss_raw_cell as t1
                LEFT JOIN celcom.stats.cell_mapping as t2
                    ON t1."EUtranCellFDD"=t2."CELLname"
                WHERE "EUtranCellFDD" IN ${sql(cells)}
                    AND "Date" >=${startDate}
                    AND "Date" <=${endDate}
                GROUP BY 
                    "Date"
                ORDER BY
                    "Date"
                `
    }
    const {headers, values} = arrayToCsv(results);
    return response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
        cellList: `list=${cells.join(',')}`
    });


}

const getClusterStats = (tech) => async (request, response) => {
    const {clusterId} = request.query;
    const layerColumn = tech.toLowerCase() === 'lte' ? 'Layer' : 'SystemID';
    const table = `celcom.stats.${tech.toLowerCase()}_aggregates`;
    const results = await sql`
    SELECT 
        "Date"::varchar(10) as "time",
        ${sql(layerColumn)} as "object",
       *
    FROM ${sql(table)} 
    WHERE "Net_Cluster_Code"=${clusterId}
    ORDER BY "Date"
    `;
    results.forEach(d => {
        d['Date'] = d['Date'].toISOString().split('T')[0];
        d[layerColumn] = d[layerColumn] === null ? 'All' : d[layerColumn];
    });
    response.status(200).json({
        success: true,
        data: results.map(result => renameProps(result, ["Date", layerColumn], ['time', 'object']))
    });
}

async function excelTestFunc(request, response) {
    console.log(request);
    const format = request.body.format || 'csv';
    const tech = request.query.tech || request.body.tech || 'LTE';

    const region = request.body['Region'];
    const filterRegion = !!region;
    const regionFilterFunc = () => filterRegion ? sql` "Region" IN ${sql(region)}` : sql` "Region" is not null`;

    const state = request.body['State'];
    const filterState = !!state;
    const stateFilterFunc = () => filterState ? sql`and "State" IN ${sql(state)}` : sql`and "State" is not null`;

    const cboClusterCode = request.body['cbo_cluster_code'];
    const filterCboClusterCode = !!cboClusterCode;
    const cboClusterFilterFunc = () => filterCboClusterCode ? sql`and cbo_cluster_code IN ${sql(cboClusterCode)}` : sql`and cbo_cluster_code is not null`;

    const netClusterCode = request.body['Net_Cluster_Code'];
    const filterNetClusterCode = !!netClusterCode;
    const netClusterFilterFunc = () => filterNetClusterCode ? sql`and "Net_Cluster_Code" IN ${sql(netClusterCode)}` : sql`and "Net_Cluster_Code" is not null`;

    const cellNames = request.body['CELLname'];
    const filterCellNames = !!cellNames;
    const cellNamesFilterFunc = () => filterCellNames ? sql`and "CELLname" IN ${sql(cellNames)}` : sql``;

    const filterConditions = () => sql` ${regionFilterFunc()} ${stateFilterFunc()} ${cboClusterFilterFunc()} ${netClusterFilterFunc()} `;

    const startTime = new Date()
    const sqlLTE = () => sql`WITH LCOUNTERS AS (SELECT t1."Date",
                                                       -- <editor-fold desc="Columns">
                                                       SUM("L02_RRC CSSR (%) Num")                              AS "L02_RRC CSSR (%) Num",
                                                       SUM("L02_RRC CSSR (%) Denom")                            AS "L02_RRC CSSR (%) Denom",
                                                       SUM("L03_RRC CSSR Serv (%) Num")                         AS "L03_RRC CSSR Serv (%) Num",
                                                       SUM("L03_RRC CSSR Serv (%) Denom")                       AS "L03_RRC CSSR Serv (%) Denom",
                                                       SUM("L04_ERAB CSSR (%) Num")                             AS "L04_ERAB CSSR (%) Num",
                                                       SUM("L04_ERAB CSSR (%) Denom")                           AS "L04_ERAB CSSR (%) Denom",
                                                       SUM("L05_ERAB DR (%) Num")                               AS "L05_ERAB DR (%) Num",
                                                       SUM("L05_ERAB DR (%) Denom")                             AS "L05_ERAB DR (%) Denom",
                                                       SUM("L06_ERAB DR (%) Num")                               AS "L06_ERAB DR (%) Num",
                                                       SUM("L06_ERAB DR (%) Denom")                             AS "L06_ERAB DR (%) Denom",
                                                       SUM("L06_ERAB Drop due to Cell Down Time")               AS "L06_ERAB Drop due to Cell Down Time",
                                                       SUM("L06_ERAB Drop due to Cell Down Time (PNR)")         AS "L06_ERAB Drop due to Cell Down Time (PNR)",
                                                       SUM("L06_ERAB Drop due to contact with UE lost")         AS "L06_ERAB Drop due to contact with UE lost",
                                                       SUM("L06_ERAB Drop due to HO Exe failure")               AS "L06_ERAB Drop due to HO Exe failure",
                                                       SUM("L06_ERAB Drop due to HO Preparation")               AS "L06_ERAB Drop due to HO Preparation",
                                                       SUM("L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail")  AS "L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail",
                                                       SUM("L06_ERAB Drop due to UE Pre-emption")               AS "L06_ERAB Drop due to UE Pre-emption",
                                                       SUM("L07_Packet Loss Rate DL (%) Num")                   AS "L07_Packet Loss Rate DL (%) Num",
                                                       SUM("L07_Packet Loss Rate DL (%) Denom")                 AS "L07_Packet Loss Rate DL (%) Denom",
                                                       SUM("L08_Packet Loss Rate UL (%) Num")                   AS "L08_Packet Loss Rate UL (%) Num",
                                                       SUM("L08_Packet Loss Rate UL (%) Denom")                 AS "L08_Packet Loss Rate UL (%) Denom",
                                                       SUM("L09_IAF HO Prep SR (%) Num")                        AS "L09_IAF HO Prep SR (%) Num",
                                                       SUM("L09_IAF HO Prep SR (%) Denom")                      AS "L09_IAF HO Prep SR (%) Denom",
                                                       SUM("L09_IAF HO Exe SR (%) Num")                         AS "L09_IAF HO Exe SR (%) Num",
                                                       SUM("L09_IAF HO Exe SR (%) Denom")                       AS "L09_IAF HO Exe SR (%) Denom",
                                                       SUM("L10_IEF HO Prep SR (%) Num")                        AS "L10_IEF HO Prep SR (%) Num",
                                                       SUM("L10_IEF HO Prep SR (%) Denom")                      AS "L10_IEF HO Prep SR (%) Denom",
                                                       SUM("L10_IEF HO Exe SR (%) Num")                         AS "L10_IEF HO Exe SR (%) Num",
                                                       SUM("L10_IEF HO Exe SR (%) Denom")                       AS "L10_IEF HO Exe SR (%) Denom",
                                                       SUM("L11_Intra LTE HO Prep SR (%) Num")                  AS "L11_Intra LTE HO Prep SR (%) Num",
                                                       SUM("L11_Intra LTE HO Prep SR (%) Denom")                AS "L11_Intra LTE HO Prep SR (%) Denom",
                                                       SUM("L11_Intra LTE HO Exe SR (%) Num")                   AS "L11_Intra LTE HO Exe SR (%) Num",
                                                       SUM("L11_Intra LTE HO Exe SR (%) Denom")                 AS "L11_Intra LTE HO Exe SR (%) Denom",
                                                       SUM("L12_IRAT HOSR (%) Num")                             AS "L12_IRAT HOSR (%) Num",
                                                       SUM("L12_IRAT HOSR (%) Denom")                           AS "L12_IRAT HOSR (%) Denom",
                                                       SUM("L13_Cell NotAvail (%) Num")                         AS "L13_Cell NotAvail (%) Num",
                                                       SUM("L13_Cell NotAvail (%) Denom")                       AS "L13_Cell NotAvail (%) Denom",
                                                       SUM("L14_PSDL Trf (GB)")                                 AS "L14_PSDL Trf (GB)",
                                                       SUM("L14_PSUL Trf (GB)")                                 AS "L14_PSUL Trf (GB)",
                                                       SUM("L15_Integrity DL Latency (ms) Num")                 AS "L15_Integrity DL Latency (ms) Num",
                                                       SUM("L15_Integrity DL Latency (ms) Denom")               AS "L15_Integrity DL Latency (ms) Denom",
                                                       SUM("L16_Avg DL Thp Cell (Mbps) Num")                    AS "L16_Avg DL Thp Cell (Mbps) Num",
                                                       SUM("L16_Avg DL Thp Cell (Mbps) Denom")                  AS "L16_Avg DL Thp Cell (Mbps) Denom",
                                                       SUM("L17_Avg UL Thp Cell (Mbps) Num")                    AS "L17_Avg UL Thp Cell (Mbps) Num",
                                                       SUM("L17_Avg UL Thp Cell (Mbps) Denom")                  AS "L17_Avg UL Thp Cell (Mbps) Denom",
                                                       SUM("L18_Avg DL Thp User (Mbps) Num")                    AS "L18_Avg DL Thp User (Mbps) Num",
                                                       SUM("L18_Avg DL Thp User (Mbps) Denom")                  AS "L18_Avg DL Thp User (Mbps) Denom",
                                                       SUM("L19_Avg UL Thp User (Mbps) Num")                    AS "L19_Avg UL Thp User (Mbps) Num",
                                                       SUM("L19_Avg UL Thp User (Mbps) Denom")                  AS "L19_Avg UL Thp User (Mbps) Denom",
                                                       SUM("L23_Avg UL Interference PUSCH (dBm) Num")           AS "L23_Avg UL Interference PUSCH (dBm) Num",
                                                       SUM("L23_Avg UL Interference PUSCH (dBm) Denom")         AS "L23_Avg UL Interference PUSCH (dBm) Denom",
                                                       SUM("L23_Avg UL Interference PUCCH (dBm) Num")           AS "L23_Avg UL Interference PUCCH (dBm) Num",
                                                       SUM("L23_Avg UL Interference PUCCH (dBm) Denom")         AS "L23_Avg UL Interference PUCCH (dBm) Denom",
                                                       SUM("L24_PRB Util DL (%) Num")                           AS "L24_PRB Util DL (%) Num",
                                                       SUM("L24_PRB Util DL (%) Denom")                         AS "L24_PRB Util DL (%) Denom",
                                                       SUM("L25_PRB Util UL (%) Num")                           AS "L25_PRB Util UL (%) Num",
                                                       SUM("L25_PRB Util UL (%) Denom")                         AS "L25_PRB Util UL (%) Denom",
                                                       SUM("L26_CA User (#) Num")                               AS "L26_CA User (#) Num",
                                                       SUM("L26_CA User (#) Denom")                             AS "L26_CA User (#) Denom",
                                                       SUM("L27_CA Capable User (#) Num")                       AS "L27_CA Capable User (#) Num",
                                                       SUM("L27_CA Capable User (#) Denom")                     AS "L27_CA Capable User (#) Denom",
                                                       SUM("L29_CA Thpt (Mbps) Num")                            AS "L29_CA Thpt (Mbps) Num",
                                                       SUM("L29_CA Thpt (Mbps) Denom")                          AS "L29_CA Thpt (Mbps) Denom",
                                                       SUM("L30_DL BLER (%) Num")                               AS "L30_DL BLER (%) Num",
                                                       SUM("L30_DL BLER (%) Denom")                             AS "L30_DL BLER (%) Denom",
                                                       SUM("L31_UL BLER (%) Num")                               AS "L31_UL BLER (%) Num",
                                                       SUM("L31_UL BLER (%) Denom")                             AS "L31_UL BLER (%) Denom",
                                                       SUM("L32_Modulation DL QPSK (#)")                        AS "L32_Modulation DL QPSK (#)",
                                                       SUM("L32_Modulation DL 16QAM (#)")                       AS "L32_Modulation DL 16QAM (#)",
                                                       SUM("L32_Modulation DL 64QAM (#)")                       AS "L32_Modulation DL 64QAM (#)",
                                                       SUM("L32_Modulation DL 256QAM (#)")                      AS "L32_Modulation DL 256QAM (#)",
                                                       SUM("L33_Average CQI (#) Num")                           AS "L33_Average CQI (#) Num",
                                                       SUM("L33_Average CQI (#) Denom")                         AS "L33_Average CQI (#) Denom",
                                                       SUM("L34_Average RSRP (dBm) Num")                        AS "L34_Average RSRP (dBm) Num",
                                                       SUM("L34_Average RSRP (dBm) Denom")                      AS "L34_Average RSRP (dBm) Denom",
                                                       SUM("L35_RSRP <-110 dBm (%) Num")                        AS "L35_RSRP <-110 dBm (%) Num",
                                                       SUM("L35_RSRP <-110 dBm (%) Denom")                      AS "L35_RSRP <-110 dBm (%) Denom",
                                                       SUM("L36_Average SINR PUSCH (dB) Num")                   AS "L36_Average SINR PUSCH (dB) Num",
                                                       SUM("L36_Average SINR PUSCH (dB) Denom")                 AS "L36_Average SINR PUSCH (dB) Denom",
                                                       SUM("L36_Average SINR PUCCH (dB) Num")                   AS "L36_Average SINR PUCCH (dB) Num",
                                                       SUM("L36_Average SINR PUCCH (dB) Denom")                 AS "L36_Average SINR PUCCH (dB) Denom",
                                                       SUM("L37_Spectral Efficiency (Bit/s/Hz) Num")            AS "L37_Spectral Efficiency (Bit/s/Hz) Num",
                                                       SUM("L37_Spectral Efficiency (Bit/s/Hz) Denom")          AS "L37_Spectral Efficiency (Bit/s/Hz) Denom",
                                                       SUM("S01_Accessibility SIP QCI5 (%) Num")                AS "S01_Accessibility SIP QCI5 (%) Num",
                                                       SUM("S01_Accessibility SIP QCI5 (%) Denom")              AS "S01_Accessibility SIP QCI5 (%) Denom",
                                                       SUM("S01_Retainability SIP QCI5 (%) Num")                AS "S01_Retainability SIP QCI5 (%) Num",
                                                       SUM("S01_Retainability SIP QCI5 (%) Denom")              AS "S01_Retainability SIP QCI5 (%) Denom",
                                                       SUM("S01_RRC Re-estab SR QCI5 (%) Num")                  AS "S01_RRC Re-estab SR QCI5 (%) Num",
                                                       SUM("S01_RRC Re-estab SR QCI5 (%) Denom")                AS "S01_RRC Re-estab SR QCI5 (%) Denom",
                                                       SUM("V01_E-RAB Establisment SR QCI1 (%) Num")            AS "V01_E-RAB Establisment SR QCI1 (%) Num",
                                                       SUM("V01_E-RAB Establisment SR QCI1 (%) Denom")          AS "V01_E-RAB Establisment SR QCI1 (%) Denom",
                                                       SUM("V02_E-RAB Retainability QCI1 (%) Num")              AS "V02_E-RAB Retainability QCI1 (%) Num",
                                                       SUM("V02_E-RAB Retainability QCI1 (%) Denom")            AS "V02_E-RAB Retainability QCI1 (%) Denom",
                                                       SUM("V02_VoLTE Drop due to Cell Down Time")              AS "V02_VoLTE Drop due to Cell Down Time",
                                                       SUM("V02_VoLTE Drop due to contact with UE lost")        AS "V02_VoLTE Drop due to contact with UE lost",
                                                       SUM("V02_VoLTE Drop due to HO Exe Failure")              AS "V02_VoLTE Drop due to HO Exe Failure",
                                                       SUM("V02_VoLTE Drop due to HO Preparation")              AS "V02_VoLTE Drop due to HO Preparation",
                                                       SUM("V02_VoLTE Drop due to part. ERAB path switch fail") AS "V02_VoLTE Drop due to part. ERAB path switch fail",
                                                       SUM("V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail") AS "V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail",
                                                       SUM("V03_RRC Re-estab SR QCI1 (%) Num")                  AS "V03_RRC Re-estab SR QCI1 (%) Num",
                                                       SUM("V03_RRC Re-estab SR QCI1 (%) Denom")                AS "V03_RRC Re-estab SR QCI1 (%) Denom",
                                                       SUM("V04_VoLTE Traffic (Erlang)")                        AS "V04_VoLTE Traffic (Erlang)",
                                                       SUM("V05_VoLTE User (#)")                                AS "V05_VoLTE User (#)",
                                                       SUM("V06_VoLTE Packet Loss DL (%) Num")                  AS "V06_VoLTE Packet Loss DL (%) Num",
                                                       SUM("V06_VoLTE Packet Loss DL (%) Denom")                AS "V06_VoLTE Packet Loss DL (%) Denom",
                                                       SUM("V07_VoLTE Packet Loss UL (%) Num")                  AS "V07_VoLTE Packet Loss UL (%) Num",
                                                       SUM("V07_VoLTE Packet Loss UL (%) Denom")                AS "V07_VoLTE Packet Loss UL (%) Denom",
                                                       SUM("V08_VoLTE IAF HO SR (%) Num")                       AS "V08_VoLTE IAF HO SR (%) Num",
                                                       SUM("V08_VoLTE IAF HO SR (%) Denom")                     AS "V08_VoLTE IAF HO SR (%) Denom",
                                                       SUM("V09_VoLTE IEF HO SR (%) Num")                       AS "V09_VoLTE IEF HO SR (%) Num",
                                                       SUM("V09_VoLTE IEF HO SR (%) Denom")                     AS "V09_VoLTE IEF HO SR (%) Denom",
                                                       SUM("V10_RRC RwR CSFB L2G (#)")                          AS "V10_RRC RwR CSFB L2G (#)",
                                                       SUM("V10_RRC RwR CSFB L2U (#)")                          AS "V10_RRC RwR CSFB L2U (#)",
                                                       SUM("V10_CSFB Indicators Received (#)")                  AS "V10_CSFB Indicators Received (#)",
                                                       SUM("V10_RRC RwR SC L2G (#)")                            AS "V10_RRC RwR SC L2G (#)",
                                                       SUM("V10_RRC RwR SC L2U (#)")                            AS "V10_RRC RwR SC L2U (#)",
                                                       SUM("V11_VoLTE Integrity DL Latency (ms) Num")           AS "V11_VoLTE Integrity DL Latency (ms) Num",
                                                       SUM("V11_VoLTE Integrity DL Latency (ms) Denom")         AS "V11_VoLTE Integrity DL Latency (ms) Denom",
                                                       SUM("V12_VoLTE Integrity Cell (%) Num")                  AS "V12_VoLTE Integrity Cell (%) Num",
                                                       SUM("V12_VoLTE Integrity Cell (%) Denom")                AS "V12_VoLTE Integrity Cell (%) Denom",
                                                       SUM("V13_VoLTE Integrity UE (%) Num")                    AS "V13_VoLTE Integrity UE (%) Num",
                                                       SUM("V13_VoLTE Integrity UE (%) Denom")                  AS "V13_VoLTE Integrity UE (%) Denom",
                                                       SUM("V14_DL Silent exp per VoLTE user (ms) Num")         AS "V14_DL Silent exp per VoLTE user (ms) Num",
                                                       SUM("V14_DL Silent exp per VoLTE user (ms) Denom")       AS "V14_DL Silent exp per VoLTE user (ms) Denom",
                                                       SUM("V15_UL Silent exp per VoLTE user (ms) Num")         AS "V15_UL Silent exp per VoLTE user (ms) Num",
                                                       SUM("V15_UL Silent exp per VoLTE user (ms) Denom")       AS "V15_UL Silent exp per VoLTE user (ms) Denom",
                                                       SUM("V16_SRVCC HO to GERAN Prep SR (%) Num")             AS "V16_SRVCC HO to GERAN Prep SR (%) Num",
                                                       SUM("V16_SRVCC HO to GERAN Prep SR (%) Denom")           AS "V16_SRVCC HO to GERAN Prep SR (%) Denom",
                                                       SUM("V16_SRVCC HO to GERAN Exe SR (%) Num")              AS "V16_SRVCC HO to GERAN Exe SR (%) Num",
                                                       SUM("V16_SRVCC HO to GERAN Exe SR (%) Denom")            AS "V16_SRVCC HO to GERAN Exe SR (%) Denom",
                                                       SUM("V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0")       AS "V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0",
                                                       SUM("V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0")     AS "V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0",
                                                       SUM("V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0")        AS "V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0",
                                                       SUM("V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0")      AS "V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0",
                                                       SUM("V18_SRVCC HO to UTRAN Prep SR (%) Num")             AS "V18_SRVCC HO to UTRAN Prep SR (%) Num",
                                                       SUM("V18_SRVCC HO to UTRAN Prep SR (%) Denom")           AS "V18_SRVCC HO to UTRAN Prep SR (%) Denom",
                                                       SUM("V18_SRVCC HO to UTRAN Exe SR (%) Num")              AS "V18_SRVCC HO to UTRAN Exe SR (%) Num",
                                                       SUM("V18_SRVCC HO to UTRAN Exe SR (%) Denom")            AS "V18_SRVCC HO to UTRAN Exe SR (%) Denom",
                                                       SUM("L20_Active UE User (#)")                            AS "L20_Active UE User (#)",
                                                       SUM("L21_Avg RRC User (#)")                              AS "L21_Avg RRC User (#)",
                                                       SUM("L21_Max RRC User (#)")                              AS "L21_Max RRC User (#)",
                                                       SUM("L22_Avg ERAB User (#)")                             AS "L22_Avg ERAB User (#)",
                                                       SUM("L22_Max ERAB User (#)")                             AS "L22_Max ERAB User (#)",
                                                       SUM("L26_CA User (#)")                                   AS "L26_CA User (#)",
                                                       SUM("L27_CA Capable User (#)")                           AS "L27_CA Capable User (#)",
                                                       SUM("V10_CSFB Initiation Success Rate (%) Num")          AS "V10_CSFB Initiation Success Rate (%) Num",
                                                       SUM("V10_CSFB Indicators Received (#) Denom")            AS "V10_CSFB Indicators Received (#) Denom",
                                                       SUM("V19_L2G SRVCC (%) Num")                             AS "V19_L2G SRVCC (%) Num",
                                                       SUM("V19_L2G SRVCC (%) Denom")                           AS "V19_L2G SRVCC (%) Denom",
                                                       SUM("V20_CSFB 2G (%) Num")                               AS "V20_CSFB 2G (%) Num",
                                                       SUM("V20_CSFB 2G (%) Denom")                             AS "V20_CSFB 2G (%) Denom",
                                                       SUM("V21_VoLTE UL Audio Gap < 6s (%) Num")               AS "V21_VoLTE UL Audio Gap < 6s (%) Num",
                                                       SUM("V21_VoLTE UL Audio Gap < 6s (%) Denom")             AS "V21_VoLTE UL Audio Gap < 6s (%) Denom"
                                                       -- </editor-fold>
                                                FROM celcom.stats.lte_aggregates_columns as t1
                                                WHERE ${filterConditions()}
                                                GROUP BY "Date")
                             SELECT "Date":: varchar(10)                                             as "Date",
                   
                   -- <editor-fold desc="LTE COUNTERS"> "L02_RRC CSSR (%) Num",
                                    "L02_RRC CSSR (%) Denom",
                                    "L03_RRC CSSR Serv (%) Num",
                                    "L03_RRC CSSR Serv (%) Denom",
                                    "L04_ERAB CSSR (%) Num",
                                    "L04_ERAB CSSR (%) Denom",
                                    "L05_ERAB DR (%) Num",
                                    "L05_ERAB DR (%) Denom",
                                    "L06_ERAB DR (%) Num",
                                    "L06_ERAB DR (%) Denom",
                                    "L06_ERAB Drop due to Cell Down Time",
                                    "L06_ERAB Drop due to Cell Down Time (PNR)",
                                    "L06_ERAB Drop due to contact with UE lost",
                                    "L06_ERAB Drop due to HO Exe failure",
                                    "L06_ERAB Drop due to HO Preparation",
                                    "L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail",
                                    "L06_ERAB Drop due to UE Pre-emption",
                                    "L07_Packet Loss Rate DL (%) Num",
                                    "L07_Packet Loss Rate DL (%) Denom",
                                    "L08_Packet Loss Rate UL (%) Num",
                                    "L08_Packet Loss Rate UL (%) Denom",
                                    "L09_IAF HO Prep SR (%) Num",
                                    "L09_IAF HO Prep SR (%) Denom",
                                    "L09_IAF HO Exe SR (%) Num",
                                    "L09_IAF HO Exe SR (%) Denom",
                                    "L10_IEF HO Prep SR (%) Num",
                                    "L10_IEF HO Prep SR (%) Denom",
                                    "L10_IEF HO Exe SR (%) Num",
                                    "L10_IEF HO Exe SR (%) Denom",
                                    "L11_Intra LTE HO Prep SR (%) Num",
                                    "L11_Intra LTE HO Prep SR (%) Denom",
                                    "L11_Intra LTE HO Exe SR (%) Num",
                                    "L11_Intra LTE HO Exe SR (%) Denom",
                                    "L12_IRAT HOSR (%) Num",
                                    "L12_IRAT HOSR (%) Denom",
                                    "L13_Cell NotAvail (%) Num",
                                    "L13_Cell NotAvail (%) Denom",
                                    "L14_PSDL Trf (GB)",
                                    "L14_PSUL Trf (GB)",
                                    "L15_Integrity DL Latency (ms) Num",
                                    "L15_Integrity DL Latency (ms) Denom",
                                    "L16_Avg DL Thp Cell (Mbps) Num",
                                    "L16_Avg DL Thp Cell (Mbps) Denom",
                                    "L17_Avg UL Thp Cell (Mbps) Num",
                                    "L17_Avg UL Thp Cell (Mbps) Denom",
                                    "L18_Avg DL Thp User (Mbps) Num",
                                    "L18_Avg DL Thp User (Mbps) Denom",
                                    "L19_Avg UL Thp User (Mbps) Num",
                                    "L19_Avg UL Thp User (Mbps) Denom",
                                    "L23_Avg UL Interference PUSCH (dBm) Num",
                                    "L23_Avg UL Interference PUSCH (dBm) Denom",
                                    "L23_Avg UL Interference PUCCH (dBm) Num",
                                    "L23_Avg UL Interference PUCCH (dBm) Denom",
                                    "L24_PRB Util DL (%) Num",
                                    "L24_PRB Util DL (%) Denom",
                                    "L25_PRB Util UL (%) Num",
                                    "L25_PRB Util UL (%) Denom",
                                    "L26_CA User (#) Num",
                                    "L26_CA User (#) Denom",
                                    "L27_CA Capable User (#) Num",
                                    "L27_CA Capable User (#) Denom",
                                    "L29_CA Thpt (Mbps) Num",
                                    "L29_CA Thpt (Mbps) Denom",
                                    "L30_DL BLER (%) Num",
                                    "L30_DL BLER (%) Denom",
                                    "L31_UL BLER (%) Num",
                                    "L31_UL BLER (%) Denom",
                                    "L32_Modulation DL QPSK (#)",
                                    "L32_Modulation DL 16QAM (#)",
                                    "L32_Modulation DL 64QAM (#)",
                                    "L32_Modulation DL 256QAM (#)",
                                    "L33_Average CQI (#) Num",
                                    "L33_Average CQI (#) Denom",
                                    "L34_Average RSRP (dBm) Num",
                                    "L34_Average RSRP (dBm) Denom",
                                    "L35_RSRP <-110 dBm (%) Num",
                                    "L35_RSRP <-110 dBm (%) Denom",
                                    "L36_Average SINR PUSCH (dB) Num",
                                    "L36_Average SINR PUSCH (dB) Denom",
                                    "L36_Average SINR PUCCH (dB) Num",
                                    "L36_Average SINR PUCCH (dB) Denom",
                                    "L37_Spectral Efficiency (Bit/s/Hz) Num",
                                    "L37_Spectral Efficiency (Bit/s/Hz) Denom",
                                    "S01_Accessibility SIP QCI5 (%) Num",
                                    "S01_Accessibility SIP QCI5 (%) Denom",
                                    "S01_Retainability SIP QCI5 (%) Num",
                                    "S01_Retainability SIP QCI5 (%) Denom",
                                    "S01_RRC Re-estab SR QCI5 (%) Num",
                                    "S01_RRC Re-estab SR QCI5 (%) Denom",
                                    "V01_E-RAB Establisment SR QCI1 (%) Num",
                                    "V01_E-RAB Establisment SR QCI1 (%) Denom",
                                    "V02_E-RAB Retainability QCI1 (%) Num",
                                    "V02_E-RAB Retainability QCI1 (%) Denom",
                                    "V02_VoLTE Drop due to Cell Down Time",
                                    "V02_VoLTE Drop due to contact with UE lost",
                                    "V02_VoLTE Drop due to HO Exe Failure",
                                    "V02_VoLTE Drop due to HO Preparation",
                                    "V02_VoLTE Drop due to part. ERAB path switch fail",
                                    "V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail",
                                    "V03_RRC Re-estab SR QCI1 (%) Num",
                                    "V03_RRC Re-estab SR QCI1 (%) Denom",
                                    "V04_VoLTE Traffic (Erlang)",
                                    "V05_VoLTE User (#)",
                                    "V06_VoLTE Packet Loss DL (%) Num",
                                    "V06_VoLTE Packet Loss DL (%) Denom",
                                    "V07_VoLTE Packet Loss UL (%) Num",
                                    "V07_VoLTE Packet Loss UL (%) Denom",
                                    "V08_VoLTE IAF HO SR (%) Num",
                                    "V08_VoLTE IAF HO SR (%) Denom",
                                    "V09_VoLTE IEF HO SR (%) Num",
                                    "V09_VoLTE IEF HO SR (%) Denom",
                                    "V10_RRC RwR CSFB L2G (#)",
                                    "V10_RRC RwR CSFB L2U (#)",
                                    "V10_CSFB Indicators Received (#)",
                                    "V10_RRC RwR SC L2G (#)",
                                    "V10_RRC RwR SC L2U (#)",
                                    "V11_VoLTE Integrity DL Latency (ms) Num",
                                    "V11_VoLTE Integrity DL Latency (ms) Denom",
                                    "V12_VoLTE Integrity Cell (%) Num",
                                    "V12_VoLTE Integrity Cell (%) Denom",
                                    "V13_VoLTE Integrity UE (%) Num",
                                    "V13_VoLTE Integrity UE (%) Denom",
                                    "V14_DL Silent exp per VoLTE user (ms) Num",
                                    "V14_DL Silent exp per VoLTE user (ms) Denom",
                                    "V15_UL Silent exp per VoLTE user (ms) Num",
                                    "V15_UL Silent exp per VoLTE user (ms) Denom",
                                    "V16_SRVCC HO to GERAN Prep SR (%) Num",
                                    "V16_SRVCC HO to GERAN Prep SR (%) Denom",
                                    "V16_SRVCC HO to GERAN Exe SR (%) Num",
                                    "V16_SRVCC HO to GERAN Exe SR (%) Denom",
                                    "V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0",
                                    "V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0",
                                    "V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0",
                                    "V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0",
                                    "V18_SRVCC HO to UTRAN Prep SR (%) Num",
                                    "V18_SRVCC HO to UTRAN Prep SR (%) Denom",
                                    "V18_SRVCC HO to UTRAN Exe SR (%) Num",
                                    "V18_SRVCC HO to UTRAN Exe SR (%) Denom",
                                    "L20_Active UE User (#)",
                                    "L21_Avg RRC User (#)",
                                    "L21_Max RRC User (#)",
                                    "L22_Avg ERAB User (#)",
                                    "L22_Max ERAB User (#)",
                                    "L26_CA User (#)",
                                    "L27_CA Capable User (#)",
                                    "V10_CSFB Initiation Success Rate (%) Num",
                                    "V10_CSFB Indicators Received (#) Denom",
                                    "V19_L2G SRVCC (%) Num",
                                    "V19_L2G SRVCC (%) Denom",
                                    "V20_CSFB 2G (%) Num",
                                    "V20_CSFB 2G (%) Denom",
                                    "V21_VoLTE UL Audio Gap < 6s (%) Num",
                                    "V21_VoLTE UL Audio Gap < 6s (%) Denom",
                                    -- </editor-fold>

                                    -- <editor-fold desc="LTE KPIS">
                                    ("L02_RRC CSSR (%) Num") / nullif(("L02_RRC CSSR (%) Denom"), 0) as "L02_RRC CSSR (%)",
                                    ("L03_RRC CSSR Serv (%) Num") /
                                    nullif(("L03_RRC CSSR Serv (%) Denom"), 0)                       as "L03_RRC CSSR Serv (%)",
                                    ("L04_ERAB CSSR (%) Num") /
                                    nullif(("L04_ERAB CSSR (%) Denom"), 0)                           as "L04_ERAB CSSR (%)",
                                    ("L05_ERAB DR (%) Num") /
                                    nullif(("L05_ERAB DR (%) Denom"), 0)                             as "L05_ERAB DR (%)",
                                    ("L06_ERAB DR (%) Num") /
                                    nullif(("L06_ERAB DR (%) Denom"), 0)                             as "L06_ERAB DR (%)",
                                    ("L07_Packet Loss Rate DL (%) Num") /
                                    nullif(("L07_Packet Loss Rate DL (%) Denom"), 0)                 as "L07_Packet Loss Rate DL (%)",
                                    ("L08_Packet Loss Rate UL (%) Num") /
                                    nullif(("L08_Packet Loss Rate UL (%) Denom"), 0)                 as "L08_Packet Loss Rate UL (%)",
                                    ("L09_IAF HO Prep SR (%) Num") /
                                    nullif(("L09_IAF HO Prep SR (%) Denom"), 0)                      as "L09_IAF HO Prep SR (%)",
                                    ("L09_IAF HO Exe SR (%) Num") /
                                    nullif(("L09_IAF HO Exe SR (%) Denom"), 0)                       as "L09_IAF HO Exe SR (%)",
                                    ("L10_IEF HO Prep SR (%) Num") /
                                    nullif(("L10_IEF HO Prep SR (%) Denom"), 0)                      as "L10_IEF HO Prep SR (%)",
                                    ("L10_IEF HO Exe SR (%) Num") /
                                    nullif(("L10_IEF HO Exe SR (%) Denom"), 0)                       as "L10_IEF HO Exe SR (%)",
                                    ("L11_Intra LTE HO Prep SR (%) Num") /
                                    nullif(("L11_Intra LTE HO Prep SR (%) Denom"), 0)                as "L11_Intra LTE HO Prep SR (%)",
                                    ("L11_Intra LTE HO Exe SR (%) Num") /
                                    nullif(("L11_Intra LTE HO Exe SR (%) Denom"), 0)                 as "L11_Intra LTE HO Exe SR (%)",
                                    ("L12_IRAT HOSR (%) Num") /
                                    nullif(("L12_IRAT HOSR (%) Denom"), 0)                           as "L12_IRAT HOSR (%)",
                                    ("L13_Cell NotAvail (%) Num") /
                                    nullif(("L13_Cell NotAvail (%) Denom"), 0)                       as "L13_Cell NotAvail (%)",
                                    ("L15_Integrity DL Latency (ms) Num") /
                                    nullif(("L15_Integrity DL Latency (ms) Denom"), 0)               as "L15_Integrity DL Latency (ms)",
                                    ("L16_Avg DL Thp Cell (Mbps) Num") /
                                    nullif(("L16_Avg DL Thp Cell (Mbps) Denom"), 0)                  as "L16_Avg DL Thp Cell (Mbps)",
                                    ("L17_Avg UL Thp Cell (Mbps) Num") /
                                    nullif(("L17_Avg UL Thp Cell (Mbps) Denom"), 0)                  as "L17_Avg UL Thp Cell (Mbps)",
                                    ("L18_Avg DL Thp User (Mbps) Num") /
                                    nullif(("L18_Avg DL Thp User (Mbps) Denom"), 0)                  as "L18_Avg DL Thp User (Mbps)",
                                    ("L19_Avg UL Thp User (Mbps) Num") /
                                    nullif(("L19_Avg UL Thp User (Mbps) Denom"), 0)                  as "L19_Avg UL Thp User (Mbps)",

                                    ("L20_Active UE User (#)")                                       as "L20_Active UE User (#)",
                                    ("L21_Avg RRC User (#)")                                         as "L21_Avg RRC User (#)",
                                    ("L21_Max RRC User (#)")                                         as "L21_Max RRC User (#)",
                                    ("L22_Avg ERAB User (#)")                                        as "L22_Avg ERAB User (#)",
                                    ("L22_Max ERAB User (#)")                                        as "L22_Max ERAB User (#)",

                                    ("L23_Avg UL Interference PUSCH (dBm) Num") /
                                    nullif(("L23_Avg UL Interference PUSCH (dBm) Denom"), 0)         as "L23_Avg UL Interference PUSCH (dBm)",
                                    ("L23_Avg UL Interference PUCCH (dBm) Num") /
                                    nullif(("L23_Avg UL Interference PUCCH (dBm) Denom"), 0)         as "L23_Avg UL Interference PUCCH (dBm)",
                                    ("L24_PRB Util DL (%) Num") /
                                    nullif(("L24_PRB Util DL (%) Denom"), 0)                         as "L24_PRB Util DL (%)",
                                    ("L25_PRB Util UL (%) Num") /
                                    nullif(("L25_PRB Util UL (%) Denom"), 0)                         as "L25_PRB Util UL (%)",


                                    ("L26_CA User (#)")                                              as "L26_CA User (#)",
                                    ("L27_CA Capable User (#)")                                      as "L27_CA Capable User (#)",


                                    ("L29_CA Thpt (Mbps) Num") /
                                    nullif(("L29_CA Thpt (Mbps) Denom"), 0)                          as "L29_CA Thpt (Mbps)",
                                    ("L30_DL BLER (%) Num") /
                                    nullif(("L30_DL BLER (%) Denom"), 0)                             as "L30_DL BLER (%)",
                                    ("L31_UL BLER (%) Num") /
                                    nullif(("L31_UL BLER (%) Denom"), 0)                             as "L31_UL BLER (%)",
                                    ("L33_Average CQI (#) Num") /
                                    nullif(("L33_Average CQI (#) Denom"), 0)                         as "L33_Average CQI (#)",
                                    ("L34_Average RSRP (dBm) Num") /
                                    nullif(("L34_Average RSRP (dBm) Denom"), 0)                      as "L34_Average RSRP (dBm)",
                                    ("L35_RSRP <-110 dBm (%) Num") /
                                    nullif(("L35_RSRP <-110 dBm (%) Denom"), 0)                      as "L35_RSRP <-110 dBm (%)",
                                    ("L36_Average SINR PUSCH (dB) Num") /
                                    nullif(("L36_Average SINR PUSCH (dB) Denom"), 0)                 as "L36_Average SINR PUSCH (dB)",
                                    ("L36_Average SINR PUCCH (dB) Num") /
                                    nullif(("L36_Average SINR PUCCH (dB) Denom"), 0)                 as "L36_Average SINR PUCCH (dB)",
                                    ("L37_Spectral Efficiency (Bit/s/Hz) Num") /
                                    nullif(("L37_Spectral Efficiency (Bit/s/Hz) Denom"), 0)          as "L37_Spectral Efficiency (Bit/s/Hz)",
                                    ("S01_Accessibility SIP QCI5 (%) Num") /
                                    nullif(("S01_Accessibility SIP QCI5 (%) Denom"), 0)              as "S01_Accessibility SIP QCI5 (%)",
                                    ("S01_Retainability SIP QCI5 (%) Num") /
                                    nullif(("S01_Retainability SIP QCI5 (%) Denom"), 0)              as "S01_Retainability SIP QCI5 (%)",
                                    ("S01_RRC Re-estab SR QCI5 (%) Num") /
                                    nullif(("S01_RRC Re-estab SR QCI5 (%) Denom"), 0)                as "S01_RRC Re-estab SR QCI5 (%)",
                                    ("V01_E-RAB Establisment SR QCI1 (%) Num") /
                                    nullif(("V01_E-RAB Establisment SR QCI1 (%) Denom"), 0)          as "V01_E-RAB Establisment SR QCI1 (%)",
                                    ("V02_E-RAB Retainability QCI1 (%) Num") /
                                    nullif(("V02_E-RAB Retainability QCI1 (%) Denom"), 0)            as "V02_E-RAB Retainability QCI1 (%)",
                                    ("V03_RRC Re-estab SR QCI1 (%) Num") /
                                    nullif(("V03_RRC Re-estab SR QCI1 (%) Denom"), 0)                as "V03_RRC Re-estab SR QCI1 (%)",
                                    ("V06_VoLTE Packet Loss DL (%) Num") /
                                    nullif(("V06_VoLTE Packet Loss DL (%) Denom"), 0)                as "V06_VoLTE Packet Loss DL (%)",
                                    ("V07_VoLTE Packet Loss UL (%) Num") /
                                    nullif(("V07_VoLTE Packet Loss UL (%) Denom"), 0)                as "V07_VoLTE Packet Loss UL (%)",
                                    ("V08_VoLTE IAF HO SR (%) Num") /
                                    nullif(("V08_VoLTE IAF HO SR (%) Denom"), 0)                     as "V08_VoLTE IAF HO SR (%)",
                                    ("V09_VoLTE IEF HO SR (%) Num") /
                                    nullif(("V09_VoLTE IEF HO SR (%) Denom"), 0)                     as "V09_VoLTE IEF HO SR (%)",
                                    ("V11_VoLTE Integrity DL Latency (ms) Num") /
                                    nullif(("V11_VoLTE Integrity DL Latency (ms) Denom"), 0)         as "V11_VoLTE Integrity DL Latency (ms)",
                                    ("V12_VoLTE Integrity Cell (%) Num") /
                                    nullif(("V12_VoLTE Integrity Cell (%) Denom"), 0)                as "V12_VoLTE Integrity Cell (%)",
                                    ("V13_VoLTE Integrity UE (%) Num") /
                                    nullif(("V13_VoLTE Integrity UE (%) Denom"), 0)                  as "V13_VoLTE Integrity UE (%)",
                                    ("V14_DL Silent exp per VoLTE user (ms) Num") /
                                    nullif(("V14_DL Silent exp per VoLTE user (ms) Denom"), 0)       as "V14_DL Silent exp per VoLTE user (ms)",
                                    ("V15_UL Silent exp per VoLTE user (ms) Num") /
                                    nullif(("V15_UL Silent exp per VoLTE user (ms) Denom"), 0)       as "V15_UL Silent exp per VoLTE user (ms)",
                                    ("V16_SRVCC HO to GERAN Prep SR (%) Num") /
                                    nullif(("V16_SRVCC HO to GERAN Prep SR (%) Denom"), 0)           as "V16_SRVCC HO to GERAN Prep SR (%)",
                                    ("V16_SRVCC HO to GERAN Exe SR (%) Num") /
                                    nullif(("V16_SRVCC HO to GERAN Exe SR (%) Denom"), 0)            as "V16_SRVCC HO to GERAN Exe SR (%)",
                                    ("V17_SRVCC HO to GERAN Prep SR (%) Num PLMN0") /
                                    nullif(("V17_SRVCC HO to GERAN Prep SR (%) Denom PLMN0"), 0)     as "V17_SRVCC HO to GERAN Prep SR (%) PLMN0",
                                    ("V17_SRVCC HO to GERAN Exe SR (%) Num PLMN0") /
                                    nullif(("V17_SRVCC HO to GERAN Exe SR (%) Denom PLMN0"), 0)      as "V17_SRVCC HO to GERAN Exe SR (%) PLMN0",
                                    ("V18_SRVCC HO to UTRAN Prep SR (%) Num") /
                                    nullif(("V18_SRVCC HO to UTRAN Prep SR (%) Denom"), 0)           as "V18_SRVCC HO to UTRAN Prep SR (%)",
                                    ("V18_SRVCC HO to UTRAN Exe SR (%) Num") /
                                    nullif(("V18_SRVCC HO to UTRAN Exe SR (%) Denom"), 0)            as "V18_SRVCC HO to UTRAN Exe SR (%)",
                                    ("L06_ERAB Drop due to Cell Down Time")                          AS "L06_ERAB Drop due to Cell Down Time",
                                    ("L06_ERAB Drop due to Cell Down Time (PNR)")                    AS "L06_ERAB Drop due to Cell Down Time (PNR)",
                                    ("L06_ERAB Drop due to contact with UE lost")                    AS "L06_ERAB Drop due to contact with UE lost",
                                    ("L06_ERAB Drop due to HO Exe failure")                          AS "L06_ERAB Drop due to HO Exe failure",
                                    ("L06_ERAB Drop due to HO Preparation")                          AS "L06_ERAB Drop due to HO Preparation",
                                    ("L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail")             AS "L06_ERAB Drop due to S1/X2 Down / Tn Res Unavail",
                                    ("L06_ERAB Drop due to UE Pre-emption")                          AS "L06_ERAB Drop due to UE Pre-emption",
                                    ("L14_PSDL Trf (GB)")                                            AS "L14_PSDL Trf (GB)",
                                    ("L14_PSUL Trf (GB)")                                            AS "L14_PSUL Trf (GB)",
                                    ("L32_Modulation DL QPSK (#)")                                   AS "L32_Modulation DL QPSK (#)",
                                    ("L32_Modulation DL 16QAM (#)")                                  AS "L32_Modulation DL 16QAM (#)",
                                    ("L32_Modulation DL 64QAM (#)")                                  AS "L32_Modulation DL 64QAM (#)",
                                    ("L32_Modulation DL 256QAM (#)")                                 AS "L32_Modulation DL 256QAM (#)",
                                    ("V02_VoLTE Drop due to Cell Down Time")                         AS "V02_VoLTE Drop due to Cell Down Time",
                                    ("V02_VoLTE Drop due to contact with UE lost")                   AS "V02_VoLTE Drop due to contact with UE lost",
                                    ("V02_VoLTE Drop due to HO Exe Failure")                         AS "V02_VoLTE Drop due to HO Exe Failure",
                                    ("V02_VoLTE Drop due to HO Preparation")                         AS "V02_VoLTE Drop due to HO Preparation",
                                    ("V02_VoLTE Drop due to part. ERAB path switch fail")            AS "V02_VoLTE Drop due to part. ERAB path switch fail",
                                    ("V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail")            AS "V02_VoLTE Drop due to S1/X2 Down / Tn Res Unavail",
                                    ("V04_VoLTE Traffic (Erlang)")                                   AS "V04_VoLTE Traffic (Erlang)",
                                    ("V05_VoLTE User (#)")                                           AS "V05_VoLTE User (#)",
                                    ("V10_RRC RwR CSFB L2G (#)")                                     AS "V10_RRC RwR CSFB L2G (#)",
                                    ("V10_RRC RwR CSFB L2U (#)")                                     AS "V10_RRC RwR CSFB L2U (#)",
                                    ("V10_CSFB Indicators Received (#)")                             AS "V10_CSFB Indicators Received (#)",
                                    ("V10_RRC RwR SC L2G (#)")                                       AS "V10_RRC RwR SC L2G (#)",
                                    ("V10_RRC RwR SC L2U (#)")                                       AS "V10_RRC RwR SC L2U (#)",
                                    ("V19_L2G SRVCC (%) Num") /
                                    nullif(("V19_L2G SRVCC (%) Denom"), 0)                           as "V19_L2G SRVCC (%)",
                                    ("V20_CSFB 2G (%) Num") /
                                    nullif(("V20_CSFB 2G (%) Denom"), 0)                             as "V20_CSFB 2G (%)",
                                    ("V21_VoLTE UL Audio Gap < 6s (%) Num") /
                                    nullif(("V21_VoLTE UL Audio Gap < 6s (%) Denom"), 0)             as "V21_VoLTE UL Audio Gap < 6s (%)"
                                    -- </editor-fold>

                             FROM LCOUNTERS
                             ORDER BY "Date"
    ;
    `;
    const sqlGSM = () => sql`With GCOUNTERS as (SELECT t1."Date",
                                                       -- <editor-fold desc="columns">
                                                       SUM("G01_Availability Cell (%) Num")         AS "G01_Availability Cell (%) Num",
                                                       SUM("G01_Availability Cell (%) Denom")       AS "G01_Availability Cell (%) Denom",
                                                       SUM("G02_Availability TCH (%) Num")          AS "G02_Availability TCH (%) Num",
                                                       SUM("G02_Availability TCH (%) Denom")        AS "G02_Availability TCH (%) Denom",
                                                       SUM("G03_CSSR SD Block Nom1")                AS "G03_CSSR SD Block Nom1",
                                                       SUM("G03_CSSR SD Block Denom1")              AS "G03_CSSR SD Block Denom1",
                                                       SUM("G03_CSSR SD Drop Nom2")                 AS "G03_CSSR SD Drop Nom2",
                                                       SUM("G03_CSSR SD Drop Denom2")               AS "G03_CSSR SD Drop Denom2",
                                                       SUM("G03_CSSR TCH Assign Fail Num3")         AS "G03_CSSR TCH Assign Fail Num3",
                                                       SUM("G03_CSSR TCH Assign Fail Denom3")       AS "G03_CSSR TCH Assign Fail Denom3",
                                                       SUM("G04_SD Blocked Rate (%) Num")           AS "G04_SD Blocked Rate (%) Num",
                                                       SUM("G04_SD Blocked Rate (%) Denom")         AS "G04_SD Blocked Rate (%) Denom",
                                                       SUM("G05_SD Drop Rate (%) Num")              AS "G05_SD Drop Rate (%) Num",
                                                       SUM("G05_SD Drop Rate (%) Denom")            AS "G05_SD Drop Rate (%) Denom",
                                                       SUM("G10_PDCH Block (#)")                    AS "G10_PDCH Block (#)",
                                                       SUM("G11_DL TBF Establishment SR (%) Num")   AS "G11_DL TBF Establishment SR (%) Num",
                                                       SUM("G11_DL TBF Establishment SR (%) Denom") AS "G11_DL TBF Establishment SR (%) Denom",
                                                       SUM("G12_UL TBF Establishment SR (%) Num")   AS "G12_UL TBF Establishment SR (%) Num",
                                                       SUM("G12_UL TBF Establishment SR (%) Denom") AS "G12_UL TBF Establishment SR (%) Denom",
                                                       SUM("G13_PS Drop Rate (%) Num")              AS "G13_PS Drop Rate (%) Num",
                                                       SUM("G13_PS Drop Rate (%) Denom")            AS "G13_PS Drop Rate (%) Denom",
                                                       SUM("G14_PS Traffic (GBytes)")               AS "G14_PS Traffic (GBytes)",
                                                       SUM("G15_TCH Traffic (Erl)")                 AS "G15_TCH Traffic (Erl)",
                                                       SUM("G16_SDCCH Traffic (Erl)")               AS "G16_SDCCH Traffic (Erl)",
                                                       SUM("G17_Handover SR (%) Num")               AS "G17_Handover SR (%) Num",
                                                       SUM("G17_Handover SR (%) Denom")             AS "G17_Handover SR (%) Denom",
                                                       SUM("G18_ICM Band 1 (%) Num")                AS "G18_ICM Band 1 (%) Num",
                                                       SUM("G19_ICM Band 2 (%) Num")                AS "G19_ICM Band 2 (%) Num",
                                                       SUM("G20_ICM Band 3 (%) Num")                AS "G20_ICM Band 3 (%) Num",
                                                       SUM("G21_ICM Band 4 (%) Num")                AS "G21_ICM Band 4 (%) Num",
                                                       SUM("G22_ICM Band 5 (%) Num")                AS "G22_ICM Band 5 (%) Num",
                                                       SUM("G23_Bad ICM (%) Num")                   AS "G23_Bad ICM (%) Num",
                                                       SUM("G24_ICM Band (#) Denom")                AS "G24_ICM Band (#) Denom",
                                                       SUM("G25_Rx Qual DL Good (%) Num")           AS "G25_Rx Qual DL Good (%) Num",
                                                       SUM("G26_Rx Qual DL Bad (%) Num")            AS "G26_Rx Qual DL Bad (%) Num",
                                                       SUM("G27_Rx Qual DL (#) Denom")              AS "G27_Rx Qual DL (#) Denom",
                                                       SUM("G28_Rx Qual UL Good (%) Num")           AS "G28_Rx Qual UL Good (%) Num",
                                                       SUM("G29_Rx Qual UL Bad (%) Num")            AS "G29_Rx Qual UL Bad (%) Num",
                                                       SUM("G30_RxQual UL (#) Denom")               AS "G30_RxQual UL (#) Denom",
                                                       SUM("G31_SQI Good DL (%) Num")               AS "G31_SQI Good DL (%) Num",
                                                       SUM("G32_SQI Accpt DL (%) Num")              AS "G32_SQI Accpt DL (%) Num",
                                                       SUM("G33_SQI Bad DL (%) Num")                AS "G33_SQI Bad DL (%) Num",
                                                       SUM("G34_SQI DL (#) Denom")                  AS "G34_SQI DL (#) Denom",
                                                       SUM("G35_SQI Good UL (%) Num")               AS "G35_SQI Good UL (%) Num",
                                                       SUM("G36_SQI Accpt UL (%) Num")              AS "G36_SQI Accpt UL (%) Num",
                                                       SUM("G37_SQI Bad UL (%) Num")                AS "G37_SQI Bad UL (%) Num",
                                                       SUM("G38_SQI UL (#) Denom")                  AS "G38_SQI UL (#) Denom",
                                                       SUM("G06_TCH Assignment SR (%) Denom")       AS "G06_TCH Assignment SR (%) Denom",
                                                       SUM("G06_TCH Assignment SR (%) Num")         AS "G06_TCH Assignment SR (%) Num",
                                                       SUM("G07_TCH Blocked Rate (%) Denom")        AS "G07_TCH Blocked Rate (%) Denom",
                                                       SUM("G07_TCH Blocked Rate (%) Num")          AS "G07_TCH Blocked Rate (%) Num",
                                                       SUM("G08_TCH Drop Rate (%) Denom")           AS "G08_TCH Drop Rate (%) Denom",
                                                       SUM("G08_TCH Drop Rate (%) Num")             AS "G08_TCH Drop Rate (%) Num",
                                                       SUM("G09_PDCH Establishment SR (%) Denom")   AS "G09_PDCH Establishment SR (%) Denom",
                                                       SUM("G09_PDCH Establishment SR (%) Num")     AS "G09_PDCH Establishment SR (%) Num",
                                                       SUM("G10_PDCH Congestion Rate (%) Denum")    AS "G10_PDCH Congestion Rate (%) Denum",
                                                       SUM("G10_PDCH Congestion Rate (%) Num")      AS "G10_PDCH Congestion Rate (%) Num",
                                                       SUM("G13_DL TBF Drop Rate (%) Denom")        AS "G13_DL TBF Drop Rate (%) Denom",
                                                       SUM("G13_DL TBF Drop Rate (%) Num")          AS "G13_DL TBF Drop Rate (%) Num",
                                                       SUM("G39_2G to 4G Fast Return (#)")          AS "G39_2G to 4G Fast Return (#)"
                                                       -- </editor-fold>
                                                FROM celcom.stats.gsm_aggregates_columns as t1
                                                WHERE ${filterConditions()}
                                                GROUP BY t1."Date")
                             SELECT "Date":: varchar(10)                                             as "Date",
                                    
                                    -- <editor-fold desc="GSM COUNTERS"> "G01_Availability Cell (%) Num",
                                    "G01_Availability Cell (%) Denom",
                                    "G02_Availability TCH (%) Num",
                                    "G02_Availability TCH (%) Denom",
                                    "G03_CSSR SD Block Nom1",
                                    "G03_CSSR SD Block Denom1",
                                    "G03_CSSR SD Drop Nom2",
                                    "G03_CSSR SD Drop Denom2",
                                    "G03_CSSR TCH Assign Fail Num3",
                                    "G03_CSSR TCH Assign Fail Denom3",
                                    "G04_SD Blocked Rate (%) Num",
                                    "G04_SD Blocked Rate (%) Denom",
                                    "G05_SD Drop Rate (%) Num",
                                    "G05_SD Drop Rate (%) Denom",
                                    "G10_PDCH Block (#)",
                                    "G11_DL TBF Establishment SR (%) Num",
                                    "G11_DL TBF Establishment SR (%) Denom",
                                    "G12_UL TBF Establishment SR (%) Num",
                                    "G12_UL TBF Establishment SR (%) Denom",
                                    "G13_PS Drop Rate (%) Num",
                                    "G13_PS Drop Rate (%) Denom",
                                    "G14_PS Traffic (GBytes)",
                                    "G15_TCH Traffic (Erl)",
                                    "G16_SDCCH Traffic (Erl)",
                                    "G17_Handover SR (%) Num",
                                    "G17_Handover SR (%) Denom",
                                    "G18_ICM Band 1 (%) Num",
                                    "G19_ICM Band 2 (%) Num",
                                    "G20_ICM Band 3 (%) Num",
                                    "G21_ICM Band 4 (%) Num",
                                    "G22_ICM Band 5 (%) Num",
                                    "G23_Bad ICM (%) Num",
                                    "G24_ICM Band (#) Denom",
                                    "G25_Rx Qual DL Good (%) Num",
                                    "G26_Rx Qual DL Bad (%) Num",
                                    "G27_Rx Qual DL (#) Denom",
                                    "G28_Rx Qual UL Good (%) Num",
                                    "G29_Rx Qual UL Bad (%) Num",
                                    "G30_RxQual UL (#) Denom",
                                    "G31_SQI Good DL (%) Num",
                                    "G32_SQI Accpt DL (%) Num",
                                    "G33_SQI Bad DL (%) Num",
                                    "G34_SQI DL (#) Denom",
                                    "G35_SQI Good UL (%) Num",
                                    "G36_SQI Accpt UL (%) Num",
                                    "G37_SQI Bad UL (%) Num",
                                    "G38_SQI UL (#) Denom",
                                    "G06_TCH Assignment SR (%) Denom",
                                    "G06_TCH Assignment SR (%) Num",
                                    "G07_TCH Blocked Rate (%) Denom",
                                    "G07_TCH Blocked Rate (%) Num",
                                    "G08_TCH Drop Rate (%) Denom",
                                    "G08_TCH Drop Rate (%) Num",
                                    "G09_PDCH Establishment SR (%) Denom",
                                    "G09_PDCH Establishment SR (%) Num",
                                    "G10_PDCH Congestion Rate (%) Denum",
                                    "G10_PDCH Congestion Rate (%) Num",
                                    "G13_DL TBF Drop Rate (%) Denom",
                                    "G13_DL TBF Drop Rate (%) Num",
                                    "G39_2G to 4G Fast Return (#)",
                                    -- </editor-fold>

                                    -- <editor-fold desc="GSM KPIs">
                                    1 - (("G01_Availability Cell (%) Num")) /
                                        nullif(("G01_Availability Cell (%) Denom"), 0)                       AS "G01_Availability Cell (%)",
                                    ("G02_Availability TCH (%) Num") /
                                    nullif(("G02_Availability TCH (%) Denom"), 0)                            AS "G02_Availability TCH (%)",
                                    (1 - (("G03_CSSR SD Block Nom1") / nullif(("G03_CSSR SD Block Denom1"), 0))) *
                                    (1 - (("G03_CSSR SD Drop Nom2") / nullif(("G03_CSSR SD Drop Denom2"), 0))) *
                                    (("G03_CSSR TCH Assign Fail Num3") /
                                     nullif(("G03_CSSR TCH Assign Fail Denom3"), 0))                         AS "G03_CSSR (%)",
                                    ("G04_SD Blocked Rate (%) Num") /
                                    nullif(("G04_SD Blocked Rate (%) Denom"), 0)                             AS "G04_SD Blocked Rate (%)",

                                    ("G05_SD Drop Rate (%) Num") /
                                    nullif(("G05_SD Drop Rate (%) Denom"), 0)                                AS "G05_SD Drop Rate (%)",

                                    --     NEW KPI
                                    ("G06_TCH Assignment SR (%) Num") /
                                    nullif(("G06_TCH Assignment SR (%) Denom"), 0)                           AS "G06_TCH Assignment SR (%)",

                                    --     UPDATED KPI
                                    ("G07_TCH Blocked Rate (%) Num") /
                                    nullif(("G07_TCH Blocked Rate (%) Denom"), 0)                            AS "G07_TCH Blocked Rate (%)",
                                    --     UPDATED KPI
                                    ("G08_TCH Drop Rate (%) Num") /
                                    nullif(("G08_TCH Drop Rate (%) Denom"), 0)                               AS "G08_TCH Drop Rate (%)",
                                    --     UPDATED KPI
                                    1 - ("G09_PDCH Establishment SR (%) Num") /
                                        nullif(("G09_PDCH Establishment SR (%) Denom"), 0)                   AS "G09_PDCH Establishment SR (%)",
                                    --     UPDATED KPI
                                    ("G10_PDCH Congestion Rate (%) Num") /
                                    nullif(("G10_PDCH Congestion Rate (%) Denum"), 0)                        AS "G10_PDCH Congestion Rate (%)",

                                    ("G10_PDCH Block (#)")                                                   AS "G10_PDCH Block (#)",

                                    ("G11_DL TBF Establishment SR (%) Num") /
                                    NULLIF(("G11_DL TBF Establishment SR (%) Denom"), 0)                     AS "G11_DL TBF Establishment SR (%)",

                                    ("G12_UL TBF Establishment SR (%) Num") /
                                    nullif(("G12_UL TBF Establishment SR (%) Denom"), 0)                     AS "G12_UL TBF Establishment SR (%)",

                                    ("G13_PS Drop Rate (%) Num") /
                                    nullif(("G13_PS Drop Rate (%) Denom"), 0)                                AS "G13_PS Drop Rate (%)",

                                    --     NEW KPI
                                    ("G13_DL TBF Drop Rate (%) Num") /
                                    nullif(("G13_DL TBF Drop Rate (%) Denom"), 0)                            as "G13_DL TBF Drop Rate (%)",

                                    ("G14_PS Traffic (GBytes)")                                              AS "G14_PS Traffic (GBytes)",
                                    ("G15_TCH Traffic (Erl)")                                                AS "G15_TCH Traffic (Erl)",
                                    ("G16_SDCCH Traffic (Erl)")                                              AS "G16_SDCCH Traffic (Erl)",
                                    ("G17_Handover SR (%) Num") /
                                    nullif(("G17_Handover SR (%) Denom"), 0)                                 AS "G17_Handover SR (%)",
                                    ("G18_ICM Band 1 (%) Num") /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G18_ICM Band 1 (%)",
                                    ("G19_ICM Band 2 (%) Num") /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G19_ICM Band 2 (%)",
                                    ("G20_ICM Band 3 (%) Num") /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G20_ICM Band 3 (%)",
                                    ("G21_ICM Band 4 (%) Num") /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G21_ICM Band 4 (%)",
                                    ("G22_ICM Band 5 (%) Num") /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G22_ICM Band 5 (%)",
                                    (("G20_ICM Band 3 (%) Num") + ("G21_ICM Band 4 (%) Num") +
                                     ("G22_ICM Band 5 (%) Num")) /
                                    nullif(("G24_ICM Band (#) Denom"), 0)                                    AS "G23_Bad ICM (%)",
                                    ("G25_Rx Qual DL Good (%) Num") /
                                    nullif(("G27_Rx Qual DL (#) Denom"), 0)                                  AS "G25_Rx Qual DL Good (%)",
                                    ("G26_Rx Qual DL Bad (%) Num") /
                                    nullif(("G27_Rx Qual DL (#) Denom"), 0)                                  AS "G26_Rx Qual DL Bad (%)",
                                    ("G28_Rx Qual UL Good (%) Num") /
                                    nullif(("G30_RxQual UL (#) Denom"), 0)                                   AS "G28_Rx Qual UL Good (%)",
                                    ("G29_Rx Qual UL Bad (%) Num") /
                                    nullif(("G30_RxQual UL (#) Denom"), 0)                                   AS "G29_Rx Qual UL Bad (%)",
                                    ("G31_SQI Good DL (%) Num") /
                                    nullif(("G34_SQI DL (#) Denom"), 0)                                      AS "G31_SQI Good DL (%)",
                                    ("G32_SQI Accpt DL (%) Num") /
                                    nullif(("G34_SQI DL (#) Denom"), 0)                                      AS "G32_SQI Accpt DL (%)",
                                    ("G33_SQI Bad DL (%) Num") /
                                    nullif(("G34_SQI DL (#) Denom"), 0)                                      AS "G33_SQI Bad DL (%)",
                                    ("G35_SQI Good UL (%) Num") /
                                    nullif(("G38_SQI UL (#) Denom"), 0)                                      AS "G35_SQI Good UL (%)",
                                    ("G36_SQI Accpt UL (%) Num") /
                                    nullif(("G38_SQI UL (#) Denom"), 0)                                      AS "G36_SQI Accpt UL (%)",
                                    ("G37_SQI Bad UL (%) Num") /
                                    nullif(("G38_SQI UL (#) Denom"), 0)                                      AS "G37_SQI Bad UL (%)",
                                    ("G39_2G to 4G Fast Return (#)")                                         as "G39_2G to 4G Fast Return (#)"
                                    -- </editor-fold>

                             FROM GCOUNTERS
    `;
    const results = tech === 'LTE' ? await sql`${sqlLTE()}` : await sql`${sqlGSM()}`;
    const endTime = new Date();

    console.log(`${(endTime - startTime) / 1000}s`);

    if (format === 'json') {
        response.status(200).json({
                success: true,
                data: results,
            }
        );
        return;
    }
    const {headers, values} = arrayToCsv(results, false);
    response.status(200).json({
            success: true,
            headers: headers.join('\t'),
            data: values.join('\n'),
        }
    );
}

module.exports = {
    getAggregatedStats,
    getAggregatedStatsWeek,
    getCellStats,
    getCellMapping,
    getGroupedCellsStats,
    excelTestFunc,
    getClusterStats
};
