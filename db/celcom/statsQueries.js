const sql = require('./PgJsBackend');
const {response} = require("express");
const {lteAggColumns} = require("./constants");

const arrayToCsv = (results, parseDate = true) => {
    const headers = Object.keys(results[0]);
    if (parseDate) {
        results.forEach(d => {
            d['Date'] = d['Date'].toISOString().split('T')[0]
        })
    }
    const values = results.map(d => Object.values(d).join('\t'));
    return {headers, values};
};

const getAggregatedStats = (tech) => async (request, response) => {

    let {page, size, format, startDate, endDate} = request.query;

    page = page === undefined ? 1 : parseInt(page);
    size = size === undefined ? 1000 : parseInt(size);
    startDate = startDate === undefined ? '2022-04-01' : startDate;
    endDate = endDate === undefined ? '2022-12-31' : endDate;
    format = format === undefined ? 'csv' : 'json';
    let table;
    switch (tech) {
        case 'GSM':
            table = 'celcom.stats.gsm_aggregates';
            break;
        case 'LTE':
        default:
            table = 'celcom.stats.lte_aggregates';
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
        totalPages = Math.ceil(parseInt(totalRecords[0]['k'] / size))
    }

    const results = await sql`
    SELECT * FROM ${sql(table)} WHERE "Date" is not null
    AND "Date" >= ${startDate}
    AND "Date" <= ${endDate}
    ORDER BY "Date", "id"
    LIMIT ${size} OFFSET ${(page - 1) * size}
    `

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
    let {page, size, format, startWeek, startYear, endWeek, endYear} = request.query;
    page = page === undefined ? 1 : parseInt(page);
    size = size === undefined ? 1000 : parseInt(size);
    format = format === undefined ? 'csv' : 'json';
    startYear = startYear === undefined ? 2022 : parseInt(startYear);
    endYear = endYear === undefined ? 2022 : parseInt(endYear);
    startWeek = startWeek === undefined ? 1 : parseInt(startWeek);
    endWeek = endWeek === undefined ? 52 : parseInt(endWeek);
    let table;
    switch (tech) {
        case 'GSM':
            table = 'celcom.stats.gsm_aggregates_week';
            break;
        case 'LTE':
        default:
            table = 'celcom.stats.lte_aggregates_week';
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
                SELECT "Date",
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
    }
    const {headers, values} = arrayToCsv(results);
    return response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
    });
};

const getCellMapping = (tech) => async (request, response) => {
    let {format} = request.query;
    format = format === undefined ? 'csv' : 'json';
    let table;
    switch (tech) {
        case 'GSM':
            table = 'celcom.stats.cell_mapping_gsm';
            break;
        case 'LTE':
        default:
            table = 'celcom.stats.cell_mapping';
            break;
    }
    const results = await sql`
    SELECT * FROM ${sql(table)}
    `;
    const {headers, values} = arrayToCsv(results, false);
    return response.status(200).json({
        success: true,
        headers: headers.join('\t'),
        data: format === 'csv' ? values.join('\n') : results,
    });

};

const getGroupedCellsStats = (tech) => async (request, response) => {
    let {cells, format, clusterName, startDate, endDate} = request.query;

    cells = cells.split(',');
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

module.exports = {
    getAggregatedStats,
    getAggregatedStatsWeek,
    getCellStats,
    getCellMapping,
    getGroupedCellsStats,
};
