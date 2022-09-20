Select current_database();
SET search_path = stats_v3_hourly, public;

create or replace function notify_new_data() returns trigger
    language plpgsql
as
$$
BEGIN
    PERFORM pg_notify('new_data',
                      TG_TABLE_NAME);
    RETURN null;
END;
$$;

CREATE TRIGGER new_data
AFTER INSERT
ON stats_v3_hourly."NRCELLCU"
    REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE PROCEDURE notify_new_data();


CREATE TRIGGER new_data
AFTER INSERT
ON stats_v3_hourly."EUTRANCELLFDD"
    REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE PROCEDURE notify_new_data();


CREATE TRIGGER new_data
AFTER INSERT
ON stats_v3_hourly.test_data
    REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE PROCEDURE notify_new_data();


create table logging.history
(
    update_time timestamp default now(),
    tbl_name    varchar(100),
    num_rows    bigint,
    operation   varchar(50)
);

alter table logging.history
    owner to postgres;



CREATE TRIGGER new_data_cell_mapping
    AFTER INSERT OR UPDATE OR DELETE
    ON rfdb.cell_mapping
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_history();

SET search_path = rfdb;

-- DROP TRIGGER IF EXISTS new_data_tbl_4g_cell_info ON tbl_4g_cell_info;
-- DROP TRIGGER IF EXISTS new_data_tbl_5g_cell_info ON tbl_5g_cell_info;
-- DROP TRIGGER IF EXISTS new_data_tbl_ran_celldata_myedb ON tbl_ran_celldata_myedb;
-- DROP TRIGGER IF EXISTS new_data_tbl_ran_sectordata_myesite ON tbl_ran_sectordata_myesite;
-- DROP TRIGGER IF EXISTS new_data_tbl_ran_sitedata_myesite ON tbl_ran_sitedata_myesite;
-- DROP TRIGGER IF EXISTS new_data_cell_mapping ON cell_mapping;


drop function update_history();
create function update_history() returns trigger
    language plpgsql
as
$$
BEGIN
    INSERT INTO logging.history (tbl_name, operation) VALUES (TG_TABLE_NAME, TG_OP);
    RETURN null;
END;
$$;

SELECT current_database(),
       event_object_schema,
       event_object_table,
       trigger_name,
       event_manipulation,
       action_statement,
       action_timing
FROM information_schema.triggers
WHERE action_statement LIKE '%update_hist%'
ORDER BY event_object_table, event_manipulation;

-- drop table dnb.stats.history;
-- drop table dnb.rfdb.history;


CREATE TRIGGER history_cell_mapping
    AFTER INSERT OR UPDATE OR DELETE
    ON rfdb.cell_mapping
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_history();


SELECT (t1."DAY" + interval '1 hour' * t1."HOUR")::timestamp(0) without time zone AS "time",
       t1."Cluster_ID"                                          as object,
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
FROM dnb.stats_group_hourly."DataTableClusterKPI" as t1
         LEFT JOIN dnb.stats_group_hourly."CPULoadClusterKPI" as t2 on t1."Cluster_ID" = t2."Cluster_ID"
    AND t1."DAY" = t2."DAY"
    AND t1."HOUR" = t2."HOUR"
         LEFT JOIN dnb.stats_group_hourly."PacketLossClusterKPI" as t3 on t1."Cluster_ID" = t3."Cluster_ID"
    AND t1."DAY" = t3."DAY"
    AND t1."HOUR" = t3."HOUR"
WHERE t1."Cluster_ID" = 'PKUL_01'
ORDER BY t1."DAY", t1."HOUR";
