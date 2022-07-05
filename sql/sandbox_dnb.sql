Select current_database();
SET search_path = stats, public;

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
