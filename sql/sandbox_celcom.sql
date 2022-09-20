
Select current_database();

SET search_path =  stats, public;

-- alter table rfdb.rf_nominal
-- 	add last_user varchar(150);
--
-- alter table rfdb.rf_nominal
-- 	add last_update timestamp;
--
-- alter table rfdb.rf_nominal alter column last_update set default now();
--
--
--
--

create function notify_change() returns trigger
	language plpgsql
as $$
BEGIN
PERFORM pg_notify('db_change',
    row_to_json(NEW)::text);
RETURN null;
END;
$$;

alter function notify_change() owner to postgres;

create or replace function notify_new_data() returns trigger
	language plpgsql
as $$
BEGIN
PERFORM pg_notify('new_data',
    TG_TABLE_NAME);
RETURN null;
END;
$$;




DROP trigger new_data on lte_oss_raw_cell;

CREATE Table stats.test_data AS
    SELECT * FROM stats.lte_oss_raw_cell order by random()
LIMIT 10;


SET SEARCH_PATH TO stats, public;



create or replace function update_history() returns trigger
	language plpgsql
as $$
BEGIN
INSERT INTO stats.history (tbl_name, operation) VALUES (TG_TABLE_NAME, TG_OP);
RETURN null;
END;
$$;

DROP TRIGGER IF EXISTS new_data_all_cells on stats.all_cells;

CREATE TRIGGER new_data_all_cells
AFTER INSERT OR UPDATE OR DELETE
ON stats.all_cells
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_history();



SELECT current_database(), event_object_schema,event_object_table,trigger_name,event_manipulation,action_statement,action_timing FROM information_schema.triggers ORDER BY event_object_table,event_manipulation;


-- Generic trigger function, can be used for multiple triggers:
CREATE OR REPLACE FUNCTION trg_notify_after()
  RETURNS trigger
  LANGUAGE plpgsql AS
$$
BEGIN
   PERFORM pg_notify(TG_TABLE_NAME, TG_OP);
   RETURN NULL;
END;
$$;


CREATE TRIGGER history_test_data
AFTER INSERT OR UPDATE OR DELETE
ON stats.test_data
    FOR EACH STATEMENT
EXECUTE PROCEDURE update_history();

SELECT * FROM information_schema.columns WHERE table_name = 'gsm_aggregates_columns';




CREATE TRIGGER new_data2
AFTER UPDATE
ON celcom.stats.lte_aggregates_week
    REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE PROCEDURE celcom.stats.notify_new_data();