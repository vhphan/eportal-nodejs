
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

CREATE TRIGGER new_data
AFTER INSERT
ON stats.gsm_aggregates_week
    REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE PROCEDURE notify_new_data();



SELECT event_object_table,trigger_name,event_manipulation,action_statement,action_timing FROM information_schema.triggers ORDER BY event_object_table,event_manipulation;