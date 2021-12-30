alter table rfdb.rf_nominal
	add last_user varchar(150);

alter table rfdb.rf_nominal
	add last_update timestamp;

alter table rfdb.rf_nominal alter column last_update set default now();





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

create trigger logging_trigger
    after insert
    on t_history
    for each row
execute procedure public.notify_change();