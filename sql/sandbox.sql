alter table rfdb.rf_nominal
	add last_user varchar(150);

alter table rfdb.rf_nominal
	add last_update timestamp;

alter table rfdb.rf_nominal alter column last_update set default now();