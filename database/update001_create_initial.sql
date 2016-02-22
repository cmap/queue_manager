drop table if exists queue_type;
create table queue_type (
	id						integer primary key,
	name					text not null,
	description				text
);

drop table if exists queue;
create table queue (
	id				integer primary key,
	plate_id			text not null,
	queue_type_id			integer not null,
	datetime_added			timestamp not null default current_timestamp,
	priority			real not null default 100.0,
	is_being_processed		integer not null default 0,

	foreign key (plate_id) references plate(id),
	foreign key (queue_type_id) references queue_type(id),
	unique (plate_id, queue_type_id)
);

drop table if exists workflow_template;
create table workflow_template (
	id						integer primary key,
	name					text not null
);

drop table if exists workflow_template_pair;
create table workflow_template_pair (
	id						integer primary key,
	workflow_template_id	integer not null,
	prev_queue_type_id		integer not null,
	next_queue_type_id		integer not null,

	foreign key (workflow_template_id) references workflow_template(id),
	foreign key (prev_queue_type_id) references queue_type(id),
	foreign key (next_queue_type_id) references queue_type(id),
	unique (workflow_template_id, prev_queue_type_id, next_queue_type_id)
);

drop table if exists workflow;
create table workflow (
	id						integer primary key,
	plate_id				text not null,
	prev_queue_type_id		integer not null,
	next_queue_type_id		integer not null,

	foreign key(prev_queue_type_id) references queue_type(id),
	foreign key(next_queue_type_id) references queue_type(id),
	unique (plate_id, prev_queue_type_id, next_queue_type_id)
);
