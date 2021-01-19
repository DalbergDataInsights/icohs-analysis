CREATE TABLE public."indicator" (
	indicatorcode varchar(32) NOT NULL,
	indicatorname varchar(255) NOT NULL,
	CONSTRAINT indicator_pk PRIMARY KEY (indicatorcode)
);
CREATE TABLE public."location" (
	facilitycode varchar(32) NOT NULL,
	facilityname varchar(255) NOT NULL,
	districtname varchar(255) NOT NULL,
	CONSTRAINT location_pk PRIMARY KEY (facilitycode)
);
CREATE TABLE public.main (
	id serial NOT NULL,
	facilitycode varchar(32) NULL,
	indicatorcode varchar(32) NULL,
	"year" int2 NULL,
	"month" varchar(32) NULL,
	value float8 NULL,
	CONSTRAINT main_pk PRIMARY KEY (id),
	CONSTRAINT main_fk1 FOREIGN KEY (facilitycode) REFERENCES location(facilitycode),
	CONSTRAINT main_fk2 FOREIGN KEY (indicatorcode) REFERENCES indicator(indicatorcode)
);
CREATE TABLE public.pop (
	id serial NOT NULL,
	district_name varchar(255) NOT NULL,
	"year" int2 NULL,
	male int4 NULL,
	female int4 NULL,
	total int4 NULL,
	childbearing_age float8 NULL,
	pregnant float8 NULL,
	not_pregnant float8 NULL,
	births_estimated float8 NULL,
	u1 float8 NULL,
	u5 float8 NULL,
	u15 float8 NULL,
	suspect_tb float8 NULL,
	girls_10 float8 NULL,
	per_thousand float8 NULL,
	per_hundred_thousand float8 NULL,
	per_hundred_thousand_u5 float8 NULL
);
CREATE TABLE public.report (
	id serial NOT NULL,
	facilitycode varchar(32) NULL,
	indicatorcode varchar(32) NULL,
	"year" int2 NULL,
	"month" varchar(32) NULL,
	value int2 NULL,
	CONSTRAINT report_pk PRIMARY KEY (id),
	CONSTRAINT report_fk1 FOREIGN KEY (facilitycode) REFERENCES location(facilitycode),
	CONSTRAINT report_fk2 FOREIGN KEY (indicatorcode) REFERENCES indicator(indicatorcode)
);