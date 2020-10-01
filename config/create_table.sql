DROP TABLE IF EXISTS 
	location,
	Indicator,
	Pop,
	Main,
	Report
cascade;

CREATE TABLE Location (
	FacilityCode varchar(32) UNIQUE,
	FacilityName varchar(255) NOT NULL,
	DistrictName varchar(255) NOT NULL,
	CONSTRAINT Location_pk PRIMARY KEY (FacilityCode)
);

CREATE TABLE Indicator(
	IndicatorCode varchar(32) UNIQUE,
	IndicatorName varchar(255) NOT NULL,
	CONSTRAINT Indicator_pk PRIMARY KEY (IndicatorCode)
);

CREATE TABLE Pop(
	ID serial NOT NULL, 
	DistrictName varchar(255),
	year smallint,
	Male int,
	Female int,
	Total int,
	Age smallint,
	CONSTRAINT Pop_pk PRIMARY KEY (ID)
);

CREATE TABLE Main(
	ID serial NOT NULL, 
	FacilityCode varchar(32),
	IndicatorCode varchar(32),
	year smallint,
	month varchar(32),
	value double precision,
	CONSTRAINT Main_pk PRIMARY KEY (ID)
);

CREATE TABLE Report(
	ID serial NOT NULL, 
	FacilityCode varchar(32),
	IndicatorCode varchar(32),
	year smallint,
	month varchar(32),
	value smallint,
	CONSTRAINT Report_pk PRIMARY KEY (ID)
);


ALTER TABLE Main ADD CONSTRAINT Main_fk1 FOREIGN KEY (FacilityCode) REFERENCES Location(FacilityCode);
ALTER TABLE Main ADD CONSTRAINT Main_fk2 FOREIGN KEY (IndicatorCode) REFERENCES Indicator(IndicatorCode);

ALTER TABLE Report ADD CONSTRAINT Report_fk1 FOREIGN KEY (FacilityCode) REFERENCES Location(FacilityCode);
ALTER TABLE Report ADD CONSTRAINT Report_fk2 FOREIGN KEY (IndicatorCode) REFERENCES Indicator(IndicatorCode);