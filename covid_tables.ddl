# Dylan DeWolfe
# Covid Research Final Project
# covid ddl 
# CPSC 408


## creates date key table

create table auto_covid_data.Date_Key
(
	DateID int not null
		primary key,
	Date longtext null
);


 ## creates date key table
 
 create table auto_covid_data.State_Key
(
	StateID int not null
		primary key,
	State varchar(36) null,
	Abbreviation varchar(2) null
);


## creates Demo Data

create table Demo1
(
	Temperature int null,
	DateID VARCHAR(32) null,
	StateID VARCHAR(32) null,
	Cases int null,
	Deaths int null
);

CREATE INDEX temp ON Demo1(Temperature);

CREATE INDEX da on Demo(DateID); 







  
 