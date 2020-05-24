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


# creates old demo tables for the gui

create table auto_covid_data.Demo
(
	Temp double not null,
	DateID int not null,
	StateID int not null,
	Total_Cases int not null,
	Total_Deaths int not null,
	Active_Cases int not null,
	Total_Cases_1m_POP int not null,
	Total_Deaths_1m_POP int not null,
	Total_Tests int not null,
	Total_Tests_1m_pop int not null,
	isDeleted tinyint(1) not null
);








  
 
