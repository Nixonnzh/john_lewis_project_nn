-- Edit database, schema, and end table in below queries as needed

-- Query to load status data from JLP labels table to STATUS table 
INSERT INTO JLP_BPS.INITIAL_SCHEMA.STATUSES
SELECT SUBSTRING("Status. Participant status", 1, CHARINDEX(' ', "Status. Participant status", 1) +1), 
SUBSTRING("Status. Participant status", CHARINDEX('''', "Status. Participant status", 1) +1, LEN("Status. Participant status") - CHARINDEX('''', "Status. Participant status", 1) -1)
FROM JLP_BPS.INITIAL_SCHEMA.JLP_LABELS
WHERE "Status. Participant status" IS NOT NULL
;

-- Query to load country data from JLP labels table to COUNTRIES table
INSERT INTO JLP_BPS.INITIAL_SCHEMA.COUNTRIES
SELECT SUBSTRING("Country from URL", 1, CHARINDEX(' ', "Country from URL", 2)), 
SUBSTRING("Country from URL", CHARINDEX('''', "Country from URL", 1) +1, LEN("Country from URL") - CHARINDEX('''', "Country from URL", 1) -1)
FROM JLP_BPS.INITIAL_SCHEMA.JLP_LABELS
WHERE "Country from URL" IS NOT NULL
;

-- Test queries to see if data has loaded correctly
SELECT * FROM JLP_BPS.INITIAL_SCHEMA.STATUSES;
SELECT * FROM JLP_BPS.INITIAL_SCHEMA.COUNTRIES;

------------------------------------------------------------------
/* IGNORE BELOW - statements to recreate the tables if necessary 
------------------------------------------------------------------

create or replace TABLE JLP_BPS.INITIAL_SCHEMA.COUNTRIES (
	COUNTRY_ID VARCHAR(8) NOT NULL,
	COUNTRY_NAME VARCHAR(25) NOT NULL,
	constraint PK_COUNTRIES primary key (COUNTRY_ID)
);

create or replace TABLE JLP_BPS.INITIAL_SCHEMA.STATUSES (
	STATUS VARCHAR(8) NOT NULL,
	STATUS_NAME VARCHAR(25) NOT NULL,
	constraint PK_STATUSES primary key (STATUS)
);

*/
