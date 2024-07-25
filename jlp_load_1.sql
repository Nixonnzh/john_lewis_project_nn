------------------------------------------------------------------- snowsql start
-- load jlp table in snowflake - load_jlp_csv_file_to_table.sql

/* snowsql -a accountname -u username */
/* EXAMPLE */
-- snowsql -a ed81217.uk-south.azure -u robbierichards

/* USE ROLE ROLENAME */
/* EXAMPLE */
/* USE ROLE ACCOUNTADMIN */

/* CREATE WAREHOUSE */
/* EXAMPLE */
/*
CREATE OR REPLACE WAREHOUSE SNOWSQL_WH 
WAREHOUSE_SIZE=XSMALL 
AUTO_SUSPEND=120;
USE WAREHOUSE SNOWSQL_WH;
*/

/* CREATE DATABASE DATABASENAME*/
/* EXAMPLE */
/* 
create database jlp_bps;
*/

/* use database DATABASENAME; */
/* EXAMPLE */
--use database jlp_bps;

/* CREATE SCHEMA SCHEMANAME*/
/* EXAMPLE */
/* 
create schema initial_schema;
*/

/* use schema SCHEMANAME */
/* EXAMPLE */
--use schema initial_schema;

/* create file format */
/* EXAMPLE */
create or replace file format jlp_format type = CSV SKIP_HEADER = 1 field_delimiter = ',' empty_field_as_null = TRUE;

/* load file into stage */
/* put file:// enter file location @DATABASE.SCHEMA.%STAGENAME; */

/* EXAMPLE */
put 'file://JohnLewisCSVCutDown.csv' @JLP_BPS.INITIAL_SCHEMA.%jlp_table;

/* 
copy into TABLENAME
from @%STAGENAME
file_format = (format_name = 'FILEFORMAT')
pattern = '.*FILENAME';
*/
/* EXAMPLE */
copy into jlp_table
from @%jlp_table
file_format = (format_name = 'jlp_format')
pattern = '.*JohnLewisCSVCutDown.csv.gz';

-- Load jlp_labels dataset onto snowflake

create or replace file format jlp_format2 type = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' field_delimiter = ',' empty_field_as_null = TRUE;

put 'file://LabelsTrimmed.csv' @JLP_BPS.INITIAL_SCHEMA.%jlp_labels AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

copy into jlp_labels

from @%jlp_labels

file_format = (format_name = 'jlp_format2')

pattern = '.*LabelsTrimmed.*';

-- Load jlp_labels_transposed dataset onto snowflake

create or replace file format jlp_format2 type = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' field_delimiter = ',' empty_field_as_null = TRUE;

put 'file://trimmed_labels_transposed.csv' @JLP_BPS.INITIAL_SCHEMA.%jlp_labels_transposed AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

copy into jlp_labels_transposed

from @%jlp_labels_transposed

file_format = (format_name = 'jlp_format2')

pattern = '.*trimmed_labels_transposed.*';
--------------------------------------------------------------snowsql end

-- create jlp_table_pks main table and load table - add_pk_main_table.sql

/* Make sure you're in correct database and schema */

/* Clone original landing table into new table */
-- CREATE TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS CLONE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE;

/* Add new column which will be the primary key */
ALTER TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE
ADD response_id INT;

/* Create sequence to increment from 1 to the number of rows (N.B. Might skip certain values) */
CREATE OR REPLACE SEQUENCE my_seq
START WITH 1
INCREMENT BY 1
;

/* Insert sequence into newly created column/table */
UPDATE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE
SET response_id = my_seq.nextval;

/* Make new column the Primary Key */
ALTER TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE
ADD CONSTRAINT PK_JLP PRIMARY KEY(response_id);

-- load date_dim table - date_dim_load.sql

INSERT OVERWRITE INTO jlp_bps.initial_schema.date_dim
SELECT DISTINCT TRY_TO_NUMBER(DWEEK) , TRY_TO_NUMBER(DMONTH), TRY_TO_NUMBER(DQUARTERJL)
FROM JLP_TABLE
;

-- load statuses and countries table - load_data_countries_statuses.sql

INSERT INTO JLP_BPS.INITIAL_SCHEMA.STATUSES
SELECT SUBSTRING("Status. Participant status", 1, CHARINDEX(' ', "Status. Participant status", 1) +1), 
SUBSTRING("Status. Participant status", CHARINDEX('''', "Status. Participant status", 1) +1, LEN("Status. Participant status") - CHARINDEX('''', "Status. Participant status", 1) -1)
FROM JLP_BPS.INITIAL_SCHEMA.JLP_LABELS
WHERE "Status. Participant status" IS NOT NULL
;

INSERT INTO JLP_BPS.INITIAL_SCHEMA.COUNTRIES
SELECT SUBSTRING("Country from URL", 1, CHARINDEX(' ', "Country from URL", 2)), 
SUBSTRING("Country from URL", CHARINDEX('''', "Country from URL", 1) +1, LEN("Country from URL") - CHARINDEX('''', "Country from URL", 1) -1)
FROM JLP_BPS.INITIAL_SCHEMA.JLP_LABELS
WHERE "Country from URL" IS NOT NULL
;

-- load dwave table - dwave_insert.sql

INSERT OVERWRITE INTO JLP_BPS.INITIAL_SCHEMA.DWAVE
SELECT DISTINCT TRY_TO_NUMBER(DWAVE), TRY_TO_NUMBER(STATUS), TRY_TO_NUMBER(COUNTRY)
FROM JLP_TABLE
;

-- load dwave_rotation table - load_dwave_rotation.sql

INSERT OVERWRITE INTO jlp_bps.initial_schema.dwave_rotation
SELECT DISTINCT TRY_TO_NUMBER(DWAVE), TRY_TO_NUMBER(DWEEK), TRY_TO_NUMBER(DMONTH), TRY_TO_NUMBER(DQUARTERJL),
TRY_TO_NUMBER(DSPLITSECTION_1), TRY_TO_NUMBER(dsplitsection_2), TRY_TO_NUMBER(DSPLITSECTION_3),
TRY_TO_NUMBER(DSPLITSECTION_4), TRY_TO_NUMBER(dsplitsection_5), TRY_TO_NUMBER(dsplitsection_6)
FROM JLP_TABLE
;

-- load response_main - response_main.sql

INSERT OVERWRITE INTO jlp_bps.initial_schema.response_main
SELECT TRY_TO_NUMBER(response_id), TRY_TO_NUMBER(dwave), TRY_TO_NUMBER(dweek), TRY_TO_NUMBER(dmonth),
TRY_TO_NUMBER(S2) as S2_AGE, TRY_TO_NUMBER(FC10) as FC10_FAMILY_MEMBERS, WEIGHT
FROM jlp_table
;
-- load label_doms table - load_label_dom_table.sql

INSERT OVERWRITE INTO jlp_bps.initial_schema.label_dom

WITH cte_label_doms AS (
SELECT CASE WHEN "1" LIKE 'D4 / B5.%. %' THEN substring("1",1, charindex('. ',"1"))
WHEN "1" LIKE 'D4 / B5:%' THEN substring("1",1, charindex(':',"1"))
WHEN "1" LIKE 'D1 / %.%' THEN substring("1",1, charindex('. ',"1"))
WHEN "1" LIKE 'D1 / %' THEN substring("1",1, charindex(': ',"1"))
WHEN charindex(' ',"1")>0 THEN substring("1", 1, charindex(' ', "1")-1) 
ELSE "1" END AS label_code,

CASE WHEN "1" LIKE 'D4 / B5.%. %' THEN substring("1", charindex('. ',"1")+1)
WHEN "1" LIKE 'D4 / B5:%' THEN substring("1",charindex(':',"1")+1)
WHEN "1" LIKE 'D1 / %.%' THEN substring("1",charindex('. ',"1")+1)
WHEN "1" LIKE 'D1 / %' THEN substring("1", charindex(': ',"1")+1)
WHEN charindex(' ',"1")>0 THEN substring("1", charindex(' ', "1")) 
ELSE "1" END AS question_code,

"1" AS orig_label,

CASE WHEN label_code IN (
'A1.', 'A10.1.','A10.2.','A10.3.','A10.4.','A2.','BF1.','BF6.','BR5.','CS3.','CS5.','D3.1.','D3.10.','D3.2.','D3.3.','D3.4.','D3.5.','D3.6.','D3.7.','D3.8.','D3.9.','DA8.','DFINALBRAND','DFINALBRANDS','DROTATIONSECTION','DXAWARE','E3.','F3.1.','F3.10.','F3.11.','F3.2.','F3.3.','F3.4.','F3.5.','F3.6.','F3.7.','F3.8.','F3.9.','F4a.1.','F4a.10.','F4a.11.','F4a.2.','F4a.3.','F4a.4.','F4a.5.','F4a.6.','F4a.7.','F4a.8.','F4a.9.','FC17.','FC2a.','FC2b.','FC3A.','FC3B.','FC4.','FC5c.','FC6.','FC7.','G1.','G2.1.','G2.10.','G2.11.','G2.2.','G2.3.','G2.4.','G2.5.','G2.6.','G2.7.','G2.8.','G2.9.','H0a.','H0b.','HAVAILBRAND','HAVAILBRANDS','I1.','I2.','J5.','J6.','J7.','JS2a.','JS2b.','N2.','PBF1.','PBF2.'
) THEN 'Y'
ELSE NULL END AS multi_cols
FROM jlp_labels_transposed
)
SELECT CASE WHEN right(cte_label_doms.label_code, 1) = '.' THEN rtrim(substr(cte_label_doms.label_code, 1, LEN(cte_label_doms.label_code)-1))
WHEN right(cte_label_doms.label_code, 1) = ':' THEN rtrim(substr(cte_label_doms.label_code, 1, LEN(cte_label_doms.label_code)-1))
ELSE rtrim(cte_label_doms.label_code) END AS label_code, cte_label_doms.question_code, cte_label_doms.orig_label, cte_label_doms.multi_cols

FROM cte_label_doms
;
