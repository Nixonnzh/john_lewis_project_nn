-- load_label_codes_csv_to_jlp__lc_table.sql


-- INSERT OVERWRITE INTO jlp_bps.initial_schema.label_codes

-- select ld.label_code, cast(substring(lc.value,1,charindex(' ',lc.value)+2) as int) as response_no, trim(replace(replace(substring(lc.value,charindex(' ',lc.value)+3),''),'\'','')) as Text
-- from label_dom as ld
-- join jlp_lc as lc on ld.orig_label = lc.variable
-- order by ld.label_code, response_no
-- ;

--  snowsql -a accountname -u username
--  EXAMPLE 
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
create or replace file format jlp_format type = CSV field_delimiter = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' empty_field_as_null = TRUE;

create or replace table jlp_lc ( variable varchar(500), value varchar(500) );

/* load file into stage */
/* put file:// enter file location @DATABASE.SCHEMA.%STAGENAME; */

/* EXAMPLE */
put 'file://label_codes.csv' @JLP_BPS.INITIAl_SCHEMA.%jlp_lc;

/* 
copy into TABLENAME
from @%STAGENAME
file_format = (format_name = 'FILEFORMAT')
pattern = '.*FILENAME';
*/
/* EXAMPLE */
copy into jlp_lc
from @%jlp_lc
file_format = (format_name = 'jlp_format')
pattern = '.*label_codes.csv.gz';
---------------------------------------------------------------------------------------------------------end

-- load label_codes table - load_label_codes_table.sql

INSERT OVERWRITE INTO jlp_bps.initial_schema.label_codes

select ld.label_code, cast(substring(lc.value,1,charindex(' ',lc.value)+2) as int) as response_no, 
--replace(substring(lc.value,charindex(' ',lc.value)+3),"'",'') as Text
substring(lc.value,charindex(' ',lc.value)+3) as Text
from label_dom as ld
join jlp_lc as lc on ld.orig_label = lc.variable
;

-- load response_single table - response_single.sql

CREATE OR REPLACE PROCEDURE JLP_BPS.PUBLIC.POP_RESPONSE_SINGLE()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
	v_table_name VARCHAR(100);
	v_column_name VARCHAR(100);
	v_label_code VARCHAR(100);
	v_sql VARCHAR(1000);

	v_sql1 VARCHAR(1000) DEFAULT 'INSERT INTO response_single SELECT 
	drv.response_id
	,';

	c_cur CURSOR FOR
	SELECT table_name,column_name
    FROM JLP_BPS.INFORMATION_SCHEMA.COLUMNS col
    JOIN JLP_BPS.INITIAL_SCHEMA.LABEL_DOM lab
    ON REPLACE(lab.label_code,'.','_') = col.column_name
    WHERE table_name like 'JLP_TABLE%'
    AND COALESCE(lab.Multi_cols,'N') = 'N'
    AND col.COLUMN_NAME NOT IN ('S2','FC10','WEIGHT','COUNTRY','DWAVE','DWEEK','DMONTH','DQUARTERJL');
    --AND col.column_name = 'DAGE'
    

	BEGIN
		--TRUNCATE TABLE response_single;
		v_sql := v_sql1;


		FOR record in c_cur DO
  			v_sql := v_sql1
			v_sql  := CONCAT(v_sql,REPLACE(record.column_name,'_','.'),' label_code, drv.',record.column_name,' response FROM JLPmain drv WHERE drv.',record.column_name,' <> ''.'';')
			EXECUTE IMMEDIATE (v_sql)
		END FOR
		RETURN (v_sql)
	END

   call POP_RESPONSE_SINGLE()

INSERT INTO response_single 
SELECT drv.response_id
	,DAGE label_code
    , drv.DAGE response 
    FROM JLP_TABLE drv 
    WHERE drv.DAGE <> '.';

-- load response_multi table - response_multi.sql

CREATE OR REPLACE PROCEDURE JLP_BPS.PUBLIC.POP_RESPONSE_MULTI()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
	v_table_name VARCHAR(100);
	v_column_name VARCHAR(100);
	v_label_code VARCHAR(100);
	v_sql VARCHAR(1000);

	v_sql1 VARCHAR(1000) DEFAULT 'INSERT INTO JLP_BPS.INITIAL_SCHEMA.response_multi SELECT drv.response_id
	,\'';

	c_cur CURSOR FOR
	SELECT table_name,column_name
    FROM JLP_BPS.INFORMATION_SCHEMA.COLUMNS col
    INNER JOIN JLP_BPS.INITIAL_SCHEMA.LABEL_DOM lab
    --ON REPLACE(CASE WHEN right(label_code, 1) = '.' THEN substr(lab.label_code, 1, LEN(lab.label_code)-1) ELSE lab.label_code END,'.','_') = col.column_name
    ON REPLACE(col.column_name,'_','.') LIKE concat(lab.label_code,'.%')
    WHERE table_name like 'JLP_TABLE%'
    AND lab.multi_cols IS NOT NULL
    --AND col.COLUMN_NAME NOT IN ('S2','FC10','WEIGHT','COUNTRY','DWAVE','DWEEK','DMONTH','DQUARTERJL')
    ;

	BEGIN
		--TRUNCATE TABLE response_single;
		v_sql := v_sql1;


		FOR record in c_cur DO
  			v_sql := v_sql1;
			v_sql  := CONCAT(v_sql,REPLACE(record.column_name,'_','.'),
            '\' label_code, drv.',record.column_name,
            ' response_no FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE drv WHERE drv.',
            record.column_name,' <> \'.\';');
			EXECUTE IMMEDIATE (v_sql);
		END FOR;
		RETURN (v_sql);
	END;

   call  POP_RESPONSE_MULTI();