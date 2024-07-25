/* snowsql -a accountname -u username */
/* EXAMPLE */
snowsql -a ed81217.uk-south.azure -u robbierichards

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
use database jlp_bps;

/* CREATE SCHEMA SCHEMANAME*/
/* EXAMPLE */
/* 
create schema initial_schema;
*/

/* use schema SCHEMANAME */
/* EXAMPLE */
use schema initial_schema;

/* create file format */
/* EXAMPLE */
create or replace file format jlp_format type = CSV field_delimiter = ',' empty_field_as_null = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"';

create or replace table jlp_lc ( variable varchar(500), value varchar(500) );

/* load file into stage */
/* put file:// enter file location @DATABASE.SCHEMA.%STAGENAME; */

/* EXAMPLE */
put 'file://C:\\Users\\Robbie Richards\\OneDrive - Kubrick Group\\Core_Python\\label_codes.csv' @JLP_BPS.INITIAl_SCHEMA.%jlp_lc;

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