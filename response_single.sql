CREATE OR REPLACE PROCEDURE JLP_BPS.PUBLIC.POP_RESPONSE_SINGLE()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
	 v_table_name  VARCHAR(100);
	 v_column_name VARCHAR(100);
	 v_label_code  VARCHAR(100);
	 v_sql VARCHAR(1000);

	 v_sql1 VARCHAR(1000) DEFAULT 'INSERT INTO JLP_BPS.INITIAL_SCHEMA.response_single SELECT
	 drv.response_id
	,\'';

	c_cur CURSOR FOR
	SELECT table_name,column_name
    FROM JLP_BPS.INFORMATION_SCHEMA.COLUMNS col
    JOIN JLP_BPS.INITIAL_SCHEMA.LABEL_DOM lab
    ON REPLACE(CASE WHEN right(label_code, 1) = '.' THEN substr(lab.label_code, 1, LEN(lab.label_code)-1) ELSE lab.label_code END,'.','_') = col.column_name
    WHERE table_name like 'JLP_TABLE_PKS%'
    AND lab.multi_cols IS NULL
    AND col.COLUMN_NAME NOT IN ('S2','FC10','WEIGHT','COUNTRY','DWAVE','DWEEK','DMONTH','DQUARTERJL')
    ;

	BEGIN
		v_sql := v_sql1;


		FOR record in c_cur DO
  			v_sql := v_sql1;
			v_sql  := CONCAT(v_sql,REPLACE(record.column_name,'_','.'),
            '\' label_code, drv.',record.column_name,
            ' response_no FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS drv WHERE drv.',
            record.column_name,' <> \'.\';');
			EXECUTE IMMEDIATE (v_sql);
		END FOR;
		RETURN (v_sql);
	END;

   call  POP_RESPONSE_SINGLE();

INSERT INTO JLP_BPS.INITIAL_SCHEMA.response_single 
SELECT  drv.response_id ,'DAGE' label_code, drv.DAGE response_no 
FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS drv 
WHERE drv.DAGE <> '.';

   