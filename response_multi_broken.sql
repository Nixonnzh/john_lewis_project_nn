CREATE OR REPLACE PROCEDURE JLP_BPS.PUBLIC.POP_RESPONSE_MULTI()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
	 v_table_name  VARCHAR(100);
	 v_column_name VARCHAR(100);
	 v_label_code  VARCHAR(100);
	 v_sql VARCHAR(1000);

	 v_sql1 VARCHAR(1000) DEFAULT 'INSERT INTO JLP_BPS.INITIAL_SCHEMA.response_multi SELECT drv.response_id
	,\'';

	c_cur CURSOR FOR
	SELECT table_name,column_name
    FROM JLP_BPS.INFORMATION_SCHEMA.COLUMNS col
    INNER JOIN JLP_BPS.INITIAL_SCHEMA.LABEL_DOM lab
    --ON REPLACE(CASE WHEN right(label_code, 1) = '.' THEN substr(lab.label_code, 1, LEN(lab.label_code)-1) ELSE lab.label_code END,'.','_') = col.column_name
    ON REPLACE(col.column_name,'_','.') LIKE concat(lab.label_code,'.%')
    WHERE table_name like 'JLP_TABLE_PKS%'
    AND lab.multi_cols IS NOT NULL
    --AND col.COLUMN_NAME NOT IN ('S2','FC10','WEIGHT','COUNTRY','DWAVE','DWEEK','DMONTH','DQUARTERJL')
    ;

	BEGIN
		--TRUNCATE TABLE response_single;
		v_sql := v_sql1;


		FOR record in c_cur DO
  			v_sql := v_sql1;
			v_sql  := CONCAT(v_sql, substr(record.column_name, 1, charindex('_', record.column_name)-1) ,
            '\' label_code, ',
            substr(record.column_name, charindex('_', record.column_name)+1, (length(record.column_name) - charindex('_', record.column_name)+1)),
            ' response_no FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS drv WHERE drv.',
            record.column_name,' <> \'.\';');
			--EXECUTE IMMEDIATE (v_sql);
		END FOR;
		RETURN (v_sql);
	END;

   call  POP_RESPONSE_MULTI();
