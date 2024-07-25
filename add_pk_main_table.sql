-- Make sure you're in correct database and schema

-- Clone original landing table into new table
CREATE TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS CLONE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE;

-- Add new column which will be the primary key
ALTER TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS
ADD response_id INT;

-- Create sequence to increment from 1 to the number of rows (N.B. Might skip certain values)
CREATE SEQUENCE my_seq
START WITH 1
INCREMENT BY 1
;

-- Insert sequence into newly created column/table
UPDATE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS
SET response_id = my_seq.nextval;

-- Make new column the Primary Key
ALTER TABLE JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS
ADD CONSTRAINT PK_JLP PRIMARY KEY(response_id);


-- Inspect table to see if result as expected
SELECT response_id FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS
ORDER BY response_id;

SELECT response_id FROM JLP_BPS.INITIAL_SCHEMA.JLP_TABLE_PKS
WHERE response_id IS NULL;