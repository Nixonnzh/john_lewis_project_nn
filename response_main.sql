INSERT OVERWRITE INTO jlp_bps.initial_schema.response_main
SELECT response_id, dwave, dweek, dmonth, S2 as S2_AGE, FC10 as FC10_FAMILY_MEMBERS, WEIGHT
FROM jlp_table_pks