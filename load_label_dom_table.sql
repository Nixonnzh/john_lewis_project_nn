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
FROM transposed_jlp_labels
ORDER BY label_code
)
SELECT CASE WHEN right(cte_label_doms.label_code, 1) = '.' THEN rtrim(substr(cte_label_doms.label_code, 1, LEN(cte_label_doms.label_code)-1))
WHEN right(cte_label_doms.label_code, 1) = ':' THEN rtrim(substr(cte_label_doms.label_code, 1, LEN(cte_label_doms.label_code)-1))
ELSE rtrim(cte_label_doms.label_code) END AS label_code, cte_label_doms.question_code, cte_label_doms.orig_label, cte_label_doms.multi_cols

FROM cte_label_doms
;