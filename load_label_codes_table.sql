INSERT OVERWRITE INTO jlp_bps.initial_schema.label_codes

select ld.label_code, cast(substring(lc.value,1,charindex(' ',lc.value)+2) as int) as response_no, trim(replace(replace(substring(lc.value,charindex(' ',lc.value)+3),''),'\'','')) as Text
from label_dom as ld
join jlp_lc as lc on ld.orig_label = lc.variable
order by ld.label_code, response_no
;