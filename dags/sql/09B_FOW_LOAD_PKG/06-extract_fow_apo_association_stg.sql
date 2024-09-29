select	
	APO_ASSOCIATION.ID	   C1_ID,
	APO_ASSOCIATION.VERSION	   C2_VERSION,
	APO_ASSOCIATION.APO_ID	   C3_APO_ID,
	APO_ASSOCIATION.ASSOCIATED_APO_TAG	   C4_ASSOCIATED_APO_TAG,
	APO_ASSOCIATION.CREATED_BY	   C5_CREATED_BY,
	APO_ASSOCIATION.DATE_CREATED	   C6_DATE_CREATED
from	FOW_OWN.APO_ASSOCIATION   APO_ASSOCIATION
where	(1=1)
And (APO_ASSOCIATION.DATE_CREATED>= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_oms_overlap)