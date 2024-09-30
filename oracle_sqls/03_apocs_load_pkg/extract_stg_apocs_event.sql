select	
	AE.BUSINESS_KEY	   C1_BUSINESS_KEY,
	AE.AUDIT_MODIFY_DATE	   C2_AUDIT_MODIFY_DATE,
	AE.AUDIT_MODIFIED_BY	   C3_AUDIT_MODIFIED_BY,
	AE.MAPPED_KEY	   C4_MAPPED_KEY,
	AE.EVENT_ID	   C5_EVENT_ID,
	APO.BUSINESS_KEY	   C6_APO_ID,
	AE.AUDIT_CREATE_DATE	   C7_AUDIT_CREATE_DATE,
	AE.AUDIT_CREATED_BY	   C8_AUDIT_CREATED_BY
from	<schema_name>.APOCS_EVENT   AE, APOCS_OWN.APO   APO
where	(1=1)
And (AE.AUDIT_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_oms_overlap)
 And (length(AE.EVENT_ID) <= 20)
 And (AE.APO_ID=APO.APO_ID (+))