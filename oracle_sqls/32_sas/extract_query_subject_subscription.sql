select	
	SUBSCRIPTION.SUBJECT_SUBSCRIPTION_ID	   C1_SUBJECT_SUBSCRIPTION_ID,
	SUBSCRIPTION.SUBJECT_ID	   C2_SUBJECT_ID,
	SUBSCRIPTION.SUBSCRIPTION	   C3_SUBSCRIPTION,
	SUBSCRIPTION.SUBSCRIPTION_TYPE_ID	   C4_SUBSCRIPTION_TYPE_ID,
	SUBSCRIPTION.SUBSCRIPTION_OPT_IN	   C5_SUBSCRIPTION_OPT_IN,
	SUBSCRIPTION.SUBSCRIPTION_OPT_IN_TEXT	   C6_SUBSCRIPTION_OPT_IN_TEXT,
	SUBSCRIPTION.SUBSCRIPTION_RELATIONSHIP_ID	   C7_SUBSCRIPTION_RELATIONSHIP_I,
	SUBSCRIPTION.SUBSCRIPTION_ORDER	   C8_SUBSCRIPTION_ORDER,
	SUBSCRIPTION.SOURCE_IP_ADDRESS	   C9_SOURCE_IP_ADDRESS,
	SUBSCRIPTION.AUDIT_CREATE_DATE	   C10_AUDIT_CREATE_DATE,
	SUBSCRIPTION.AUDIT_CREATED_BY	   C11_AUDIT_CREATED_BY,
	SUBSCRIPTION.AUDIT_MODIFIED_BY	   C12_AUDIT_MODIFIED_BY,
	SUBSCRIPTION.AUDIT_MODIFY_DATE	   C13_AUDIT_MODIFY_DATE
from	<schema_name>.SUBJECT_SUBSCRIPTION   SUBSCRIPTION
where	(1=1)
And (SUBSCRIPTION.AUDIT_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap
)

