select	
	CUSTOMER_ORDER_LINE_DETAIL.TYPE	   C1_TYPE,
	CUSTOMER_ORDER_LINE_DETAIL.CUSTOMER_ORDER_LINE_DETAIL_ID	   C2_CUSTOMER_ORDER_LINE_DETAIL_,
	CUSTOMER_ORDER_LINE_DETAIL.AUDIT_CREATE_DATE	   C3_AUDIT_CREATE_DATE,
	CUSTOMER_ORDER_LINE_DETAIL.AUDIT_CREATED_BY	   C4_AUDIT_CREATED_BY,
	CUSTOMER_ORDER_LINE_DETAIL.AUDIT_MODIFIED_BY	   C5_AUDIT_MODIFIED_BY,
	CUSTOMER_ORDER_LINE_DETAIL.AUDIT_MODIFY_DATE	   C6_AUDIT_MODIFY_DATE,
	CUSTOMER_ORDER_LINE_DETAIL.QUANTITY	   C7_QUANTITY,
	CUSTOMER_ORDER_LINE_DETAIL.CUSTOMER_ORDER_LINE_ID	   C8_CUSTOMER_ORDER_LINE_ID,
	CUSTOMER_ORDER_LINE_DETAIL.ITEM_NUMBER	   C9_ITEM_NUMBER,
	CUSTOMER_ORDER_LINE_DETAIL.DELETED_IND	   C10_DELETED_IND
from	<schema_name>.CUSTOMER_ORDER_LINE_DETAIL   CUSTOMER_ORDER_LINE_DETAIL
where	(1=1)
And (CUSTOMER_ORDER_LINE_DETAIL.AUDIT_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap)
