select 	
	SFLY_COUPON_V2_TMP.SUBJECT_FIRST_NAME	   SUBJECT_FIRST_NAME,
	SFLY_COUPON_V2_TMP.CUST_EMAIL_ADDRESS	   CUSTOMER_EMAILID,
	SFLY_COUPON_V2_TMP.PAYMENT_VOUCHER_ID	   ORDER_FORM_ID,
	SFLY_COUPON_V2_TMP.AUDIT_CREATE_DATE	   ORDER_DATE,
	SFLY_COUPON_V2_TMP.STATE	   STATE,
	SFLY_COUPON_V2_TMP.SCHOOL_YEAR	   SCHOOL_YEAR,
	SFLY_COUPON_V2_TMP.SUB_PROGRAM	   SUB_PROGRAM,
	SFLY_COUPON_V2_TMP.ORDER_TYPE	   ORDER_TYPE,
	SFLY_COUPON_V2_TMP.PHOTOGRAPHY_DATE	   PHOTOGRAPHY_DATE
from	RAX_APP_USER.SFLY_COUPON_V2_TMP   SFLY_COUPON_V2_TMP
where	
	(1=1)	