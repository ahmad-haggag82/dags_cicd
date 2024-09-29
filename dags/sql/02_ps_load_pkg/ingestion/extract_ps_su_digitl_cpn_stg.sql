select	
	SU_DIGITAL_COUPON.SU_DIGITAL_COUPON_ID	   C1_SU_DIGITAL_COUPON_ID,
	SU_DIGITAL_COUPON.COUPON_CODE	   C2_COUPON_CODE,
	SU_DIGITAL_COUPON.SU_COUPON_BATCH	   C3_SU_COUPON_BATCH,
	SU_DIGITAL_COUPON.MAX_REDEMPTIONS	   C4_MAX_REDEMPTIONS,
	SU_DIGITAL_COUPON.NUM_REDEMPTIONS	   C5_NUM_REDEMPTIONS,
	SU_DIGITAL_COUPON.REDEEM_DATE	   C6_REDEEM_DATE,
	SU_DIGITAL_COUPON.AUDIT_CREATE_DATE	   C7_AUDIT_CREATE_DATE,
	SU_DIGITAL_COUPON.AUDIT_CREATED_BY	   C8_AUDIT_CREATED_BY,
	SU_DIGITAL_COUPON.AUDIT_MODIFY_DATE	   C9_AUDIT_MODIFY_DATE,
	SU_DIGITAL_COUPON.AUDIT_MODIFIED_BY	   C10_AUDIT_MODIFIED_BY
from	PS_OWN.SU_DIGITAL_COUPON   SU_DIGITAL_COUPON
where	(1=1)
And (NVL(SU_DIGITAL_COUPON.AUDIT_MODIFY_DATE, SU_DIGITAL_COUPON.AUDIT_CREATE_DATE) >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap)