select 	
	case when ACT_COUNT.COUNTRY_NAME ='.' then ' '
else ACT_COUNT.COUNTRY_NAME end	   COUNTRY_NAME,
	ACT_COUNT.COMPANY_NAME	   COMPANY_NAME,
	ACT_COUNT.AREA_NAME	   AREA_NAME,
	ACT_COUNT.REGION_NAME	   REGION_NAME,
	ACT_COUNT.TERRITORY_CODE	   TERRITORY_CODE,
	ACT_COUNT.TERRITORY_NAME	   TERRITORY_NAME,
	ACT_COUNT.JOB_NBR	   JOB_NBR,
	ACT_COUNT.AMS_BUSINESS_UNIT_NAME	   AMS_BUSINESS_UNIT_NAME,
	ACT_COUNT.BANK_CODE	   BANK_CODE,
	to_char(ACT_COUNT.DEPOSIT_DATE,'MM/DD/YYYY')	   DEPOSIT_DATE,
	to_char(ACT_COUNT.PHOTOGRAPHY_DATE,'MM/DD/YYYY')	   PHOTOGRAPHY_DATE,
	to_char( ACT_COUNT.SHIP_DATE,'MM/DD/YYYY')	   SHIP_DATE,
	ACT_COUNT.SELLINGMETHOD_DESC	   SELLINGMETHOD_DESC,
	ACT_COUNT.SERIAL_NBR	   SERIAL_NBR,
	ACT_COUNT.CASHTXN_AMT	   CASHTXN_AMT,
	to_char(ACT_COUNT.CASHTXN_DATE,'MM/DD/YYYY')	   CASHTXN_DATE,
	ACT_COUNT.COUNT_FEE_PERC	   COUNT_FEE_PERC,
	ACT_COUNT.SELLINGMETHODCODE	   SELLINGMETHODCODE,
	ACT_COUNT.COUNT_FEE_FLAG	   COUNT_FEE_FLAG,
	ACT_COUNT.DAYS_TO_BANK	   DAYS_TO_BANK,
	ACT_COUNT.COUNT_FEE_AMT	   COUNT_FEE_AMT,
	ACT_COUNT.DAYS_DEPDATE_KEYDATE	   DAYS_DEPDATE_KEYDATE,
	ACT_COUNT.FISCAL_YEAR	   FISCAL_YEAR
from	RAX_APP_USER.ACTUATE_COUNTING_FEE_NEW_STAGE   ACT_COUNT
where	
	(1=1)	
	





/* Order the output by the UD1, UD2, UD3, UD4, UD5, UD6 Fields. */