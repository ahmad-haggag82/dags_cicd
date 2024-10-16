/* TASK No. 1 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 1 */




/*-----------------------------------------------*/
/* TASK No. 2 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 2 */




/*-----------------------------------------------*/
/* TASK No. 3 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 3 */




/*-----------------------------------------------*/
/* TASK No. 4 */
/* Drop work table */

-- drop table RAX_APP_USER.C$_0SAS_COL_PROMO_STG purge

-- &


/*-----------------------------------------------*/
/* TASK No. 5 */
/* Create work table */

-- create table RAX_APP_USER.C$_0SAS_COL_PROMO_STG
-- (
-- 	C1_CUSTOMER_ORDER_LINE_ID	NUMBER(10) NULL,
-- 	C2_PROMOTION_CODE	VARCHAR2(4) NULL,
-- 	C3_DISCOUNT_AMOUNT	NUMBER(19,2) NULL,
-- 	C4_CUSTOMER_ORDER_LINE_PROMO_I	NUMBER(19) NULL,
-- 	C5_ITEM_NUMBER	VARCHAR2(16) NULL,
-- 	C6_DELETED_IND	VARCHAR2(1) NULL,
-- 	C7_AUDIT_CREATE_DATE	TIMESTAMP(6) NULL,
-- 	C8_AUDIT_MODIFY_DATE	TIMESTAMP(6) NULL
-- )
-- NOLOGGING

-- &


/*-----------------------------------------------*/
/* TASK No. 6 */
/* Load data */

/* SOURCE CODE */


-- select	
-- 	CUSTOMER_ORDER_LINE_PROMOTION.CUSTOMER_ORDER_LINE_ID	   C1_CUSTOMER_ORDER_LINE_ID,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.PROMOTION_CODE	   C2_PROMOTION_CODE,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.DISCOUNT_AMOUNT	   C3_DISCOUNT_AMOUNT,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.CUSTOMER_ORDER_LINE_PROMO_ID	   C4_CUSTOMER_ORDER_LINE_PROMO_I,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.ITEM_NUMBER	   C5_ITEM_NUMBER,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.DELETED_IND	   C6_DELETED_IND,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.AUDIT_CREATE_DATE	   C7_AUDIT_CREATE_DATE,
-- 	CUSTOMER_ORDER_LINE_PROMOTION.AUDIT_MODIFY_DATE	   C8_AUDIT_MODIFY_DATE
-- from	SAS_SIT_OWN.CUSTOMER_ORDER_LINE_PROMOTION   CUSTOMER_ORDER_LINE_PROMOTION
-- where	(1=1)
-- And (CUSTOMER_ORDER_LINE_PROMOTION.AUDIT_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap)







-- &

/* TARGET CODE */
-- insert /*+ append */ into RAX_APP_USER.C$_0SAS_COL_PROMO_STG
-- (
-- 	C1_CUSTOMER_ORDER_LINE_ID,
-- 	C2_PROMOTION_CODE,
-- 	C3_DISCOUNT_AMOUNT,
-- 	C4_CUSTOMER_ORDER_LINE_PROMO_I,
-- 	C5_ITEM_NUMBER,
-- 	C6_DELETED_IND,
-- 	C7_AUDIT_CREATE_DATE,
-- 	C8_AUDIT_MODIFY_DATE
-- )
-- values
-- (
-- 	:C1_CUSTOMER_ORDER_LINE_ID,
-- 	:C2_PROMOTION_CODE,
-- 	:C3_DISCOUNT_AMOUNT,
-- 	:C4_CUSTOMER_ORDER_LINE_PROMO_I,
-- 	:C5_ITEM_NUMBER,
-- 	:C6_DELETED_IND,
-- 	:C7_AUDIT_CREATE_DATE,
-- 	:C8_AUDIT_MODIFY_DATE
-- )

-- &


/*-----------------------------------------------*/
/* TASK No. 7 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'RAX_APP_USER',
    tabname =>	'C$_0SAS_COL_PROMO_STG',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 9 */
/* Set vID */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 9 */




/*-----------------------------------------------*/
/* TASK No. 10 */
/* Drop flow table */

-- drop table RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;


&


/*-----------------------------------------------*/
/* TASK No. 11 */
/* Create flow table I$ */

create table RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
(
	CUSTOMER_ORDER_LINE_ID	NUMBER(10) NULL,
	PROMOTION_CODE	VARCHAR2(4) NULL,
	DISCOUNT_AMOUNT	NUMBER(19,2) NULL,
	CUSTOMER_ORDER_LINE_PROMO_ID	NUMBER(19) NULL,
	ITEM_NUMBER	VARCHAR2(16) NULL,
	ODS_CREATE_DATE	DATE NULL,
	ODS_MODIFY_DATE	DATE NULL,
	DELETED_IND	VARCHAR2(1) NULL,
	AUDIT_CREATE_DATE	TIMESTAMP(6) NULL,
	AUDIT_MODIFY_DATE	TIMESTAMP(6) NULL
	,IND_UPDATE		char(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 12 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
(
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE,
	IND_UPDATE
)
select 
CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE,
	IND_UPDATE
 from (


select 	 
	
	C1_CUSTOMER_ORDER_LINE_ID CUSTOMER_ORDER_LINE_ID,
	C2_PROMOTION_CODE PROMOTION_CODE,
	C3_DISCOUNT_AMOUNT DISCOUNT_AMOUNT,
	C4_CUSTOMER_ORDER_LINE_PROMO_I CUSTOMER_ORDER_LINE_PROMO_ID,
	C5_ITEM_NUMBER ITEM_NUMBER,
	C6_DELETED_IND DELETED_IND,
	C7_AUDIT_CREATE_DATE AUDIT_CREATE_DATE,
	C8_AUDIT_MODIFY_DATE AUDIT_MODIFY_DATE,

	'I' IND_UPDATE

from	RAX_APP_USER.C$_0SAS_COL_PROMO_STG
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG T
	where	T.CUSTOMER_ORDER_LINE_PROMO_ID	= S.CUSTOMER_ORDER_LINE_PROMO_ID 
		 and ((T.CUSTOMER_ORDER_LINE_ID = S.CUSTOMER_ORDER_LINE_ID) or (T.CUSTOMER_ORDER_LINE_ID IS NULL and S.CUSTOMER_ORDER_LINE_ID IS NULL)) and
		((T.PROMOTION_CODE = S.PROMOTION_CODE) or (T.PROMOTION_CODE IS NULL and S.PROMOTION_CODE IS NULL)) and
		((T.DISCOUNT_AMOUNT = S.DISCOUNT_AMOUNT) or (T.DISCOUNT_AMOUNT IS NULL and S.DISCOUNT_AMOUNT IS NULL)) and
		((T.ITEM_NUMBER = S.ITEM_NUMBER) or (T.ITEM_NUMBER IS NULL and S.ITEM_NUMBER IS NULL)) and
		((T.DELETED_IND = S.DELETED_IND) or (T.DELETED_IND IS NULL and S.DELETED_IND IS NULL)) and
		((T.AUDIT_CREATE_DATE = S.AUDIT_CREATE_DATE) or (T.AUDIT_CREATE_DATE IS NULL and S.AUDIT_CREATE_DATE IS NULL)) and
		((T.AUDIT_MODIFY_DATE = S.AUDIT_MODIFY_DATE) or (T.AUDIT_MODIFY_DATE IS NULL and S.AUDIT_MODIFY_DATE IS NULL))
        )

  
  

  



&


/*-----------------------------------------------*/
/* TASK No. 13 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'RAX_APP_USER',
	tabname => 'I$_SAS_COL_PROMO_STG1388001',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 14 */
/* Create Index on flow table */

-- create index	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
-- on		RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 (CUSTOMER_ORDER_LINE_PROMO_ID)
-- NOLOGGING

BEGIN
  EXECUTE IMMEDIATE 'create index RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
on  RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 (CUSTOMER_ORDER_LINE_PROMO_ID)';
EXCEPTION
  WHEN OTHERS THEN
   -- Handle the case where the identifier is too long (ORA-00972)
   IF SQLCODE = -972 or  SQLCODE = -1408 or SQLCODE = -955  THEN
     DBMS_OUTPUT.PUT_LINE('Identifier is too long. Skipping creation of index.');
   ELSE
     RAISE;
   END IF;
END;

&


/*-----------------------------------------------*/
/* TASK No. 15 */
/* create check table */

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.SNP_CHECK_TAB';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;

&

-- create table RAX_APP_USER.SNP_CHECK_TAB
-- (
-- 	CATALOG_NAME	VARCHAR2(100 CHAR) NULL ,
-- 	SCHEMA_NAME	VARCHAR2(100 CHAR) NULL ,
-- 	RESOURCE_NAME	VARCHAR2(100 CHAR) NULL,
-- 	FULL_RES_NAME	VARCHAR2(100 CHAR) NULL,
-- 	ERR_TYPE		VARCHAR2(1 CHAR) NULL,
-- 	ERR_MESS		VARCHAR2(250 CHAR) NULL ,
-- 	CHECK_DATE	DATE NULL,
-- 	ORIGIN		VARCHAR2(100 CHAR) NULL,
-- 	CONS_NAME	VARCHAR2(35 CHAR) NULL,
-- 	CONS_TYPE		VARCHAR2(2 CHAR) NULL,
-- 	ERR_COUNT		NUMBER(10) NULL
-- )

BEGIN
   EXECUTE IMMEDIATE '
      create table RAX_APP_USER.SNP_CHECK_TAB
		(
			CATALOG_NAME	VARCHAR2(100 CHAR) NULL ,
			SCHEMA_NAME	VARCHAR2(100 CHAR) NULL ,
			RESOURCE_NAME	VARCHAR2(100 CHAR) NULL,
			FULL_RES_NAME	VARCHAR2(100 CHAR) NULL,
			ERR_TYPE		VARCHAR2(1 CHAR) NULL,
			ERR_MESS		VARCHAR2(250 CHAR) NULL ,
			CHECK_DATE	DATE NULL,
			ORIGIN		VARCHAR2(100 CHAR) NULL,
			CONS_NAME	VARCHAR2(35 CHAR) NULL,
			CONS_TYPE		VARCHAR2(2 CHAR) NULL,
			ERR_COUNT		NUMBER(10) NULL
		)
   ';
END;


&


/*-----------------------------------------------*/
/* TASK No. 16 */
/* delete previous check sum */

delete from	RAX_APP_USER.SNP_CHECK_TAB
where	SCHEMA_NAME	= 'ODS_STAGE'
and	ORIGIN 		= '(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT'
and	ERR_TYPE 		= 'F'


&


/*-----------------------------------------------*/
/* TASK No. 17 */
/* create error table */

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;

&

create table RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_ROW_ID 		UROWID,
	ODI_ERR_TYPE		VARCHAR2(1 CHAR) NULL, 
	ODI_ERR_MESS		VARCHAR2(250 CHAR) NULL,
	ODI_CHECK_DATE	DATE NULL, 
	CUSTOMER_ORDER_LINE_ID	NUMBER(10) NULL,
	PROMOTION_CODE	VARCHAR2(4) NULL,
	DISCOUNT_AMOUNT	NUMBER(19,2) NULL,
	CUSTOMER_ORDER_LINE_PROMO_ID	NUMBER(19) NULL,
	ITEM_NUMBER	VARCHAR2(16) NULL,
	ODS_CREATE_DATE	DATE NULL,
	ODS_MODIFY_DATE	DATE NULL,
	DELETED_IND	VARCHAR2(1) NULL,
	AUDIT_CREATE_DATE	TIMESTAMP(6) NULL,
	AUDIT_MODIFY_DATE	TIMESTAMP(6) NULL,
	ODI_ORIGIN		VARCHAR2(100 CHAR) NULL,
	ODI_CONS_NAME	VARCHAR2(35 CHAR) NULL,
	ODI_CONS_TYPE		VARCHAR2(2 CHAR) NULL,
	ODI_PK			VARCHAR2(32 CHAR) PRIMARY KEY,
	ODI_SESS_NO		VARCHAR2(19 CHAR)
)



&


/*-----------------------------------------------*/
/* TASK No. 18 */
/* delete previous errors */

delete from 	RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
where	(ODI_ERR_TYPE = 'S'	and 'F' = 'S')
or	(ODI_ERR_TYPE = 'F'	and ODI_ORIGIN = '(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT')


&


/*-----------------------------------------------*/
/* TASK No. 19 */
/* Create index on PK */

 
/* FLOW CONTROL CREATE THE iNDEX ON THE I$TABLE */
-- create index 	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
-- on	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 (CUSTOMER_ORDER_LINE_PROMO_ID)

BEGIN
  EXECUTE IMMEDIATE 'create index RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
on   RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 (CUSTOMER_ORDER_LINE_PROMO_ID)';
EXCEPTION
  WHEN OTHERS THEN
   -- Handle the case where the identifier is too long (ORA-00972)
   IF SQLCODE = -972 or  SQLCODE = -1408 or SQLCODE = -955  THEN
     DBMS_OUTPUT.PUT_LINE('Identifier is too long. Skipping creation of index.');
   ELSE
     RAISE;
   END IF;
END;

&


/*-----------------------------------------------*/
/* TASK No. 20 */
/* insert PK errors */

DECLARE
               CheckTable                             VarChar2(60);
               TargetTable                            VarChar2(60);
               VariableCheckTable                     VarChar2(60);

BEGIN
               SELECT 'RAX_APP_USER.I$_SAS_COL_PROMO_STG' INTO CheckTable FROM DUAL;
               SELECT 'ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG' INTO TargetTable FROM DUAL;

IF CheckTable = TargetTable THEN
   VariableCheckTable := CheckTable; 
ELSE
   VariableCheckTable := CheckTable || '1388001';
END IF;

execute immediate '
insert into RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_PK,
	ODI_SESS_NO,
	ODI_ROW_ID,
	ODI_ERR_TYPE,
	ODI_ERR_MESS,
	ODI_ORIGIN,
	ODI_CHECK_DATE,
	ODI_CONS_NAME,
	ODI_CONS_TYPE,
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
)
select	SYS_GUID(),
	:v_sess_no, 
	rowid,
	''F'', 
	''ODI-15064: The primary key CUST_ORD_LINE_PROMO_PK is not unique.'',
	''(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT'',
	sysdate,
	''CUST_ORD_LINE_PROMO_PK'',
	''PK'',	
	SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_ID,
	SAS_COL_PROMO_STG.PROMOTION_CODE,
	SAS_COL_PROMO_STG.DISCOUNT_AMOUNT,
	SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_PROMO_ID,
	SAS_COL_PROMO_STG.ITEM_NUMBER,
	SAS_COL_PROMO_STG.ODS_CREATE_DATE,
	SAS_COL_PROMO_STG.ODS_MODIFY_DATE,
	SAS_COL_PROMO_STG.DELETED_IND,
	SAS_COL_PROMO_STG.AUDIT_CREATE_DATE,
	SAS_COL_PROMO_STG.AUDIT_MODIFY_DATE
from	'
 || VariableCheckTable || 
' SAS_COL_PROMO_STG 
where	exists  (
		select	SUB1.CUSTOMER_ORDER_LINE_PROMO_ID
		from 	' 
|| VariableCheckTable ||
'  SUB1
		where 	SUB1.CUSTOMER_ORDER_LINE_PROMO_ID=SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_PROMO_ID
		group by 	SUB1.CUSTOMER_ORDER_LINE_PROMO_ID
		having 	count(1) > 1
		)
';

END;


&


/*-----------------------------------------------*/
/* TASK No. 21 */
/* Create index on AK */

 
/* FLOW CONTROL CREATE THE iNDEX ON THE I$TABLE */
-- create index 	SAS_CUST_ORD_LINE_PROMO_PK_flow
-- on	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 
-- 	(CUSTOMER_ORDER_LINE_PROMO_ID)

BEGIN
  EXECUTE IMMEDIATE 'create index SAS_CUST_ORD_LINE_PROMO_PK_flow
on   RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 (CUSTOMER_ORDER_LINE_PROMO_ID)';
EXCEPTION
  WHEN OTHERS THEN
   -- Handle the case where the identifier is too long (ORA-00972)
   IF SQLCODE = -972 or  SQLCODE = -1408 or SQLCODE = -955  THEN
     DBMS_OUTPUT.PUT_LINE('Identifier is too long. Skipping creation of index.');
   ELSE
     RAISE;
   END IF;
END;

&


/*-----------------------------------------------*/
/* TASK No. 22 */
/* insert AK errors */

DECLARE
               CheckTable                             VarChar2(60);
               TargetTable                            VarChar2(60);
               VariableCheckTable                VarChar2(60);

BEGIN
               SELECT 'RAX_APP_USER.I$_SAS_COL_PROMO_STG' INTO CheckTable FROM DUAL;
               SELECT 'ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG' INTO TargetTable FROM DUAL;

IF CheckTable = TargetTable THEN
   VariableCheckTable := CheckTable; 
ELSE
   VariableCheckTable := CheckTable || '1388001';
END IF;

execute immediate '
insert into RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_PK,
	ODI_SESS_NO,
	ODI_ROW_ID,
	ODI_ERR_TYPE,
	ODI_ERR_MESS,
	ODI_ORIGIN,
	ODI_CHECK_DATE,
	ODI_CONS_NAME,
	ODI_CONS_TYPE,
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
)
select	SYS_GUID(),
	:v_sess_no, 
	rowid,
	''F'', 
	''ODI-15063: The alternate key SAS_CUST_ORD_LINE_PROMO_PK is not unique.'',
	''(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT'',
	sysdate,
	''SAS_CUST_ORD_LINE_PROMO_PK'',
	''AK'',	
	SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_ID,
	SAS_COL_PROMO_STG.PROMOTION_CODE,
	SAS_COL_PROMO_STG.DISCOUNT_AMOUNT,
	SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_PROMO_ID,
	SAS_COL_PROMO_STG.ITEM_NUMBER,
	SAS_COL_PROMO_STG.ODS_CREATE_DATE,
	SAS_COL_PROMO_STG.ODS_MODIFY_DATE,
	SAS_COL_PROMO_STG.DELETED_IND,
	SAS_COL_PROMO_STG.AUDIT_CREATE_DATE,
	SAS_COL_PROMO_STG.AUDIT_MODIFY_DATE
from              '	
 || VariableCheckTable || 
' SAS_COL_PROMO_STG
where	exists  (
		select	SUB.CUSTOMER_ORDER_LINE_PROMO_ID
		from 	'
 || VariableCheckTable || 
' SUB
		where 	SUB.CUSTOMER_ORDER_LINE_PROMO_ID=SAS_COL_PROMO_STG.CUSTOMER_ORDER_LINE_PROMO_ID
		group by 	SUB.CUSTOMER_ORDER_LINE_PROMO_ID
		having 	count(1) > 1
		)
 ';

END;

/*  Checked Datastore =RAX_APP_USER.I$_SAS_COL_PROMO_STG  */
/*  Integration Datastore =RAX_APP_USER.I$_SAS_COL_PROMO_STG   */
/*  Target Datastore =ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG */



&


/*-----------------------------------------------*/
/* TASK No. 23 */
/* insert Not Null errors */

insert into RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_PK,
	ODI_SESS_NO,
	ODI_ROW_ID,
	ODI_ERR_TYPE,
	ODI_ERR_MESS,
	ODI_CHECK_DATE,
	ODI_ORIGIN,
	ODI_CONS_NAME,
	ODI_CONS_TYPE,
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
)
select
	SYS_GUID(),
	:v_sess_no, 
	rowid,
	'F', 
	'ODI-15066: The column CUSTOMER_ORDER_LINE_ID cannot be null.',
	sysdate,
	'(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT',
	'CUSTOMER_ORDER_LINE_ID',
	'NN',	
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
from	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
where	CUSTOMER_ORDER_LINE_ID is null



&


/*-----------------------------------------------*/
/* TASK No. 24 */
/* insert Not Null errors */

insert into RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_PK,
	ODI_SESS_NO,
	ODI_ROW_ID,
	ODI_ERR_TYPE,
	ODI_ERR_MESS,
	ODI_CHECK_DATE,
	ODI_ORIGIN,
	ODI_CONS_NAME,
	ODI_CONS_TYPE,
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
)
select
	SYS_GUID(),
	:v_sess_no, 
	rowid,
	'F', 
	'ODI-15066: The column PROMOTION_CODE cannot be null.',
	sysdate,
	'(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT',
	'PROMOTION_CODE',
	'NN',	
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
from	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
where	PROMOTION_CODE is null



&


/*-----------------------------------------------*/
/* TASK No. 25 */
/* insert Not Null errors */

insert into RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
(
	ODI_PK,
	ODI_SESS_NO,
	ODI_ROW_ID,
	ODI_ERR_TYPE,
	ODI_ERR_MESS,
	ODI_CHECK_DATE,
	ODI_ORIGIN,
	ODI_CONS_NAME,
	ODI_CONS_TYPE,
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
)
select
	SYS_GUID(),
	:v_sess_no, 
	rowid,
	'F', 
	'ODI-15066: The column CUSTOMER_ORDER_LINE_PROMO_ID cannot be null.',
	sysdate,
	'(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT',
	'CUSTOMER_ORDER_LINE_PROMO_ID',
	'NN',	
	CUSTOMER_ORDER_LINE_ID,
	PROMOTION_CODE,
	DISCOUNT_AMOUNT,
	CUSTOMER_ORDER_LINE_PROMO_ID,
	ITEM_NUMBER,
	ODS_CREATE_DATE,
	ODS_MODIFY_DATE,
	DELETED_IND,
	AUDIT_CREATE_DATE,
	AUDIT_MODIFY_DATE
from	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001
where	CUSTOMER_ORDER_LINE_PROMO_ID is null



&


/*-----------------------------------------------*/
/* TASK No. 26 */
/* create index on error table */

 
/* FLOW CONTROL CREATE INDEX ON THE E$TABLE */
-- create index 	RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
-- on	RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001 (ODI_ROW_ID)

BEGIN
  EXECUTE IMMEDIATE 'create index RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001
on  RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001 (ODI_ROW_ID)';
EXCEPTION
  WHEN OTHERS THEN
   -- Handle the case where the identifier is too long (ORA-00972)
   IF SQLCODE = -972 or  SQLCODE = -1408 or SQLCODE = -955  THEN
     DBMS_OUTPUT.PUT_LINE('Identifier is too long. Skipping creation of index.');
   ELSE
     RAISE;
   END IF;
END;

&


/*-----------------------------------------------*/
/* TASK No. 27 */
/* delete errors from controlled table */

delete from	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001  T
where	exists 	(
		select	1
		from	RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001 E
		where ODI_SESS_NO = :v_sess_no
		and T.rowid = E.ODI_ROW_ID
		)


&


/*-----------------------------------------------*/
/* TASK No. 28 */
/* insert check sum into check table */

insert into RAX_APP_USER.SNP_CHECK_TAB
(
	SCHEMA_NAME,
	RESOURCE_NAME,
	FULL_RES_NAME,
	ERR_TYPE,
	ERR_MESS,
	CHECK_DATE,
	ORIGIN,
	CONS_NAME,
	CONS_TYPE,
	ERR_COUNT
)
select	
	'ODS_STAGE',
	'SAS_COL_PROMO_STG',
	'ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG1388001',
	E.ODI_ERR_TYPE,
	E.ODI_ERR_MESS,
	E.ODI_CHECK_DATE,
	E.ODI_ORIGIN,
	E.ODI_CONS_NAME,
	E.ODI_CONS_TYPE,
	count(1) 
from	RAX_APP_USER.E$_SAS_COL_PROMO_STG1388001 E
where	E.ODI_ERR_TYPE	= 'F'
and	E.ODI_ORIGIN 	= '(1388001)ODS_Project.LOAD_SAS_CUST_ORD_LINE_PROMO_STG_INT'
group by	E.ODI_ERR_TYPE,
	E.ODI_ERR_MESS,
	E.ODI_CHECK_DATE,
	E.ODI_ORIGIN,
	E.ODI_CONS_NAME,
	E.ODI_CONS_TYPE


&


/*-----------------------------------------------*/
/* TASK No. 29 */
/* Merge Rows */

merge into	ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG T
using	RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001 S
on	(
		T.CUSTOMER_ORDER_LINE_PROMO_ID=S.CUSTOMER_ORDER_LINE_PROMO_ID
	)
when matched
then update set
	T.CUSTOMER_ORDER_LINE_ID	= S.CUSTOMER_ORDER_LINE_ID,
	T.PROMOTION_CODE	= S.PROMOTION_CODE,
	T.DISCOUNT_AMOUNT	= S.DISCOUNT_AMOUNT,
	T.ITEM_NUMBER	= S.ITEM_NUMBER,
	T.DELETED_IND	= S.DELETED_IND,
	T.AUDIT_CREATE_DATE	= S.AUDIT_CREATE_DATE,
	T.AUDIT_MODIFY_DATE	= S.AUDIT_MODIFY_DATE
	,       T.ODS_MODIFY_DATE	= sysdate
when not matched
then insert
	(
	T.CUSTOMER_ORDER_LINE_ID,
	T.PROMOTION_CODE,
	T.DISCOUNT_AMOUNT,
	T.CUSTOMER_ORDER_LINE_PROMO_ID,
	T.ITEM_NUMBER,
	T.DELETED_IND,
	T.AUDIT_CREATE_DATE,
	T.AUDIT_MODIFY_DATE
	,        T.ODS_CREATE_DATE,
	T.ODS_MODIFY_DATE
	)
values
	(
	S.CUSTOMER_ORDER_LINE_ID,
	S.PROMOTION_CODE,
	S.DISCOUNT_AMOUNT,
	S.CUSTOMER_ORDER_LINE_PROMO_ID,
	S.ITEM_NUMBER,
	S.DELETED_IND,
	S.AUDIT_CREATE_DATE,
	S.AUDIT_MODIFY_DATE
	,        sysdate,
	sysdate
	)

&


/*-----------------------------------------------*/
/* TASK No. 30 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 31 */
/* Drop flow table */

-- drop table RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.I$_SAS_COL_PROMO_STG1388001';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;


&


/*-----------------------------------------------*/
/* TASK No. 1000008 */
/* Drop work table */

-- drop table RAX_APP_USER.C$_0SAS_COL_PROMO_STG purge

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.C$_0SAS_COL_PROMO_STG purge';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;

&


/*-----------------------------------------------*/
/* TASK No. 32 */
/* MERGE INTO SAS_CUST_ORD_LINE_PROMO_XR */

-- ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_XR
MERGE INTO ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_XR d
USING (
select * from
    (  Select
        XR.CUSTOMER_ORDER_LINE_PROMO_OID as CUSTOMER_ORDER_LINE_PROMO_OID,
        STG.CUSTOMER_ORDER_LINE_PROMO_ID as SK_CUSTOMER_ORD_LINE_PROMO_ID      ,
        STG.CUSTOMER_ORDER_LINE_ID as SK_CUSTOMER_ORDER_LINE_ID,
        sysdate as ODS_CREATE_DATE,
        sysdate as ODS_MODIFY_DATE
    FROM 
        ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG stg
        ,ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_XR xr
    WHERE (1=1)
        and stg.ODS_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap
        and STG.CUSTOMER_ORDER_LINE_PROMO_ID=XR.SK_CUSTOMER_ORD_LINE_PROMO_ID(+)
     ) s
where NOT EXISTS 
    ( select 1 from ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_XR T
    where    T.SK_CUSTOMER_ORD_LINE_PROMO_ID = S.SK_CUSTOMER_ORD_LINE_PROMO_ID 
         and
        ((T.CUSTOMER_ORDER_LINE_PROMO_OID = S.CUSTOMER_ORDER_LINE_PROMO_OID) or (T.CUSTOMER_ORDER_LINE_PROMO_OID  IS NULL and S.CUSTOMER_ORDER_LINE_PROMO_OID   IS NULL)) and
        ((T.SK_CUSTOMER_ORDER_LINE_ID = S.SK_CUSTOMER_ORDER_LINE_ID) or (T.SK_CUSTOMER_ORDER_LINE_ID IS NULL and S.SK_CUSTOMER_ORDER_LINE_ID IS NULL)) 
    )    
) s 
ON
  (s.SK_CUSTOMER_ORD_LINE_PROMO_ID =d.SK_CUSTOMER_ORD_LINE_PROMO_ID)
WHEN MATCHED
THEN
UPDATE SET
  d.CUSTOMER_ORDER_LINE_PROMO_OID = s.CUSTOMER_ORDER_LINE_PROMO_OID,
  d.SK_CUSTOMER_ORDER_LINE_ID = s.SK_CUSTOMER_ORDER_LINE_ID,
  d.ODS_MODIFY_DATE = s.ODS_MODIFY_DATE
WHEN NOT MATCHED
THEN
INSERT (
  CUSTOMER_ORDER_LINE_PROMO_OID, SK_CUSTOMER_ORDER_LINE_ID, SK_CUSTOMER_ORD_LINE_PROMO_ID,
   ODS_CREATE_DATE, ODS_MODIFY_DATE)
VALUES (
  ODS_STAGE.CUST_ORD_LINE_PROMO_OID_SEQ.nextval, s.SK_CUSTOMER_ORDER_LINE_ID, s.SK_CUSTOMER_ORD_LINE_PROMO_ID,
   s.ODS_CREATE_DATE, s.ODS_MODIFY_DATE
  )

&


/*-----------------------------------------------*/
/* TASK No. 33 */
/* MERGE INTO SAS_CUSTOMER_ORDER_LINE_PROMO */

MERGE INTO ODS_OWN.SAS_CUSTOMER_ORDER_LINE_PROMO d
USING (
select * from (
      Select
        xr.CUSTOMER_ORDER_LINE_PROMO_OID  as CUSTOMER_ORDER_LINE_PROMO_OID ,
        axr.CUSTOMER_ORDER_LINE_OID as  CUSTOMER_ORDER_LINE_OID,
        stg.AUDIT_CREATE_DATE as AUDIT_CREATE_DATE,
        stg.AUDIT_MODIFY_DATE as  AUDIT_MODIFY_DATE,             
        stg.DISCOUNT_AMOUNT,           
        stg.PROMOTION_CODE,      
        stg.ITEM_NUMBER,
        stg.DELETED_IND                ,
        sysdate as ODS_CREATE_DATE,
        sysdate as ODS_MODIFY_DATE,
       SS.SOURCE_SYSTEM_OID as SOURCE_SYSTEM_OID
 
    FROM 
        ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_STG stg
        ,ODS_STAGE.SAS_CUST_ORD_LINE_PROMO_XR xr
        ,ODS_STAGE.SAS_CUSTOMER_ORDER_LINE_XR axr
       ,ODS_OWN.SOURCE_SYSTEM SS
    WHERE (1=1)
        and stg.ODS_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  -:v_cdc_overlap
  and SS.SOURCE_SYSTEM_SHORT_NAME='SAS'
        and stg.CUSTOMER_ORDER_LINE_PROMO_ID =xr.SK_CUSTOMER_ORD_LINE_PROMO_ID(+)
        and xr.SK_CUSTOMER_ORDER_LINE_ID =AXR.SK_CUSTOMER_ORDER_LINE_ID (+)
   ) s
where NOT EXISTS 
    ( select 1 from ODS_OWN.SAS_CUSTOMER_ORDER_LINE_PROMO T
    where    T.CUSTOMER_ORDER_LINE_PROMO_OID    = S.CUSTOMER_ORDER_LINE_PROMO_OID 
     and ((T.CUSTOMER_ORDER_LINE_PROMO_OID = S.CUSTOMER_ORDER_LINE_PROMO_OID) or (T.CUSTOMER_ORDER_LINE_PROMO_OID IS NULL and S.CUSTOMER_ORDER_LINE_PROMO_OID IS NULL)) and
 ((T.CUSTOMER_ORDER_LINE_OID = S.CUSTOMER_ORDER_LINE_OID) or (T.CUSTOMER_ORDER_LINE_OID IS NULL and S.CUSTOMER_ORDER_LINE_OID IS NULL)) and
 ((T.AUDIT_CREATE_DATE = S.AUDIT_CREATE_DATE) or (T.AUDIT_CREATE_DATE IS NULL and S.AUDIT_CREATE_DATE IS NULL)) and
        ((T.AUDIT_MODIFY_DATE = S.AUDIT_MODIFY_DATE) or (T.AUDIT_MODIFY_DATE IS NULL and S.AUDIT_MODIFY_DATE IS NULL)) and
 ((T.SOURCE_SYSTEM_OID = S.SOURCE_SYSTEM_OID) or (T.SOURCE_SYSTEM_OID IS NULL and S.SOURCE_SYSTEM_OID IS NULL)) AND
 ((T.DISCOUNT_AMOUNT=S.DISCOUNT_AMOUNT) or(T.DISCOUNT_AMOUNT  IS NULL and S.DISCOUNT_AMOUNT IS NULL)) AND    
 ((T.PROMOTION_CODE=S.PROMOTION_CODE) or(T.PROMOTION_CODE  IS NULL and S.PROMOTION_CODE IS NULL)) AND  
((T.ITEM_NUMBER =S.ITEM_NUMBER) or(T.ITEM_NUMBER  IS NULL and S.ITEM_NUMBER IS NULL)) AND  
((T.DELETED_IND  =S.DELETED_IND  ) or(T.DELETED_IND  IS NULL and S.DELETED_IND  IS NULL))   
        ) 
) s 
ON
  (d.CUSTOMER_ORDER_LINE_PROMO_OID = s.CUSTOMER_ORDER_LINE_PROMO_OID)
WHEN MATCHED
THEN
UPDATE SET
  d.CUSTOMER_ORDER_LINE_OID = s.CUSTOMER_ORDER_LINE_OID,
  d.AUDIT_CREATE_DATE = s.AUDIT_CREATE_DATE,
  d.AUDIT_MODIFY_DATE = s.AUDIT_MODIFY_DATE,
  d.SOURCE_SYSTEM_OID = s.SOURCE_SYSTEM_OID,
  d.DISCOUNT_AMOUNT=s.DISCOUNT_AMOUNT,
  d.PROMOTION_CODE=s.PROMOTION_CODE,
  d.ITEM_NUMBER=s.ITEM_NUMBER,
  d.ODS_MODIFY_DATE=s.ODS_MODIFY_DATE,
  d.DELETED_IND=s.DELETED_IND
WHEN NOT MATCHED
THEN
INSERT (CUSTOMER_ORDER_LINE_PROMO_OID,CUSTOMER_ORDER_LINE_OID, 
  AUDIT_CREATE_DATE,
 AUDIT_MODIFY_DATE, DISCOUNT_AMOUNT ,ODS_MODIFY_DATE,ODS_CREATE_DATE,DELETED_IND,
PROMOTION_CODE,ITEM_NUMBER

)VALUES(s.CUSTOMER_ORDER_LINE_PROMO_OID,s.CUSTOMER_ORDER_LINE_OID,
  s.AUDIT_CREATE_DATE,
   s.AUDIT_MODIFY_DATE,s.DISCOUNT_AMOUNT, s.ODS_MODIFY_DATE,s.ODS_CREATE_DATE,s.DELETED_IND,s.PROMOTION_CODE,
s.ITEM_NUMBER

)



&


/*-----------------------------------------------*/
/* TASK No. 34 */
/* Update CDC Load Status */

UPDATE ODS_OWN.ODS_CDC_LOAD_STATUS
SET LAST_CDC_COMPLETION_DATE=TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS')
+ nvl((TIMEZONE_OFFSET/24), 0) 
WHERE ODS_TABLE_NAME=:v_cdc_load_table_name
AND CONTEXT_NAME = :v_env

/*
UPDATE ODS_OWN.ODS_CDC_LOAD_STATUS
SET LAST_CDC_COMPLETION_DATE=TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS')
WHERE ODS_TABLE_NAME=:v_cdc_load_table_name
AND CONTEXT_NAME = :v_env
*/

&


/*-----------------------------------------------*/
/* TASK No. 35 */
/* Insert CDC Audit Record */

INSERT INTO RAX_APP_USER.ODS_CDC_LOAD_STATUS_AUDIT
(TABLE_NAME,
SESS_NO,                      
SESS_NAME,                    
SCEN_VERSION,                 
SESS_BEG,                     
ORIG_LAST_CDC_COMPLETION_DATE,
OVERLAP,
CREATE_DATE,
CONTEXT_NAME,
TIMEZONE_OFFSET              
)
select 
:v_cdc_load_table_name
,:v_sess_no
,'LOAD_SAS_CUST_ORD_LINE_PROMO_PKG'
,'004'
,TO_DATE(SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS')
,TO_DATE (SUBSTR(:v_cdc_load_date, 1, 19),'YYYY-MM-DD HH24:MI:SS')
,:v_cdc_overlap
,SYSDATE
,:v_env
,TIMEZONE_OFFSET
from 
ODS_OWN.ODS_CDC_LOAD_STATUS
WHERE ODS_TABLE_NAME=:v_cdc_load_table_name
AND CONTEXT_NAME = :v_env

/*
INSERT INTO RAX_APP_USER.ODS_CDC_LOAD_STATUS_AUDIT
(TABLE_NAME,
SESS_NO,                      
SESS_NAME,                    
SCEN_VERSION,                 
SESS_BEG,                     
ORIG_LAST_CDC_COMPLETION_DATE,
OVERLAP,
CREATE_DATE,
CONTEXT_NAME              
)
values (
:v_cdc_load_table_name,
:v_sess_no,
'LOAD_SAS_CUST_ORD_LINE_PROMO_PKG',
'004',
TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS'),
TO_DATE (SUBSTR (:v_cdc_load_date, 1, 19),
                           'YYYY-MM-DD HH24:MI:SS'
                          )
,:v_cdc_overlap,
SYSDATE,
 :v_env)
*/


&


/*-----------------------------------------------*/
