DECLARE create_query CLOB;  
   BEGIN  
      BEGIN  
         EXECUTE IMMEDIATE 'drop table RAX_APP_USER.C$_0OMS2_LT_SB_A_TIE_STG purge';  
      EXCEPTION  
         WHEN OTHERS THEN NULL;  
      END;  
      create_query := 'create table RAX_APP_USER.C$_0OMS2_LT_SB_A_TIE_STG
(
	C1_SUB_APO_ASSOC_KEY	VARCHAR2(24) NULL,
	C2_SUBJECT_KEY	VARCHAR2(24) NULL,
	C3_APO_KEY	VARCHAR2(24) NULL,
	C4_STATUS	VARCHAR2(15) NULL,
	C5_CREATETS	DATE NULL,
	C6_MODIFYTS	DATE NULL,
	C7_CREATEUSERID	VARCHAR2(40) NULL,
	C8_MODIFYUSERID	VARCHAR2(40) NULL,
	C9_CREATEPROGID	VARCHAR2(40) NULL,
	C10_MODIFYPROGID	VARCHAR2(40) NULL,
	C11_LOCKID	NUMBER NULL,
	C12_SUBJECT_APO_SEQUENCE	VARCHAR2(4) NULL
)
NOLOGGING';  
      EXECUTE IMMEDIATE create_query;  
   END;  