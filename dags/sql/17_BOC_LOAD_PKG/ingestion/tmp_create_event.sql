DECLARE create_query CLOB;  
   BEGIN  
      BEGIN  
         EXECUTE IMMEDIATE 'DROP TABLE RAX_APP_USER.C$_0BOC_EVENT';  
      EXCEPTION  
         WHEN OTHERS THEN NULL;  
      END;  
      create_query := 'create table RAX_APP_USER.C$_0BOC_EVENT ( C1_EVENT_ID	NUMBER(19) NULL, C2_CREATEDATE	TIMESTAMP(6) NULL, C3_CREATEDBY	VARCHAR2(255) NULL, C4_MODIFIEDBY	VARCHAR2(255) NULL, C5_MODIFYDATE	TIMESTAMP(6) NULL, C6_EVENT_DATE	TIMESTAMP(6) NULL, C7_EVENT_REF_ID	VARCHAR2(12) NULL, C8_EVENT_STATUS	VARCHAR2(255) NULL, C9_IS_XORDERS_CREATED	NUMBER(1) NULL, C10_ACCOUNT_PROGRAM_OCCURENCE_	NUMBER(19) NULL, C11_IS_STAFF_ORDERS_CREATED	NUMBER(1) NULL, C12_IS_MISSING_XORDERS_CREATED	NUMBER(1) NULL, C13_IS_EVENT_IMAGE_COMPLETED	NUMBER(1) NULL, C14_IS_RETAKE_ORDERS_CREATED	NUMBER(1) NULL )';  
      EXECUTE IMMEDIATE create_query;
   END;  