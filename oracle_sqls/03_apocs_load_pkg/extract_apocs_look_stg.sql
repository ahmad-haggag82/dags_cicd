SELECT      LOOK_ID        
,AUDIT_CREATE_DATE      
,AUDIT_CREATED_BY   
,AUDIT_MODIFIED_BY
,AUDIT_MODIFY_DATE   
,BUSINESS_KEY    
,LOCKID         
,LOOK_DESC   
,LOOK_NO             
,LOOK_PREF_SEQ     
,LAYOUT_GROUP_ID 
  FROM <schema_name>.LOOK
 WHERE AUDIT_MODIFY_DATE >= TO_DATE(SUBSTR(:v_cdc_load_date, 1, 19), 'YYYY-MM-DD HH24:MI:SS')  - 1