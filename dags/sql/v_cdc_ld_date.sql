SELECT LAST_CDC_COMPLETION_DATE
FROM ODS_OWN.ODS_CDC_LOAD_STATUS
WHERE ODS_TABLE_NAME=:v_cdc_load_table_name