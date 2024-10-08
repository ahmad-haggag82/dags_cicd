BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.gl_trans_dtl_dump_all';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;
&


/*-----------------------------------------------*/
/* TASK No. 10 */
/* Create and load gl_trans_dtl_dmp_all */

/* LOAD_GL_TRANS_DTL_DMP_PRC */
CREATE TABLE RAX_APP_USER.gl_trans_dtl_dump_all
AS
   SELECT case when gtd.event_payment_oid is not null then 'Event_Payment'
                         when gtd.sales_recognition_oid is not null then 'Sales_Rec'
                         when gtd.matched_sales_recognition_oid is not null then 'Matched_Sales_Rec'
                         when gtd.event_adjustment_oid is not null then 'Event_Adj'
                         when gtd.chargeback_fact_oid is not null then 'Chargeback'
                         else 'Other'
                 end AS transaction_source,
          gtd.event_ref_id event_ref_id,
          gtd.gl_transaction_detail_oid,
          gtd.gl_transaction_oid,
          gtd.ods_create_date,
          gtd.ods_modify_date,
          gtd.gl_account_type,
          gtd.gl_debit_credit_ind,
          gtd.posting_date,
          gtd.amount,
          gtd.currency_code,
          gtd.country,
          gtd.sales_tax_state_code,
          gtd.territory_code,
          gtd.business_unit,
          gtd.program_name,
          gtd.sub_program_name,
          gtd.bank_code,
          gtd.gl_company,
          gtd.gl_account,
          gtd.gl_sub_account,
          gtd.gl_accounting_unit,
          REGEXP_REPLACE (gtd.gl_description, '[^[:alnum:] ]*', '')
             gl_description,
          gtd.gl_program_code,
          gtd.event_payment_oid,
          gtd.sales_recognition_oid,
          gtd.matched_sales_recognition_oid,
          gtd.event_adjustment_oid,
          gtd.chargeback_fact_oid,
          gtd.gl_activity,
          gtd.inv_event_pay_oid,
          gtd.record_status,
          gtd.apo_type_name as APO_TYPE
,gtd.company_code
,gtd.profit_center
,gtd.run_group
     FROM ODS_OWN.gl_transaction_detail gtd
    WHERE    1=1
          AND gtd.record_status != 'IGNORE'
          AND gtd.program_name = 'Yearbook'
            AND gtd.posting_date BETWEEN TRUNC (TRUNC (SYSDATE , 'MM') - 1, 'MM')
            AND TRUNC (SYSDATE, 'MM') - 1

&


/*-----------------------------------------------*/
/* TASK No. 11 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 11 */




/*-----------------------------------------------*/
/* TASK No. 12 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 12 */




/*-----------------------------------------------*/
/* TASK No. 13 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 13 */




/*-----------------------------------------------*/
/* TASK No. 14 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 14 */




/*-----------------------------------------------*/
/* TASK No. 15 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 15 */




/*-----------------------------------------------*/
/* TASK No. 16 */
/* Drop table */

BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.GL_TRANS_DTL_DUMP';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;
&


/*-----------------------------------------------*/
/* TASK No. 17 */
/* Create and load dump table */

CREATE TABLE RAX_APP_USER.GL_TRANS_DTL_DUMP

AS
   SELECT transaction_source,
          event_ref_id,
          gl_transaction_detail_oid,
          gl_transaction_oid,
          ods_create_date,
          ods_modify_date,
          gl_account_type,
          gl_debit_credit_ind,
          posting_date,
          amount,
          currency_code,
          country,
          sales_tax_state_code,
          territory_code,
          business_unit,
          program_name,
          sub_program_name,
          bank_code,
          gl_company,
          gl_account,
          gl_sub_account,
          gl_accounting_unit,
          gl_description,
          gl_program_code,
          event_payment_oid,
          sales_recognition_oid,
          matched_sales_recognition_oid,
          event_adjustment_oid,
          chargeback_fact_oid,
          gl_activity,
          inv_event_pay_oid,
          record_status,
          APO_TYPE
,company_code
,profit_center
,run_group
     FROM (SELECT transaction_source,
                  event_ref_id,
                  gl_transaction_detail_oid,
                  gl_transaction_oid,
                  ods_create_date,
                  ods_modify_date,
                  gl_account_type,
                  gl_debit_credit_ind,
                  posting_date,
                  amount,
                  currency_code,
                  country,
                  sales_tax_state_code,
                  territory_code,
                  business_unit,
                  program_name,
                  sub_program_name,
                  bank_code,
                  gl_company,
                  gl_account,
                  gl_sub_account,
                  gl_accounting_unit,
                  gl_description,
                  gl_program_code,
                  event_payment_oid,
                  sales_recognition_oid,
                  matched_sales_recognition_oid,
                  event_adjustment_oid,
                  chargeback_fact_oid,
                  gl_activity,
                  inv_event_pay_oid,
                  record_status,
                  APO_TYPE,
                  ROW_NUMBER () OVER (ORDER BY gl_transaction_detail_oid) r
,company_code
,profit_center
,run_group
             FROM RAX_APP_USER.gl_trans_dtl_dump_all)
 WHERE r BETWEEN :v_gl_dump_start_ct AND :v_gl_dump_start_ct + :v_gl_dump_file_max - 1
