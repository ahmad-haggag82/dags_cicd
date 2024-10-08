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

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 4 */




/*-----------------------------------------------*/
/* TASK No. 5 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 5 */




/*-----------------------------------------------*/
/* TASK No. 6 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 6 */




/*-----------------------------------------------*/
/* TASK No. 7 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 7 */




/*-----------------------------------------------*/
/* TASK No. 8 */
/* drop ep_trans_dtl_dump_all */


BEGIN
  EXECUTE IMMEDIATE 'drop table RAX_APP_USER.ep_trans_dtl_dump_all';
EXCEPTION
  WHEN OTHERS THEN
   IF SQLCODE != -942 THEN
     RAISE;
   END IF;
END;


&


/*-----------------------------------------------*/
/* TASK No. 9 */
/* CREATE AND LOAD EP_TRANS_DTL_DMP */

CREATE TABLE EP_TRANS_DTL_DUMP_ALL
AS
 SELECT 'Event_Payment' AS transaction_source,
           ep.event_ref_id event_ref_id,
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
           AP.APO_TYPE
      FROM ods_own.gl_transaction_detail gtd,
           ODS_OWN.EVENT e,
           ODS_OWN.APO AP,
           ods_own.event_payment ep
     WHERE     gtd.event_payment_oid IS NOT NULL
           AND GTD.EVENT_OID = E.EVENT_OID
           AND E.APO_OID = AP.APO_OID
           AND gtd.event_payment_oid = ep.event_payment_oid(+)
           AND gtd.record_status != 'IGNORE'
           AND gtd.transmit_date IS NOT NULL
            AND gtd.posting_date BETWEEN TRUNC (TRUNC (SYSDATE, 'MM') - 1, 'MM')
            AND TRUNC (SYSDATE, 'MM') - 1


&


/*-----------------------------------------------*/
/* TASK No. 10 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 10 */




/*-----------------------------------------------*/
/* TASK No. 11 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 11 */




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
/* TASK No. 16 */
/* Create and load dump table */

CREATE TABLE GL_TRANS_DTL_DUMP
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
             FROM ep_trans_dtl_dump_all)
 WHERE r BETWEEN :v_ep_dump_start_ct AND :v_ep_dump_start_ct + :v_ep_dump_file_max - 1
