/* TASK No. 1 */

/* SELECT STATEMENT FOUND, CHECK ODI TASK NO. 1 */




/*-----------------------------------------------*/
/* TASK No. 2 */

/* NONE or SET VARIABLE STATEMENT FOUND, CHECK ODI TASK NO. 2 */




/*-----------------------------------------------*/
/* TASK No. 3 */
/* Copy WO Pending Class Pic exception scenario to Shipped  */

-- Copy the WO which has all products fulfilled except classpic to Shipped Table
-- This will make sure all shipped products are accounted except Class pic for that WO


MERGE INTO ODS_STAGE.X1_WO_SHIPPED_HIST TGT
    USING (
    SELECT
    PLATFORM_DETAIL             	,
    WORK_ORDER_ID               	,
    EVENT_REF_ID                	,
    JOB_TYPE                    		,
    LID                         		,
    ACCT_NAME                   	,
    SHIP_PROPSL_RECEIVED        	,
    MAIL_RECEIVED               	,
    BATCH_ID                    		,
    WORK_ORDER_ALIAS_ID         	,
    SUBJECT_COUNT               	,
    WORK_ORDER_FORM_PRT         	,
    RET                         		,
    RET_QTY                     		,
    RET_PRT                     		,
    PKG                         		,
    PKG_QTY                     		,
    PKG_PRT                     		,
    POS                         		,
    POS_QTY                     		,
    POS_PRT                     		,
    DIGITAL_MEDIA1       	       	,
    DIGITAL_MEDIA1_QTY          	,
    DIGITAL_MEDIA1_PRT          	,	
    DIGITAL_MEDIA2              	,	
    DIGITAL_MEDIA2_QTY          	,
    DIGITAL_MEDIA2_PRT          	,
    LAMINATE                  	 	,
    LAMINATE_QTY                	,
    LAMINATE_PRT               	,
    MAGNET                      		,
    MAGNET_QTY                  	,
    MAGNET_PRT                  	,
    METALLIC                    		,
    METALLIC_QTY                	,
    METALLIC_PRT               	,
    SPECIALITY                  		,
    SPECIALITY_QTY              	,
    SPECIALITY_PRODUCED         	,
    INDIGO_PRODUCT1             	,
    INDIGO_PRODUCT1_QTY         	,
    INDIGO_PRODUCT1_PRODUCED    	,
    INDIGO_PRODUCT2             	,
    INDIGO_PRODUCT2_QTY         	,
    INDIGO_PRODUCT2_PRODUCED    	,
    PROOF_PRODUCT              	,
    PROOF_QTY                   	,
    PROOF_PRODUCED              	,
    CLASS_DELIVERY              	,
    CLASS                       		,
    CLASS_QTY                   		,
    CLASS_PRT                   		,
    DID                         		,
    DID_QTY                     		,
    DID_PRT                     		,
    DSSK                        		,
    DSSK_QTY                    		,
    DSSK_PRT                    		,
    EZP                         		,
    EZP_QTY                     		,	
    EZP_PRT                     		,
    SPECIAL_REQUEST             	,	  
    WORK_ORDER_STATUS           	,
    PROCESSING_COMMENT          	,
    SHIP_GOAL_DATE              	,
    WOMS_NODE                   	,
    MANUAL_DEACTIVATED_FLAG     	,
    PENDING_CLASS_PRODUCT       	,
    MANUALLY_SHIPPED       
                     FROM ODS_STAGE.X1_WO_UNSHIPPED_HIST UNSHIPPED                                           
                     WHERE UNSHIPPED.PENDING_CLASS_PRODUCT IS NOT NULL
                     AND UNSHIPPED.ODS_MODIFY_DATE > ( SELECT LAST_CDC_COMPLETION_DATE - .010 
                                                                                         FROM ODS.DW_CDC_LOAD_STATUS
                                                                                         WHERE DW_TABLE_NAME = 'PROC_CLASSPIC_EXC_TO_SHIPPED')
    )SRC
    ON ( TGT.WORK_ORDER_ID = SRC.WORK_ORDER_ID 
         AND TGT.WOMS_NODE = SRC.WOMS_NODE
         AND TGT.LID = SRC.LID
        )
  WHEN MATCHED THEN
    UPDATE 
    SET 
TGT.PLATFORM_DETAIL         	=    SRC.PLATFORM_DETAIL         	,
TGT.EVENT_REF_ID            	=    SRC.EVENT_REF_ID            	,
TGT.JOB_TYPE                		=    SRC.JOB_TYPE                	,
TGT.ACCT_NAME               	=    SRC.ACCT_NAME               	,
TGT.SHIP_PROPSL_RECEIVED    	=    SRC.SHIP_PROPSL_RECEIVED    	,
TGT.MAIL_RECEIVED           	=    SRC.MAIL_RECEIVED           	,
TGT.BATCH_ID                		=    SRC.BATCH_ID                	,
TGT.WORK_ORDER_ALIAS_ID     	=    SRC.WORK_ORDER_ALIAS_ID     	,
TGT.SUBJECT_COUNT           	=    SRC.SUBJECT_COUNT           	,
TGT.WORK_ORDER_FORM_PRT     	=    SRC.WORK_ORDER_FORM_PRT     ,
TGT.RET                     		=    SRC.RET        		,
TGT.RET_QTY                 		=    SRC.RET_QTY    		,
TGT.RET_PRT                 		=    SRC.RET_PRT    		,
TGT.PKG                     		=    SRC.PKG        		,
TGT.PKG_QTY                 		=    SRC.PKG_QTY    		,
TGT.PKG_PRT                 		=    SRC.PKG_PRT    		,
TGT.POS                     		=    SRC.POS        		,
TGT.POS_QTY                 		=    SRC.POS_QTY    		,
TGT.POS_PRT                 		=    SRC.POS_PRT    		,
TGT.DIGITAL_MEDIA1              	=    SRC.DIGITAL_MEDIA1               	,
TGT.DIGITAL_MEDIA1_QTY          	=    SRC.DIGITAL_MEDIA1_QTY           	,
TGT.DIGITAL_MEDIA1_PRT          	=    SRC.DIGITAL_MEDIA1_PRT           	,
TGT.DIGITAL_MEDIA2              	=    SRC.DIGITAL_MEDIA2               	,
TGT.DIGITAL_MEDIA2_QTY          	=    SRC.DIGITAL_MEDIA2_QTY           	,
TGT.DIGITAL_MEDIA2_PRT          	=    SRC.DIGITAL_MEDIA2_PRT           	,
TGT.LAMINATE                    	=    SRC.LAMINATE                       	,
TGT.LAMINATE_QTY                	=    SRC.LAMINATE_QTY                   	,
TGT.LAMINATE_PRT                	=    SRC.LAMINATE_PRT                   	,
TGT.MAGNET                      	=    SRC.MAGNET                         	,
TGT.MAGNET_QTY                  	=    SRC.MAGNET_QTY                           ,
TGT.MAGNET_PRT                  	=    SRC.MAGNET_PRT                           ,
TGT.METALLIC                    	=    SRC.METALLIC                             	,
TGT.METALLIC_QTY                	=    SRC.METALLIC_QTY                         ,
TGT.METALLIC_PRT                	=    SRC.METALLIC_PRT                         ,
TGT.SPECIALITY                  	=    SRC.SPECIALITY                           	,
TGT.SPECIALITY_QTY              	=    SRC.SPECIALITY_QTY                      ,
TGT.SPECIALITY_PRODUCED         	=    SRC.SPECIALITY_PRODUCED          ,
TGT.INDIGO_PRODUCT1             	=    SRC.INDIGO_PRODUCT1                 ,
TGT.INDIGO_PRODUCT1_QTY         	=    TGT.INDIGO_PRODUCT1_QTY                  	,
TGT.INDIGO_PRODUCT1_PRODUCED    =    TGT.INDIGO_PRODUCT1_PRODUCED             	,
TGT.INDIGO_PRODUCT2             	=    TGT.INDIGO_PRODUCT2                      	,
TGT.INDIGO_PRODUCT2_QTY         	=    TGT.INDIGO_PRODUCT2_QTY                  	,
TGT.INDIGO_PRODUCT2_PRODUCED    =    TGT.INDIGO_PRODUCT2_PRODUCED             	,
TGT.PROOF_PRODUCT               	=    SRC.PROOF_PRODUCT                        	,
TGT.PROOF_QTY                   	=    SRC.PROOF_QTY                            		,
TGT.PROOF_PRODUCED              	=    SRC.PROOF_PRODUCED                       	,
TGT.CLASS_DELIVERY              	=    SRC.CLASS_DELIVERY                      	 ,
TGT.CLASS            		=    SRC.CLASS                         	,
TGT.CLASS_QTY        		=    NULL                              	,
TGT.CLASS_PRT        		=    NULL                              	,
TGT.DID              		=    SRC.DID                           	,
TGT.DID_QTY          		=    SRC.DID_QTY                       	,
TGT.DID_PRT          		=    SRC.DID_PRT                       	,
TGT.DSSK             		=    SRC.DSSK                          	,
TGT.DSSK_QTY        		=    SRC.DSSK_QTY                      	,
TGT.DSSK_PRT         		=    SRC.DSSK_PRT                      	,
TGT.EZP              		=    SRC.EZP                           	,
TGT.EZP_QTY          		=    SRC.EZP_QTY                       	,
TGT.EZP_PRT          		=    SRC.EZP_PRT                       	,
TGT.SPECIAL_REQUEST       	=    SRC.SPECIAL_REQUEST          	,
TGT.WORK_ORDER_STATUS     	=    SRC.WORK_ORDER_STATUS        	,
TGT.PROCESSING_COMMENT    	=    SRC.PROCESSING_COMMENT       	,
TGT.SHIP_GOAL_DATE        	=    SRC.SHIP_GOAL_DATE           	,
TGT.CHANGE_FLAG                   	=    'C'                  		,
TGT.MANUAL_DEACTIVATED_FLAG       =    SRC.MANUAL_DEACTIVATED_FLAG    ,
TGT.PENDING_CLASS_PRODUCT         	=    SRC.PENDING_CLASS_PRODUCT        ,
TGT.MANUALLY_SHIPPED              	=    SRC.MANUALLY_SHIPPED                    ,              
TGT.ODS_MODIFY_DATE               	=    SYSDATE                            

  WHEN NOT MATCHED THEN
INSERT
(
TGT.PLATFORM_DETAIL    ,
TGT.WORK_ORDER_ID     ,
TGT.EVENT_REF_ID        	,
TGT.JOB_TYPE            	,
TGT.LID                 	,
TGT.ACCT_NAME               ,
TGT.SHIP_PROPSL_RECEIVED     ,
TGT.MAIL_RECEIVED                   ,
TGT.BATCH_ID                	          ,
TGT.WORK_ORDER_ALIAS_ID    ,
TGT.SUBJECT_COUNT                 ,
TGT.WORK_ORDER_FORM_PRT     ,
TGT.RET              ,
TGT.RET_QTY     ,
TGT.RET_PRT     ,
TGT.PKG              ,
TGT.PKG_QTY     ,
TGT.PKG_PRT     ,
TGT.POS             ,
TGT.POS_QTY    ,
TGT.POS_PRT     ,
TGT.DIGITAL_MEDIA1              ,
TGT.DIGITAL_MEDIA1_QTY      ,
TGT.DIGITAL_MEDIA1_PRT      ,
TGT.DIGITAL_MEDIA2               ,
TGT.DIGITAL_MEDIA2_QTY      ,
TGT.DIGITAL_MEDIA2_PRT      ,
TGT.LAMINATE                ,
TGT.LAMINATE_QTY    ,
TGT.LAMINATE_PRT    ,
TGT.MAGNET          ,
TGT.MAGNET_QTY      ,
TGT.MAGNET_PRT      ,
TGT.METALLIC        ,
TGT.METALLIC_QTY    ,
TGT.METALLIC_PRT    ,
TGT.SPECIALITY      ,
TGT.SPECIALITY_QTY                       ,
TGT.SPECIALITY_PRODUCED                  ,
TGT.INDIGO_PRODUCT1                      ,
TGT.INDIGO_PRODUCT1_QTY                  ,
TGT.INDIGO_PRODUCT1_PRODUCED             ,
TGT.INDIGO_PRODUCT2                      ,
TGT.INDIGO_PRODUCT2_QTY                  ,
TGT.INDIGO_PRODUCT2_PRODUCED             ,
TGT.PROOF_PRODUCT                        ,
TGT.PROOF_QTY                            ,
TGT.PROOF_PRODUCED      ,
TGT.CLASS_DELIVERY      ,
TGT.CLASS        ,
TGT.CLASS_QTY    ,
TGT.CLASS_PRT    ,
TGT.DID          ,
TGT.DID_QTY      ,
TGT.DID_PRT      ,
TGT.DSSK         ,
TGT.DSSK_QTY       ,
TGT.DSSK_PRT       ,
TGT.EZP            ,
TGT.EZP_QTY        ,
TGT.EZP_PRT        ,
TGT.SPECIAL_REQUEST       ,
TGT.WORK_ORDER_STATUS     ,
TGT.PROCESSING_COMMENT    ,
TGT.SHIP_GOAL_DATE        ,
TGT.WOMS_NODE                   ,
TGT.CHANGE_FLAG                 ,
TGT.MANUAL_DEACTIVATED_FLAG     ,
TGT.PENDING_CLASS_PRODUCT       ,
TGT.MANUALLY_SHIPPED            ,
TGT.ODS_CREATE_DATE             ,
TGT.ODS_MODIFY_DATE  
) VALUES
(
SRC.PLATFORM_DETAIL        ,
SRC.WORK_ORDER_ID          ,
SRC.EVENT_REF_ID           ,
SRC.JOB_TYPE               ,
SRC.LID                    ,
SRC.ACCT_NAME              ,
SRC.SHIP_PROPSL_RECEIVED   ,
SRC.MAIL_RECEIVED          ,
SRC.BATCH_ID               ,
SRC.WORK_ORDER_ALIAS_ID    ,
SRC.SUBJECT_COUNT          ,
SRC.WORK_ORDER_FORM_PRT    ,
SRC.RET            ,
SRC.RET_QTY        ,
SRC.RET_PRT        ,
SRC.PKG            ,
SRC.PKG_QTY        ,
SRC.PKG_PRT        ,
SRC.POS            ,
SRC.POS_QTY        ,
SRC.POS_PRT        ,
SRC.DIGITAL_MEDIA1              ,
SRC.DIGITAL_MEDIA1_QTY          ,
SRC.DIGITAL_MEDIA1_PRT          ,
SRC.DIGITAL_MEDIA2              ,
SRC.DIGITAL_MEDIA2_QTY          ,
SRC.DIGITAL_MEDIA2_PRT          ,
SRC.LAMINATE            ,
SRC.LAMINATE_QTY        ,
SRC.LAMINATE_PRT        ,
SRC.MAGNET              ,
SRC.MAGNET_QTY          ,
SRC.MAGNET_PRT          ,
SRC.METALLIC            ,
SRC.METALLIC_QTY        ,
SRC.METALLIC_PRT        ,
SRC.SPECIALITY          ,
SRC.SPECIALITY_QTY          ,
SRC.SPECIALITY_PRODUCED     ,
SRC.INDIGO_PRODUCT1             ,
SRC.INDIGO_PRODUCT1_QTY         ,
SRC.INDIGO_PRODUCT1_PRODUCED    ,
SRC.INDIGO_PRODUCT2             ,
SRC.INDIGO_PRODUCT2_QTY         ,
SRC.INDIGO_PRODUCT2_PRODUCED    ,
SRC.PROOF_PRODUCT               ,
SRC.PROOF_QTY                   ,
SRC.PROOF_PRODUCED              ,
SRC.CLASS_DELIVERY              ,
SRC.CLASS                       ,
NULL                ,
NULL                ,
SRC.DID             ,
SRC.DID_QTY         ,
SRC.DID_PRT         ,
SRC.DSSK            ,
SRC.DSSK_QTY        ,
SRC.DSSK_PRT        ,
SRC.EZP             ,
SRC.EZP_QTY         ,
SRC.EZP_PRT         ,
SRC.SPECIAL_REQUEST       ,
SRC.WORK_ORDER_STATUS     ,
SRC.PROCESSING_COMMENT    ,
SRC.SHIP_GOAL_DATE        ,
SRC.WOMS_NODE                 ,
'C'                           ,
SRC.MANUAL_DEACTIVATED_FLAG   ,
SRC.PENDING_CLASS_PRODUCT     ,
SRC.MANUALLY_SHIPPED          ,
SYSDATE                       ,
SYSDATE                            
)





&


/*-----------------------------------------------*/
/* TASK No. 4 */
/* Update CDC Load Status */

UPDATE ODS.DW_CDC_LOAD_STATUS
SET LAST_CDC_COMPLETION_DATE=TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS')
WHERE DW_TABLE_NAME=:v_cdc_load_table_name

&


/*-----------------------------------------------*/
/* TASK No. 5 */
/* Insert Audit Record */

INSERT INTO ODS_ETL_OWNER.DW_CDC_LOAD_STATUS_AUDIT
(TABLE_NAME,
SESS_NO,                      
SESS_NAME,                    
SCEN_VERSION,                 
SESS_BEG,                     
ORIG_LAST_CDC_COMPLETION_DATE,
OVERLAP,
CREATE_DATE              
)
values (
:v_cdc_load_table_name,
:v_sess_no,
'PROCESS_CLASS_PIC_EXCEPTION_TO_SHIPPED_PKG',
'002',
TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS'),
TO_DATE (SUBSTR (:v_cdc_load_date_ODS_DM, 1, 19),
                           'YYYY-MM-DD HH24:MI:SS'
                          ),
:v_cdc_overlap,
SYSDATE)



&


/*-----------------------------------------------*/
