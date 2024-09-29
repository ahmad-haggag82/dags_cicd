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

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERTYPE 

&


/*-----------------------------------------------*/
/* TASK No. 5 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBUSERTYPE
(
	C1_JOBUSERTYPEID	NUMBER NULL,
	C2_DESCRIPTION	VARCHAR2(500) NULL,
	C3_ROLERANK	NUMBER NULL,
	C4_ROLECODE	VARCHAR2(100) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 6 */
/* Load data */

/* SOURCE CODE */
select	
	JOBUSERTYPE.JOBUSERTYPEID	 as  C1_JOBUSERTYPEID,
	JOBUSERTYPE.DESCRIPTION	 as  C2_DESCRIPTION,
	JOBUSERTYPE.ROLERANK	 as  C3_ROLERANK,
	JOBUSERTYPE.ROLECODE	 as  C4_ROLECODE
from	Nextools2009Prod.dbo.JobUserType as JOBUSERTYPE
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBUSERTYPE
(
	C1_JOBUSERTYPEID,
	C2_DESCRIPTION,
	C3_ROLERANK,
	C4_ROLECODE
)
values
(
	:C1_JOBUSERTYPEID,
	:C2_DESCRIPTION,
	:C3_ROLERANK,
	:C4_ROLECODE
)

&


/*-----------------------------------------------*/
/* TASK No. 7 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBUSERTYPE',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 9 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE 

&


/*-----------------------------------------------*/
/* TASK No. 10 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE
(
	JOBUSERTYPEID		NUMBER NULL,
	DESCRIPTION		VARCHAR2(500) NULL,
	ROLERANK		NUMBER NULL,
	ROLECODE		VARCHAR2(100) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 11 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE
(
	JOBUSERTYPEID,
	DESCRIPTION,
	ROLERANK,
	ROLECODE,
	IND_UPDATE
)
select 
JOBUSERTYPEID,
	DESCRIPTION,
	ROLERANK,
	ROLECODE,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBUSERTYPEID JOBUSERTYPEID,
	C2_DESCRIPTION DESCRIPTION,
	C3_ROLERANK ROLERANK,
	C4_ROLECODE ROLECODE,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBUSERTYPE
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBUSERTYPE T
	where	T.JOBUSERTYPEID	= S.JOBUSERTYPEID 
		 and ((T.DESCRIPTION = S.DESCRIPTION) or (T.DESCRIPTION IS NULL and S.DESCRIPTION IS NULL)) and
		((T.ROLERANK = S.ROLERANK) or (T.ROLERANK IS NULL and S.ROLERANK IS NULL)) and
		((T.ROLECODE = S.ROLECODE) or (T.ROLECODE IS NULL and S.ROLECODE IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 12 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE_IDX
on		ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE (JOBUSERTYPEID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 13 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBUSERTYPE',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 14 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE
set	IND_UPDATE = 'U'
where	(JOBUSERTYPEID)
	in	(
		select	JOBUSERTYPEID
		from	ODS.NXTL_JOBUSERTYPE
		)



&


/*-----------------------------------------------*/
/* TASK No. 15 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 16 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBUSERTYPE T
set 	
	(
	T.DESCRIPTION,
	T.ROLERANK,
	T.ROLECODE
	) =
		(
		select	S.DESCRIPTION,
			S.ROLERANK,
			S.ROLECODE
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE S
		where	T.JOBUSERTYPEID	=S.JOBUSERTYPEID
	    	 )
	,   T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(JOBUSERTYPEID)
	in	(
		select	JOBUSERTYPEID
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 17 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBUSERTYPE T
	(
	JOBUSERTYPEID,
	DESCRIPTION,
	ROLERANK,
	ROLECODE
	,    EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	JOBUSERTYPEID,
	DESCRIPTION,
	ROLERANK,
	ROLECODE
	,    sysdate,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 18 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 19 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERTYPE 

&


/*-----------------------------------------------*/
/* TASK No. 1000008 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERTYPE 

&


/*-----------------------------------------------*/
/* TASK No. 20 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSER 

&


/*-----------------------------------------------*/
/* TASK No. 21 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBUSER
(
	C1_JOBID	NUMBER NULL,
	C2_JOBUSERID	NUMBER NULL,
	C3_JOBUSERTYPEID	NUMBER NULL,
	C4_STARTDATE	DATE NULL,
	C5_LASTSIGNIN	DATE NULL,
	C6_SIGNINCOUNT	NUMBER NULL,
	C7_ISACTIVE	NUMBER NULL,
	C8_LEGALACCEPTED	NUMBER NULL,
	C9_FIRSTNAME	VARCHAR2(50) NULL,
	C10_LASTNAME	VARCHAR2(50) NULL,
	C11_LOGINNAME	VARCHAR2(50) NULL,
	C12_EMAILADDRESS	VARCHAR2(250) NULL,
	C13_EMPLOYEEID	VARCHAR2(20) NULL,
	C14_PAGEACCESSTYPE	NUMBER NULL,
	C15_ACCOUNTPRINCIPALID	VARCHAR2(64) NULL,
	C16_INVITATIONID	VARCHAR2(64) NULL,
	C17_ISUSERMANAGED	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 22 */
/* Load data */

/* SOURCE CODE */
select	
	JOBUSER.JOBID	 as  C1_JOBID,
	JOBUSER.JOBUSERID	 as  C2_JOBUSERID,
	JOBUSER.JOBUSERTYPEID	 as  C3_JOBUSERTYPEID,
	JOBUSER.STARTDATE	 as  C4_STARTDATE,
	JOBUSER.LASTSIGNIN	 as  C5_LASTSIGNIN,
	JOBUSER.SIGNINCOUNT	 as  C6_SIGNINCOUNT,
	JOBUSER.ISACTIVE	 as  C7_ISACTIVE,
	JOBUSER.LEGALACCEPTED	 as  C8_LEGALACCEPTED,
	JOBUSER.FIRSTNAME	 as  C9_FIRSTNAME,
	JOBUSER.LASTNAME	 as  C10_LASTNAME,
	JOBUSER.LOGINNAME	 as  C11_LOGINNAME,
	JOBUSER.EMAILADDRESS	 as  C12_EMAILADDRESS,
	JOBUSER.EMPLOYEEID	 as  C13_EMPLOYEEID,
	JOBUSER.PAGEACCESSTYPE	 as  C14_PAGEACCESSTYPE,
	JOBUSER.ACCOUNTPRINCIPALID	 as  C15_ACCOUNTPRINCIPALID,
	JOBUSER.INVITATIONID	 as  C16_INVITATIONID,
	JOBUSER.ISUSERMANAGED	 as  C17_ISUSERMANAGED
from	Nextools2009Prod.dbo.JobUser as JOBUSER
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBUSER
(
	C1_JOBID,
	C2_JOBUSERID,
	C3_JOBUSERTYPEID,
	C4_STARTDATE,
	C5_LASTSIGNIN,
	C6_SIGNINCOUNT,
	C7_ISACTIVE,
	C8_LEGALACCEPTED,
	C9_FIRSTNAME,
	C10_LASTNAME,
	C11_LOGINNAME,
	C12_EMAILADDRESS,
	C13_EMPLOYEEID,
	C14_PAGEACCESSTYPE,
	C15_ACCOUNTPRINCIPALID,
	C16_INVITATIONID,
	C17_ISUSERMANAGED
)
values
(
	:C1_JOBID,
	:C2_JOBUSERID,
	:C3_JOBUSERTYPEID,
	:C4_STARTDATE,
	:C5_LASTSIGNIN,
	:C6_SIGNINCOUNT,
	:C7_ISACTIVE,
	:C8_LEGALACCEPTED,
	:C9_FIRSTNAME,
	:C10_LASTNAME,
	:C11_LOGINNAME,
	:C12_EMAILADDRESS,
	:C13_EMPLOYEEID,
	:C14_PAGEACCESSTYPE,
	:C15_ACCOUNTPRINCIPALID,
	:C16_INVITATIONID,
	:C17_ISUSERMANAGED
)

&


/*-----------------------------------------------*/
/* TASK No. 23 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBUSER',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 25 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSER 

&


/*-----------------------------------------------*/
/* TASK No. 26 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBUSER
(
	JOBID		NUMBER NULL,
	JOBUSERID		NUMBER NULL,
	JOBUSERTYPEID		NUMBER NULL,
	STARTDATE		DATE NULL,
	LASTSIGNIN		DATE NULL,
	SIGNINCOUNT		NUMBER NULL,
	ISACTIVE		NUMBER NULL,
	LEGALACCEPTED		NUMBER NULL,
	FIRSTNAME		VARCHAR2(50) NULL,
	LASTNAME		VARCHAR2(50) NULL,
	LOGINNAME		VARCHAR2(50) NULL,
	EMAILADDRESS		VARCHAR2(250) NULL,
	EMPLOYEEID		VARCHAR2(20) NULL,
	PAGEACCESSTYPE		NUMBER NULL,
	ACCOUNTPRINCIPALID		VARCHAR2(64) NULL,
	INVITATIONID		VARCHAR2(64) NULL,
	ISUSERMANAGED		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 27 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBUSER
(
	JOBID,
	JOBUSERID,
	JOBUSERTYPEID,
	STARTDATE,
	LASTSIGNIN,
	SIGNINCOUNT,
	ISACTIVE,
	LEGALACCEPTED,
	FIRSTNAME,
	LASTNAME,
	LOGINNAME,
	EMAILADDRESS,
	EMPLOYEEID,
	PAGEACCESSTYPE,
	ACCOUNTPRINCIPALID,
	INVITATIONID,
	ISUSERMANAGED,
	IND_UPDATE
)
select 
JOBID,
	JOBUSERID,
	JOBUSERTYPEID,
	STARTDATE,
	LASTSIGNIN,
	SIGNINCOUNT,
	ISACTIVE,
	LEGALACCEPTED,
	FIRSTNAME,
	LASTNAME,
	LOGINNAME,
	EMAILADDRESS,
	EMPLOYEEID,
	PAGEACCESSTYPE,
	ACCOUNTPRINCIPALID,
	INVITATIONID,
	ISUSERMANAGED,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBID JOBID,
	C2_JOBUSERID JOBUSERID,
	C3_JOBUSERTYPEID JOBUSERTYPEID,
	C4_STARTDATE STARTDATE,
	C5_LASTSIGNIN LASTSIGNIN,
	C6_SIGNINCOUNT SIGNINCOUNT,
	C7_ISACTIVE ISACTIVE,
	C8_LEGALACCEPTED LEGALACCEPTED,
	C9_FIRSTNAME FIRSTNAME,
	C10_LASTNAME LASTNAME,
	C11_LOGINNAME LOGINNAME,
	C12_EMAILADDRESS EMAILADDRESS,
	C13_EMPLOYEEID EMPLOYEEID,
	C14_PAGEACCESSTYPE PAGEACCESSTYPE,
	C15_ACCOUNTPRINCIPALID ACCOUNTPRINCIPALID,
	C16_INVITATIONID INVITATIONID,
	C17_ISUSERMANAGED ISUSERMANAGED,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBUSER
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBUSER T
	where	T.JOBUSERID	= S.JOBUSERID 
		 and ((T.JOBID = S.JOBID) or (T.JOBID IS NULL and S.JOBID IS NULL)) and
		((T.JOBUSERTYPEID = S.JOBUSERTYPEID) or (T.JOBUSERTYPEID IS NULL and S.JOBUSERTYPEID IS NULL)) and
		((T.STARTDATE = S.STARTDATE) or (T.STARTDATE IS NULL and S.STARTDATE IS NULL)) and
		((T.LASTSIGNIN = S.LASTSIGNIN) or (T.LASTSIGNIN IS NULL and S.LASTSIGNIN IS NULL)) and
		((T.SIGNINCOUNT = S.SIGNINCOUNT) or (T.SIGNINCOUNT IS NULL and S.SIGNINCOUNT IS NULL)) and
		((T.ISACTIVE = S.ISACTIVE) or (T.ISACTIVE IS NULL and S.ISACTIVE IS NULL)) and
		((T.LEGALACCEPTED = S.LEGALACCEPTED) or (T.LEGALACCEPTED IS NULL and S.LEGALACCEPTED IS NULL)) and
		((T.FIRSTNAME = S.FIRSTNAME) or (T.FIRSTNAME IS NULL and S.FIRSTNAME IS NULL)) and
		((T.LASTNAME = S.LASTNAME) or (T.LASTNAME IS NULL and S.LASTNAME IS NULL)) and
		((T.LOGINNAME = S.LOGINNAME) or (T.LOGINNAME IS NULL and S.LOGINNAME IS NULL)) and
		((T.EMAILADDRESS = S.EMAILADDRESS) or (T.EMAILADDRESS IS NULL and S.EMAILADDRESS IS NULL)) and
		((T.EMPLOYEEID = S.EMPLOYEEID) or (T.EMPLOYEEID IS NULL and S.EMPLOYEEID IS NULL)) and
		((T.PAGEACCESSTYPE = S.PAGEACCESSTYPE) or (T.PAGEACCESSTYPE IS NULL and S.PAGEACCESSTYPE IS NULL)) and
		((T.ACCOUNTPRINCIPALID = S.ACCOUNTPRINCIPALID) or (T.ACCOUNTPRINCIPALID IS NULL and S.ACCOUNTPRINCIPALID IS NULL)) and
		((T.INVITATIONID = S.INVITATIONID) or (T.INVITATIONID IS NULL and S.INVITATIONID IS NULL)) and
		((T.ISUSERMANAGED = S.ISUSERMANAGED) or (T.ISUSERMANAGED IS NULL and S.ISUSERMANAGED IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 28 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBUSER_IDX
on		ODS_ETL_OWNER.I$_NXTL_JOBUSER (JOBUSERID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 29 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBUSER',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 30 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBUSER
set	IND_UPDATE = 'U'
where	(JOBUSERID)
	in	(
		select	JOBUSERID
		from	ODS.NXTL_JOBUSER
		)



&


/*-----------------------------------------------*/
/* TASK No. 31 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 32 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBUSER T
set 	
	(
	T.JOBID,
	T.JOBUSERTYPEID,
	T.STARTDATE,
	T.LASTSIGNIN,
	T.SIGNINCOUNT,
	T.ISACTIVE,
	T.LEGALACCEPTED,
	T.FIRSTNAME,
	T.LASTNAME,
	T.LOGINNAME,
	T.EMAILADDRESS,
	T.EMPLOYEEID,
	T.PAGEACCESSTYPE,
	T.ACCOUNTPRINCIPALID,
	T.INVITATIONID,
	T.ISUSERMANAGED
	) =
		(
		select	S.JOBID,
			S.JOBUSERTYPEID,
			S.STARTDATE,
			S.LASTSIGNIN,
			S.SIGNINCOUNT,
			S.ISACTIVE,
			S.LEGALACCEPTED,
			S.FIRSTNAME,
			S.LASTNAME,
			S.LOGINNAME,
			S.EMAILADDRESS,
			S.EMPLOYEEID,
			S.PAGEACCESSTYPE,
			S.ACCOUNTPRINCIPALID,
			S.INVITATIONID,
			S.ISUSERMANAGED
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSER S
		where	T.JOBUSERID	=S.JOBUSERID
	    	 )
	,                T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(JOBUSERID)
	in	(
		select	JOBUSERID
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSER
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 33 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBUSER T
	(
	JOBID,
	JOBUSERID,
	JOBUSERTYPEID,
	STARTDATE,
	LASTSIGNIN,
	SIGNINCOUNT,
	ISACTIVE,
	LEGALACCEPTED,
	FIRSTNAME,
	LASTNAME,
	LOGINNAME,
	EMAILADDRESS,
	EMPLOYEEID,
	PAGEACCESSTYPE,
	ACCOUNTPRINCIPALID,
	INVITATIONID,
	ISUSERMANAGED
	,                 EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	JOBID,
	JOBUSERID,
	JOBUSERTYPEID,
	STARTDATE,
	LASTSIGNIN,
	SIGNINCOUNT,
	ISACTIVE,
	LEGALACCEPTED,
	FIRSTNAME,
	LASTNAME,
	LOGINNAME,
	EMAILADDRESS,
	EMPLOYEEID,
	PAGEACCESSTYPE,
	ACCOUNTPRINCIPALID,
	INVITATIONID,
	ISUSERMANAGED
	,                 sysdate,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_JOBUSER S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 34 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 35 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSER 

&


/*-----------------------------------------------*/
/* TASK No. 1000024 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSER 

&


/*-----------------------------------------------*/
/* TASK No. 36 */
/* Truncate table nxtl_jobuser_stg */

truncate table ODS_ETL_OWNER.NXTL_JOBUSER_STG


&


/*-----------------------------------------------*/
/* TASK No. 37 */
/* Load table nxtl_jobuser_stg */

/* SOURCE CODE */
with data as
(
select jobuserid
from Nextools2009Prod.dbo.jobuser
)
select jobuserid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_JOBUSER_STG
(jobuserid)
select
:jobuserid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 38 */
/* Update active_ind on nxtl_jobuser */

update ods.nxtl_jobuser ju
set ju.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_jobuser_stg jus
where (ju.jobuserid = jus.jobuserid))



&


/*-----------------------------------------------*/
/* TASK No. 39 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOB 

&


/*-----------------------------------------------*/
/* TASK No. 40 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOB
(
	C1_JOBID	NUMBER NULL,
	C2_BOOKID	NUMBER NULL,
	C3_ORGANIZATIONID	NUMBER NULL,
	C4_JOBSTATUSID	NUMBER NULL,
	C5_HARDCOUNT	NUMBER NULL,
	C6_SOFTCOUNT	NUMBER NULL,
	C7_FINALCOUNTSUBMITDATE	DATE NULL,
	C8_FINALCOUNTSUBMITUSERID	NUMBER NULL,
	C9_JOBCODE	NUMBER NULL,
	C10_JOBNAME	VARCHAR2(64) NULL,
	C11_MESSAGE	VARCHAR2(500) NULL,
	C12_SCHOOLNAME	VARCHAR2(100) NULL,
	C13_TRIMSIZEID	NUMBER NULL,
	C14_PRINTTYPE	NUMBER NULL,
	C15_EXPORTTOCUSTOMDICTIONARY	NUMBER NULL,
	C16_YBPAYACTIVATIONDATE	DATE NULL,
	C17_YBPAYCLOSEDATE	DATE NULL,
	C18_ISTAXABLE	NUMBER NULL,
	C19_TAXRATE	NUMBER NULL,
	C20_PRICINGLASTMODIFEDDATE	DATE NULL,
	C21_ADVERTISEMENTPURCHASEBYDAT	DATE NULL,
	C22_ADVERTISEMENTSUBMITBYDATE	DATE NULL,
	C23_ISADBUILDERENABLED	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 41 */
/* Load data */

/* SOURCE CODE */
select	
	JOB.JOBID	 as  C1_JOBID,
	JOB."BookId"	 as  C2_BOOKID,
	JOB.ORGANIZATIONID	 as  C3_ORGANIZATIONID,
	JOB.JOBSTATUSID	 as  C4_JOBSTATUSID,
	JOB.HARDCOUNT	 as  C5_HARDCOUNT,
	JOB.SOFTCOUNT	 as  C6_SOFTCOUNT,
	JOB.FINALCOUNTSUBMITDATE	 as  C7_FINALCOUNTSUBMITDATE,
	JOB.FINALCOUNTSUBMITUSERID	 as  C8_FINALCOUNTSUBMITUSERID,
	JOB.JOBCODE	 as  C9_JOBCODE,
	JOB.JOBNAME	 as  C10_JOBNAME,
	JOB.MESSAGE	 as  C11_MESSAGE,
	JOB.SCHOOLNAME	 as  C12_SCHOOLNAME,
	JOB.TRIMSIZEID	 as  C13_TRIMSIZEID,
	JOB.PRINTTYPE	 as  C14_PRINTTYPE,
	JOB.EXPORTTOCUSTOMDICTIONARY	 as  C15_EXPORTTOCUSTOMDICTIONARY,
	JOB.YBPAYACTIVATIONDATE	 as  C16_YBPAYACTIVATIONDATE,
	JOB.YBPAYCLOSEDATE	 as  C17_YBPAYCLOSEDATE,
	JOB.ISTAXABLE	 as  C18_ISTAXABLE,
	JOB.TAXRATE	 as  C19_TAXRATE,
	JOB."PricingLastModifedDate"	 as  C20_PRICINGLASTMODIFEDDATE,
	JOB.ADVERTISEMENTPURCHASEBYDATE	 as  C21_ADVERTISEMENTPURCHASEBYDAT,
	JOB.ADVERTISEMENTSUBMITBYDATE	 as  C22_ADVERTISEMENTSUBMITBYDATE,
	JOB.ISADBUILDERENABLED	 as  C23_ISADBUILDERENABLED
from	Nextools2009Prod.dbo.Job as JOB
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOB
(
	C1_JOBID,
	C2_BOOKID,
	C3_ORGANIZATIONID,
	C4_JOBSTATUSID,
	C5_HARDCOUNT,
	C6_SOFTCOUNT,
	C7_FINALCOUNTSUBMITDATE,
	C8_FINALCOUNTSUBMITUSERID,
	C9_JOBCODE,
	C10_JOBNAME,
	C11_MESSAGE,
	C12_SCHOOLNAME,
	C13_TRIMSIZEID,
	C14_PRINTTYPE,
	C15_EXPORTTOCUSTOMDICTIONARY,
	C16_YBPAYACTIVATIONDATE,
	C17_YBPAYCLOSEDATE,
	C18_ISTAXABLE,
	C19_TAXRATE,
	C20_PRICINGLASTMODIFEDDATE,
	C21_ADVERTISEMENTPURCHASEBYDAT,
	C22_ADVERTISEMENTSUBMITBYDATE,
	C23_ISADBUILDERENABLED
)
values
(
	:C1_JOBID,
	:C2_BOOKID,
	:C3_ORGANIZATIONID,
	:C4_JOBSTATUSID,
	:C5_HARDCOUNT,
	:C6_SOFTCOUNT,
	:C7_FINALCOUNTSUBMITDATE,
	:C8_FINALCOUNTSUBMITUSERID,
	:C9_JOBCODE,
	:C10_JOBNAME,
	:C11_MESSAGE,
	:C12_SCHOOLNAME,
	:C13_TRIMSIZEID,
	:C14_PRINTTYPE,
	:C15_EXPORTTOCUSTOMDICTIONARY,
	:C16_YBPAYACTIVATIONDATE,
	:C17_YBPAYCLOSEDATE,
	:C18_ISTAXABLE,
	:C19_TAXRATE,
	:C20_PRICINGLASTMODIFEDDATE,
	:C21_ADVERTISEMENTPURCHASEBYDAT,
	:C22_ADVERTISEMENTSUBMITBYDATE,
	:C23_ISADBUILDERENABLED
)

&


/*-----------------------------------------------*/
/* TASK No. 42 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOB',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 44 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOB 

&


/*-----------------------------------------------*/
/* TASK No. 45 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOB
(
	JOBID		NUMBER NULL,
	BOOKID		NUMBER NULL,
	ORGANIZATIONID		NUMBER NULL,
	JOBSTATUSID		NUMBER NULL,
	HARDCOUNT		NUMBER NULL,
	SOFTCOUNT		NUMBER NULL,
	FINALCOUNTSUBMITDATE		DATE NULL,
	FINALCOUNTSUBMITUSERID		NUMBER NULL,
	JOBCODE		NUMBER NULL,
	JOBNAME		VARCHAR2(64) NULL,
	MESSAGE		VARCHAR2(500) NULL,
	SCHOOLNAME		VARCHAR2(100) NULL,
	TRIMSIZEID		NUMBER NULL,
	PRINTTYPE		NUMBER NULL,
	EXPORTTOCUSTOMDICTIONARY		NUMBER NULL,
	YBPAYACTIVATIONDATE		DATE NULL,
	YBPAYCLOSEDATE		DATE NULL,
	ISTAXABLE		NUMBER NULL,
	TAXRATE		NUMBER NULL,
	PRICINGLASTMODIFEDDATE		DATE NULL,
	ADVERTISEMENTPURCHASEBYDATE		DATE NULL,
	ADVERTISEMENTSUBMITBYDATE		DATE NULL,
	ISADBUILDERENABLED		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 46 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOB
(
	JOBID,
	BOOKID,
	ORGANIZATIONID,
	JOBSTATUSID,
	HARDCOUNT,
	SOFTCOUNT,
	FINALCOUNTSUBMITDATE,
	FINALCOUNTSUBMITUSERID,
	JOBCODE,
	JOBNAME,
	MESSAGE,
	SCHOOLNAME,
	TRIMSIZEID,
	PRINTTYPE,
	EXPORTTOCUSTOMDICTIONARY,
	YBPAYACTIVATIONDATE,
	YBPAYCLOSEDATE,
	ISTAXABLE,
	TAXRATE,
	PRICINGLASTMODIFEDDATE,
	ADVERTISEMENTPURCHASEBYDATE,
	ADVERTISEMENTSUBMITBYDATE,
	ISADBUILDERENABLED,
	IND_UPDATE
)
select 
JOBID,
	BOOKID,
	ORGANIZATIONID,
	JOBSTATUSID,
	HARDCOUNT,
	SOFTCOUNT,
	FINALCOUNTSUBMITDATE,
	FINALCOUNTSUBMITUSERID,
	JOBCODE,
	JOBNAME,
	MESSAGE,
	SCHOOLNAME,
	TRIMSIZEID,
	PRINTTYPE,
	EXPORTTOCUSTOMDICTIONARY,
	YBPAYACTIVATIONDATE,
	YBPAYCLOSEDATE,
	ISTAXABLE,
	TAXRATE,
	PRICINGLASTMODIFEDDATE,
	ADVERTISEMENTPURCHASEBYDATE,
	ADVERTISEMENTSUBMITBYDATE,
	ISADBUILDERENABLED,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBID JOBID,
	C2_BOOKID BOOKID,
	C3_ORGANIZATIONID ORGANIZATIONID,
	C4_JOBSTATUSID JOBSTATUSID,
	C5_HARDCOUNT HARDCOUNT,
	C6_SOFTCOUNT SOFTCOUNT,
	C7_FINALCOUNTSUBMITDATE FINALCOUNTSUBMITDATE,
	C8_FINALCOUNTSUBMITUSERID FINALCOUNTSUBMITUSERID,
	C9_JOBCODE JOBCODE,
	C10_JOBNAME JOBNAME,
	C11_MESSAGE MESSAGE,
	C12_SCHOOLNAME SCHOOLNAME,
	C13_TRIMSIZEID TRIMSIZEID,
	C14_PRINTTYPE PRINTTYPE,
	C15_EXPORTTOCUSTOMDICTIONARY EXPORTTOCUSTOMDICTIONARY,
	C16_YBPAYACTIVATIONDATE YBPAYACTIVATIONDATE,
	C17_YBPAYCLOSEDATE YBPAYCLOSEDATE,
	C18_ISTAXABLE ISTAXABLE,
	C19_TAXRATE TAXRATE,
	C20_PRICINGLASTMODIFEDDATE PRICINGLASTMODIFEDDATE,
	C21_ADVERTISEMENTPURCHASEBYDAT ADVERTISEMENTPURCHASEBYDATE,
	C22_ADVERTISEMENTSUBMITBYDATE ADVERTISEMENTSUBMITBYDATE,
	C23_ISADBUILDERENABLED ISADBUILDERENABLED,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOB
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOB T
	where	T.JOBCODE	= S.JOBCODE 
		 and ((T.JOBID = S.JOBID) or (T.JOBID IS NULL and S.JOBID IS NULL)) and
		((T.BOOKID = S.BOOKID) or (T.BOOKID IS NULL and S.BOOKID IS NULL)) and
		((T.ORGANIZATIONID = S.ORGANIZATIONID) or (T.ORGANIZATIONID IS NULL and S.ORGANIZATIONID IS NULL)) and
		((T.JOBSTATUSID = S.JOBSTATUSID) or (T.JOBSTATUSID IS NULL and S.JOBSTATUSID IS NULL)) and
		((T.HARDCOUNT = S.HARDCOUNT) or (T.HARDCOUNT IS NULL and S.HARDCOUNT IS NULL)) and
		((T.SOFTCOUNT = S.SOFTCOUNT) or (T.SOFTCOUNT IS NULL and S.SOFTCOUNT IS NULL)) and
		((T.FINALCOUNTSUBMITDATE = S.FINALCOUNTSUBMITDATE) or (T.FINALCOUNTSUBMITDATE IS NULL and S.FINALCOUNTSUBMITDATE IS NULL)) and
		((T.FINALCOUNTSUBMITUSERID = S.FINALCOUNTSUBMITUSERID) or (T.FINALCOUNTSUBMITUSERID IS NULL and S.FINALCOUNTSUBMITUSERID IS NULL)) and
		((T.JOBNAME = S.JOBNAME) or (T.JOBNAME IS NULL and S.JOBNAME IS NULL)) and
		((T.MESSAGE = S.MESSAGE) or (T.MESSAGE IS NULL and S.MESSAGE IS NULL)) and
		((T.SCHOOLNAME = S.SCHOOLNAME) or (T.SCHOOLNAME IS NULL and S.SCHOOLNAME IS NULL)) and
		((T.TRIMSIZEID = S.TRIMSIZEID) or (T.TRIMSIZEID IS NULL and S.TRIMSIZEID IS NULL)) and
		((T.PRINTTYPE = S.PRINTTYPE) or (T.PRINTTYPE IS NULL and S.PRINTTYPE IS NULL)) and
		((T.EXPORTTOCUSTOMDICTIONARY = S.EXPORTTOCUSTOMDICTIONARY) or (T.EXPORTTOCUSTOMDICTIONARY IS NULL and S.EXPORTTOCUSTOMDICTIONARY IS NULL)) and
		((T.YBPAYACTIVATIONDATE = S.YBPAYACTIVATIONDATE) or (T.YBPAYACTIVATIONDATE IS NULL and S.YBPAYACTIVATIONDATE IS NULL)) and
		((T.YBPAYCLOSEDATE = S.YBPAYCLOSEDATE) or (T.YBPAYCLOSEDATE IS NULL and S.YBPAYCLOSEDATE IS NULL)) and
		((T.ISTAXABLE = S.ISTAXABLE) or (T.ISTAXABLE IS NULL and S.ISTAXABLE IS NULL)) and
		((T.TAXRATE = S.TAXRATE) or (T.TAXRATE IS NULL and S.TAXRATE IS NULL)) and
		((T.PRICINGLASTMODIFEDDATE = S.PRICINGLASTMODIFEDDATE) or (T.PRICINGLASTMODIFEDDATE IS NULL and S.PRICINGLASTMODIFEDDATE IS NULL)) and
		((T.ADVERTISEMENTPURCHASEBYDATE = S.ADVERTISEMENTPURCHASEBYDATE) or (T.ADVERTISEMENTPURCHASEBYDATE IS NULL and S.ADVERTISEMENTPURCHASEBYDATE IS NULL)) and
		((T.ADVERTISEMENTSUBMITBYDATE = S.ADVERTISEMENTSUBMITBYDATE) or (T.ADVERTISEMENTSUBMITBYDATE IS NULL and S.ADVERTISEMENTSUBMITBYDATE IS NULL)) and
		((T.ISADBUILDERENABLED = S.ISADBUILDERENABLED) or (T.ISADBUILDERENABLED IS NULL and S.ISADBUILDERENABLED IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 47 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOB_IDX
on		ODS_ETL_OWNER.I$_NXTL_JOB (JOBCODE)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 48 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOB',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 49 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOB
set	IND_UPDATE = 'U'
where	(JOBCODE)
	in	(
		select	JOBCODE
		from	ODS.NXTL_JOB
		)



&


/*-----------------------------------------------*/
/* TASK No. 50 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 51 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOB T
set 	
	(
	T.JOBID,
	T.BOOKID,
	T.ORGANIZATIONID,
	T.JOBSTATUSID,
	T.HARDCOUNT,
	T.SOFTCOUNT,
	T.FINALCOUNTSUBMITDATE,
	T.FINALCOUNTSUBMITUSERID,
	T.JOBNAME,
	T.MESSAGE,
	T.SCHOOLNAME,
	T.TRIMSIZEID,
	T.PRINTTYPE,
	T.EXPORTTOCUSTOMDICTIONARY,
	T.YBPAYACTIVATIONDATE,
	T.YBPAYCLOSEDATE,
	T.ISTAXABLE,
	T.TAXRATE,
	T.PRICINGLASTMODIFEDDATE,
	T.ADVERTISEMENTPURCHASEBYDATE,
	T.ADVERTISEMENTSUBMITBYDATE,
	T.ISADBUILDERENABLED
	) =
		(
		select	S.JOBID,
			S.BOOKID,
			S.ORGANIZATIONID,
			S.JOBSTATUSID,
			S.HARDCOUNT,
			S.SOFTCOUNT,
			S.FINALCOUNTSUBMITDATE,
			S.FINALCOUNTSUBMITUSERID,
			S.JOBNAME,
			S.MESSAGE,
			S.SCHOOLNAME,
			S.TRIMSIZEID,
			S.PRINTTYPE,
			S.EXPORTTOCUSTOMDICTIONARY,
			S.YBPAYACTIVATIONDATE,
			S.YBPAYCLOSEDATE,
			S.ISTAXABLE,
			S.TAXRATE,
			S.PRICINGLASTMODIFEDDATE,
			S.ADVERTISEMENTPURCHASEBYDATE,
			S.ADVERTISEMENTSUBMITBYDATE,
			S.ISADBUILDERENABLED
		from	ODS_ETL_OWNER.I$_NXTL_JOB S
		where	T.JOBCODE	=S.JOBCODE
	    	 )
	,                      T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(JOBCODE)
	in	(
		select	JOBCODE
		from	ODS_ETL_OWNER.I$_NXTL_JOB
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 52 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOB T
	(
	JOBID,
	BOOKID,
	ORGANIZATIONID,
	JOBSTATUSID,
	HARDCOUNT,
	SOFTCOUNT,
	FINALCOUNTSUBMITDATE,
	FINALCOUNTSUBMITUSERID,
	JOBCODE,
	JOBNAME,
	MESSAGE,
	SCHOOLNAME,
	TRIMSIZEID,
	PRINTTYPE,
	EXPORTTOCUSTOMDICTIONARY,
	YBPAYACTIVATIONDATE,
	YBPAYCLOSEDATE,
	ISTAXABLE,
	TAXRATE,
	PRICINGLASTMODIFEDDATE,
	ADVERTISEMENTPURCHASEBYDATE,
	ADVERTISEMENTSUBMITBYDATE,
	ISADBUILDERENABLED
	,                       EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	JOBID,
	BOOKID,
	ORGANIZATIONID,
	JOBSTATUSID,
	HARDCOUNT,
	SOFTCOUNT,
	FINALCOUNTSUBMITDATE,
	FINALCOUNTSUBMITUSERID,
	JOBCODE,
	JOBNAME,
	MESSAGE,
	SCHOOLNAME,
	TRIMSIZEID,
	PRINTTYPE,
	EXPORTTOCUSTOMDICTIONARY,
	YBPAYACTIVATIONDATE,
	YBPAYCLOSEDATE,
	ISTAXABLE,
	TAXRATE,
	PRICINGLASTMODIFEDDATE,
	ADVERTISEMENTPURCHASEBYDATE,
	ADVERTISEMENTSUBMITBYDATE,
	ISADBUILDERENABLED
	,                       sysdate,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_JOB S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 53 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 54 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOB 

&


/*-----------------------------------------------*/
/* TASK No. 1000043 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOB 

&


/*-----------------------------------------------*/
/* TASK No. 55 */
/* Truncate table nxtl_job_stg */

truncate table ODS_ETL_OWNER.NXTL_JOB_STG


&


/*-----------------------------------------------*/
/* TASK No. 56 */
/* Load table nxtl_job_stg */

/* SOURCE CODE */
with data as
(
select jobcode
from Nextools2009Prod.dbo.job
)
select jobcode
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_JOB_STG
(jobcode)
select
:jobcode
from dual

&


/*-----------------------------------------------*/
/* TASK No. 57 */
/* Update active_ind on nxtl_job */

update ods.nxtl_job j
set j.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_job_stg js
where (j.jobcode = js.jobcode))



&


/*-----------------------------------------------*/
/* TASK No. 58 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_BOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 59 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_BOOKOPTION
(
	C1_BOOKOPTIONID	NUMBER NULL,
	C2_DESCRIPTION	VARCHAR2(255) NULL,
	C3_NAME	VARCHAR2(30) NULL,
	C4_BOOKOPTIONTYPE	NUMBER NULL,
	C5_DISPLAYORDER	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 60 */
/* Load data */

/* SOURCE CODE */
select	
	BOOKOPTION.BookOptionId	 as  C1_BOOKOPTIONID,
	BOOKOPTION.Description	 as  C2_DESCRIPTION,
	BOOKOPTION.Name	 as  C3_NAME,
	BOOKOPTION.BookOptionType	 as  C4_BOOKOPTIONTYPE,
	BOOKOPTION.DisplayOrder	 as  C5_DISPLAYORDER
from	Nextools2009Prod.dbo.BookOption as BOOKOPTION
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_BOOKOPTION
(
	C1_BOOKOPTIONID,
	C2_DESCRIPTION,
	C3_NAME,
	C4_BOOKOPTIONTYPE,
	C5_DISPLAYORDER
)
values
(
	:C1_BOOKOPTIONID,
	:C2_DESCRIPTION,
	:C3_NAME,
	:C4_BOOKOPTIONTYPE,
	:C5_DISPLAYORDER
)

&


/*-----------------------------------------------*/
/* TASK No. 61 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_BOOKOPTION',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 63 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_BOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 64 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_BOOKOPTION
(
	BOOKOPTIONID		NUMBER NULL,
	DESCRIPTION		VARCHAR2(255) NULL,
	NAME		VARCHAR2(30) NULL,
	BOOKOPTIONTYPE		NUMBER NULL,
	DISPLAYORDER		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 65 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION
(
	BOOKOPTIONID,
	DESCRIPTION,
	NAME,
	BOOKOPTIONTYPE,
	DISPLAYORDER,
	IND_UPDATE
)
select 
BOOKOPTIONID,
	DESCRIPTION,
	NAME,
	BOOKOPTIONTYPE,
	DISPLAYORDER,
	IND_UPDATE
 from (


select 	 
	
	C1_BOOKOPTIONID BOOKOPTIONID,
	C2_DESCRIPTION DESCRIPTION,
	C3_NAME NAME,
	C4_BOOKOPTIONTYPE BOOKOPTIONTYPE,
	C5_DISPLAYORDER DISPLAYORDER,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_BOOKOPTION
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_BOOKOPTION T
	where	T.BOOKOPTIONID	= S.BOOKOPTIONID 
		 and ((T.DESCRIPTION = S.DESCRIPTION) or (T.DESCRIPTION IS NULL and S.DESCRIPTION IS NULL)) and
		((T.NAME = S.NAME) or (T.NAME IS NULL and S.NAME IS NULL)) and
		((T.BOOKOPTIONTYPE = S.BOOKOPTIONTYPE) or (T.BOOKOPTIONTYPE IS NULL and S.BOOKOPTIONTYPE IS NULL)) and
		((T.DISPLAYORDER = S.DISPLAYORDER) or (T.DISPLAYORDER IS NULL and S.DISPLAYORDER IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 66 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION_IDX
on		ODS_ETL_OWNER.I$_NXTL_BOOKOPTION (BOOKOPTIONID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 67 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_BOOKOPTION',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 68 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION
set	IND_UPDATE = 'U'
where	(BOOKOPTIONID)
	in	(
		select	BOOKOPTIONID
		from	ODS.NXTL_BOOKOPTION
		)



&


/*-----------------------------------------------*/
/* TASK No. 69 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 70 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_BOOKOPTION T
set 	
	(
	T.DESCRIPTION,
	T.NAME,
	T.BOOKOPTIONTYPE,
	T.DISPLAYORDER
	) =
		(
		select	S.DESCRIPTION,
			S.NAME,
			S.BOOKOPTIONTYPE,
			S.DISPLAYORDER
		from	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION S
		where	T.BOOKOPTIONID	=S.BOOKOPTIONID
	    	 )
	,    T.EFFECTIVE_DATE = sysdate
,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(BOOKOPTIONID)
	in	(
		select	BOOKOPTIONID
		from	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 71 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_BOOKOPTION T
	(
	BOOKOPTIONID,
	DESCRIPTION,
	NAME,
	BOOKOPTIONTYPE,
	DISPLAYORDER
	,     EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	BOOKOPTIONID,
	DESCRIPTION,
	NAME,
	BOOKOPTIONTYPE,
	DISPLAYORDER
	,     sysdate
,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_BOOKOPTION S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 72 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 73 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_BOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 1000062 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_BOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 74 */
/* Truncate table nxtl_bookoption_stg */

truncate table ODS_ETL_OWNER.NXTL_BOOKOPTION_STG


&


/*-----------------------------------------------*/
/* TASK No. 75 */
/* Load table nxtl_bookoption_stg */

/* SOURCE CODE */
with data as
(
select bookoptionid
from Nextools2009Prod.dbo.bookoption
)
select bookoptionid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_BOOKOPTION_STG
(bookoptionid)
select
:bookoptionid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 76 */
/* Update active_ind on nxtl_bookoption */

update ods.nxtl_bookoption bo
set bo.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_bookoption_stg bos
where (bo.bookoptionid = bos.bookoptionid))



&


/*-----------------------------------------------*/
/* TASK No. 77 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_GRADE 

&


/*-----------------------------------------------*/
/* TASK No. 78 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_GRADE
(
	C1_GRADEID	NUMBER NULL,
	C2_CODE	VARCHAR2(3) NULL,
	C3_DESCRIPTION	VARCHAR2(50) NULL,
	C4_SORTORDER	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 79 */
/* Load data */

/* SOURCE CODE */
select	
	GRADE.GradeId	 as  C1_GRADEID,
	GRADE.Code	 as  C2_CODE,
	GRADE.Description	 as  C3_DESCRIPTION,
	GRADE.SortOrder	 as  C4_SORTORDER
from	Nextools2009Prod.dbo.Grade as GRADE
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_GRADE
(
	C1_GRADEID,
	C2_CODE,
	C3_DESCRIPTION,
	C4_SORTORDER
)
values
(
	:C1_GRADEID,
	:C2_CODE,
	:C3_DESCRIPTION,
	:C4_SORTORDER
)

&


/*-----------------------------------------------*/
/* TASK No. 80 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_GRADE',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 82 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_GRADE 

&


/*-----------------------------------------------*/
/* TASK No. 83 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_GRADE
(
	GRADEID		NUMBER NULL,
	CODE		VARCHAR2(3) NULL,
	DESCRIPTION		VARCHAR2(50) NULL,
	SORTORDER		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 84 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_GRADE
(
	GRADEID,
	CODE,
	DESCRIPTION,
	SORTORDER,
	IND_UPDATE
)
select 
GRADEID,
	CODE,
	DESCRIPTION,
	SORTORDER,
	IND_UPDATE
 from (


select 	 
	
	C1_GRADEID GRADEID,
	C2_CODE CODE,
	C3_DESCRIPTION DESCRIPTION,
	C4_SORTORDER SORTORDER,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_GRADE
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_GRADE T
	where	T.GRADEID	= S.GRADEID 
		 and ((T.CODE = S.CODE) or (T.CODE IS NULL and S.CODE IS NULL)) and
		((T.DESCRIPTION = S.DESCRIPTION) or (T.DESCRIPTION IS NULL and S.DESCRIPTION IS NULL)) and
		((T.SORTORDER = S.SORTORDER) or (T.SORTORDER IS NULL and S.SORTORDER IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 85 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_GRADE_IDX
on		ODS_ETL_OWNER.I$_NXTL_GRADE (GRADEID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 86 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_GRADE',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 87 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_GRADE
set	IND_UPDATE = 'U'
where	(GRADEID)
	in	(
		select	GRADEID
		from	ODS.NXTL_GRADE
		)



&


/*-----------------------------------------------*/
/* TASK No. 88 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 89 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_GRADE T
set 	
	(
	T.CODE,
	T.DESCRIPTION,
	T.SORTORDER
	) =
		(
		select	S.CODE,
			S.DESCRIPTION,
			S.SORTORDER
		from	ODS_ETL_OWNER.I$_NXTL_GRADE S
		where	T.GRADEID	=S.GRADEID
	    	 )
	,   T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(GRADEID)
	in	(
		select	GRADEID
		from	ODS_ETL_OWNER.I$_NXTL_GRADE
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 90 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_GRADE T
	(
	GRADEID,
	CODE,
	DESCRIPTION,
	SORTORDER
	,    EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	GRADEID,
	CODE,
	DESCRIPTION,
	SORTORDER
	,    sysdate,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_GRADE S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 91 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 92 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_GRADE 

&


/*-----------------------------------------------*/
/* TASK No. 1000081 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_GRADE 

&


/*-----------------------------------------------*/
/* TASK No. 93 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBFINAL 

&


/*-----------------------------------------------*/
/* TASK No. 94 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBFINAL
(
	C1_JOBID	NUMBER NULL,
	C2_BOOKS	NUMBER NULL,
	C3_HARDCOUNT	NUMBER NULL,
	C4_SOFTCOUNT	NUMBER NULL,
	C5_BOOKSUBMITDATE	DATE NULL,
	C6_BOOKSUBMITUSERID	NUMBER NULL,
	C7_AUTOGRAPHSIGNPENCOUNT	NUMBER NULL,
	C8_COVERKEEPERCOUNT	NUMBER NULL,
	C9_TAPEDZOOMCOUNT	NUMBER NULL,
	C10_ENHANCEMENTSUBMITDATE	DATE NULL,
	C11_ENHANCEMENTSUBMITUSERID	NUMBER NULL,
	C12_TOSELLVIABESTSELLER	NUMBER NULL,
	C13_AUTOGRAPHINSERTCOUNT	NUMBER NULL,
	C14_YEARBOOKSTICKYCOUNT	NUMBER NULL,
	C15_SIGNPENONLYCOUNT	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 95 */
/* Load data */

/* SOURCE CODE */
select	
	JOBFINAL.JobId	 as  C1_JOBID,
	JOBFINAL.Books	 as  C2_BOOKS,
	JOBFINAL.HardCount	 as  C3_HARDCOUNT,
	JOBFINAL.SoftCount	 as  C4_SOFTCOUNT,
	JOBFINAL.BookSubmitDate	 as  C5_BOOKSUBMITDATE,
	JOBFINAL.BookSubmitUserId	 as  C6_BOOKSUBMITUSERID,
	JOBFINAL.AutographSignPenCount	 as  C7_AUTOGRAPHSIGNPENCOUNT,
	JOBFINAL.CoverKeeperCount	 as  C8_COVERKEEPERCOUNT,
	JOBFINAL.TapedZoomCount	 as  C9_TAPEDZOOMCOUNT,
	JOBFINAL.EnhancementSubmitDate	 as  C10_ENHANCEMENTSUBMITDATE,
	JOBFINAL.EnhancementSubmitUserId	 as  C11_ENHANCEMENTSUBMITUSERID,
	JOBFINAL.ToSellViaBestSeller	 as  C12_TOSELLVIABESTSELLER,
	JOBFINAL.AutographInsertCount	 as  C13_AUTOGRAPHINSERTCOUNT,
	JOBFINAL.YearbookStickyCount	 as  C14_YEARBOOKSTICKYCOUNT,
	JOBFINAL.SignPenOnlyCount	 as  C15_SIGNPENONLYCOUNT
from	Nextools2009Prod.dbo.JobFinal as JOBFINAL
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBFINAL
(
	C1_JOBID,
	C2_BOOKS,
	C3_HARDCOUNT,
	C4_SOFTCOUNT,
	C5_BOOKSUBMITDATE,
	C6_BOOKSUBMITUSERID,
	C7_AUTOGRAPHSIGNPENCOUNT,
	C8_COVERKEEPERCOUNT,
	C9_TAPEDZOOMCOUNT,
	C10_ENHANCEMENTSUBMITDATE,
	C11_ENHANCEMENTSUBMITUSERID,
	C12_TOSELLVIABESTSELLER,
	C13_AUTOGRAPHINSERTCOUNT,
	C14_YEARBOOKSTICKYCOUNT,
	C15_SIGNPENONLYCOUNT
)
values
(
	:C1_JOBID,
	:C2_BOOKS,
	:C3_HARDCOUNT,
	:C4_SOFTCOUNT,
	:C5_BOOKSUBMITDATE,
	:C6_BOOKSUBMITUSERID,
	:C7_AUTOGRAPHSIGNPENCOUNT,
	:C8_COVERKEEPERCOUNT,
	:C9_TAPEDZOOMCOUNT,
	:C10_ENHANCEMENTSUBMITDATE,
	:C11_ENHANCEMENTSUBMITUSERID,
	:C12_TOSELLVIABESTSELLER,
	:C13_AUTOGRAPHINSERTCOUNT,
	:C14_YEARBOOKSTICKYCOUNT,
	:C15_SIGNPENONLYCOUNT
)

&


/*-----------------------------------------------*/
/* TASK No. 96 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBFINAL',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 98 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBFINAL 

&


/*-----------------------------------------------*/
/* TASK No. 99 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBFINAL
(
	JOBID		NUMBER NULL,
	BOOKS		NUMBER NULL,
	HARDCOUNT		NUMBER NULL,
	SOFTCOUNT		NUMBER NULL,
	BOOKSUBMITDATE		DATE NULL,
	BOOKSUBMITUSERID		NUMBER NULL,
	AUTOGRAPHSIGNPENCOUNT		NUMBER NULL,
	COVERKEEPERCOUNT		NUMBER NULL,
	TAPEDZOOMCOUNT		NUMBER NULL,
	ENHANCEMENTSUBMITDATE		DATE NULL,
	ENHANCEMENTSUBMITUSERID		NUMBER NULL,
	TOSELLVIABESTSELLER		NUMBER NULL,
	AUTOGRAPHINSERTCOUNT		NUMBER NULL,
	YEARBOOKSTICKYCOUNT		NUMBER NULL,
	SIGNPENONLYCOUNT		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	DW_CREATE_DATE		DATE NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 100 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBFINAL
(
	JOBID,
	BOOKS,
	HARDCOUNT,
	SOFTCOUNT,
	BOOKSUBMITDATE,
	BOOKSUBMITUSERID,
	AUTOGRAPHSIGNPENCOUNT,
	COVERKEEPERCOUNT,
	TAPEDZOOMCOUNT,
	ENHANCEMENTSUBMITDATE,
	ENHANCEMENTSUBMITUSERID,
	TOSELLVIABESTSELLER,
	AUTOGRAPHINSERTCOUNT,
	YEARBOOKSTICKYCOUNT,
	SIGNPENONLYCOUNT,
	IND_UPDATE
)
select 
JOBID,
	BOOKS,
	HARDCOUNT,
	SOFTCOUNT,
	BOOKSUBMITDATE,
	BOOKSUBMITUSERID,
	AUTOGRAPHSIGNPENCOUNT,
	COVERKEEPERCOUNT,
	TAPEDZOOMCOUNT,
	ENHANCEMENTSUBMITDATE,
	ENHANCEMENTSUBMITUSERID,
	TOSELLVIABESTSELLER,
	AUTOGRAPHINSERTCOUNT,
	YEARBOOKSTICKYCOUNT,
	SIGNPENONLYCOUNT,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBID JOBID,
	C2_BOOKS BOOKS,
	C3_HARDCOUNT HARDCOUNT,
	C4_SOFTCOUNT SOFTCOUNT,
	C5_BOOKSUBMITDATE BOOKSUBMITDATE,
	C6_BOOKSUBMITUSERID BOOKSUBMITUSERID,
	C7_AUTOGRAPHSIGNPENCOUNT AUTOGRAPHSIGNPENCOUNT,
	C8_COVERKEEPERCOUNT COVERKEEPERCOUNT,
	C9_TAPEDZOOMCOUNT TAPEDZOOMCOUNT,
	C10_ENHANCEMENTSUBMITDATE ENHANCEMENTSUBMITDATE,
	C11_ENHANCEMENTSUBMITUSERID ENHANCEMENTSUBMITUSERID,
	C12_TOSELLVIABESTSELLER TOSELLVIABESTSELLER,
	C13_AUTOGRAPHINSERTCOUNT AUTOGRAPHINSERTCOUNT,
	C14_YEARBOOKSTICKYCOUNT YEARBOOKSTICKYCOUNT,
	C15_SIGNPENONLYCOUNT SIGNPENONLYCOUNT,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBFINAL
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBFINAL T
	where	T.JOBID	= S.JOBID 
		 and ((T.BOOKS = S.BOOKS) or (T.BOOKS IS NULL and S.BOOKS IS NULL)) and
		((T.HARDCOUNT = S.HARDCOUNT) or (T.HARDCOUNT IS NULL and S.HARDCOUNT IS NULL)) and
		((T.SOFTCOUNT = S.SOFTCOUNT) or (T.SOFTCOUNT IS NULL and S.SOFTCOUNT IS NULL)) and
		((T.BOOKSUBMITDATE = S.BOOKSUBMITDATE) or (T.BOOKSUBMITDATE IS NULL and S.BOOKSUBMITDATE IS NULL)) and
		((T.BOOKSUBMITUSERID = S.BOOKSUBMITUSERID) or (T.BOOKSUBMITUSERID IS NULL and S.BOOKSUBMITUSERID IS NULL)) and
		((T.AUTOGRAPHSIGNPENCOUNT = S.AUTOGRAPHSIGNPENCOUNT) or (T.AUTOGRAPHSIGNPENCOUNT IS NULL and S.AUTOGRAPHSIGNPENCOUNT IS NULL)) and
		((T.COVERKEEPERCOUNT = S.COVERKEEPERCOUNT) or (T.COVERKEEPERCOUNT IS NULL and S.COVERKEEPERCOUNT IS NULL)) and
		((T.TAPEDZOOMCOUNT = S.TAPEDZOOMCOUNT) or (T.TAPEDZOOMCOUNT IS NULL and S.TAPEDZOOMCOUNT IS NULL)) and
		((T.ENHANCEMENTSUBMITDATE = S.ENHANCEMENTSUBMITDATE) or (T.ENHANCEMENTSUBMITDATE IS NULL and S.ENHANCEMENTSUBMITDATE IS NULL)) and
		((T.ENHANCEMENTSUBMITUSERID = S.ENHANCEMENTSUBMITUSERID) or (T.ENHANCEMENTSUBMITUSERID IS NULL and S.ENHANCEMENTSUBMITUSERID IS NULL)) and
		((T.TOSELLVIABESTSELLER = S.TOSELLVIABESTSELLER) or (T.TOSELLVIABESTSELLER IS NULL and S.TOSELLVIABESTSELLER IS NULL)) and
		((T.AUTOGRAPHINSERTCOUNT = S.AUTOGRAPHINSERTCOUNT) or (T.AUTOGRAPHINSERTCOUNT IS NULL and S.AUTOGRAPHINSERTCOUNT IS NULL)) and
		((T.YEARBOOKSTICKYCOUNT = S.YEARBOOKSTICKYCOUNT) or (T.YEARBOOKSTICKYCOUNT IS NULL and S.YEARBOOKSTICKYCOUNT IS NULL)) and
		((T.SIGNPENONLYCOUNT = S.SIGNPENONLYCOUNT) or (T.SIGNPENONLYCOUNT IS NULL and S.SIGNPENONLYCOUNT IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 101 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBFINAL_IDX
on		ODS_ETL_OWNER.I$_NXTL_JOBFINAL (JOBID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 102 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBFINAL',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 103 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBFINAL
set	IND_UPDATE = 'U'
where	(JOBID)
	in	(
		select	JOBID
		from	ODS.NXTL_JOBFINAL
		)



&


/*-----------------------------------------------*/
/* TASK No. 104 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 105 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBFINAL T
set 	
	(
	T.BOOKS,
	T.HARDCOUNT,
	T.SOFTCOUNT,
	T.BOOKSUBMITDATE,
	T.BOOKSUBMITUSERID,
	T.AUTOGRAPHSIGNPENCOUNT,
	T.COVERKEEPERCOUNT,
	T.TAPEDZOOMCOUNT,
	T.ENHANCEMENTSUBMITDATE,
	T.ENHANCEMENTSUBMITUSERID,
	T.TOSELLVIABESTSELLER,
	T.AUTOGRAPHINSERTCOUNT,
	T.YEARBOOKSTICKYCOUNT,
	T.SIGNPENONLYCOUNT
	) =
		(
		select	S.BOOKS,
			S.HARDCOUNT,
			S.SOFTCOUNT,
			S.BOOKSUBMITDATE,
			S.BOOKSUBMITUSERID,
			S.AUTOGRAPHSIGNPENCOUNT,
			S.COVERKEEPERCOUNT,
			S.TAPEDZOOMCOUNT,
			S.ENHANCEMENTSUBMITDATE,
			S.ENHANCEMENTSUBMITUSERID,
			S.TOSELLVIABESTSELLER,
			S.AUTOGRAPHINSERTCOUNT,
			S.YEARBOOKSTICKYCOUNT,
			S.SIGNPENONLYCOUNT
		from	ODS_ETL_OWNER.I$_NXTL_JOBFINAL S
		where	T.JOBID	=S.JOBID
	    	 )
	,              T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(JOBID)
	in	(
		select	JOBID
		from	ODS_ETL_OWNER.I$_NXTL_JOBFINAL
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 106 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBFINAL T
	(
	JOBID,
	BOOKS,
	HARDCOUNT,
	SOFTCOUNT,
	BOOKSUBMITDATE,
	BOOKSUBMITUSERID,
	AUTOGRAPHSIGNPENCOUNT,
	COVERKEEPERCOUNT,
	TAPEDZOOMCOUNT,
	ENHANCEMENTSUBMITDATE,
	ENHANCEMENTSUBMITUSERID,
	TOSELLVIABESTSELLER,
	AUTOGRAPHINSERTCOUNT,
	YEARBOOKSTICKYCOUNT,
	SIGNPENONLYCOUNT
	,               EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID,
	DW_CREATE_DATE
	)
select 	JOBID,
	BOOKS,
	HARDCOUNT,
	SOFTCOUNT,
	BOOKSUBMITDATE,
	BOOKSUBMITUSERID,
	AUTOGRAPHSIGNPENCOUNT,
	COVERKEEPERCOUNT,
	TAPEDZOOMCOUNT,
	ENHANCEMENTSUBMITDATE,
	ENHANCEMENTSUBMITUSERID,
	TOSELLVIABESTSELLER,
	AUTOGRAPHINSERTCOUNT,
	YEARBOOKSTICKYCOUNT,
	SIGNPENONLYCOUNT
	,               sysdate,
	'A',
	1,
	sysdate
from	ODS_ETL_OWNER.I$_NXTL_JOBFINAL S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 107 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 108 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBFINAL 

&


/*-----------------------------------------------*/
/* TASK No. 1000097 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBFINAL 

&


/*-----------------------------------------------*/
/* TASK No. 109 */
/* Truncate table nxtl_jobfinal_stg */

truncate table ODS_ETL_OWNER.NXTL_JOBFINAL_STG


&


/*-----------------------------------------------*/
/* TASK No. 110 */
/* Load table nxtl_jobfinal_stg */

/* SOURCE CODE */
with data as
(
select jobid
from Nextools2009Prod.dbo.jobfinal
)
select jobid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_JOBFINAL_STG
(jobid)
select
:jobid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 111 */
/* Update active_ind on nxtl_jobfinal */

update ods.nxtl_jobfinal jf
set jf.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_jobfinal_stg jfs
where (jf.jobid = jfs.jobid))



&


/*-----------------------------------------------*/
/* TASK No. 112 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBGRADE 

&


/*-----------------------------------------------*/
/* TASK No. 113 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBGRADE
(
	C1_JOBGRADEID	NUMBER NULL,
	C2_JOBID	NUMBER NULL,
	C3_GRADEID	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 114 */
/* Load data */

/* SOURCE CODE */
select	
	JOBGRADE.JobGradeId	 as  C1_JOBGRADEID,
	JOBGRADE.JobId	 as  C2_JOBID,
	JOBGRADE.GradeId	 as  C3_GRADEID
from	Nextools2009Prod.dbo.JobGrade as JOBGRADE
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBGRADE
(
	C1_JOBGRADEID,
	C2_JOBID,
	C3_GRADEID
)
values
(
	:C1_JOBGRADEID,
	:C2_JOBID,
	:C3_GRADEID
)

&


/*-----------------------------------------------*/
/* TASK No. 115 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBGRADE',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 117 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBGRADE 

&


/*-----------------------------------------------*/
/* TASK No. 118 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBGRADE
(
	JOBGRADEID		NUMBER NULL,
	JOBID		NUMBER NULL,
	GRADEID		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	DW_CREATE_DATE		DATE NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 119 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBGRADE
(
	JOBGRADEID,
	JOBID,
	GRADEID,
	IND_UPDATE
)
select 
JOBGRADEID,
	JOBID,
	GRADEID,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBGRADEID JOBGRADEID,
	C2_JOBID JOBID,
	C3_GRADEID GRADEID,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBGRADE
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBGRADE T
	where	T.JOBGRADEID	= S.JOBGRADEID 
		 and ((T.JOBID = S.JOBID) or (T.JOBID IS NULL and S.JOBID IS NULL)) and
		((T.GRADEID = S.GRADEID) or (T.GRADEID IS NULL and S.GRADEID IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 120 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBGRADE_IDX
on		ODS_ETL_OWNER.I$_NXTL_JOBGRADE (JOBGRADEID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 121 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBGRADE',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 122 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBGRADE
set	IND_UPDATE = 'U'
where	(JOBGRADEID)
	in	(
		select	JOBGRADEID
		from	ODS.NXTL_JOBGRADE
		)



&


/*-----------------------------------------------*/
/* TASK No. 123 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 124 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBGRADE T
set 	
	(
	T.JOBID,
	T.GRADEID
	) =
		(
		select	S.JOBID,
			S.GRADEID
		from	ODS_ETL_OWNER.I$_NXTL_JOBGRADE S
		where	T.JOBGRADEID	=S.JOBGRADEID
	    	 )
	,  T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(JOBGRADEID)
	in	(
		select	JOBGRADEID
		from	ODS_ETL_OWNER.I$_NXTL_JOBGRADE
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 125 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBGRADE T
	(
	JOBGRADEID,
	JOBID,
	GRADEID
	,   EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID,
	DW_CREATE_DATE
	)
select 	JOBGRADEID,
	JOBID,
	GRADEID
	,   sysdate,
	'A',
	1,
	sysdate
from	ODS_ETL_OWNER.I$_NXTL_JOBGRADE S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 126 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 127 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBGRADE 

&


/*-----------------------------------------------*/
/* TASK No. 1000116 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBGRADE 

&


/*-----------------------------------------------*/
/* TASK No. 128 */
/* Truncate table nxtl_jobgrade_stg */

truncate table ODS_ETL_OWNER.NXTL_JOBGRADE_STG


&


/*-----------------------------------------------*/
/* TASK No. 129 */
/* Load table nxtl_jobgrade_stg */

/* SOURCE CODE */
with data as
(
select jobgradeid
from Nextools2009Prod.dbo.jobgrade
)
select jobgradeid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_JOBGRADE_STG
(jobgradeid)
select
:jobgradeid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 130 */
/* Update active_ind on nxtl_jobgrade */

update ods.nxtl_jobgrade jg
set jg.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_jobgrade_stg jgs
where (jg.jobgradeid = jgs.jobgradeid))



&


/*-----------------------------------------------*/
/* TASK No. 131 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_ORDERORIGIN 

&


/*-----------------------------------------------*/
/* TASK No. 132 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_ORDERORIGIN
(
	C1_ID	NUMBER NULL,
	C2_DESCRIPTION	VARCHAR2(50) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 133 */
/* Load data */

/* SOURCE CODE */
select	
	ORDERORIGIN.Id	 as  C1_ID,
	ORDERORIGIN.Description	 as  C2_DESCRIPTION
from	Nextools2009Prod.dbo.OrderOrigin as ORDERORIGIN
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_ORDERORIGIN
(
	C1_ID,
	C2_DESCRIPTION
)
values
(
	:C1_ID,
	:C2_DESCRIPTION
)

&


/*-----------------------------------------------*/
/* TASK No. 134 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_ORDERORIGIN',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 136 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN 

&


/*-----------------------------------------------*/
/* TASK No. 137 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN
(
	ID		NUMBER NULL,
	DESCRIPTION		VARCHAR2(50) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 138 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN
(
	ID,
	DESCRIPTION,
	IND_UPDATE
)
select 
ID,
	DESCRIPTION,
	IND_UPDATE
 from (


select 	 
	
	C1_ID ID,
	C2_DESCRIPTION DESCRIPTION,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_ORDERORIGIN
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_ORDERORIGIN T
	where	T.ID	= S.ID 
		 and ((T.DESCRIPTION = S.DESCRIPTION) or (T.DESCRIPTION IS NULL and S.DESCRIPTION IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 139 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN_IDX
on		ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN (ID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 140 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_ORDERORIGIN',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 141 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN
set	IND_UPDATE = 'U'
where	(ID)
	in	(
		select	ID
		from	ODS.NXTL_ORDERORIGIN
		)



&


/*-----------------------------------------------*/
/* TASK No. 142 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 143 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_ORDERORIGIN T
set 	
	(
	T.DESCRIPTION
	) =
		(
		select	S.DESCRIPTION
		from	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN S
		where	T.ID	=S.ID
	    	 )
	, T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(ID)
	in	(
		select	ID
		from	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 144 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_ORDERORIGIN T
	(
	ID,
	DESCRIPTION
	,  EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	ID,
	DESCRIPTION
	,  sysdate,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 145 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 146 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_ORDERORIGIN 

&


/*-----------------------------------------------*/
/* TASK No. 1000135 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_ORDERORIGIN 

&


/*-----------------------------------------------*/
/* TASK No. 147 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PAYMENT 

&


/*-----------------------------------------------*/
/* TASK No. 148 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_PAYMENT
(
	C1_ID	NUMBER NULL,
	C2_PERSONID	NUMBER NULL,
	C3_AMOUNT	NUMBER NULL,
	C4_TRANSACTIONDATE	DATE NULL,
	C5_PAYMENTTYPEID	NUMBER NULL,
	C6_ORIGINID	NUMBER NULL,
	C7_MEMO	VARCHAR2(100) NULL,
	C8_CREATEDUSER	VARCHAR2(50) NULL,
	C9_CREATEDDATE	DATE NULL,
	C10_LASTMODIFIEDUSER	VARCHAR2(50) NULL,
	C11_LASTMODIFIEDDATE	DATE NULL,
	C12_VERIFICATIONRESULTCODE	NUMBER NULL,
	C13_VERIFICATIONRESULTMESSAGE	VARCHAR2(200) NULL,
	C14_PNREF	VARCHAR2(50) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 149 */
/* Load data */

/* SOURCE CODE */
select	
	PAYMENT.Id	 as  C1_ID,
	PAYMENT.PersonId	 as  C2_PERSONID,
	PAYMENT.Amount	 as  C3_AMOUNT,
	PAYMENT.TransactionDate	 as  C4_TRANSACTIONDATE,
	PAYMENT.PaymentTypeId	 as  C5_PAYMENTTYPEID,
	PAYMENT.OriginId	 as  C6_ORIGINID,
	PAYMENT.Memo	 as  C7_MEMO,
	PAYMENT.CreatedUser	 as  C8_CREATEDUSER,
	PAYMENT.CreatedDate	 as  C9_CREATEDDATE,
	PAYMENT.LastModifiedUser	 as  C10_LASTMODIFIEDUSER,
	PAYMENT.LastModifiedDate	 as  C11_LASTMODIFIEDDATE,
	PAYMENT.VerificationResultCode	 as  C12_VERIFICATIONRESULTCODE,
	PAYMENT.VerificationResultMessage	 as  C13_VERIFICATIONRESULTMESSAGE,
	PAYMENT.PnRef	 as  C14_PNREF
from	Nextools2009Prod.dbo.Payment as PAYMENT
where	(1=1)
And (PAYMENT.LastModifiedDate >= convert(datetime,SUBSTRING('#WAREHOUSE_PROJECT.v_cdc_load_date', 1, 19),120) - 7.0)





&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_PAYMENT
(
	C1_ID,
	C2_PERSONID,
	C3_AMOUNT,
	C4_TRANSACTIONDATE,
	C5_PAYMENTTYPEID,
	C6_ORIGINID,
	C7_MEMO,
	C8_CREATEDUSER,
	C9_CREATEDDATE,
	C10_LASTMODIFIEDUSER,
	C11_LASTMODIFIEDDATE,
	C12_VERIFICATIONRESULTCODE,
	C13_VERIFICATIONRESULTMESSAGE,
	C14_PNREF
)
values
(
	:C1_ID,
	:C2_PERSONID,
	:C3_AMOUNT,
	:C4_TRANSACTIONDATE,
	:C5_PAYMENTTYPEID,
	:C6_ORIGINID,
	:C7_MEMO,
	:C8_CREATEDUSER,
	:C9_CREATEDDATE,
	:C10_LASTMODIFIEDUSER,
	:C11_LASTMODIFIEDDATE,
	:C12_VERIFICATIONRESULTCODE,
	:C13_VERIFICATIONRESULTMESSAGE,
	:C14_PNREF
)

&


/*-----------------------------------------------*/
/* TASK No. 150 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_PAYMENT',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 152 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PAYMENT 

&


/*-----------------------------------------------*/
/* TASK No. 153 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_PAYMENT
(
	ID		NUMBER NULL,
	PERSONID		NUMBER NULL,
	AMOUNT		NUMBER NULL,
	TRANSACTIONDATE		DATE NULL,
	PAYMENTTYPEID		NUMBER NULL,
	ORIGINID		NUMBER NULL,
	MEMO		VARCHAR2(100) NULL,
	CREATEDUSER		VARCHAR2(50) NULL,
	CREATEDDATE		DATE NULL,
	LASTMODIFIEDUSER		VARCHAR2(50) NULL,
	LASTMODIFIEDDATE		DATE NULL,
	VERIFICATIONRESULTCODE		NUMBER NULL,
	VERIFICATIONRESULTMESSAGE		VARCHAR2(200) NULL,
	PNREF		VARCHAR2(50) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	DW_CREATE_DATE		DATE NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 154 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_PAYMENT
(
	ID,
	PERSONID,
	AMOUNT,
	TRANSACTIONDATE,
	PAYMENTTYPEID,
	ORIGINID,
	MEMO,
	CREATEDUSER,
	CREATEDDATE,
	LASTMODIFIEDUSER,
	LASTMODIFIEDDATE,
	VERIFICATIONRESULTCODE,
	VERIFICATIONRESULTMESSAGE,
	PNREF,
	IND_UPDATE
)
select 
ID,
	PERSONID,
	AMOUNT,
	TRANSACTIONDATE,
	PAYMENTTYPEID,
	ORIGINID,
	MEMO,
	CREATEDUSER,
	CREATEDDATE,
	LASTMODIFIEDUSER,
	LASTMODIFIEDDATE,
	VERIFICATIONRESULTCODE,
	VERIFICATIONRESULTMESSAGE,
	PNREF,
	IND_UPDATE
 from (


select 	 
	
	C1_ID ID,
	C2_PERSONID PERSONID,
	C3_AMOUNT AMOUNT,
	C4_TRANSACTIONDATE TRANSACTIONDATE,
	C5_PAYMENTTYPEID PAYMENTTYPEID,
	C6_ORIGINID ORIGINID,
	C7_MEMO MEMO,
	C8_CREATEDUSER CREATEDUSER,
	C9_CREATEDDATE CREATEDDATE,
	C10_LASTMODIFIEDUSER LASTMODIFIEDUSER,
	C11_LASTMODIFIEDDATE LASTMODIFIEDDATE,
	C12_VERIFICATIONRESULTCODE VERIFICATIONRESULTCODE,
	C13_VERIFICATIONRESULTMESSAGE VERIFICATIONRESULTMESSAGE,
	C14_PNREF PNREF,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_PAYMENT
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_PAYMENT T
	where	T.ID	= S.ID 
		 and ((T.PERSONID = S.PERSONID) or (T.PERSONID IS NULL and S.PERSONID IS NULL)) and
		((T.AMOUNT = S.AMOUNT) or (T.AMOUNT IS NULL and S.AMOUNT IS NULL)) and
		((T.TRANSACTIONDATE = S.TRANSACTIONDATE) or (T.TRANSACTIONDATE IS NULL and S.TRANSACTIONDATE IS NULL)) and
		((T.PAYMENTTYPEID = S.PAYMENTTYPEID) or (T.PAYMENTTYPEID IS NULL and S.PAYMENTTYPEID IS NULL)) and
		((T.ORIGINID = S.ORIGINID) or (T.ORIGINID IS NULL and S.ORIGINID IS NULL)) and
		((T.MEMO = S.MEMO) or (T.MEMO IS NULL and S.MEMO IS NULL)) and
		((T.CREATEDUSER = S.CREATEDUSER) or (T.CREATEDUSER IS NULL and S.CREATEDUSER IS NULL)) and
		((T.CREATEDDATE = S.CREATEDDATE) or (T.CREATEDDATE IS NULL and S.CREATEDDATE IS NULL)) and
		((T.LASTMODIFIEDUSER = S.LASTMODIFIEDUSER) or (T.LASTMODIFIEDUSER IS NULL and S.LASTMODIFIEDUSER IS NULL)) and
		((T.LASTMODIFIEDDATE = S.LASTMODIFIEDDATE) or (T.LASTMODIFIEDDATE IS NULL and S.LASTMODIFIEDDATE IS NULL)) and
		((T.VERIFICATIONRESULTCODE = S.VERIFICATIONRESULTCODE) or (T.VERIFICATIONRESULTCODE IS NULL and S.VERIFICATIONRESULTCODE IS NULL)) and
		((T.VERIFICATIONRESULTMESSAGE = S.VERIFICATIONRESULTMESSAGE) or (T.VERIFICATIONRESULTMESSAGE IS NULL and S.VERIFICATIONRESULTMESSAGE IS NULL)) and
		((T.PNREF = S.PNREF) or (T.PNREF IS NULL and S.PNREF IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 155 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_PAYMENT_IDX
on		ODS_ETL_OWNER.I$_NXTL_PAYMENT (ID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 156 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_PAYMENT',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 157 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_PAYMENT
set	IND_UPDATE = 'U'
where	(ID)
	in	(
		select	ID
		from	ODS.NXTL_PAYMENT
		)



&


/*-----------------------------------------------*/
/* TASK No. 158 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 159 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_PAYMENT T
set 	
	(
	T.PERSONID,
	T.AMOUNT,
	T.TRANSACTIONDATE,
	T.PAYMENTTYPEID,
	T.ORIGINID,
	T.MEMO,
	T.CREATEDUSER,
	T.CREATEDDATE,
	T.LASTMODIFIEDUSER,
	T.LASTMODIFIEDDATE,
	T.VERIFICATIONRESULTCODE,
	T.VERIFICATIONRESULTMESSAGE,
	T.PNREF
	) =
		(
		select	S.PERSONID,
			S.AMOUNT,
			S.TRANSACTIONDATE,
			S.PAYMENTTYPEID,
			S.ORIGINID,
			S.MEMO,
			S.CREATEDUSER,
			S.CREATEDDATE,
			S.LASTMODIFIEDUSER,
			S.LASTMODIFIEDDATE,
			S.VERIFICATIONRESULTCODE,
			S.VERIFICATIONRESULTMESSAGE,
			S.PNREF
		from	ODS_ETL_OWNER.I$_NXTL_PAYMENT S
		where	T.ID	=S.ID
	    	 )
	,             T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(ID)
	in	(
		select	ID
		from	ODS_ETL_OWNER.I$_NXTL_PAYMENT
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 160 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_PAYMENT T
	(
	ID,
	PERSONID,
	AMOUNT,
	TRANSACTIONDATE,
	PAYMENTTYPEID,
	ORIGINID,
	MEMO,
	CREATEDUSER,
	CREATEDDATE,
	LASTMODIFIEDUSER,
	LASTMODIFIEDDATE,
	VERIFICATIONRESULTCODE,
	VERIFICATIONRESULTMESSAGE,
	PNREF
	,              EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID,
	DW_CREATE_DATE
	)
select 	ID,
	PERSONID,
	AMOUNT,
	TRANSACTIONDATE,
	PAYMENTTYPEID,
	ORIGINID,
	MEMO,
	CREATEDUSER,
	CREATEDDATE,
	LASTMODIFIEDUSER,
	LASTMODIFIEDDATE,
	VERIFICATIONRESULTCODE,
	VERIFICATIONRESULTMESSAGE,
	PNREF
	,              sysdate,
	'A',
	1,
	sysdate
from	ODS_ETL_OWNER.I$_NXTL_PAYMENT S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 161 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 162 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PAYMENT 

&


/*-----------------------------------------------*/
/* TASK No. 1000151 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PAYMENT 

&


/*-----------------------------------------------*/
/* TASK No. 163 */
/* Truncate table nxtl_payment_stg */

truncate table ODS_ETL_OWNER.NXTL_PAYMENT_STG


&


/*-----------------------------------------------*/
/* TASK No. 164 */
/* Load table nxtl_payment_stg */

/* SOURCE CODE */
with data as
(
select id
from Nextools2009Prod.dbo.Payment
)
select id
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_PAYMENT_STG
(id)
select
:id
from dual

&


/*-----------------------------------------------*/
/* TASK No. 165 */
/* Update active_ind on nxtl_payment */

update ods.nxtl_payment p
set p.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_payment_stg ps
where (p.id = ps.id))



&


/*-----------------------------------------------*/
/* TASK No. 166 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PERSONBOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 167 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_PERSONBOOKOPTION
(
	C1_PERSONID	NUMBER NULL,
	C2_BOOKOPTIONID	NUMBER NULL,
	C3_DISCOUNTOVERRIDE	NUMBER NULL,
	C4_REFUNDDATE	DATE NULL,
	C5_IMPRINTLINE1EFFECTIVEPRICE	NUMBER NULL,
	C6_IMPRINTLINE2EFFECTIVEPRICE	NUMBER NULL,
	C7_ICONEFFECTIVEPRICE	NUMBER NULL,
	C8_EFFECTIVEPRICE	NUMBER NULL,
	C9_ISPACKAGECOMPONENT	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 168 */
/* Load data */

/* SOURCE CODE */
select	
	PERSONBOOKOPTION.PersonId	 as  C1_PERSONID,
	PERSONBOOKOPTION.BookOptionId	 as  C2_BOOKOPTIONID,
	PERSONBOOKOPTION.DiscountOverride	 as  C3_DISCOUNTOVERRIDE,
	PERSONBOOKOPTION.RefundDate	 as  C4_REFUNDDATE,
	PERSONBOOKOPTION.ImprintLine1EffectivePrice	 as  C5_IMPRINTLINE1EFFECTIVEPRICE,
	PERSONBOOKOPTION.ImprintLine2EffectivePrice	 as  C6_IMPRINTLINE2EFFECTIVEPRICE,
	PERSONBOOKOPTION.IconEffectivePrice	 as  C7_ICONEFFECTIVEPRICE,
	PERSONBOOKOPTION.EffectivePrice	 as  C8_EFFECTIVEPRICE,
	PERSONBOOKOPTION.IsPackageComponent	 as  C9_ISPACKAGECOMPONENT
from	Nextools2009Prod.dbo.PersonBookOption as PERSONBOOKOPTION
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_PERSONBOOKOPTION
(
	C1_PERSONID,
	C2_BOOKOPTIONID,
	C3_DISCOUNTOVERRIDE,
	C4_REFUNDDATE,
	C5_IMPRINTLINE1EFFECTIVEPRICE,
	C6_IMPRINTLINE2EFFECTIVEPRICE,
	C7_ICONEFFECTIVEPRICE,
	C8_EFFECTIVEPRICE,
	C9_ISPACKAGECOMPONENT
)
values
(
	:C1_PERSONID,
	:C2_BOOKOPTIONID,
	:C3_DISCOUNTOVERRIDE,
	:C4_REFUNDDATE,
	:C5_IMPRINTLINE1EFFECTIVEPRICE,
	:C6_IMPRINTLINE2EFFECTIVEPRICE,
	:C7_ICONEFFECTIVEPRICE,
	:C8_EFFECTIVEPRICE,
	:C9_ISPACKAGECOMPONENT
)

&


/*-----------------------------------------------*/
/* TASK No. 169 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_PERSONBOOKOPTION',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 171 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 172 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION
(
	PERSONID		NUMBER NULL,
	BOOKOPTIONID		NUMBER NULL,
	DISCOUNTOVERRIDE		NUMBER NULL,
	REFUNDDATE		DATE NULL,
	IMPRINTLINE1EFFECTIVEPRICE		NUMBER NULL,
	IMPRINTLINE2EFFECTIVEPRICE		NUMBER NULL,
	ICONEFFECTIVEPRICE		NUMBER NULL,
	EFFECTIVEPRICE		NUMBER NULL,
	ISPACKAGECOMPONENT		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	DW_CREATE_DATE		DATE NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 173 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION
(
	PERSONID,
	BOOKOPTIONID,
	DISCOUNTOVERRIDE,
	REFUNDDATE,
	IMPRINTLINE1EFFECTIVEPRICE,
	IMPRINTLINE2EFFECTIVEPRICE,
	ICONEFFECTIVEPRICE,
	EFFECTIVEPRICE,
	ISPACKAGECOMPONENT,
	IND_UPDATE
)
select 
PERSONID,
	BOOKOPTIONID,
	DISCOUNTOVERRIDE,
	REFUNDDATE,
	IMPRINTLINE1EFFECTIVEPRICE,
	IMPRINTLINE2EFFECTIVEPRICE,
	ICONEFFECTIVEPRICE,
	EFFECTIVEPRICE,
	ISPACKAGECOMPONENT,
	IND_UPDATE
 from (


select 	 
	
	C1_PERSONID PERSONID,
	C2_BOOKOPTIONID BOOKOPTIONID,
	C3_DISCOUNTOVERRIDE DISCOUNTOVERRIDE,
	C4_REFUNDDATE REFUNDDATE,
	C5_IMPRINTLINE1EFFECTIVEPRICE IMPRINTLINE1EFFECTIVEPRICE,
	C6_IMPRINTLINE2EFFECTIVEPRICE IMPRINTLINE2EFFECTIVEPRICE,
	C7_ICONEFFECTIVEPRICE ICONEFFECTIVEPRICE,
	C8_EFFECTIVEPRICE EFFECTIVEPRICE,
	C9_ISPACKAGECOMPONENT ISPACKAGECOMPONENT,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_PERSONBOOKOPTION
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_PERSONBOOKOPTION T
	where	T.PERSONID	= S.PERSONID
	and	T.BOOKOPTIONID	= S.BOOKOPTIONID 
		 and ((T.DISCOUNTOVERRIDE = S.DISCOUNTOVERRIDE) or (T.DISCOUNTOVERRIDE IS NULL and S.DISCOUNTOVERRIDE IS NULL)) and
		((T.REFUNDDATE = S.REFUNDDATE) or (T.REFUNDDATE IS NULL and S.REFUNDDATE IS NULL)) and
		((T.IMPRINTLINE1EFFECTIVEPRICE = S.IMPRINTLINE1EFFECTIVEPRICE) or (T.IMPRINTLINE1EFFECTIVEPRICE IS NULL and S.IMPRINTLINE1EFFECTIVEPRICE IS NULL)) and
		((T.IMPRINTLINE2EFFECTIVEPRICE = S.IMPRINTLINE2EFFECTIVEPRICE) or (T.IMPRINTLINE2EFFECTIVEPRICE IS NULL and S.IMPRINTLINE2EFFECTIVEPRICE IS NULL)) and
		((T.ICONEFFECTIVEPRICE = S.ICONEFFECTIVEPRICE) or (T.ICONEFFECTIVEPRICE IS NULL and S.ICONEFFECTIVEPRICE IS NULL)) and
		((T.EFFECTIVEPRICE = S.EFFECTIVEPRICE) or (T.EFFECTIVEPRICE IS NULL and S.EFFECTIVEPRICE IS NULL)) and
		((T.ISPACKAGECOMPONENT = S.ISPACKAGECOMPONENT) or (T.ISPACKAGECOMPONENT IS NULL and S.ISPACKAGECOMPONENT IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 174 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION_IDX
on		ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION (PERSONID, BOOKOPTIONID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 175 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_PERSONBOOKOPTION',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 176 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION
set	IND_UPDATE = 'U'
where	(PERSONID, BOOKOPTIONID)
	in	(
		select	PERSONID,
			BOOKOPTIONID
		from	ODS.NXTL_PERSONBOOKOPTION
		)



&


/*-----------------------------------------------*/
/* TASK No. 177 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 178 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_PERSONBOOKOPTION T
set 	
	(
	T.DISCOUNTOVERRIDE,
	T.REFUNDDATE,
	T.IMPRINTLINE1EFFECTIVEPRICE,
	T.IMPRINTLINE2EFFECTIVEPRICE,
	T.ICONEFFECTIVEPRICE,
	T.EFFECTIVEPRICE,
	T.ISPACKAGECOMPONENT
	) =
		(
		select	S.DISCOUNTOVERRIDE,
			S.REFUNDDATE,
			S.IMPRINTLINE1EFFECTIVEPRICE,
			S.IMPRINTLINE2EFFECTIVEPRICE,
			S.ICONEFFECTIVEPRICE,
			S.EFFECTIVEPRICE,
			S.ISPACKAGECOMPONENT
		from	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION S
		where	T.PERSONID	=S.PERSONID
		and	T.BOOKOPTIONID	=S.BOOKOPTIONID
	    	 )
	,       T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(PERSONID, BOOKOPTIONID)
	in	(
		select	PERSONID,
			BOOKOPTIONID
		from	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 179 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_PERSONBOOKOPTION T
	(
	PERSONID,
	BOOKOPTIONID,
	DISCOUNTOVERRIDE,
	REFUNDDATE,
	IMPRINTLINE1EFFECTIVEPRICE,
	IMPRINTLINE2EFFECTIVEPRICE,
	ICONEFFECTIVEPRICE,
	EFFECTIVEPRICE,
	ISPACKAGECOMPONENT
	,         EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID,
	DW_CREATE_DATE
	)
select 	PERSONID,
	BOOKOPTIONID,
	DISCOUNTOVERRIDE,
	REFUNDDATE,
	IMPRINTLINE1EFFECTIVEPRICE,
	IMPRINTLINE2EFFECTIVEPRICE,
	ICONEFFECTIVEPRICE,
	EFFECTIVEPRICE,
	ISPACKAGECOMPONENT
	,         sysdate,
	'A',
	1,
	sysdate
from	ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 180 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 181 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PERSONBOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 1000170 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PERSONBOOKOPTION 

&


/*-----------------------------------------------*/
/* TASK No. 182 */
/* Truncate table nxtl_personbookoption_stg */

truncate table ODS_ETL_OWNER.NXTL_PERSONBOOKOPTION_STG


&


/*-----------------------------------------------*/
/* TASK No. 183 */
/* Load table nxtl_personbookoption_stg */

/* SOURCE CODE */
with data as
(
select personid
,bookoptionid
from Nextools2009Prod.dbo.personbookoption
)
select personid
,bookoptionid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_PERSONBOOKOPTION_STG
(personid
,bookoptionid)
select
:personid
,:bookoptionid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 184 */
/* Update active_ind on nxtl_personbookoption */

update ods.nxtl_personbookoption pbo
set pbo.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_personbookoption_stg pbos
where (pbo.personid = pbos.personid and pbo.bookoptionid = pbos.bookoptionid))



&


/*-----------------------------------------------*/
/* TASK No. 185 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PERSON 

&


/*-----------------------------------------------*/
/* TASK No. 186 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_PERSON
(
	C1_PERSONID	NUMBER NULL,
	C2_PORTRAITGROUPTYPEID	NUMBER NULL,
	C3_FIRSTNAME	VARCHAR2(80) NULL,
	C4_LASTNAME	VARCHAR2(80) NULL,
	C5_PERSONTYPE	NUMBER NULL,
	C6_COURTESY	VARCHAR2(5) NULL,
	C7_JOBID	NUMBER NULL,
	C8_STDBOOKCOUNT	NUMBER NULL,
	C9_IMPRINTLNONE	VARCHAR2(32) NULL,
	C10_IMPRINTLNTWO	VARCHAR2(32) NULL,
	C11_MODIFIEDBY	NUMBER NULL,
	C12_MODIFIEDDATE	DATE NULL,
	C13_UPLOADCODE	VARCHAR2(50) NULL,
	C14_CREATEDDATE	DATE NULL,
	C15_ISTEACHER	NUMBER NULL,
	C16_PAYMENTDATE	DATE NULL,
	C17_PAYMENTREC	NUMBER NULL,
	C18_NOTES	VARCHAR2(255) NULL,
	C19_EXTERNALID	NUMBER NULL,
	C20_CURRENTPORTRAIT	NUMBER NULL,
	C21_ORDERORIGINID	NUMBER NULL,
	C22_ORIGINREFERENCENUMBER	VARCHAR2(50) NULL,
	C23_PACKAGESCHOOLPRICINGID	NUMBER NULL,
	C24_EFFECTIVEPRICE	NUMBER NULL,
	C25_TOTALAMOUNT	NUMBER NULL,
	C26_TOTALTAX	NUMBER NULL,
	C27_DONATIONAMOUNT	NUMBER NULL,
	C28_PORTRAITIMAGEURL	VARCHAR2(200) NULL,
	C29_ORDERDATE	DATE NULL,
	C30_LASTMODIFIEDDATE	DATE NULL,
	C31_LASTMODIFIEDBY	VARCHAR2(50) NULL,
	C32_DISCOUNT	NUMBER NULL,
	C33_GRADEID	NUMBER NULL,
	C34_JOBPRICINGOVERRIDEID	NUMBER NULL,
	C35_INCLUDEDLINE	NUMBER NULL,
	C36_INCLUDEDICON	NUMBER NULL,
	C37_CIRID	VARCHAR2(50) NULL,
	C38_PORTRAITPOSEENABLE	NUMBER NULL,
	C39_CURRENTPORTRAITPOSE	NUMBER NULL,
	C40_CUSTOMEREMAIL	VARCHAR2(256) NULL,
	C41_SUBJECT_KEY	VARCHAR2(255) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 187 */
/* Load data */

/* SOURCE CODE */
select	
	PERSON.PersonId	 as  C1_PERSONID,
	PERSON.PortraitGroupTypeId	 as  C2_PORTRAITGROUPTYPEID,
	PERSON.FirstName	 as  C3_FIRSTNAME,
	PERSON.LastName	 as  C4_LASTNAME,
	PERSON.PersonType	 as  C5_PERSONTYPE,
	PERSON.Courtesy	 as  C6_COURTESY,
	PERSON.JobId	 as  C7_JOBID,
	PERSON.StdBookCount	 as  C8_STDBOOKCOUNT,
	PERSON.ImprintLnOne	 as  C9_IMPRINTLNONE,
	PERSON.ImprintLnTwo	 as  C10_IMPRINTLNTWO,
	PERSON.ModifiedBy	 as  C11_MODIFIEDBY,
	PERSON.ModifiedDate	 as  C12_MODIFIEDDATE,
	PERSON.UploadCode	 as  C13_UPLOADCODE,
	PERSON.CreatedDate	 as  C14_CREATEDDATE,
	PERSON.IsTeacher	 as  C15_ISTEACHER,
	PERSON.PaymentDate	 as  C16_PAYMENTDATE,
	PERSON.PaymentRec	 as  C17_PAYMENTREC,
	PERSON.Notes	 as  C18_NOTES,
	PERSON.ExternalId	 as  C19_EXTERNALID,
	PERSON.CurrentPortrait	 as  C20_CURRENTPORTRAIT,
	PERSON.OrderOriginId	 as  C21_ORDERORIGINID,
	PERSON.OriginReferenceNumber	 as  C22_ORIGINREFERENCENUMBER,
	PERSON.PackageSchoolPricingId	 as  C23_PACKAGESCHOOLPRICINGID,
	PERSON.EffectivePrice	 as  C24_EFFECTIVEPRICE,
	PERSON.TotalAmount	 as  C25_TOTALAMOUNT,
	PERSON.TotalTax	 as  C26_TOTALTAX,
	PERSON.DonationAmount	 as  C27_DONATIONAMOUNT,
	PERSON.PortraitImageUrl	 as  C28_PORTRAITIMAGEURL,
	PERSON.OrderDate	 as  C29_ORDERDATE,
	PERSON.LastModifiedDate	 as  C30_LASTMODIFIEDDATE,
	PERSON.LastModifiedBy	 as  C31_LASTMODIFIEDBY,
	PERSON.Discount	 as  C32_DISCOUNT,
	PERSON.GradeId	 as  C33_GRADEID,
	PERSON.JobPricingOverrideId	 as  C34_JOBPRICINGOVERRIDEID,
	PERSON.IncludedLine	 as  C35_INCLUDEDLINE,
	PERSON.IncludedIcon	 as  C36_INCLUDEDICON,
	PERSON.CIRId	 as  C37_CIRID,
	PERSON.PortraitPoseEnable	 as  C38_PORTRAITPOSEENABLE,
	PERSON.CurrentPortraitPose	 as  C39_CURRENTPORTRAITPOSE,
	PERSON.CustomerEmail	 as  C40_CUSTOMEREMAIL,
	PERSON.SubjectKey	 as  C41_SUBJECT_KEY
from	Nextools2009Prod.dbo.Person as PERSON, Nextools2009Prod.dbo.Job as JOB
where	(1=1)
And (JOB.JobCode%100 >= 14 AND
((PERSON.CreatedDate >= convert(datetime,SUBSTRING('#WAREHOUSE_PROJECT.v_cdc_load_date', 1, 19),120) - 7.0)
or
(PERSON.ModifiedDate >= convert(datetime,SUBSTRING('#WAREHOUSE_PROJECT.v_cdc_load_date', 1, 19),120) - 7.0)
or
(PERSON.LastModifiedDate >= convert(datetime,SUBSTRING('#WAREHOUSE_PROJECT.v_cdc_load_date', 1, 19),120) - 7.0)))
 And (PERSON.JobId=JOB.JobId)




&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_PERSON
(
	C1_PERSONID,
	C2_PORTRAITGROUPTYPEID,
	C3_FIRSTNAME,
	C4_LASTNAME,
	C5_PERSONTYPE,
	C6_COURTESY,
	C7_JOBID,
	C8_STDBOOKCOUNT,
	C9_IMPRINTLNONE,
	C10_IMPRINTLNTWO,
	C11_MODIFIEDBY,
	C12_MODIFIEDDATE,
	C13_UPLOADCODE,
	C14_CREATEDDATE,
	C15_ISTEACHER,
	C16_PAYMENTDATE,
	C17_PAYMENTREC,
	C18_NOTES,
	C19_EXTERNALID,
	C20_CURRENTPORTRAIT,
	C21_ORDERORIGINID,
	C22_ORIGINREFERENCENUMBER,
	C23_PACKAGESCHOOLPRICINGID,
	C24_EFFECTIVEPRICE,
	C25_TOTALAMOUNT,
	C26_TOTALTAX,
	C27_DONATIONAMOUNT,
	C28_PORTRAITIMAGEURL,
	C29_ORDERDATE,
	C30_LASTMODIFIEDDATE,
	C31_LASTMODIFIEDBY,
	C32_DISCOUNT,
	C33_GRADEID,
	C34_JOBPRICINGOVERRIDEID,
	C35_INCLUDEDLINE,
	C36_INCLUDEDICON,
	C37_CIRID,
	C38_PORTRAITPOSEENABLE,
	C39_CURRENTPORTRAITPOSE,
	C40_CUSTOMEREMAIL,
	C41_SUBJECT_KEY
)
values
(
	:C1_PERSONID,
	:C2_PORTRAITGROUPTYPEID,
	:C3_FIRSTNAME,
	:C4_LASTNAME,
	:C5_PERSONTYPE,
	:C6_COURTESY,
	:C7_JOBID,
	:C8_STDBOOKCOUNT,
	:C9_IMPRINTLNONE,
	:C10_IMPRINTLNTWO,
	:C11_MODIFIEDBY,
	:C12_MODIFIEDDATE,
	:C13_UPLOADCODE,
	:C14_CREATEDDATE,
	:C15_ISTEACHER,
	:C16_PAYMENTDATE,
	:C17_PAYMENTREC,
	:C18_NOTES,
	:C19_EXTERNALID,
	:C20_CURRENTPORTRAIT,
	:C21_ORDERORIGINID,
	:C22_ORIGINREFERENCENUMBER,
	:C23_PACKAGESCHOOLPRICINGID,
	:C24_EFFECTIVEPRICE,
	:C25_TOTALAMOUNT,
	:C26_TOTALTAX,
	:C27_DONATIONAMOUNT,
	:C28_PORTRAITIMAGEURL,
	:C29_ORDERDATE,
	:C30_LASTMODIFIEDDATE,
	:C31_LASTMODIFIEDBY,
	:C32_DISCOUNT,
	:C33_GRADEID,
	:C34_JOBPRICINGOVERRIDEID,
	:C35_INCLUDEDLINE,
	:C36_INCLUDEDICON,
	:C37_CIRID,
	:C38_PORTRAITPOSEENABLE,
	:C39_CURRENTPORTRAITPOSE,
	:C40_CUSTOMEREMAIL,
	:C41_SUBJECT_KEY
)

&


/*-----------------------------------------------*/
/* TASK No. 188 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_PERSON',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 190 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PERSON 

&


/*-----------------------------------------------*/
/* TASK No. 191 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_PERSON
(
	PERSONID		NUMBER NULL,
	PORTRAITGROUPTYPEID		NUMBER NULL,
	FIRSTNAME		VARCHAR2(80) NULL,
	LASTNAME		VARCHAR2(80) NULL,
	PERSONTYPE		NUMBER NULL,
	COURTESY		VARCHAR2(5) NULL,
	JOBID		NUMBER NULL,
	STDBOOKCOUNT		NUMBER NULL,
	IMPRINTLNONE		VARCHAR2(32) NULL,
	IMPRINTLNTWO		VARCHAR2(32) NULL,
	MODIFIEDBY		NUMBER NULL,
	MODIFIEDDATE		DATE NULL,
	UPLOADCODE		VARCHAR2(50) NULL,
	CREATEDDATE		DATE NULL,
	ISTEACHER		NUMBER NULL,
	PAYMENTDATE		DATE NULL,
	PAYMENTREC		NUMBER NULL,
	NOTES		VARCHAR2(255) NULL,
	EXTERNALID		NUMBER NULL,
	CURRENTPORTRAIT		NUMBER NULL,
	ORDERORIGINID		NUMBER NULL,
	ORIGINREFERENCENUMBER		VARCHAR2(50) NULL,
	PACKAGESCHOOLPRICINGID		NUMBER NULL,
	EFFECTIVEPRICE		NUMBER NULL,
	TOTALAMOUNT		NUMBER NULL,
	TOTALTAX		NUMBER NULL,
	DONATIONAMOUNT		NUMBER NULL,
	PORTRAITIMAGEURL		VARCHAR2(200) NULL,
	ORDERDATE		DATE NULL,
	LASTMODIFIEDDATE		DATE NULL,
	LASTMODIFIEDBY		VARCHAR2(50) NULL,
	DISCOUNT		NUMBER NULL,
	GRADEID		NUMBER NULL,
	JOBPRICINGOVERRIDEID		NUMBER NULL,
	INCLUDEDLINE		NUMBER NULL,
	INCLUDEDICON		NUMBER NULL,
	CIRID		VARCHAR2(50) NULL,
	PORTRAITPOSEENABLE		NUMBER NULL,
	CURRENTPORTRAITPOSE		NUMBER NULL,
	CUSTOMEREMAIL		VARCHAR2(256) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	DW_CREATE_DATE		DATE NULL,
	SUBJECT_KEY		VARCHAR2(255) NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 192 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_PERSON
(
	PERSONID,
	PORTRAITGROUPTYPEID,
	FIRSTNAME,
	LASTNAME,
	PERSONTYPE,
	COURTESY,
	JOBID,
	STDBOOKCOUNT,
	IMPRINTLNONE,
	IMPRINTLNTWO,
	MODIFIEDBY,
	MODIFIEDDATE,
	UPLOADCODE,
	CREATEDDATE,
	ISTEACHER,
	PAYMENTDATE,
	PAYMENTREC,
	NOTES,
	EXTERNALID,
	CURRENTPORTRAIT,
	ORDERORIGINID,
	ORIGINREFERENCENUMBER,
	PACKAGESCHOOLPRICINGID,
	EFFECTIVEPRICE,
	TOTALAMOUNT,
	TOTALTAX,
	DONATIONAMOUNT,
	PORTRAITIMAGEURL,
	ORDERDATE,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBY,
	DISCOUNT,
	GRADEID,
	JOBPRICINGOVERRIDEID,
	INCLUDEDLINE,
	INCLUDEDICON,
	CIRID,
	PORTRAITPOSEENABLE,
	CURRENTPORTRAITPOSE,
	CUSTOMEREMAIL,
	SUBJECT_KEY,
	IND_UPDATE
)
select 
PERSONID,
	PORTRAITGROUPTYPEID,
	FIRSTNAME,
	LASTNAME,
	PERSONTYPE,
	COURTESY,
	JOBID,
	STDBOOKCOUNT,
	IMPRINTLNONE,
	IMPRINTLNTWO,
	MODIFIEDBY,
	MODIFIEDDATE,
	UPLOADCODE,
	CREATEDDATE,
	ISTEACHER,
	PAYMENTDATE,
	PAYMENTREC,
	NOTES,
	EXTERNALID,
	CURRENTPORTRAIT,
	ORDERORIGINID,
	ORIGINREFERENCENUMBER,
	PACKAGESCHOOLPRICINGID,
	EFFECTIVEPRICE,
	TOTALAMOUNT,
	TOTALTAX,
	DONATIONAMOUNT,
	PORTRAITIMAGEURL,
	ORDERDATE,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBY,
	DISCOUNT,
	GRADEID,
	JOBPRICINGOVERRIDEID,
	INCLUDEDLINE,
	INCLUDEDICON,
	CIRID,
	PORTRAITPOSEENABLE,
	CURRENTPORTRAITPOSE,
	CUSTOMEREMAIL,
	SUBJECT_KEY,
	IND_UPDATE
 from (


select 	 
	
	C1_PERSONID PERSONID,
	C2_PORTRAITGROUPTYPEID PORTRAITGROUPTYPEID,
	C3_FIRSTNAME FIRSTNAME,
	C4_LASTNAME LASTNAME,
	C5_PERSONTYPE PERSONTYPE,
	C6_COURTESY COURTESY,
	C7_JOBID JOBID,
	C8_STDBOOKCOUNT STDBOOKCOUNT,
	C9_IMPRINTLNONE IMPRINTLNONE,
	C10_IMPRINTLNTWO IMPRINTLNTWO,
	C11_MODIFIEDBY MODIFIEDBY,
	C12_MODIFIEDDATE MODIFIEDDATE,
	C13_UPLOADCODE UPLOADCODE,
	C14_CREATEDDATE CREATEDDATE,
	C15_ISTEACHER ISTEACHER,
	C16_PAYMENTDATE PAYMENTDATE,
	C17_PAYMENTREC PAYMENTREC,
	C18_NOTES NOTES,
	C19_EXTERNALID EXTERNALID,
	C20_CURRENTPORTRAIT CURRENTPORTRAIT,
	C21_ORDERORIGINID ORDERORIGINID,
	C22_ORIGINREFERENCENUMBER ORIGINREFERENCENUMBER,
	C23_PACKAGESCHOOLPRICINGID PACKAGESCHOOLPRICINGID,
	C24_EFFECTIVEPRICE EFFECTIVEPRICE,
	C25_TOTALAMOUNT TOTALAMOUNT,
	C26_TOTALTAX TOTALTAX,
	C27_DONATIONAMOUNT DONATIONAMOUNT,
	C28_PORTRAITIMAGEURL PORTRAITIMAGEURL,
	C29_ORDERDATE ORDERDATE,
	C30_LASTMODIFIEDDATE LASTMODIFIEDDATE,
	C31_LASTMODIFIEDBY LASTMODIFIEDBY,
	C32_DISCOUNT DISCOUNT,
	C33_GRADEID GRADEID,
	C34_JOBPRICINGOVERRIDEID JOBPRICINGOVERRIDEID,
	C35_INCLUDEDLINE INCLUDEDLINE,
	C36_INCLUDEDICON INCLUDEDICON,
	C37_CIRID CIRID,
	C38_PORTRAITPOSEENABLE PORTRAITPOSEENABLE,
	C39_CURRENTPORTRAITPOSE CURRENTPORTRAITPOSE,
	C40_CUSTOMEREMAIL CUSTOMEREMAIL,
	C41_SUBJECT_KEY SUBJECT_KEY,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_PERSON
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_PERSON T
	where	T.PERSONID	= S.PERSONID 
		 and ((T.PORTRAITGROUPTYPEID = S.PORTRAITGROUPTYPEID) or (T.PORTRAITGROUPTYPEID IS NULL and S.PORTRAITGROUPTYPEID IS NULL)) and
		((T.FIRSTNAME = S.FIRSTNAME) or (T.FIRSTNAME IS NULL and S.FIRSTNAME IS NULL)) and
		((T.LASTNAME = S.LASTNAME) or (T.LASTNAME IS NULL and S.LASTNAME IS NULL)) and
		((T.PERSONTYPE = S.PERSONTYPE) or (T.PERSONTYPE IS NULL and S.PERSONTYPE IS NULL)) and
		((T.COURTESY = S.COURTESY) or (T.COURTESY IS NULL and S.COURTESY IS NULL)) and
		((T.JOBID = S.JOBID) or (T.JOBID IS NULL and S.JOBID IS NULL)) and
		((T.STDBOOKCOUNT = S.STDBOOKCOUNT) or (T.STDBOOKCOUNT IS NULL and S.STDBOOKCOUNT IS NULL)) and
		((T.IMPRINTLNONE = S.IMPRINTLNONE) or (T.IMPRINTLNONE IS NULL and S.IMPRINTLNONE IS NULL)) and
		((T.IMPRINTLNTWO = S.IMPRINTLNTWO) or (T.IMPRINTLNTWO IS NULL and S.IMPRINTLNTWO IS NULL)) and
		((T.MODIFIEDBY = S.MODIFIEDBY) or (T.MODIFIEDBY IS NULL and S.MODIFIEDBY IS NULL)) and
		((T.MODIFIEDDATE = S.MODIFIEDDATE) or (T.MODIFIEDDATE IS NULL and S.MODIFIEDDATE IS NULL)) and
		((T.UPLOADCODE = S.UPLOADCODE) or (T.UPLOADCODE IS NULL and S.UPLOADCODE IS NULL)) and
		((T.CREATEDDATE = S.CREATEDDATE) or (T.CREATEDDATE IS NULL and S.CREATEDDATE IS NULL)) and
		((T.ISTEACHER = S.ISTEACHER) or (T.ISTEACHER IS NULL and S.ISTEACHER IS NULL)) and
		((T.PAYMENTDATE = S.PAYMENTDATE) or (T.PAYMENTDATE IS NULL and S.PAYMENTDATE IS NULL)) and
		((T.PAYMENTREC = S.PAYMENTREC) or (T.PAYMENTREC IS NULL and S.PAYMENTREC IS NULL)) and
		((T.NOTES = S.NOTES) or (T.NOTES IS NULL and S.NOTES IS NULL)) and
		((T.EXTERNALID = S.EXTERNALID) or (T.EXTERNALID IS NULL and S.EXTERNALID IS NULL)) and
		((T.CURRENTPORTRAIT = S.CURRENTPORTRAIT) or (T.CURRENTPORTRAIT IS NULL and S.CURRENTPORTRAIT IS NULL)) and
		((T.ORDERORIGINID = S.ORDERORIGINID) or (T.ORDERORIGINID IS NULL and S.ORDERORIGINID IS NULL)) and
		((T.ORIGINREFERENCENUMBER = S.ORIGINREFERENCENUMBER) or (T.ORIGINREFERENCENUMBER IS NULL and S.ORIGINREFERENCENUMBER IS NULL)) and
		((T.PACKAGESCHOOLPRICINGID = S.PACKAGESCHOOLPRICINGID) or (T.PACKAGESCHOOLPRICINGID IS NULL and S.PACKAGESCHOOLPRICINGID IS NULL)) and
		((T.EFFECTIVEPRICE = S.EFFECTIVEPRICE) or (T.EFFECTIVEPRICE IS NULL and S.EFFECTIVEPRICE IS NULL)) and
		((T.TOTALAMOUNT = S.TOTALAMOUNT) or (T.TOTALAMOUNT IS NULL and S.TOTALAMOUNT IS NULL)) and
		((T.TOTALTAX = S.TOTALTAX) or (T.TOTALTAX IS NULL and S.TOTALTAX IS NULL)) and
		((T.DONATIONAMOUNT = S.DONATIONAMOUNT) or (T.DONATIONAMOUNT IS NULL and S.DONATIONAMOUNT IS NULL)) and
		((T.PORTRAITIMAGEURL = S.PORTRAITIMAGEURL) or (T.PORTRAITIMAGEURL IS NULL and S.PORTRAITIMAGEURL IS NULL)) and
		((T.ORDERDATE = S.ORDERDATE) or (T.ORDERDATE IS NULL and S.ORDERDATE IS NULL)) and
		((T.LASTMODIFIEDDATE = S.LASTMODIFIEDDATE) or (T.LASTMODIFIEDDATE IS NULL and S.LASTMODIFIEDDATE IS NULL)) and
		((T.LASTMODIFIEDBY = S.LASTMODIFIEDBY) or (T.LASTMODIFIEDBY IS NULL and S.LASTMODIFIEDBY IS NULL)) and
		((T.DISCOUNT = S.DISCOUNT) or (T.DISCOUNT IS NULL and S.DISCOUNT IS NULL)) and
		((T.GRADEID = S.GRADEID) or (T.GRADEID IS NULL and S.GRADEID IS NULL)) and
		((T.JOBPRICINGOVERRIDEID = S.JOBPRICINGOVERRIDEID) or (T.JOBPRICINGOVERRIDEID IS NULL and S.JOBPRICINGOVERRIDEID IS NULL)) and
		((T.INCLUDEDLINE = S.INCLUDEDLINE) or (T.INCLUDEDLINE IS NULL and S.INCLUDEDLINE IS NULL)) and
		((T.INCLUDEDICON = S.INCLUDEDICON) or (T.INCLUDEDICON IS NULL and S.INCLUDEDICON IS NULL)) and
		((T.CIRID = S.CIRID) or (T.CIRID IS NULL and S.CIRID IS NULL)) and
		((T.PORTRAITPOSEENABLE = S.PORTRAITPOSEENABLE) or (T.PORTRAITPOSEENABLE IS NULL and S.PORTRAITPOSEENABLE IS NULL)) and
		((T.CURRENTPORTRAITPOSE = S.CURRENTPORTRAITPOSE) or (T.CURRENTPORTRAITPOSE IS NULL and S.CURRENTPORTRAITPOSE IS NULL)) and
		((T.CUSTOMEREMAIL = S.CUSTOMEREMAIL) or (T.CUSTOMEREMAIL IS NULL and S.CUSTOMEREMAIL IS NULL)) and
		((T.SUBJECT_KEY = S.SUBJECT_KEY) or (T.SUBJECT_KEY IS NULL and S.SUBJECT_KEY IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 193 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_PERSON_IDX
on		ODS_ETL_OWNER.I$_NXTL_PERSON (PERSONID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 194 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_PERSON',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 195 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_PERSON
set	IND_UPDATE = 'U'
where	(PERSONID)
	in	(
		select	PERSONID
		from	ODS.NXTL_PERSON
		)



&


/*-----------------------------------------------*/
/* TASK No. 196 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 197 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_PERSON T
set 	
	(
	T.PORTRAITGROUPTYPEID,
	T.FIRSTNAME,
	T.LASTNAME,
	T.PERSONTYPE,
	T.COURTESY,
	T.JOBID,
	T.STDBOOKCOUNT,
	T.IMPRINTLNONE,
	T.IMPRINTLNTWO,
	T.MODIFIEDBY,
	T.MODIFIEDDATE,
	T.UPLOADCODE,
	T.CREATEDDATE,
	T.ISTEACHER,
	T.PAYMENTDATE,
	T.PAYMENTREC,
	T.NOTES,
	T.EXTERNALID,
	T.CURRENTPORTRAIT,
	T.ORDERORIGINID,
	T.ORIGINREFERENCENUMBER,
	T.PACKAGESCHOOLPRICINGID,
	T.EFFECTIVEPRICE,
	T.TOTALAMOUNT,
	T.TOTALTAX,
	T.DONATIONAMOUNT,
	T.PORTRAITIMAGEURL,
	T.ORDERDATE,
	T.LASTMODIFIEDDATE,
	T.LASTMODIFIEDBY,
	T.DISCOUNT,
	T.GRADEID,
	T.JOBPRICINGOVERRIDEID,
	T.INCLUDEDLINE,
	T.INCLUDEDICON,
	T.CIRID,
	T.PORTRAITPOSEENABLE,
	T.CURRENTPORTRAITPOSE,
	T.CUSTOMEREMAIL,
	T.SUBJECT_KEY
	) =
		(
		select	S.PORTRAITGROUPTYPEID,
			S.FIRSTNAME,
			S.LASTNAME,
			S.PERSONTYPE,
			S.COURTESY,
			S.JOBID,
			S.STDBOOKCOUNT,
			S.IMPRINTLNONE,
			S.IMPRINTLNTWO,
			S.MODIFIEDBY,
			S.MODIFIEDDATE,
			S.UPLOADCODE,
			S.CREATEDDATE,
			S.ISTEACHER,
			S.PAYMENTDATE,
			S.PAYMENTREC,
			S.NOTES,
			S.EXTERNALID,
			S.CURRENTPORTRAIT,
			S.ORDERORIGINID,
			S.ORIGINREFERENCENUMBER,
			S.PACKAGESCHOOLPRICINGID,
			S.EFFECTIVEPRICE,
			S.TOTALAMOUNT,
			S.TOTALTAX,
			S.DONATIONAMOUNT,
			S.PORTRAITIMAGEURL,
			S.ORDERDATE,
			S.LASTMODIFIEDDATE,
			S.LASTMODIFIEDBY,
			S.DISCOUNT,
			S.GRADEID,
			S.JOBPRICINGOVERRIDEID,
			S.INCLUDEDLINE,
			S.INCLUDEDICON,
			S.CIRID,
			S.PORTRAITPOSEENABLE,
			S.CURRENTPORTRAITPOSE,
			S.CUSTOMEREMAIL,
			S.SUBJECT_KEY
		from	ODS_ETL_OWNER.I$_NXTL_PERSON S
		where	T.PERSONID	=S.PERSONID
	    	 )
	,                                        T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(PERSONID)
	in	(
		select	PERSONID
		from	ODS_ETL_OWNER.I$_NXTL_PERSON
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 198 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_PERSON T
	(
	PERSONID,
	PORTRAITGROUPTYPEID,
	FIRSTNAME,
	LASTNAME,
	PERSONTYPE,
	COURTESY,
	JOBID,
	STDBOOKCOUNT,
	IMPRINTLNONE,
	IMPRINTLNTWO,
	MODIFIEDBY,
	MODIFIEDDATE,
	UPLOADCODE,
	CREATEDDATE,
	ISTEACHER,
	PAYMENTDATE,
	PAYMENTREC,
	NOTES,
	EXTERNALID,
	CURRENTPORTRAIT,
	ORDERORIGINID,
	ORIGINREFERENCENUMBER,
	PACKAGESCHOOLPRICINGID,
	EFFECTIVEPRICE,
	TOTALAMOUNT,
	TOTALTAX,
	DONATIONAMOUNT,
	PORTRAITIMAGEURL,
	ORDERDATE,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBY,
	DISCOUNT,
	GRADEID,
	JOBPRICINGOVERRIDEID,
	INCLUDEDLINE,
	INCLUDEDICON,
	CIRID,
	PORTRAITPOSEENABLE,
	CURRENTPORTRAITPOSE,
	CUSTOMEREMAIL,
	SUBJECT_KEY
	,                                         EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID,
	DW_CREATE_DATE
	)
select 	PERSONID,
	PORTRAITGROUPTYPEID,
	FIRSTNAME,
	LASTNAME,
	PERSONTYPE,
	COURTESY,
	JOBID,
	STDBOOKCOUNT,
	IMPRINTLNONE,
	IMPRINTLNTWO,
	MODIFIEDBY,
	MODIFIEDDATE,
	UPLOADCODE,
	CREATEDDATE,
	ISTEACHER,
	PAYMENTDATE,
	PAYMENTREC,
	NOTES,
	EXTERNALID,
	CURRENTPORTRAIT,
	ORDERORIGINID,
	ORIGINREFERENCENUMBER,
	PACKAGESCHOOLPRICINGID,
	EFFECTIVEPRICE,
	TOTALAMOUNT,
	TOTALTAX,
	DONATIONAMOUNT,
	PORTRAITIMAGEURL,
	ORDERDATE,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBY,
	DISCOUNT,
	GRADEID,
	JOBPRICINGOVERRIDEID,
	INCLUDEDLINE,
	INCLUDEDICON,
	CIRID,
	PORTRAITPOSEENABLE,
	CURRENTPORTRAITPOSE,
	CUSTOMEREMAIL,
	SUBJECT_KEY
	,                                         sysdate,
	'A',
	1,
	sysdate
from	ODS_ETL_OWNER.I$_NXTL_PERSON S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 199 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 200 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_PERSON 

&


/*-----------------------------------------------*/
/* TASK No. 1000189 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_PERSON 

&


/*-----------------------------------------------*/
/* TASK No. 201 */
/* Truncate table nxtl_person_stg */

truncate table ODS_ETL_OWNER.NXTL_PERSON_STG


&


/*-----------------------------------------------*/
/* TASK No. 202 */
/* Load table nxtl_person_stg */

/* SOURCE CODE */
with data as
(
select p.personid
from Nextools2009Prod.dbo.person p, Nextools2009Prod.dbo.job j
where p.jobid = j.jobid
and j.jobcode%100 >= 14
)
select personid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_PERSON_STG
(personid)
select
:personid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 203 */
/* Update active_ind on nxtl_person */

update ods.nxtl_person p
set p.active_ind = 'I'
,p.effective_date = sysdate
where nvl(p.active_ind,'A') = 'A'
and not exists
(select  * from ods_etl_owner.nxtl_person_stg ps
where (p.personid = ps.personid))



&


/*-----------------------------------------------*/
/* TASK No. 204 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYANSWER 

&


/*-----------------------------------------------*/
/* TASK No. 205 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYANSWER
(
	C1_JOBUSERSURVEYANSWERID	NUMBER NULL,
	C2_JOBUSERID	NUMBER NULL,
	C3_QUESTIONID	NUMBER NULL,
	C4_ANSWERSELECTION	NUMBER NULL,
	C5_ANSWERTEXT	VARCHAR2(200) NULL,
	C6_ANSWERDATE	DATE NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 206 */
/* Load data */

/* SOURCE CODE */
select	
	JOBUSERSURVEYANSWER.JobUserSurveyAnswerId	 as  C1_JOBUSERSURVEYANSWERID,
	JOBUSERSURVEYANSWER.JobUserId	 as  C2_JOBUSERID,
	JOBUSERSURVEYANSWER.QuestionId	 as  C3_QUESTIONID,
	JOBUSERSURVEYANSWER.AnswerSelection	 as  C4_ANSWERSELECTION,
	JOBUSERSURVEYANSWER.AnswerText	 as  C5_ANSWERTEXT,
	JOBUSERSURVEYANSWER.AnswerDate	 as  C6_ANSWERDATE
from	Nextools2009Prod.dbo.JobUserSurveyAnswer as JOBUSERSURVEYANSWER
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYANSWER
(
	C1_JOBUSERSURVEYANSWERID,
	C2_JOBUSERID,
	C3_QUESTIONID,
	C4_ANSWERSELECTION,
	C5_ANSWERTEXT,
	C6_ANSWERDATE
)
values
(
	:C1_JOBUSERSURVEYANSWERID,
	:C2_JOBUSERID,
	:C3_QUESTIONID,
	:C4_ANSWERSELECTION,
	:C5_ANSWERTEXT,
	:C6_ANSWERDATE
)

&


/*-----------------------------------------------*/
/* TASK No. 207 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBUSERSURVEYANSWER',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 209 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER 

&


/*-----------------------------------------------*/
/* TASK No. 210 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER
(
	JOBUSERSURVEYANSWERID		NUMBER NULL,
	JOBUSERID		NUMBER NULL,
	QUESTIONID		NUMBER NULL,
	ANSWERSELECTION		NUMBER NULL,
	ANSWERTEXT		VARCHAR2(200) NULL,
	ANSWERDATE		DATE NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 211 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER
(
	JOBUSERSURVEYANSWERID,
	JOBUSERID,
	QUESTIONID,
	ANSWERSELECTION,
	ANSWERTEXT,
	ANSWERDATE,
	IND_UPDATE
)
select 
JOBUSERSURVEYANSWERID,
	JOBUSERID,
	QUESTIONID,
	ANSWERSELECTION,
	ANSWERTEXT,
	ANSWERDATE,
	IND_UPDATE
 from (


select 	 
	
	C1_JOBUSERSURVEYANSWERID JOBUSERSURVEYANSWERID,
	C2_JOBUSERID JOBUSERID,
	C3_QUESTIONID QUESTIONID,
	C4_ANSWERSELECTION ANSWERSELECTION,
	C5_ANSWERTEXT ANSWERTEXT,
	C6_ANSWERDATE ANSWERDATE,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYANSWER
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBUSERSURVEYANSWER T
	where	T.JOBUSERSURVEYANSWERID	= S.JOBUSERSURVEYANSWERID 
		 and ((T.JOBUSERID = S.JOBUSERID) or (T.JOBUSERID IS NULL and S.JOBUSERID IS NULL)) and
		((T.QUESTIONID = S.QUESTIONID) or (T.QUESTIONID IS NULL and S.QUESTIONID IS NULL)) and
		((T.ANSWERSELECTION = S.ANSWERSELECTION) or (T.ANSWERSELECTION IS NULL and S.ANSWERSELECTION IS NULL)) and
		((T.ANSWERTEXT = S.ANSWERTEXT) or (T.ANSWERTEXT IS NULL and S.ANSWERTEXT IS NULL)) and
		((T.ANSWERDATE = S.ANSWERDATE) or (T.ANSWERDATE IS NULL and S.ANSWERDATE IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 212 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER_ID
on		ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER (JOBUSERSURVEYANSWERID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 213 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBUSERSURVEYANSWER',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 214 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER
set	IND_UPDATE = 'U'
where	(JOBUSERSURVEYANSWERID)
	in	(
		select	JOBUSERSURVEYANSWERID
		from	ODS.NXTL_JOBUSERSURVEYANSWER
		)



&


/*-----------------------------------------------*/
/* TASK No. 215 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 216 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBUSERSURVEYANSWER T
set 	
	(
	T.JOBUSERID,
	T.QUESTIONID,
	T.ANSWERSELECTION,
	T.ANSWERTEXT,
	T.ANSWERDATE
	) =
		(
		select	S.JOBUSERID,
			S.QUESTIONID,
			S.ANSWERSELECTION,
			S.ANSWERTEXT,
			S.ANSWERDATE
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER S
		where	T.JOBUSERSURVEYANSWERID	=S.JOBUSERSURVEYANSWERID
	    	 )
	,     T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = :v_sess_no

where	(JOBUSERSURVEYANSWERID)
	in	(
		select	JOBUSERSURVEYANSWERID
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 217 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBUSERSURVEYANSWER T
	(
	JOBUSERSURVEYANSWERID,
	JOBUSERID,
	QUESTIONID,
	ANSWERSELECTION,
	ANSWERTEXT,
	ANSWERDATE
	,      EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	JOBUSERSURVEYANSWERID,
	JOBUSERID,
	QUESTIONID,
	ANSWERSELECTION,
	ANSWERTEXT,
	ANSWERDATE
	,      sysdate,
	'A',
	:v_sess_no
from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 218 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 219 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYANSWER 

&


/*-----------------------------------------------*/
/* TASK No. 1000208 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYANSWER 

&


/*-----------------------------------------------*/
/* TASK No. 220 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYQUESTION 

&


/*-----------------------------------------------*/
/* TASK No. 221 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYQUESTION
(
	C1_QUESTIONID	NUMBER NULL,
	C2_QUESTION	VARCHAR2(1024) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 222 */
/* Load data */

/* SOURCE CODE */
select	
	JOBUSERSURVEYQUESTION.QuestionId	 as  C1_QUESTIONID,
	JOBUSERSURVEYQUESTION.Question	 as  C2_QUESTION
from	Nextools2009Prod.dbo.JobUserSurveyQuestion as JOBUSERSURVEYQUESTION
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYQUESTION
(
	C1_QUESTIONID,
	C2_QUESTION
)
values
(
	:C1_QUESTIONID,
	:C2_QUESTION
)

&


/*-----------------------------------------------*/
/* TASK No. 223 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_JOBUSERSURVEYQUESTION',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 225 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION 

&


/*-----------------------------------------------*/
/* TASK No. 226 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION
(
	QUESTIONID		NUMBER NULL,
	QUESTION		VARCHAR2(1024) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 227 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION
(
	QUESTIONID,
	QUESTION,
	IND_UPDATE
)
select 
QUESTIONID,
	QUESTION,
	IND_UPDATE
 from (


select 	 
	
	C1_QUESTIONID QUESTIONID,
	C2_QUESTION QUESTION,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYQUESTION
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_JOBUSERSURVEYQUESTION T
	where	T.QUESTIONID	= S.QUESTIONID 
		 and ((T.QUESTION = S.QUESTION) or (T.QUESTION IS NULL and S.QUESTION IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 228 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION_
on		ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION (QUESTIONID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 229 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_JOBUSERSURVEYQUESTION',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 230 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION
set	IND_UPDATE = 'U'
where	(QUESTIONID)
	in	(
		select	QUESTIONID
		from	ODS.NXTL_JOBUSERSURVEYQUESTION
		)



&


/*-----------------------------------------------*/
/* TASK No. 231 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 232 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_JOBUSERSURVEYQUESTION T
set 	
	(
	T.QUESTION
	) =
		(
		select	S.QUESTION
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION S
		where	T.QUESTIONID	=S.QUESTIONID
	    	 )
	, T.EFFECTIVE_DATE = sysdate,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = :v_sess_no

where	(QUESTIONID)
	in	(
		select	QUESTIONID
		from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 233 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_JOBUSERSURVEYQUESTION T
	(
	QUESTIONID,
	QUESTION
	,  EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	QUESTIONID,
	QUESTION
	,  sysdate,
	'A',
	:v_sess_no
from	ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 234 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 235 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_JOBUSERSURVEYQUESTION 

&


/*-----------------------------------------------*/
/* TASK No. 1000224 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_JOBUSERSURVEYQUESTION 

&


/*-----------------------------------------------*/
/* TASK No. 236 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_BOOK 

&


/*-----------------------------------------------*/
/* TASK No. 237 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_BOOK
(
	C1_BOOKID	NUMBER NULL,
	C2_NAME	VARCHAR2(100) NULL,
	C3_BOOKSTATUSID	NUMBER NULL,
	C4_RELEASEDATE	DATE NULL,
	C5_BOOKTYPEID	NUMBER NULL,
	C6_PREFERENCEOPTIONID	NUMBER NULL,
	C7_YEARCODE	VARCHAR2(4) NULL,
	C8_MESSAGE	VARCHAR2(500) NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 238 */
/* Load data */

/* SOURCE CODE */
select	
	BOOK.BookId	 as  C1_BOOKID,
	BOOK.Name	 as  C2_NAME,
	BOOK.BookStatusId	 as  C3_BOOKSTATUSID,
	BOOK.ReleaseDate	 as  C4_RELEASEDATE,
	BOOK.BookTypeId	 as  C5_BOOKTYPEID,
	BOOK.PreferenceOptionId	 as  C6_PREFERENCEOPTIONID,
	BOOK.YearCode	 as  C7_YEARCODE,
	BOOK.Message	 as  C8_MESSAGE
from	Nextools2009Prod.dbo.Book as BOOK
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_BOOK
(
	C1_BOOKID,
	C2_NAME,
	C3_BOOKSTATUSID,
	C4_RELEASEDATE,
	C5_BOOKTYPEID,
	C6_PREFERENCEOPTIONID,
	C7_YEARCODE,
	C8_MESSAGE
)
values
(
	:C1_BOOKID,
	:C2_NAME,
	:C3_BOOKSTATUSID,
	:C4_RELEASEDATE,
	:C5_BOOKTYPEID,
	:C6_PREFERENCEOPTIONID,
	:C7_YEARCODE,
	:C8_MESSAGE
)

&


/*-----------------------------------------------*/
/* TASK No. 239 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_BOOK',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 241 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_BOOK 

&


/*-----------------------------------------------*/
/* TASK No. 242 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_BOOK
(
	BOOKID		NUMBER NULL,
	NAME		VARCHAR2(100) NULL,
	BOOKSTATUSID		NUMBER NULL,
	RELEASEDATE		DATE NULL,
	BOOKTYPEID		NUMBER NULL,
	PREFERENCEOPTIONID		NUMBER NULL,
	YEARCODE		VARCHAR2(4) NULL,
	MESSAGE		VARCHAR2(500) NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 243 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_BOOK
(
	BOOKID,
	NAME,
	BOOKSTATUSID,
	RELEASEDATE,
	BOOKTYPEID,
	PREFERENCEOPTIONID,
	YEARCODE,
	MESSAGE,
	IND_UPDATE
)
select 
BOOKID,
	NAME,
	BOOKSTATUSID,
	RELEASEDATE,
	BOOKTYPEID,
	PREFERENCEOPTIONID,
	YEARCODE,
	MESSAGE,
	IND_UPDATE
 from (


select 	 
	
	C1_BOOKID BOOKID,
	C2_NAME NAME,
	C3_BOOKSTATUSID BOOKSTATUSID,
	C4_RELEASEDATE RELEASEDATE,
	C5_BOOKTYPEID BOOKTYPEID,
	C6_PREFERENCEOPTIONID PREFERENCEOPTIONID,
	C7_YEARCODE YEARCODE,
	C8_MESSAGE MESSAGE,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_BOOK
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_BOOK T
	where	T.BOOKID	= S.BOOKID 
		 and ((T.NAME = S.NAME) or (T.NAME IS NULL and S.NAME IS NULL)) and
		((T.BOOKSTATUSID = S.BOOKSTATUSID) or (T.BOOKSTATUSID IS NULL and S.BOOKSTATUSID IS NULL)) and
		((T.RELEASEDATE = S.RELEASEDATE) or (T.RELEASEDATE IS NULL and S.RELEASEDATE IS NULL)) and
		((T.BOOKTYPEID = S.BOOKTYPEID) or (T.BOOKTYPEID IS NULL and S.BOOKTYPEID IS NULL)) and
		((T.PREFERENCEOPTIONID = S.PREFERENCEOPTIONID) or (T.PREFERENCEOPTIONID IS NULL and S.PREFERENCEOPTIONID IS NULL)) and
		((T.YEARCODE = S.YEARCODE) or (T.YEARCODE IS NULL and S.YEARCODE IS NULL)) and
		((T.MESSAGE = S.MESSAGE) or (T.MESSAGE IS NULL and S.MESSAGE IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 244 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_BOOK_IDX
on		ODS_ETL_OWNER.I$_NXTL_BOOK (BOOKID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 245 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_BOOK',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 246 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_BOOK
set	IND_UPDATE = 'U'
where	(BOOKID)
	in	(
		select	BOOKID
		from	ODS.NXTL_BOOK
		)



&


/*-----------------------------------------------*/
/* TASK No. 247 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 248 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_BOOK T
set 	
	(
	T.NAME,
	T.BOOKSTATUSID,
	T.RELEASEDATE,
	T.BOOKTYPEID,
	T.PREFERENCEOPTIONID,
	T.YEARCODE,
	T.MESSAGE
	) =
		(
		select	S.NAME,
			S.BOOKSTATUSID,
			S.RELEASEDATE,
			S.BOOKTYPEID,
			S.PREFERENCEOPTIONID,
			S.YEARCODE,
			S.MESSAGE
		from	ODS_ETL_OWNER.I$_NXTL_BOOK S
		where	T.BOOKID	=S.BOOKID
	    	 )
	,       T.EFFECTIVE_DATE = SYSDATE,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(BOOKID)
	in	(
		select	BOOKID
		from	ODS_ETL_OWNER.I$_NXTL_BOOK
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 249 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_BOOK T
	(
	BOOKID,
	NAME,
	BOOKSTATUSID,
	RELEASEDATE,
	BOOKTYPEID,
	PREFERENCEOPTIONID,
	YEARCODE,
	MESSAGE
	,        EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	BOOKID,
	NAME,
	BOOKSTATUSID,
	RELEASEDATE,
	BOOKTYPEID,
	PREFERENCEOPTIONID,
	YEARCODE,
	MESSAGE
	,        SYSDATE,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_BOOK S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 250 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 251 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_BOOK 

&


/*-----------------------------------------------*/
/* TASK No. 1000240 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_BOOK 

&


/*-----------------------------------------------*/
/* TASK No. 252 */
/* Truncate table nxtl_book_stg */

truncate table ODS_ETL_OWNER.NXTL_BOOK_STG


&


/*-----------------------------------------------*/
/* TASK No. 253 */
/* Load table nxtl_book_stg */

/* SOURCE CODE */
with data as
(
select bookid
from Nextools2009Prod.dbo.Book
)
select bookid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_BOOK_STG
(bookid)
select
:bookid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 254 */
/* Update active_ind on nxtl_book */

update ods.nxtl_book b
set b.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_book_stg bs
where (b.bookid = bs.bookid))



&


/*-----------------------------------------------*/
/* TASK No. 255 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_SCHOOLPRICING 

&


/*-----------------------------------------------*/
/* TASK No. 256 */
/* Create work table */

create table ODS_ETL_OWNER.C$_0NXTL_SCHOOLPRICING
(
	C1_SCHOOLPRICINGID	NUMBER NULL,
	C2_JOBID	NUMBER NULL,
	C3_BOOKOPTIONCODE	NUMBER NULL,
	C4_PRICE	NUMBER NULL,
	C5_PRICEPERLINE	NUMBER NULL,
	C6_PRICEPERICON	NUMBER NULL,
	C7_QUANTITY	NUMBER NULL,
	C8_DISCOUNT	NUMBER NULL,
	C9_DROPCONFIRMED	NUMBER NULL,
	C10_DESCRIPTION	VARCHAR2(200) NULL,
	C11_PRICEPERLINE2	NUMBER NULL,
	C12_PRODUCTCATEGORYID	NUMBER NULL,
	C13_ADVERTISEMENTSIZEID	NUMBER NULL,
	C14_ADVERTISEMENTINITIALQUANTI	NUMBER NULL,
	C15_ISACTIVEYBPAY	NUMBER NULL,
	C16_QUANTITYORDERED	NUMBER NULL,
	C17_INCLUDEDLINE	NUMBER NULL,
	C18_INCLUDEDICON	NUMBER NULL,
	C19_PRECONFIGUREDPACKAGEID	NUMBER NULL
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 257 */
/* Load data */

/* SOURCE CODE */
select	
	SCHOOLPRICING.SchoolPricingId	 as  C1_SCHOOLPRICINGID,
	SCHOOLPRICING.JobId	 as  C2_JOBID,
	SCHOOLPRICING.BookOptionCode	 as  C3_BOOKOPTIONCODE,
	SCHOOLPRICING.Price	 as  C4_PRICE,
	SCHOOLPRICING.PricePerLine	 as  C5_PRICEPERLINE,
	SCHOOLPRICING.PricePerIcon	 as  C6_PRICEPERICON,
	SCHOOLPRICING.Quantity	 as  C7_QUANTITY,
	SCHOOLPRICING.Discount	 as  C8_DISCOUNT,
	SCHOOLPRICING.DropConfirmed	 as  C9_DROPCONFIRMED,
	SCHOOLPRICING.Description	 as  C10_DESCRIPTION,
	SCHOOLPRICING.PricePerLine2	 as  C11_PRICEPERLINE2,
	SCHOOLPRICING.ProductCategoryId	 as  C12_PRODUCTCATEGORYID,
	SCHOOLPRICING.AdvertisementSizeId	 as  C13_ADVERTISEMENTSIZEID,
	SCHOOLPRICING.AdvertisementInitialQuantity	 as  C14_ADVERTISEMENTINITIALQUANTI,
	SCHOOLPRICING.IsActiveYbPay	 as  C15_ISACTIVEYBPAY,
	SCHOOLPRICING.QuantityOrdered	 as  C16_QUANTITYORDERED,
	SCHOOLPRICING.IncludedLine	 as  C17_INCLUDEDLINE,
	SCHOOLPRICING.IncludedIcon	 as  C18_INCLUDEDICON,
	SCHOOLPRICING.PreConfiguredPackageId	 as  C19_PRECONFIGUREDPACKAGEID
from	Nextools2009Prod.dbo.SchoolPricing as SCHOOLPRICING
where	(1=1)






&

/* TARGET CODE */
insert into ODS_ETL_OWNER.C$_0NXTL_SCHOOLPRICING
(
	C1_SCHOOLPRICINGID,
	C2_JOBID,
	C3_BOOKOPTIONCODE,
	C4_PRICE,
	C5_PRICEPERLINE,
	C6_PRICEPERICON,
	C7_QUANTITY,
	C8_DISCOUNT,
	C9_DROPCONFIRMED,
	C10_DESCRIPTION,
	C11_PRICEPERLINE2,
	C12_PRODUCTCATEGORYID,
	C13_ADVERTISEMENTSIZEID,
	C14_ADVERTISEMENTINITIALQUANTI,
	C15_ISACTIVEYBPAY,
	C16_QUANTITYORDERED,
	C17_INCLUDEDLINE,
	C18_INCLUDEDICON,
	C19_PRECONFIGUREDPACKAGEID
)
values
(
	:C1_SCHOOLPRICINGID,
	:C2_JOBID,
	:C3_BOOKOPTIONCODE,
	:C4_PRICE,
	:C5_PRICEPERLINE,
	:C6_PRICEPERICON,
	:C7_QUANTITY,
	:C8_DISCOUNT,
	:C9_DROPCONFIRMED,
	:C10_DESCRIPTION,
	:C11_PRICEPERLINE2,
	:C12_PRODUCTCATEGORYID,
	:C13_ADVERTISEMENTSIZEID,
	:C14_ADVERTISEMENTINITIALQUANTI,
	:C15_ISACTIVEYBPAY,
	:C16_QUANTITYORDERED,
	:C17_INCLUDEDLINE,
	:C18_INCLUDEDICON,
	:C19_PRECONFIGUREDPACKAGEID
)

&


/*-----------------------------------------------*/
/* TASK No. 258 */
/* Analyze work table */



BEGIN
DBMS_STATS.GATHER_TABLE_STATS (
    ownname =>	'ODS_ETL_OWNER',
    tabname =>	'C$_0NXTL_SCHOOLPRICING',
    estimate_percent =>	DBMS_STATS.AUTO_SAMPLE_SIZE
);
END;




&


/*-----------------------------------------------*/
/* TASK No. 260 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING 

&


/*-----------------------------------------------*/
/* TASK No. 261 */
/* Create flow table I$ */

create table ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING
(
	SCHOOLPRICINGID		NUMBER NULL,
	JOBID		NUMBER NULL,
	BOOKOPTIONCODE		NUMBER NULL,
	PRICE		NUMBER NULL,
	PRICEPERLINE		NUMBER NULL,
	PRICEPERICON		NUMBER NULL,
	QUANTITY		NUMBER NULL,
	DISCOUNT		NUMBER NULL,
	DROPCONFIRMED		NUMBER NULL,
	DESCRIPTION		VARCHAR2(200) NULL,
	PRICEPERLINE2		NUMBER NULL,
	PRODUCTCATEGORYID		NUMBER NULL,
	ADVERTISEMENTSIZEID		NUMBER NULL,
	ADVERTISEMENTINITIALQUANTITY		NUMBER NULL,
	ISACTIVEYBPAY		NUMBER NULL,
	QUANTITYORDERED		NUMBER NULL,
	INCLUDEDLINE		NUMBER NULL,
	INCLUDEDICON		NUMBER NULL,
	PRECONFIGUREDPACKAGEID		NUMBER NULL,
	EFFECTIVE_DATE		DATE NULL,
	ACTIVE_IND		VARCHAR2(5) NULL,
	LOAD_ID		NUMBER NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 262 */
/* Insert flow into I$ table */

/* DETECTION_STRATEGY = NOT_EXISTS */
 


  


insert into	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING
(
	SCHOOLPRICINGID,
	JOBID,
	BOOKOPTIONCODE,
	PRICE,
	PRICEPERLINE,
	PRICEPERICON,
	QUANTITY,
	DISCOUNT,
	DROPCONFIRMED,
	DESCRIPTION,
	PRICEPERLINE2,
	PRODUCTCATEGORYID,
	ADVERTISEMENTSIZEID,
	ADVERTISEMENTINITIALQUANTITY,
	ISACTIVEYBPAY,
	QUANTITYORDERED,
	INCLUDEDLINE,
	INCLUDEDICON,
	PRECONFIGUREDPACKAGEID,
	IND_UPDATE
)
select 
SCHOOLPRICINGID,
	JOBID,
	BOOKOPTIONCODE,
	PRICE,
	PRICEPERLINE,
	PRICEPERICON,
	QUANTITY,
	DISCOUNT,
	DROPCONFIRMED,
	DESCRIPTION,
	PRICEPERLINE2,
	PRODUCTCATEGORYID,
	ADVERTISEMENTSIZEID,
	ADVERTISEMENTINITIALQUANTITY,
	ISACTIVEYBPAY,
	QUANTITYORDERED,
	INCLUDEDLINE,
	INCLUDEDICON,
	PRECONFIGUREDPACKAGEID,
	IND_UPDATE
 from (


select 	 
	
	C1_SCHOOLPRICINGID SCHOOLPRICINGID,
	C2_JOBID JOBID,
	C3_BOOKOPTIONCODE BOOKOPTIONCODE,
	C4_PRICE PRICE,
	C5_PRICEPERLINE PRICEPERLINE,
	C6_PRICEPERICON PRICEPERICON,
	C7_QUANTITY QUANTITY,
	C8_DISCOUNT DISCOUNT,
	C9_DROPCONFIRMED DROPCONFIRMED,
	C10_DESCRIPTION DESCRIPTION,
	C11_PRICEPERLINE2 PRICEPERLINE2,
	C12_PRODUCTCATEGORYID PRODUCTCATEGORYID,
	C13_ADVERTISEMENTSIZEID ADVERTISEMENTSIZEID,
	C14_ADVERTISEMENTINITIALQUANTI ADVERTISEMENTINITIALQUANTITY,
	C15_ISACTIVEYBPAY ISACTIVEYBPAY,
	C16_QUANTITYORDERED QUANTITYORDERED,
	C17_INCLUDEDLINE INCLUDEDLINE,
	C18_INCLUDEDICON INCLUDEDICON,
	C19_PRECONFIGUREDPACKAGEID PRECONFIGUREDPACKAGEID,

	'I' IND_UPDATE

from	ODS_ETL_OWNER.C$_0NXTL_SCHOOLPRICING
where	(1=1)






) S
where NOT EXISTS 
	( select 1 from ODS.NXTL_SCHOOLPRICING T
	where	T.SCHOOLPRICINGID	= S.SCHOOLPRICINGID 
		 and ((T.JOBID = S.JOBID) or (T.JOBID IS NULL and S.JOBID IS NULL)) and
		((T.BOOKOPTIONCODE = S.BOOKOPTIONCODE) or (T.BOOKOPTIONCODE IS NULL and S.BOOKOPTIONCODE IS NULL)) and
		((T.PRICE = S.PRICE) or (T.PRICE IS NULL and S.PRICE IS NULL)) and
		((T.PRICEPERLINE = S.PRICEPERLINE) or (T.PRICEPERLINE IS NULL and S.PRICEPERLINE IS NULL)) and
		((T.PRICEPERICON = S.PRICEPERICON) or (T.PRICEPERICON IS NULL and S.PRICEPERICON IS NULL)) and
		((T.QUANTITY = S.QUANTITY) or (T.QUANTITY IS NULL and S.QUANTITY IS NULL)) and
		((T.DISCOUNT = S.DISCOUNT) or (T.DISCOUNT IS NULL and S.DISCOUNT IS NULL)) and
		((T.DROPCONFIRMED = S.DROPCONFIRMED) or (T.DROPCONFIRMED IS NULL and S.DROPCONFIRMED IS NULL)) and
		((T.DESCRIPTION = S.DESCRIPTION) or (T.DESCRIPTION IS NULL and S.DESCRIPTION IS NULL)) and
		((T.PRICEPERLINE2 = S.PRICEPERLINE2) or (T.PRICEPERLINE2 IS NULL and S.PRICEPERLINE2 IS NULL)) and
		((T.PRODUCTCATEGORYID = S.PRODUCTCATEGORYID) or (T.PRODUCTCATEGORYID IS NULL and S.PRODUCTCATEGORYID IS NULL)) and
		((T.ADVERTISEMENTSIZEID = S.ADVERTISEMENTSIZEID) or (T.ADVERTISEMENTSIZEID IS NULL and S.ADVERTISEMENTSIZEID IS NULL)) and
		((T.ADVERTISEMENTINITIALQUANTITY = S.ADVERTISEMENTINITIALQUANTITY) or (T.ADVERTISEMENTINITIALQUANTITY IS NULL and S.ADVERTISEMENTINITIALQUANTITY IS NULL)) and
		((T.ISACTIVEYBPAY = S.ISACTIVEYBPAY) or (T.ISACTIVEYBPAY IS NULL and S.ISACTIVEYBPAY IS NULL)) and
		((T.QUANTITYORDERED = S.QUANTITYORDERED) or (T.QUANTITYORDERED IS NULL and S.QUANTITYORDERED IS NULL)) and
		((T.INCLUDEDLINE = S.INCLUDEDLINE) or (T.INCLUDEDLINE IS NULL and S.INCLUDEDLINE IS NULL)) and
		((T.INCLUDEDICON = S.INCLUDEDICON) or (T.INCLUDEDICON IS NULL and S.INCLUDEDICON IS NULL)) and
		((T.PRECONFIGUREDPACKAGEID = S.PRECONFIGUREDPACKAGEID) or (T.PRECONFIGUREDPACKAGEID IS NULL and S.PRECONFIGUREDPACKAGEID IS NULL))
        )

  
  

  

 


&


/*-----------------------------------------------*/
/* TASK No. 263 */
/* Create Index on flow table */

create index	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING_IDX
on		ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING (SCHOOLPRICINGID)
NOLOGGING

&


/*-----------------------------------------------*/
/* TASK No. 264 */
/* Analyze integration table */



begin
    dbms_stats.gather_table_stats(
	ownname => 'ODS_ETL_OWNER',
	tabname => 'I$_NXTL_SCHOOLPRICING',
	estimate_percent => dbms_stats.auto_sample_size
    );
end;



&


/*-----------------------------------------------*/
/* TASK No. 265 */
/* Flag rows for update */

/* DETECTION_STRATEGY = NOT_EXISTS */


update	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING
set	IND_UPDATE = 'U'
where	(SCHOOLPRICINGID)
	in	(
		select	SCHOOLPRICINGID
		from	ODS.NXTL_SCHOOLPRICING
		)



&


/*-----------------------------------------------*/
/* TASK No. 266 */
/* Flag useless rows */

/* DETECTION_STRATEGY = NOT_EXISTS */

/* Command skipped due to chosen DETECTION_STRATEGY */



/*-----------------------------------------------*/
/* TASK No. 267 */
/* Update existing rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
update	ODS.NXTL_SCHOOLPRICING T
set 	
	(
	T.JOBID,
	T.BOOKOPTIONCODE,
	T.PRICE,
	T.PRICEPERLINE,
	T.PRICEPERICON,
	T.QUANTITY,
	T.DISCOUNT,
	T.DROPCONFIRMED,
	T.DESCRIPTION,
	T.PRICEPERLINE2,
	T.PRODUCTCATEGORYID,
	T.ADVERTISEMENTSIZEID,
	T.ADVERTISEMENTINITIALQUANTITY,
	T.ISACTIVEYBPAY,
	T.QUANTITYORDERED,
	T.INCLUDEDLINE,
	T.INCLUDEDICON,
	T.PRECONFIGUREDPACKAGEID
	) =
		(
		select	S.JOBID,
			S.BOOKOPTIONCODE,
			S.PRICE,
			S.PRICEPERLINE,
			S.PRICEPERICON,
			S.QUANTITY,
			S.DISCOUNT,
			S.DROPCONFIRMED,
			S.DESCRIPTION,
			S.PRICEPERLINE2,
			S.PRODUCTCATEGORYID,
			S.ADVERTISEMENTSIZEID,
			S.ADVERTISEMENTINITIALQUANTITY,
			S.ISACTIVEYBPAY,
			S.QUANTITYORDERED,
			S.INCLUDEDLINE,
			S.INCLUDEDICON,
			S.PRECONFIGUREDPACKAGEID
		from	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING S
		where	T.SCHOOLPRICINGID	=S.SCHOOLPRICINGID
	    	 )
	,                  T.EFFECTIVE_DATE = SYSDATE,
	T.ACTIVE_IND = 'A',
	T.LOAD_ID = 1

where	(SCHOOLPRICINGID)
	in	(
		select	SCHOOLPRICINGID
		from	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING
		where	IND_UPDATE = 'U'
		)




&


/*-----------------------------------------------*/
/* TASK No. 268 */
/* Insert new rows */

/* DETECTION_STRATEGY = NOT_EXISTS */
insert into 	ODS.NXTL_SCHOOLPRICING T
	(
	SCHOOLPRICINGID,
	JOBID,
	BOOKOPTIONCODE,
	PRICE,
	PRICEPERLINE,
	PRICEPERICON,
	QUANTITY,
	DISCOUNT,
	DROPCONFIRMED,
	DESCRIPTION,
	PRICEPERLINE2,
	PRODUCTCATEGORYID,
	ADVERTISEMENTSIZEID,
	ADVERTISEMENTINITIALQUANTITY,
	ISACTIVEYBPAY,
	QUANTITYORDERED,
	INCLUDEDLINE,
	INCLUDEDICON,
	PRECONFIGUREDPACKAGEID
	,                   EFFECTIVE_DATE,
	ACTIVE_IND,
	LOAD_ID
	)
select 	SCHOOLPRICINGID,
	JOBID,
	BOOKOPTIONCODE,
	PRICE,
	PRICEPERLINE,
	PRICEPERICON,
	QUANTITY,
	DISCOUNT,
	DROPCONFIRMED,
	DESCRIPTION,
	PRICEPERLINE2,
	PRODUCTCATEGORYID,
	ADVERTISEMENTSIZEID,
	ADVERTISEMENTINITIALQUANTITY,
	ISACTIVEYBPAY,
	QUANTITYORDERED,
	INCLUDEDLINE,
	INCLUDEDICON,
	PRECONFIGUREDPACKAGEID
	,                   SYSDATE,
	'A',
	1
from	ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING S



where	IND_UPDATE = 'I'



&


/*-----------------------------------------------*/
/* TASK No. 269 */
/* Commit transaction */

/*commit*/


/*-----------------------------------------------*/
/* TASK No. 270 */
/* Drop flow table */

drop table ODS_ETL_OWNER.I$_NXTL_SCHOOLPRICING 

&


/*-----------------------------------------------*/
/* TASK No. 1000259 */
/* Drop work table */

drop table ODS_ETL_OWNER.C$_0NXTL_SCHOOLPRICING 

&


/*-----------------------------------------------*/
/* TASK No. 271 */
/* Truncate table nxtl_schoolpricing_stg */

truncate table ODS_ETL_OWNER.NXTL_SCHOOLPRICING_STG


&


/*-----------------------------------------------*/
/* TASK No. 272 */
/* Load table nxtl_schoolpricing_stg */

/* SOURCE CODE */
with data as
(
select schoolpricingid
from Nextools2009Prod.dbo.schoolpricing
)
select schoolpricingid
from data


&

/* TARGET CODE */
insert into  ODS_ETL_OWNER.NXTL_SCHOOLPRICING_STG
(schoolpricingid)
select
:schoolpricingid
from dual

&


/*-----------------------------------------------*/
/* TASK No. 273 */
/* Update active_ind on nxtl_schoolpricing */

update ods.nxtl_schoolpricing sp
set sp.active_ind = 'I'
where not exists
(select  * from ods_etl_owner.nxtl_schoolpricing_stg sps
where (sp.schoolpricingid = sps.schoolpricingid))



&


/*-----------------------------------------------*/
/* TASK No. 274 */
/* Update APO Dates */

MERGE INTO ods_own.apo t
   USING (SELECT a.apo_oid, a.lpip_job_number, n.ybpayactivationdate,
                 n.ybpayclosedate
            FROM ods_own.apo a, ods.nxtl_job n
           WHERE 1 = 1
             AND a.lpip_job_number = n.jobcode
             AND n.effective_date >= TO_DATE(SUBSTR('#WAREHOUSE_PROJECT.v_cdc_load_date',1,19),'YYYY-MM-DD HH24:MI:SS') - #WAREHOUSE_PROJECT.v_cdc_sales_ods_overlap) s
   ON (t.apo_oid = s.apo_oid)
   WHEN MATCHED THEN
      UPDATE
         SET t.ybpay_activation_date = s.ybpayactivationdate,
             t.ybpay_close_date = s.ybpayclosedate,
             ods_modify_date = SYSDATE
         WHERE (   DECODE (t.ybpay_activation_date, s.ybpayactivationdate, 1,0) = 0
                OR     DECODE (t.ybpay_close_date, s.ybpayclosedate, 1,0) = 0
                      )

&


/*-----------------------------------------------*/
/* TASK No. 275 */
/* Delete from ods.nxtl_vw_student_book_option */

delete from ods.nxtl_vw_student_book_option

&


/*-----------------------------------------------*/
/* TASK No. 276 */
/* Load vw_student_book_option */

/* SOURCE CODE */
SELECT
	PersonId,
	OriginReferenceNumber,
	TotalDiscount,
	BookOption0Ordered,
	BookOption1Ordered,
	BookOption2Ordered,
	BookOption3Ordered,
	BookOption4Ordered,
	BookOption5Ordered,
	BookOption6Ordered,
	BookOption7Ordered,
	BookOption8Ordered,
	BookOption9Ordered,
	BookOption1001Ordered,
	BookOption1002Ordered,
	BookOption1003Ordered,
	BookOption1004Ordered,
	BookOption1005Ordered,
	PaymentRec,
	TotalBookCost,
	BalanceDue,
	OrderReceived,
	PortraitGroupTypeId,
	FirstName,
	LastName,
	PersonType,
	Courtesy,
	sbo.JobId,
	StdBookCount,
	ImprintLnOne,
	ImprintLnTwo,
	ModifiedBy,
	ModifiedDate,
	UploadCode,
                     sbo.CreatedDate,
	IsTeacher,
	PaymentDate,
	Notes,
	PackageSchoolPricingId,
	GradeId,
	JobPricingOverrideId,
	DonationAmount,
	OrderDate,
	sbo.LastModifiedDate,
	IncludedLine,
	IncludedIcon,
	numberIcons,
	GradeSortValueId,
	Grade,
	GradeUsePortraitImage,
	HomeroomSortValueId,
	Homeroom,
	HomeroomUsePortraitImage,
	TeacherIdSortValueId,
	TeacherId,
	TeacherIdUsePortraitImage,
	YBPayGrade,
	BookOption10Ordered,
	BookOption11Ordered,
	BookOption12Ordered,
	BookOption13Ordered,
	SortTypeId,
	PackageDescription,
	AdvertisementSizeId1Ordered,
	AdvertisementSizeId2Ordered,
	AdvertisementSizeId3Ordered,
	AdvertisementSizeId4Ordered,
	AdvertisementSizeId5Ordered,
	AdvertisementSizeId6Ordered,
	AdvertisementSizeId7Ordered,
	AdvertisementSizeId8Ordered,
	OrderOrigin
FROM
	Nextools2009Prod.dbo.VW_StudentBookOption sbo,
	Nextools2009Prod.dbo.Job j,
	Nextools2009Prod.dbo.Book b 
	where sbo.JobId = j.JobId 
	and j.BookId = b.BookId 
	and b.YearCode >= 2019

&

/* TARGET CODE */
insert into ods.nxtl_vw_student_book_option (
	PersonId,
	OriginReferenceNumber,
	TotalDiscount,
	BookOption0Ordered,
	BookOption1Ordered,
	BookOption2Ordered,
	BookOption3Ordered,
	BookOption4Ordered,
	BookOption5Ordered,
	BookOption6Ordered,
	BookOption7Ordered,
	BookOption8Ordered,
	BookOption9Ordered,
	BookOption1001Ordered,
	BookOption1002Ordered,
	BookOption1003Ordered,
	BookOption1004Ordered,
	BookOption1005Ordered,
	PaymentRec,
	TotalBookCost,
	BalanceDue,
	OrderReceived,
	PortraitGroupTypeId,
	FirstName,
	LastName,
	PersonType,
	Courtesy,
	JobId,
	StdBookCount,
	ImprintLnOne,
	ImprintLnTwo,
	ModifiedBy,
	ModifiedDate,
	UploadCode,
	CreatedDate,
	IsTeacher,
	PaymentDate,
	Notes,
	PackageSchoolPricingId,
	GradeId,
	JobPricingOverrideId,
	DonationAmount,
	OrderDate,
	LastModifiedDate,
	IncludedLine,
	IncludedIcon,
	numberIcons,
	GradeSortValueId,
	Grade,
	GradeUsePortraitImage,
	HomeroomSortValueId,
	Homeroom,
	HomeroomUsePortraitImage,
	TeacherIdSortValueId,
	TeacherId,
	TeacherIdUsePortraitImage,
	YBPayGrade,
	BookOption10Ordered,
	BookOption11Ordered,
	BookOption12Ordered,
	BookOption13Ordered,
	SortTypeId,
	PackageDescription,
	AdvertisementSizeId1Ordered,
	AdvertisementSizeId2Ordered,
	AdvertisementSizeId3Ordered,
	AdvertisementSizeId4Ordered,
	AdvertisementSizeId5Ordered,
	AdvertisementSizeId6Ordered,
	AdvertisementSizeId7Ordered,
	AdvertisementSizeId8Ordered,
	OrderOrigin)
VALUES (
	:PersonId,
	:OriginReferenceNumber,
	:TotalDiscount,
	:BookOption0Ordered,
	:BookOption1Ordered,
	:BookOption2Ordered,
	:BookOption3Ordered,
	:BookOption4Ordered,
	:BookOption5Ordered,
	:BookOption6Ordered,
	:BookOption7Ordered,
	:BookOption8Ordered,
	:BookOption9Ordered,
	:BookOption1001Ordered,
	:BookOption1002Ordered,
	:BookOption1003Ordered,
	:BookOption1004Ordered,
	:BookOption1005Ordered,
	:PaymentRec,
	:TotalBookCost,
	:BalanceDue,
	:OrderReceived,
	:PortraitGroupTypeId,
	:FirstName,
	:LastName,
	:PersonType,
	:Courtesy,
	:JobId,
	:StdBookCount,
	:ImprintLnOne,
	:ImprintLnTwo,
	:ModifiedBy,
	:ModifiedDate,
	:UploadCode,
	:CreatedDate,
	:IsTeacher,
	:PaymentDate,
	:Notes,
	:PackageSchoolPricingId,
	:GradeId,
	:JobPricingOverrideId,
	:DonationAmount,
	:OrderDate,
	:LastModifiedDate,
	:IncludedLine,
	:IncludedIcon,
	:numberIcons,
	:GradeSortValueId,
	:Grade,
	:GradeUsePortraitImage,
	:HomeroomSortValueId,
	:Homeroom,
	:HomeroomUsePortraitImage,
	:TeacherIdSortValueId,
	:TeacherId,
	:TeacherIdUsePortraitImage,
	:YBPayGrade,
	:BookOption10Ordered,
	:BookOption11Ordered,
	:BookOption12Ordered,
	:BookOption13Ordered,
	:SortTypeId,
	:PackageDescription,
	:AdvertisementSizeId1Ordered,
	:AdvertisementSizeId2Ordered,
	:AdvertisementSizeId3Ordered,
	:AdvertisementSizeId4Ordered,
	:AdvertisementSizeId5Ordered,
	:AdvertisementSizeId6Ordered,
	:AdvertisementSizeId7Ordered,
	:AdvertisementSizeId8Ordered,
	:OrderOrigin)

&


/*-----------------------------------------------*/
/* TASK No. 277 */
/* Update CDC Load Status */

UPDATE ODS.DW_CDC_LOAD_STATUS
SET LAST_CDC_COMPLETION_DATE=TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS') 
+ nvl((TIMEZONE_OFFSET/24), 0) 
WHERE DW_TABLE_NAME=#WAREHOUSE_PROJECT.v_cdc_load_table_name
AND CONTEXT_NAME = :v_env


&


/*-----------------------------------------------*/
/* TASK No. 278 */
/* Insert Audit Record */

INSERT INTO ODS_ETL_OWNER.DW_CDC_LOAD_STATUS_AUDIT
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
#WAREHOUSE_PROJECT.v_cdc_load_table_name,
:v_sess_no,
'LOAD_NEXTOOLS_PKG',
'015',
TO_DATE(
             SUBSTR(:v_sess_beg, 1, 19), 'RRRR-MM-DD HH24:MI:SS'),
TO_DATE (SUBSTR ('#WAREHOUSE_PROJECT.v_cdc_load_date', 1, 19),
                           'YYYY-MM-DD HH24:MI:SS'
                          ),
#WAREHOUSE_PROJECT.v_cdc_ods_overlap,
SYSDATE
,:v_env
,TIMEZONE_OFFSET
from 
ODS.DW_CDC_LOAD_STATUS
WHERE DW_TABLE_NAME=#WAREHOUSE_PROJECT.v_cdc_load_table_name
AND CONTEXT_NAME = :v_env


&


/*-----------------------------------------------*/
