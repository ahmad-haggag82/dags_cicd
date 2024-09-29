select	
	_HISTORY.EnrollmentHistoryID	 as  C1_ENROLLMENTHISTORYID,
	_HISTORY.EnrollmentID	 as  C2_ENROLLMENTID,
	_HISTORY.ProgramDescription	 as  C3_PROGRAMDESCRIPTION,
	_HISTORY.WebSite	 as  C4_WEBSITE,
	_HISTORY.AdviserFirstName	 as  C5_ADVISERFIRSTNAME,
	_HISTORY.AdviserLastName	 as  C6_ADVISERLASTNAME,
	_HISTORY.AdviserEmail	 as  C7_ADVISEREMAIL,
	_HISTORY.AdviserUsername	 as  C8_ADVISERUSERNAME,
	_HISTORY.AdviserPassword	 as  C9_ADVISERPASSWORD,
	_HISTORY.SalesRepName	 as  C10_SALESREPNAME,
	_HISTORY.SalesRepEmail	 as  C11_SALESREPEMAIL,
	_HISTORY.SalesRepUsername	 as  C12_SALESREPUSERNAME,
	_HISTORY.SalesRepPassword	 as  C13_SALESREPPASSWORD,
	_HISTORY.SalesRepTerritoryCode	 as  C14_SALESREPTERRITORYCODE,
	_HISTORY.EnrollmentStatusID	 as  C15_ENROLLMENTSTATUSID,
	_HISTORY.CompletedBy	 as  C16_COMPLETEDBY,
	_HISTORY.CompletedDate	 as  C17_COMPLETEDDATE,
	_HISTORY.CompletedTime	 as  C18_COMPLETEDTIME
from	dbo.tblEnrollment09_History as _HISTORY
where	(1=1)