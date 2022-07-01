/*******************************************************************************
*  Name: 	potential_code_errors
*  Date: 	2022/05/29
*  Version: v10
*
*  Purpose: This program was developed by the Coordinating Center to help partners identify ETL errors
	    in populating CVX immunization codes; ICD9 and ICD10 diagnosis codes; ICD & CPT/HCPCS procedure codes; RXCUI codes; LOINC codes; 
	    and NDC codes. The program (1) identifes codes that violate one or more heuristics described in the 
	    Work Plan and (2) identified LOINC codes that are in the incorrect CDM table.    

*  Outputs: 
*    1) Up to 12 SAS datasets stored in /dmlocal containing all records which violate one or more of the heuristics
*    	for the given code type, and a summary dataset of the results:
*			bad_dx.sas7bdat
*	    	bad_px.sas7bdat
* 			bad_condition.sas7bdat
*			bad_immunization.sas7bdat
*	    	bad_pres.sas7bdat
*	    	bad_disp.sas7bdat
*	    	bad_lab.sas7bat
*			bad_lab_hist.sas7bdat
*			bad_medadmin.sas7bdat
*			bad_obsgen.sas7bdat
*			bad_obsclin.sas7bdat
*			misplaced_loincs.sas7bdat
*       	code_summary.sas7bdat (# and % of bad records per table)
*	 2) CSV versions of the SAS datasets stored in /dmlocal
*	 3) Potential_Code_Errors.pdf stored in /drnoc (contains a partial print of key fields from each of the above datasets)
*	 4) [DataMartID]_[Date]_code_summary.cpt stored in /drnoc
*	 5) SAS log file of potential code errors stored in /drnoc (<DataMart Id>_<response date>_potential_code_errors.log)

*  Requirements: Program run in SAS 9.3 or higher

********************************************************************************/

options validvarname=upcase errors=0 ls=max;

********************************************************************************;
* Flush anything/everything that might be in WORK
*******************************************************************************;
proc datasets  kill noprint;
quit;


********************************************************************************;
*- Set LIBNAMES for data and output
********************************************************************************;
libname dpath "&dpath" access=readonly; 
libname qpath "&qpath";    
libname drnoc "&qpath./drnoc";  
libname dmlocal "&qpath./dmlocal";


********************************************************************************;
* Create macro variables from DataMart ID and program run date 
********************************************************************************;
data _null_;
     set dpath.harvest;
     call symput("dmid",strip(datamartid));
     call symput("tday",left(put("&sysdate"d,yymmddn8.)));
run;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._potential_code_errors.log" lrecl=200;
proc printto log=qlog  new ;
run ;

*******************************************************************************;
* Import the LOINC reference table
*******************************************************************************;
proc cimport library=work infile="&qpath./infolder/loinc.cpt";
run;

********************************************************************************;
* Determine which datasets are available
********************************************************************************;
proc contents data=dpath._all_ noprint out=dpath;
run;

proc sort data=dpath(keep=memname) nodupkey;
     by memname;
run;

proc sql noprint;
     select count(*) into :_ydispensing from dpath
            where memname="DISPENSING";
     select count(*) into :_ylab_result_cm from dpath
            where memname="LAB_RESULT_CM";
     select count(*) into :_yprescribing from dpath
            where memname="PRESCRIBING";
     select count(*) into :_yobs_clin from dpath
            where memname="OBS_CLIN";
     select count(*) into :_yobs_gen from dpath
            where memname="OBS_GEN";
     select count(*) into :_ymed_admin from dpath
            where memname="MED_ADMIN";
     select count(*) into :_ycondition from dpath
            where memname="CONDITION";
     select count(*) into :_yimmunization from dpath
            where memname="IMMUNIZATION";
     select count(*) into :_ylab_history from dpath
            where memname="LAB_HISTORY";
quit;


********************************************************************************;
* Create macros to use throughout
********************************************************************************;
*TBLSUM: summarize records and codes by code type;
%macro tblsum (tbl=);
proc sql;
	create table &tbl._sum
	as select table,
	code_type,
	count(*) as records format comma16.,
	count(distinct code) as codes format comma16.
	from &tbl.
	group by 1,2;
	quit;
%mend tblsum;


*BADSUM: summarize bad records and codes by code type and code and calculate % of total;
%macro badsum (ds=,tbl=);

proc sql;
	create table &ds._sum1 as
	select table,
	code_type,
	count(code) as bad_records format comma16.,
	count(distinct code) as bad_codes format comma16.
	from dmlocal.&ds.
	group by 1,2;
	quit;
	
proc sql;
	create table &ds._sum2 as
	select distinct table,
	code_type,
	code,
	unexp_length,
	code_length,
	unexp_string,
	unexp_alpha,
	unexp_numeric,
	count(*) as records format comma16.
	from dmlocal.&ds.
	group by 1,2,3,4,5,6,7,8
	order by 2,9 desc;
	quit;
		
proc sql;
	create table &tbl._summary
	as select a.table,
	a.code_type,
	coalesce(b.bad_records,0) as bad_records,
	coalesce(b.bad_codes,0) as bad_codes,
	a.records,
	a.codes
	from &tbl._sum a
	left join &ds._sum1 b
	on a.code_type=b.code_type
	order by 2;
	quit;
	
data &tbl._summary;
	set &tbl._summary;
	if bad_records>0 then bad_record_pct=bad_records/records*100;
	format records comma18. bad_records comma18. bad_record_pct 8.2;
	run;
	

%mend badsum;


*MISP_SUM: summarize misplaced records and codes by code type and add to tbl summary. 
Get proc contents of the table and determine the length of the code variable;
%macro misp_sum (tbl=);
proc sql;
	create table &tbl._misp_sum1
	as select table,
	code_type,
	count(code) as misplaced_records format comma16.,
	count(distinct code) as misplaced_codes format comma16.
	from misplaced_&tbl.
	group by 1,2
	order by 1,2;
	quit;

proc sql;
	create table &tbl._misp_sum2
	as select table,
	code_type,
	code,
	code_desc,
	classtype,
	count(code) as records format comma16.,
	count(distinct code) as codes format comma16.
	from misplaced_&tbl.
	group by 1,2,3,4,5
	order by 6 desc;
	quit;


data &tbl._summary;
	merge &tbl._summary &tbl._misp_sum1;
	by table code_type;
	if misplaced_records>0 then misplaced_record_pct=misplaced_records/records*100;
	format misplaced_records comma18. misplaced_record_pct 8.2;
	run;
	
proc contents data=&tbl. out=contents;
	run;
	
%global _len_&tbl.;
proc sql noprint; 
             select length into: _len_&tbl.
             from contents
             where upcase(name)='CODE';
             quit;
			
%mend misp_sum;


********************************************************************************;
* Begin codes macro
********************************************************************************;
%macro codes;

*********************************************************************************************;
* DIAGNOSIS table (ICD9 and ICD10 Diagnosis Codes)
*********************************************************************************************;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards decimals, spaces and trailing blanks, and add indicators of the first position
which has an alpha character (anyalpha) or a numeric character (anydigit);
data diagnosis (compress=yes);
	format table diagnosisid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.diagnosis (keep=diagnosisid dx dx_type);
	where not missing(dx) and dx_type in ('09','10');
	table='DIAGNOSIS';
	code_type=dx_type;
	code=dx;
	code_clean=strip(upcase(compress((code),". ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
%tblsum(tbl=diagnosis);

	
*apply heuristics to identify bad records;
data dmlocal.bad_dx (drop=dx dx_type);
	set diagnosis;
	*ICD09:length is not between 3-5 OR has alpha characters other than E or V OR has no numeric
	characters OR first 3 digits (min length) are 0;
	if code_type='09' then do;
		if anyalpha(code_clean)>0 and substr(code_clean,1,1) not in ('E','V') then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	*ICD10: length is not between 3 and 7 OR 1st character is not alpha OR first 3 digits (min length) are 0 or 9 OR 
	has no numeric characters;
	if code_type='10' then do;
		if anydigit(code_clean)=1 then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5,6,7) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000','999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_dx,tbl=diagnosis);


*********************************************************************************************;
* CONDITION table (ICD9 and ICD10 Diagnosis Codes):  added in v7
*********************************************************************************************;
%if &_ycondition=1 %then %do;


*bring in the needed information and compress.  See DIAGNOSIS;
data condition (compress=yes);
	format table conditionid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.condition (keep=conditionid condition condition_type);
	where not missing(condition) and condition_type in ('09','10');
	table='CONDITION';
	code_type=condition_type;
	code=condition;
	code_clean=strip(upcase(compress((code),". ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
%tblsum(tbl=condition);

	
*apply heuristics to identify bad records (see DIAGNOSIS);
data dmlocal.bad_condition (drop=condition condition_type);
	set condition;
	if code_type='09' then do; 
		if anyalpha(code_clean)>0 and substr(code_clean,1,1) not in ('E','V') then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if code_type='10' then do; 
		if anydigit(code_clean)=1 then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5,6,7) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000','999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_condition,tbl=condition);

%end;

*********************************************************************************************;
* PROCEDURES table (ICD and CPT/HCPCS and NDC codes). NDC codes added in v7. 
*********************************************************************************************;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces and trailing blanks as well as decimals for ICD codes and dashes and commas for CH codes (which may have modifiers).
Add indicators of the first position which has an alpha character (anyalpha) or a numeric character (anydigit);

data procedures(compress=yes);
	format table proceduresid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.procedures (keep=proceduresid px px_type);
	where not missing(px) and px_type in ('CH','09','10','ND');
	table='PROCEDURES';
	code_type=px_type;
	code=px;
	if code_type in ('09','10') then code_clean=strip(upcase(compress((code),". ")));
		else if code_type='CH' then code_clean=strip(upcase(compress((code),"-, ")));
		else if code_type='ND' then code_clean=strip(upcase(compress((code)," ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;

*summarize records by type;
%tblsum(tbl=procedures);

*apply heuristics to identify bad records;	
data dmlocal.bad_px (drop=px px_type);
	set procedures;
	*CPT/HCPCS are length <5 OR first 5 are all 0 or 9 OR no numeric;
	if code_type='CH' then do;
		unexp_alpha=.;
		if code_length<5 then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,5) in ('00000','99999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	*ICD9 is not length 3 or 4 OR any alpha OR all 0;
	if code_type='09' then do;
		unexp_alpha=.;
		if code_length not in (3,4) then unexp_length=1; else unexp_length=0;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0; 
		if code_clean in ('0000') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	*ICD10 is not length 7 OR no numeric OR all 7 are 0s or 9s;
	if code_type='10' then do;
		unexp_alpha=.;
		if code_length ne 7 then unexp_length=1; else unexp_length=0; 
		if code_clean in ('0000000','9999999') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	*NDC (added in v7, see DISPENSING);
	if code_type='ND' then do;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
		if code_length ne 11 then unexp_length=1; else unexp_length=0;
		unexp_numeric=.;
		end;				
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_px,tbl=procedures);




*********************************************************************************************;
* IMMUNIZATON table (CH, NDC, RXCUI )...added in v7
*********************************************************************************************;
%if &_yimmunization=1 %then %do;

*bring in the needed information as done in other tables;	
data immunization (compress=yes);
	length code_type $4 table $15;
	format table immunizationid code_type code code_clean code_length anyalpha anydigit;
	set dpath.immunization (keep=immunizationid vx_code vx_code_type);
	where not missing(vx_code) and vx_code_type in ('CX','ND','RX','CH');
	table='IMMUNIZATION';
	code_type=vx_code_type;
	code=vx_code;
	if code_type in ('CH') then code_clean=strip(upcase(compress((code),"-, "))); 
		else if code_type in ('CX','ND','RX') then code_clean=strip(upcase(compress((code)," ")));
	code_clean=upcase(strip(code));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;


*summarize records by type;
%tblsum(tbl=immunization);

	
*apply heuristics to identify bad records;	
data dmlocal.bad_immunization (drop=vx_code vx_code_type);
	set immunization;
	table='IMMUNIZATION';
	if code_type='RX' then do; *see PRESCRIBING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length<2 or code_length>7 then unexp_length=1; else unexp_length=0;
		unexp_string=.;
		unexp_numeric=.;
		end;		
	if code_type='ND' then do; *see DISPENSING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length ne 11 then unexp_length=1; else unexp_length=0;
		if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	if code_type='CH' then do;*see PROCEDURES;
		unexp_alpha=.;
		if code_length<5 then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,5) in ('00000','99999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if code_type='CX' then do; *any alpha or code not 2-3 char;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0; 
		if 2<=code_length<=3 then unexp_length=0; else unexp_length=1; 
		unexp_string=.;
		unexp_numeric=.;
		end;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_immunization,tbl=immunization);

%end;
	

	
*********************************************************************************************;
* PRESCRIBING table (RXCUI)
*********************************************************************************************;
%if &_yprescribing=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces trailing blanks, and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data prescribing (compress=yes);
	format table prescribingid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.prescribing (keep=prescribingid rxnorm_cui);
	where not missing(rxnorm_cui);
	table='PRESCRIBING';
	code_type='RX';
	code=rxnorm_cui;
	code_clean=strip(upcase(compress((code)," ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	

*summarize records by type;
%tblsum(tbl=prescribing);
	
	
*apply heuristics to identify bad records;	
data dmlocal.bad_pres (drop=rxnorm_cui);
	set prescribing;
	*flag codes with any alphabetical characters OR a length <2 or >7; 
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<2 or code_length>7 then unexp_length=1; else unexp_length=0;
	unexp_string=.;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_pres,tbl=prescribing);


%end;



*********************************************************************************************;
* DISPENSING table (NDC)
*********************************************************************************************;
%if &_ydispensing=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces trailing blanks, and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data dispensing(compress=yes);
	format table dispensingid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.dispensing (keep=dispensingid ndc);
	where not missing(ndc);
	table='DISPENSING';
	code_type='ND';
	code=ndc;
	code_clean=strip(upcase(compress((code)," ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
%tblsum(tbl=dispensing);
	
*apply heuristics to identify bad records;
data dmlocal.bad_disp (drop=ndc);
	set dispensing;
	*length not 11 OR any alpha OR a string of 0 or 9;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
	if code_length ne 11 then unexp_length=1; else unexp_length=0;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;

*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_disp,tbl=dispensing);

%end;	
	

*********************************************************************************************;
* MED_ADMIN table  (RXCUI and NDC)
*********************************************************************************************;
%if &_ymed_admin=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces and trailing blanks and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);	
data med_admin (compress=yes);
	format table medadminid code_type code code_clean code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.med_admin (keep=medadminid medadmin_code medadmin_type);
	where not missing(medadmin_code) and medadmin_type in ('RX','ND');
	table='MED_ADMIN';
	code_type=medadmin_type;
	code=medadmin_code;
	code_clean=strip(upcase(compress((code)," ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;


*summarize records by type;
%tblsum(tbl=med_admin);

	
*apply heuristics to identify bad records;	
data dmlocal.bad_medadmin (drop=medadmin_code medadmin_type);
	set med_admin;
	if code_type='RX' then do; *see PRESCRIBING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length<2 or code_length>7 then unexp_length=1; else unexp_length=0;
		unexp_string=.;
		unexp_numeric=.;
		end;
	if code_type='ND' then do; *see DISPENSING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
		if code_length ne 11 then unexp_length=1; else unexp_length=0;
		unexp_numeric=.;
		end;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_medadmin,tbl=med_admin);

%end;
	

	
*********************************************************************************************;
* LAB_RESULT_CM table (LOINC)
*********************************************************************************************;
%if &_ylab_result_cm=1 %then %do;


*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces and trailing blanks, a reversed version of the code, and indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data lab_result_cm (compress=yes);
	format table lab_result_cm_id code_type code code_clean code_clean_r code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.lab_result_cm (keep=lab_result_cm_id lab_loinc) end=eof;
	where not missing(lab_loinc);
/* 	*get the max length of the loinc code to use in misplaced_loincs;	 */
/* 	retain max_length 0; */
/*         raw_length=length(lab_loinc); */
/*         if raw_length>max_length then max_length=raw_length; */
/*         if eof then do; */
/*                 Call symputx("_len_cm",max_length); */
/*               end; */
    *create other variables needed for bad records;
	table='LAB_RESULT_CM';
	code_type='LC';
	code=lab_loinc;
	code_clean=strip(upcase(compress((code)," ")));
	code_clean_r=left(reverse(code_clean));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
%tblsum(tbl=lab_result_cm);

*apply heuristics to identify bad records;
data dmlocal.bad_lab (drop=lab_loinc code_clean_r);
	set lab_result_cm;
	*flag for any alphabetical characters OR a length less than 3 or greater than 7 OR the absence of a dash after
	the next to last position;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<3 or code_length>7 then unexp_length=1; else unexp_length=0;
	if substr(code_clean_r,2,1)='-' then unexp_string=0; else unexp_string=1;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;		
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_lab,tbl=lab_result_cm);
	

%end;	

	
*********************************************************************************************;
* LAB_HISTORY table (LOINC), added in v8
*********************************************************************************************;
%if &_ylab_history=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces and trailing blanks, a reversed version of the code, and indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data lab_history (compress=yes);
	format table labhistoryid code_type code code_clean code_clean_r code_length anyalpha anydigit;
	length code_type $4 table $15;
	set dpath.lab_history (keep=labhistoryid lab_loinc) end=eof;
	where not missing(lab_loinc);
	table='LAB_HISTORY';
	code_type='LC';
	code=lab_loinc;
	code_clean=strip(upcase(compress((code)," ")));
	code_clean_r=left(reverse(code_clean));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
	
*summarize records by type;
%tblsum(tbl=lab_history);

*apply heuristics to identify bad records;
data dmlocal.bad_lab_hist (drop=lab_loinc code_clean_r);
	set lab_history;
	*flag for any alphabetical characters OR a length less than 3 or greater than 7 OR the absence of a dash after
	the next to last position;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<3 or code_length>7 then unexp_length=1; else unexp_length=0;
	if substr(code_clean_r,2,1)='-' then unexp_string=0; else unexp_string=1;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;		
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_lab_hist,tbl=lab_history);
	

%end;	


*********************************************************************************************;
* OBS_CLIN table (LOINC)
*********************************************************************************************;
%if &_yobs_clin=1 %then %do;

*bring in the needed information and compress (see LAB_RESULT_CM);	
data obs_clin (compress=yes);
	length code_type $4 table $15;
	format table obsclinid code_type code code_clean code_length anyalpha anydigit;
	set dpath.obs_clin (keep=obsclinid obsclin_code obsclin_type) end=eof;
	where not missing(obsclin_code) and obsclin_type='LC';
	table='OBS_CLIN';
	code_type=obsclin_type;
	code=obsclin_code;
	code_clean=strip(upcase(compress((code)," ")));
	code_clean_r=left(reverse(code_clean));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
%tblsum(tbl=obs_clin);

	
*apply heuristics to identify bad records (see LAB_RESULT_CM);	
data dmlocal.bad_obsclin (drop=code_clean_r obsclin_code obsclin_type);
	set obs_clin;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<3 or code_length>7 then unexp_length=1; else unexp_length=0;
	if substr(code_clean_r,2,1)='-' then unexp_string=0; else unexp_string=1;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;		
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_obsclin,tbl=obs_clin);

%end;
	

*********************************************************************************************;
* OBS_GEN table (LOINC, NDC, RXCUI, ICD DX and PX, CH )
*********************************************************************************************;
%if &_yobs_gen=1 %then %do;

*bring in the needed information as done in other tables;	
data obs_gen (compress=yes);
	length code_type $4 table $15;
	format table obsgenid code_type code code_clean code_length anyalpha anydigit;
	set dpath.obs_gen (keep=obsgenid obsgen_code obsgen_type) end=eof;
	where not missing(obsgen_code) and obsgen_type in ('LC','ND','RX','09DX','09PX','10DX','10PX','CH');
	table='OBS_GEN';
	code_type=obsgen_type;
	code=obsgen_code;
	if code_type in ('09DX','09PX','10DX','10PX') then code_clean=strip(upcase(compress((code),". ")));
		else if code_type in ('CH') then code_clean=strip(upcase(compress((code),"-, ")));
		else if code_type in ('LC','ND','RX') then code_clean=strip(upcase(compress((code)," ")));
	code_clean=strip(upcase(compress((code),". ")));
	code_clean_r=left(reverse(code_clean));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;

*summarize records by type;
%tblsum(tbl=obs_gen);

	
*apply heuristics to identify bad records;	
data dmlocal.bad_obsgen (drop=code_clean_r obsgen_code obsgen_type);
	set obs_gen;
	table='OBS_GEN';
	if code_type='LC' then do; *see LAB_RESULT_CM;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length<3 or code_length>7 then unexp_length=1; else unexp_length=0;
		if substr(code_clean_r,2,1)='-' then unexp_string=0; else unexp_string=1;
		unexp_numeric=.;
		end;
	if code_type='RX' then do; *see PRESCRIBING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length<2 or code_length>7 then unexp_length=1; else unexp_length=0;
		unexp_string=.;
		unexp_numeric=.;
		end;		
	if code_type='ND' then do; *see DISPENSING;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
		if code_length ne 11 then unexp_length=1; else unexp_length=0;
		if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	if code_type='09DX' then do; *see DIAGNOSIS;
		if anyalpha(code_clean)>0 and substr(code_clean,1,1) not in ('E','V') then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if code_type='10DX' then do;*see DIAGNOSIS;
		if anydigit(code_clean)=1 then unexp_alpha=1; else unexp_alpha=0;
		if code_length not in (3,4,5,6,7) then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,3) in ('000','999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if code_type='09PX' then do; *see PROCEDURES;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0; 
		if code_length not in (3,4) then unexp_length=1; else unexp_length=0;
		if code_clean in ('0000') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	if code_type='10PX' then do; *see PROCEDURES;
		unexp_alpha=.;
		if code_length ne 7 then unexp_length=1; else unexp_length=0; 
		if code_clean in ('0000000','9999999') then unexp_string=1; else unexp_string=0;
		unexp_numeric=.;
		end;
	if code_type='CH' then do;*see PROCEDURES;
		unexp_alpha=.;
		if code_length<5 then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,5) in ('00000','99999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;	
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=bad_obsgen,tbl=obs_gen);

%end;
	
*********************************************************************************************;
* MISPLACED CODES
*********************************************************************************************;
%if &_ylab_result_cm=1 %then %do;

proc sql;
	create table misplaced_lab_result_cm
	as select distinct table,
	lab_result_cm_id as recordid,
	code_type,
	classtype,
	"1" as expected_classtype,
	code,
	long_common_name as code_desc,
	code_length
	from lab_result_cm
	inner join loinc
	on code=loinc_num
	where classtype ne "1";
	quit;
	
	
%misp_sum(tbl=lab_result_cm);


%end;



%if &_ylab_history=1 %then %do;

proc sql;
	create table misplaced_lab_history
	as select distinct table,
	labhistoryid as recordid,
	code_type,
	classtype,
	"1" as expected_classtype,
	code,
	long_common_name as code_desc,
	code_length
	from lab_history
	inner join loinc
	on code=loinc_num
	where classtype ne "1";
	quit;

%misp_sum(tbl=lab_history);
	

%end;

%if &_yobs_clin=1 %then %do;

proc sql;
	create table misplaced_obs_clin
	as select distinct table,
	obsclinid as recordid,
	code_type,
	classtype,
	"2" as expected_classtype,
	code,
	long_common_name as code_desc,
	code_length
	from obs_clin
	inner join loinc
	on code=loinc_num
	where classtype ne "2";
	quit;

%misp_sum(tbl=obs_clin);


%end;

%if &_yobs_gen=1 %then %do;

proc sql;
	create table misplaced_obs_gen
	as select distinct table,
	obsgenid as recordid,
	code_type,
	classtype,
	"2,3,or 4" as expected_classtype,
	code,
	long_common_name as code_desc,
	code_length
	from obs_gen
	inner join loinc
	on code=loinc_num
	where classtype="1";
	quit;

%misp_sum(tbl=obs_gen);

%end;


*get the max value of the code length variables;
options symbolgen;
data _null_;
  call symputx("_len_max", (max(&_len_lab_result_cm., &_len_lab_history., &_len_obs_clin., &_len_obs_gen.)));
  run;
  


*ascertain how many of the misplaced datasets exist;

proc contents data=work._all_ noprint out=datasets;
run;

proc sort data=datasets(keep=memname) nodupkey;
     by memname;
run;

proc sql noprint;
     select count(*) into :_mislabcm from datasets
            where memname="MISPLACED_LAB_RESULT_CM";
     select count(*) into :_misobsgen from datasets
            where memname="MISPLACED_OBSGEN";
     select count(*) into :_misobsclin from datasets
            where memname="MISPLACED_OBS_CLIN";
     select count(*) into :_mislabhist from datasets
            where memname="MISPLACED_LAB_HISTORY";
quit;

*stack the misplaced loinc results;
data dmlocal.misplaced_loincs (keep=table recordid code code_desc classtype expected_classtype);
                format table recordid code code_desc classtype expected_classtype;
                length recordid $1000 code $&_len_max.;
                set 
                
                %if &_mislabcm.=1 %then %do; misplaced_lab_result_cm (in=a) %end;
                %if &_misobsgen.=1 %then %do; misplaced_obs_gen (in=b) %end; 
                %if &_misobsclin.=1 %then %do; misplaced_obs_clin (in=c) %end;
                %if &_mislabhist.=1 %then %do; misplaced_lab_history (in=d) %end;
                ;
                run;        

   
********************************************************************************;
* End codes macro
********************************************************************************;
%mend codes;

%codes;




********************************************************************************;
* Create code summary
********************************************************************************;


%macro summary;

data dmlocal.code_summary (drop=i);
	format DATAMARTID QUERY_DATE TABLE CODE_TYPE RECORDS CODES BAD_RECORDS BAD_CODES BAD_RECORD_PCT MISPLACED_RECORDS MISPLACED_CODES MISPLACED_RECORD_PCT;
	format _all_;
	informat _all_;
	set diagnosis_summary 
		procedures_summary
		%if &_ycondition=1 %then %do; condition_summary %end;
		%if &_yimmunization=1 %then %do; immunization_summary %end;
		%if &_yprescribing=1 %then %do; prescribing_summary %end;
		%if &_ydispensing=1 %then %do; dispensing_summary %end;
		%if &_ymed_admin=1 %then %do; med_admin_summary %end;
		%if &_ylab_result_cm=1 %then %do; lab_result_cm_summary %end;
		%if &_yobs_clin=1 %then %do; obs_clin_summary %end;
		%if &_yobs_gen=1 %then %do; obs_gen_summary %end;
		%if &_ylab_history=1 %then %do; lab_history_summary %end;
		;
	label code_type='Code Type' table='Table' records='Total Records' codes='Total Codes' bad_records='Bad Records' 
		misplaced_records='Misplaced Records' bad_record_pct='Bad Record Percent' misplaced_record_pct='Misplaced Record Percent';
	array miss(4) misplaced_records misplaced_codes bad_records bad_codes;
	do i=1 to dim(miss);
	if miss(i)=. then miss(i)=0;
	end;
	
	array miss2(2) bad_record_pct misplaced_record_pct;
	do i=1 to dim(miss2);
	if miss2(i)=. then miss2(i)=0.0;
	end;	

	DATAMARTID="&dmid.";
	QUERY_DATE=left(put("&sysdate"d,yymmddn8.));
	run;
	
	
*save a copy in the drnoc folder;
proc cport library=dmlocal file="&qpath.drnoc/&dmid._&tday._code_summary.cpt" memtype=data;
     select code_summary;
run;

	
%mend summary;
%summary;

*******************************************************************************;
* Create CSV of all of the query datasets
*******************************************************************************;
    *- Place all dataset names into a macro variable -*;
    proc contents data=dmlocal._all_ noprint out=dmlocal;
    run;

    proc sql noprint;
         select unique memname into :workdata separated by '|'  from dmlocal;
         select count(unique memname) into :workdata_count from dmlocal;
    quit;

    *- Export each dataset into a CSV file -*;
    %macro csv;
        %do d = 1 %to &workdata_count;
        %let _dsn=%scan(&workdata,&d,"|");
        
		filename filenm "&qpath.dmlocal/&_dsn..csv";

        proc export data=dmlocal.&_dsn
            outfile=filenm
            dbms=dlm label replace;
            delimiter=',';
        run;
    %end;
    %mend csv;
%csv;




*******************************************************************************;
* Close DRN log
*******************************************************************************;
proc printto log=qlog;
run;

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;

********************************************************************************;
* Print results to the PDF
********************************************************************************;
%macro report;
ods html close;
ods listing;
ods path sashelp.tmplmst(read) library.templat(read);
ods pdf file="&qpath.drnoc/&dmid._&tday._Potential_Code_Errors.pdf" style = journal pdftoc=1;	


title1 'Potential Code Errors';
ods proclabel 'Code Summary';
title2 'Code Summary';
proc print width=min data=dmlocal.code_summary label noobs;
	var table code_type records bad_records bad_record_pct misplaced_records misplaced_record_pct;
	run;

title2 "DIAGNOSIS";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "DIAGNOSIS errors";	
proc print width=min data=bad_dx_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title2 "PROCEDURES";
title3 'Top 50 Highest Volume Codes';	
ods proclabel "PROCEDURES errors";	
proc print width=min data=bad_px_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;

	
%if &_ycondition=1 %then %do;

title2 "CONDITION";
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "CONDITION errors";		
proc print width=min data=bad_condition_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	

%end;	

%if &_yimmunization=1 %then %do;

title2 "IMMUNIZATION";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "IMMUNIZATION errors";	
proc print width=min data=bad_immunization_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
%end;

%if &_yprescribing=1 %then %do;

title2 "PRESCRIBING";
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "PRESCRIBING errors";		
proc print width=min data=bad_pres_sum2(obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	
%end;
	
%if &_ydispensing=1 %then %do;

title2 "DISPENSING";
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "DISPENSING errors";		
proc print width=min data=bad_disp_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	

%end;

%if &_ymed_admin=1 %then %do;

title2 "MED_ADMIN";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "MED_ADMIN errors";
proc print width=min data=bad_medadmin_sum2(obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	
%end;

%if &_ylab_result_cm=1 %then %do;

title2 "LAB_RESULT_CM";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "LAB_RESULT_CM errors";	
proc print width=min data=bad_lab_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title3 'Top 50 Highest Volume Misplaced Codes (expected CLASSTYPE=1)';	
ods proclabel "LAB_RESULT_CM misplaced codes";	
proc print width=min data=lab_result_cm_misp_sum2 (obs=50) label;
	var code code_desc records classtype;
	run;
	
%end;

%if &_ylab_history=1 %then %do;

title2 "LAB_HISTORY";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "LAB_HISTORY errors";	
proc print width=min data=bad_lab_hist_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;

title3 'Top 50 Highest Volume Misplaced LOINC Codes (expected CLASSTYPE=1)';	
ods proclabel "LAB_HISTORY misplaced LOINC codes";	
proc print width=min data=lab_history_misp_sum2 (obs=50) label;
	var code code_desc records classtype;
	run;

%end;

%if &_yobs_clin=1 %then %do;

title2 "OBS_CLIN";
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "OBS_CLIN errors";		
proc print width=min data=bad_obsclin_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;

title3 'Top 50 Highest Volume Misplaced LOINC Codes (expected CLASSTYPE=2)';	
ods proclabel "OBS_CLIN misplaced LOINC codes";	
proc print width=min data=obs_clin_misp_sum2 (obs=50) label;
	var code code_desc records classtype;
	run;		
	
%end;

%if &_yobs_gen=1 %then %do;

title2 "OBS_GEN";	
title3 'Top 50 Highest Volume Potentially Erroneous Codes';	
ods proclabel "OBS_GEN errors";	
proc print width=min data=bad_obsgen_sum2 (obs=50) label;
	var code_type code records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title3 'Top 50 Highest Volume Misplaced LOINC Codes (expected CLASSTYPE=2, 3 or 4)';	
ods proclabel "OBS_GEN misplaced LOINC codes";	
proc print width=min data=obs_gen_misp_sum2 (obs=50) label;
	var code code_desc records classtype;
	run;	
	
	
%end;



ods pdf close;

%mend report;
%report;



