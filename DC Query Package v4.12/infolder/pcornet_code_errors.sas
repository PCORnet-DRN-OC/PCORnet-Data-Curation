/*******************************************************************************
*  Name: 	pcornet_code_errors
*  Date: 	2018/05/03
*  Version: v5
*
*  Purpose: This program was developed by the Coordinating Center to help partners identify ETL errors
	    in populating ICD diagnosis codes; ICD & CPT/HCPCS procedure codes; RXCUI codes; LOINC codes; 
	    and NDC codes. The program identifes codes that violate one or more heuristics described in the 
	    Work Plan. Heuristics will not identify all erroneous codes. Partners are not
	    expected to clean or delete source data;  

*  Outputs: 
*    1) Up to 6 SAS datasets stored in /dmlocal containing all records which violate one or more of the heuristics
*    	for the given code type, and a summary dataset of the results:
* 		baddx.sas7bdat
*	    badpx.sas7bdat
*	    badrxcui.sas7bdat
*	    badndc.sas7bdat
*	    badloinc.sas7bat
*       bad_code_summary.sas7bdat (# and % of bad records per table)
*	 2) CSV versions of the 6 datasets stored in /dmlocal
*	 3) PCORnet_Code_Errors.pdf stored in /drnoc (contains a partial print of key fields from each of the above datasets)
* 	 4) SAS log file of pcornet code errors portion stored in /drnoc
*                (<DataMart Id>_<response date>_pcornet_code_errors.log)

*  Requirements: Program run in SAS 9.3 or higher

********************************************************************************/

options validvarname=upcase errors=0 nomprint;
ods html close;

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
* Create macro variable from DataMart ID and program run date 
********************************************************************************;
data _null_;
     set dpath.harvest;
     call symput("dmid",strip(datamartid));
     call symput("tday",left(put("&sysdate"d,yymmddn8.)));
run;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._pcornet_code_errors.log" lrecl=200;
proc printto log=qlog  new ;
run ;

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
quit;


********************************************************************************;
* Create macro to use throughout
********************************************************************************;

*summarize bad records by code type and code and calculate % of total;
%macro badsum (ds=,var=,tbl=);
proc sql;
	create table &ds._sum1 as
	select table,
	code_type,
	count(*) as bad_records format comma16.
	from dmlocal.&ds.
	group by 1,2;
	quit;
	
proc sql;
	create table &ds._sum2 as
	select distinct table,
	code_type,
	&var.,
	unexp_length,
	unexp_string,
	unexp_alpha,
	unexp_numeric,
	count(*) as records format comma16.
	from dmlocal.&ds.
	group by 1,2,3,4,5,6,7
	order by 2,8 desc;
	quit;
		
proc sql;
	create table &tbl._summary
	as select a.table,
	a.code_type,
	coalesce(b.bad_records,0) as bad_records,
	a.records
	from &tbl._sum a
	left join &ds._sum1 b
	on a.code_type=b.code_type
	order by code_type, records desc;
	quit;
	
data &tbl._summary;
	set &tbl._summary;
	if bad_records>0 then pct=bad_records/records*100;
	format records comma18. bad_records comma18. pct 8.2;
	run;
	

%mend badsum;




********************************************************************************;
* Begin codes macro
********************************************************************************;
%macro codes;

*********************************************************************************************;
* ICD9 and ICD10 Diagnosis Codes (DIAGNOSIS table)
*********************************************************************************************;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards decimals,dashes,commas,spaces and trailing blanks, and add indicators of the first position
which has an alpha character (anyalpha) or a numeric character (anydigit);
data diagnosis (compress=yes);
	format diagnosisid dx_type dx code_clean code_length anyalpha anydigit;
	set dpath.diagnosis (keep=diagnosisid dx dx_type);
	where not missing(dx) and dx_type in ('09','10');
	code_clean=strip(upcase(compress((dx),".-, ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records by type;
proc sql;
	create table diagnosis_sum as
	select 'DIAGNOSIS' as table,
	dx_type as code_type,
	count(*) as records format comma16.
	from diagnosis
	group by 2;
	quit;
	
*apply heuristics to identify bad records;
data dmlocal.baddx;
	set diagnosis;
	table='DIAGNOSIS';
	code_type=dx_type;
	*ICD09:length is not between 3-5 OR has alpha characters other than E or V OR has no numeric
	characters OR first 3 digits (min length) are 0;
	if dx_type='09' then do;
		if code_length not in (3,4,5) then unexp_length=1; else unexp_length=0; 
		if anyalpha(code_clean)>0 and substr(code_clean,1,1) not in ('E','V') then unexp_alpha=1; else unexp_alpha=0;
		if substr(code_clean,1,3) in ('000') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	*ICD10: length is not between 3 and 7 OR 1st character is not alpha OR first 3 digits (min length) are 0 or 9 OR 
	has no numeric characters;
	if dx_type='10' then do;
		if code_length not in (3,4,5,6,7) then unexp_length=1; else unexp_length=0; 
		if anydigit(code_clean)=1 then unexp_alpha=1; else unexp_alpha=0;
		if substr(code_clean,1,3) in ('000','999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	if max(unexp_length,unexp_alpha,unexp_string,unexp_numeric)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=baddx,var=dx,tbl=diagnosis);
	
*********************************************************************************************;
* PX ICD and CPT/HCPCS codes (PROCEDURES table)
*********************************************************************************************;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards decimals,dashes,commas,spaces and trailing blanks, and add indicators of the first position 
which has an alpha character (anyalpha) or numeric character (anydigit);
data procedures(compress=yes);
	format proceduresid px_type px code_clean code_length anyalpha anydigit;
	set dpath.procedures (keep=proceduresid px px_type);
	where not missing(px) and px_type in ('CH','09','10');
	code_clean=strip(upcase(compress((px),".-, ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;

*summarize records by type;
proc sql;
	create table procedures_sum as
	select 'PROCEDURES' as table,
	px_type as code_type,
	count(*) as records format comma16.
	from procedures
	group by 2;
	quit;

*apply heuristics to identify bad records;	
data dmlocal.badpx;
	set procedures;
	table='PROCEDURES';
	code_type=px_type;
	*CPT/HCPCS are length <5 OR first 5 are all 0 or 9 OR no numeric;
	if px_type='CH' then do;
		if code_length<5 then unexp_length=1; else unexp_length=0; 
		if substr(code_clean,1,5) in ('00000','99999') then unexp_string=1; else unexp_string=0;
		if anydigit(code_clean)=0 then unexp_numeric=1; else unexp_numeric=0;
		end;
	*ICD9 is not length 3 or 4 OR any alpha OR all 0;
	if px_type='09' then do;
		if code_length not in (3,4) then unexp_length=1; else unexp_length=0;
		if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0; 
		if code_clean in ('0000') then unexp_string=1; else unexp_string=0;
		end;
	*ICD10 is not length 7 OR no numeric OR all 7 are 0s or 9s;
	if px_type='10' then do;
		if code_length ne 7 then unexp_length=1; else unexp_length=0; 
		if code_clean in ('0000000','9999999') then unexp_string=1; else unexp_string=0;
		end;
	if max(unexp_length,unexp_alpha,unexp_numeric,unexp_string)=1 then output;		
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=badpx,var=px,tbl=procedures);

*********************************************************************************************;
* RXNORM_CUI codes (PRESCRIBING table)
*********************************************************************************************;
%if &_yprescribing=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards decimals,dashes,commas,spaces and trailing blanks, and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data prescribing (compress=yes);
	format prescribingid rxnorm_cui code_clean code_length anyalpha anydigit;
	set dpath.prescribing (keep=prescribingid rxnorm_cui);
	where not missing(rxnorm_cui);
	code_clean=strip(upcase(compress((rxnorm_cui),".-, ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;

*summarize records;
proc sql;
	create table prescribing_sum
	as select 'PRESCRIBING' as table,
	'RXNORM_CUI' as code_type,
	count(*) as records format comma16.
	from prescribing;
	quit;

	
*apply heuristics to identify bad records;	
data dmlocal.badrxcui;
	set prescribing;
	table='PRESCRIBING';
	code_type='RXNORM_CUI';
	*flag codes with any alphabetical characters OR a length <2 or >7; 
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<2 or code_length>7 then unexp_length=1; else unexp_length=0;
	*placeholders for additional rules, currently no applicable rules for this code type;
	unexp_string=.;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha)=1 then output;
	run;
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=badrxcui,var=rxnorm_cui,tbl=prescribing);

%end;
	
*********************************************************************************************;
* LOINC codes (LAB_RESULT_CM table)
*********************************************************************************************;
%if &_ylab_result_cm=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards trailing blanks, and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data lab_result_cm (compress=yes);
	format lab_result_cm_id lab_loinc code_clean code_clean_r code_length anyalpha anydigit;
	set dpath.lab_result_cm (keep=lab_result_cm_id lab_loinc);
	where not missing(lab_loinc);
	code_clean=upcase(strip(lab_loinc));
	code_clean_r=left(reverse(code_clean));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records;
proc sql;
	create table lab_result_cm_sum
	as select 'LAB_RESULT_CM' as table,
	'LAB_LOINC' as code_type,
	count(*) as records format comma16.
	from lab_result_cm;
	quit;

*apply heuristics to identify bad records;
data dmlocal.badloinc (drop=code_clean_r);
	set lab_result_cm;
	table='LAB_RESULT_CM';
	code_type='LAB_LOINC';
	*flag for any alphabetical characters OR a length less than 3 or greater than 7 OR the absence of a dash after
	the next to last position;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_length<3 or code_length>7 then unexp_length=1; else unexp_length=0;
	if substr(code_clean_r,2,1)='-' then unexp_string=0; else unexp_string=1;
	*placeholders for additional rules, currently no applicable rules for this code type;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string)=1 then output;
	run;		
	
*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=badloinc,var=lab_loinc,tbl=lab_result_cm);
	

%end;	


*********************************************************************************************;
* NDC codes (DISPENSING table)
*********************************************************************************************;
%if &_ydispensing=1 %then %do;

*bring in the needed information and compress.  Create a clean uppercase version of the code 
which discards spaces and trailing blanks, and add indicators of which position,
if any, has any alpha character (anyalpha) or numeric character (anydigit);
data dispensing(compress=yes);
	format dispensingid ndc code_clean code_length anyalpha anydigit;
	set dpath.dispensing (keep=dispensingid ndc);
	where not missing(ndc);
	code_clean=strip(upcase(compress((ndc)," ")));
	code_length=length(code_clean);
	anyalpha=anyalpha(code_clean);
	anydigit=anydigit(code_clean);
	run;
	
*summarize records;
proc sql;
	create table dispensing_sum
	as select 'DISPENSING' as table,
	'NDC' as code_type,
	count(*) as records format comma16.
	from dispensing;
	quit;

	
*apply heuristics to identify bad records;
data dmlocal.badndc;
	set dispensing;
	table='DISPENSING';
	code_type='NDC';
	*length not 11 OR any alpha OR a string of 0 or 9;
	if anyalpha(code_clean)>0 then unexp_alpha=1; else unexp_alpha=0;
	if code_clean in ('00000000000','99999999999') then unexp_string=1; else unexp_string=0;
	if code_length ne 11 then unexp_length=1; else unexp_length=0;
	*placeholders for additional rules, currently no applicable rules for this code type;
	unexp_numeric=.;
	if max(unexp_length,unexp_alpha,unexp_string)=1 then output;
	run;

*summarize # of bad records by code type and code and calculate % of total;	
%badsum(ds=badndc,var=ndc,tbl=dispensing);

%end;	

********************************************************************************;
* End codes macro
********************************************************************************;
%mend codes;
%codes;


********************************************************************************;
* Separate the DX and PX bad record data sets to make the PDF easier to read
********************************************************************************;

data baddx_sum209 baddx_sum210;
	set baddx_sum2;
	if code_type='09' then output baddx_sum209;
		else if code_type='10' then output baddx_sum210;
	run;
	
data badpx_sum209 badpx_sum210 badpx_sum2CH;
	set badpx_sum2;
	if code_type='09' then output badpx_sum209;
		else if code_type='10' then output badpx_sum210;
		else if code_type='CH' then output badpx_sum2CH;
	run;


********************************************************************************;
* Create overall summary
********************************************************************************;


%macro summary;
data dmlocal.bad_code_summary;
	length table $15 code_type $15;
	set procedures_summary 
		diagnosis_summary 
		%if &_ydispensing=1 %then %do; dispensing_summary %end;
		%if &_yprescribing=1 %then %do; prescribing_summary %end;
		%if &_ylab_result_cm=1 %then %do; lab_result_cm_summary %end;
		;
	label code_type='Code Type' table='Table' records='Records' bad_records='Bad Records' pct='Percent';
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

        proc export data=dmlocal.&_dsn
            outfile="&qpath.dmlocal/&_dsn..csv"
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
ods pdf file="&qpath.drnoc/&dmid._&tday._Potential_Code_Errors.pdf" style = journal;	


title1 'Bad Code Summary';
proc print width=min data=dmlocal.bad_code_summary label;
	run;
	

title1 'Highest Volume Codes (Top 50)';	
title2 "ICD09 Dx";	
proc print width=min data=baddx_sum209 (obs=50) label;
	var dx records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title2 "ICD10 Dx";	
proc print width=min data=baddx_sum210 (obs=50) label;
	var dx records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title2 "ICD09 Px";	
proc print width=min data=badpx_sum209 (obs=50) label;
	var px records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
title2 "ICD10 Px";	
proc print width=min data=badpx_sum210 (obs=50) label;
	var px records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
title2 "CPT/HCPCS";	
proc print width=min data=badpx_sum2CH (obs=50) label;
	var px records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
%if &_ydispensing=1 %then %do;

title2 "NDC";	
proc print width=min data=badndc_sum2 (obs=50) label;
	var ndc records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	

%end;

%if &_yprescribing=1 %then %do;
title2 "RXNORM_CUI Codes";	
proc print width=min data=badrxcui_sum2(obs=50) label;
	var rxnorm_cui records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;	
%end;

%if &_ylab_result_cm=1 %then %do;

title2 "LOINC Codes";	
proc print width=min data=badloinc_sum2 (obs=50) label;
	var lab_loinc records unexp_alpha unexp_length unexp_numeric unexp_string;
	run;
	
%end;

ods pdf close;

%mend report;
%report;




