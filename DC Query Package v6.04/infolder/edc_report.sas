/******************************************************************************
*  $Source: edc_report $;
*    $Date: 2021/12/19
*    Study: PCORnet
*
*  Purpose: Produce PCORnet EDC report v6.04
* 
*   Inputs: SAS program:  /sasprograms/run_edc_report.sas
*
*  Outputs: 
*           1) Print of compiled EDC report in PDF file format stored as
*                (<DataMart Id>_EDCRPT_<response date>.pdf)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/
options nodate nonumber nobyline orientation=landscape validvarname=upcase 
        formchar="|___|||___+=|_/\<>" missing=' ' /*mprint mlogic symbolgen ls=max*/;
goptions reset=all dev=png300 rotate=landscape gsfmode=replace htext=0.9 
         ftext='Albany AMT' hsize=9 vsize=5.5;
ods html close;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname normdata "&qpath.dmlocal" access=readonly;
libname prevdata "&ppath.dmlocal" access=readonly;
filename tranfile "&qpath./infolder/required_structure.cpt";
filename edc_ref "&qpath./infolder/edc_reference.cpt";
libname cptdata "&qpath.infolder";

********************************************************************************;
* Call RTF template
********************************************************************************;
%include "&qpath.infolder/edc_template.sas";

********************************************************************************;
* Create macro variable from DataMart ID and program run date 
********************************************************************************;
data _null_;
     set normdata.xtbl_l3_metadata;
     if name="DATAMARTID" then call symput("dmid",strip(value));
     else if name="SAS_ETS" then call symput("_sasets",strip(upcase(value)));
     today="&sysdate"d;
     call symput("r_daten",compress(put(today,yymmdd10.),'-'));
     if name="REFRESH_MAX" then do;
        refresh=input(strip(compress(value,'-')),yymmdd8.);
        call symput("refresh_max",put(refresh,date9.));
     end;
run;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/%upcase(&dmid)_&r_daten._EDCRPT.log" lrecl=200;
proc printto log=qlog  new ;
run ;

********************************************************************************;
* Create standard macros used in multiple queries
********************************************************************************;

*- Get titles and footnotes -;
%macro ttl_ftn;
    data _null_;
         set footers;
         if table="&_ftn";
         call symput("_fnote",strip(footnote));
    run;

    data _null_;
         set headers;
         if table="&_hdr";
         call symput("_ttl1",strip(title1));
         call symput("_ttl2",strip(title2));
    run;

%mend ttl_ftn;

********************************************************************************;
* Create working formats used in multiple queries
********************************************************************************;
proc format;
     value tbl_row
       1='DEMOGRAPHIC'
       2='ENROLLMENT'
       3='DEATH'
       4='ENCOUNTER'
       5='DIAGNOSIS'
       6='PROCEDURES'
       7='VITAL'
       8='PRESCRIBING'
       9='DISPENSING'
      10='LAB_RESULT_CM'
      11='HARVEST'
      12='CONDITION'
      13='DEATH_CAUSE'
      14='PRO_CM'
      15='PCORNET_TRIAL'
      16='PROVIDER'
      17='MED_ADMIN'
      18='OBS_CLIN'
      19='OBS_GEN'
      20='HASH_TOKEN'
      21='LDS_ADDRESS_HISTORY'
      22='IMMUNIZATION'
      23='LAB_HISTORY'
        ;

     value threshold
       .t="BT"
        other = [16.0]
        ;

     value thresholdc
       .t="BT"
        other = [comma16.0]
        ;

     value threshold1c
       .t="BT"
        other = [comma16.1]
        ;

     value $etype
        'AV'='AV (Ambulatory Visit)'
        'ED'='ED (Emergency Dept)'
        'EI'='EI (ED to IP Stay)'
        'IC'='IC (Institutional Professional Consult)'
        'IP'='IP (Inpatient Hospital Stay)'
        'IS'='IS (Non-acute Institutional Stay)'
        'OA'='OA (Other Ambulatory Visit)'
        'OS'='OS (Observation Stay)'
        'TH'='TH (Telehealth)'
        'Missing, NI, UN or OT'='Missing, NI, UN or OT'
        'TOTAL'='Total'
        ;
    
     value $svar
        'AV'='01'
        'ED'='02'
        'EI'='03'
        'IC'='04'
        'IP'='05'
        'IS'='06'
        'OA'='07'
        'OS'='08'
        'TH'='09'
        'Missing, NI, UN or OT'='10'
        'TOTAL'='11'
         ;
        
     value $cat
        'AV'='AV (Ambulatory Visit)'
        'ED'='ED (Emergency Dept)'
        'EI'='EI (ED to IP Stay)'
        'IC'='IC (Institutional Professional Consult)'
        'IP'='IP (Inpatient Hospital Stay)'
        'IS'='IS (Non-acute Institutional Stay)'
        'OA'='OA (Other Ambulatory Visit)'
        'OS'='OS (Observation Stay)'
        'TH'='TH (Telehealth)'
        ;

     value $origin
        'BI'='BI (billing)'
        'CL'='CL (claim)'
        'DR'='DR (derived)'
        'OD'='OD (Order/EHR)'
        ;

     value mnth
        1='Jan'
        2='Feb'
        3='Mar'
        4='Apr'
        5='May'
        6='Jun'
        7='Jul'
        8='Aug'
        9='Sep'
       10='Oct'
       11='Nov'
       12='Dec'
        ;
run;

********************************************************************************;
* Create working datasets for TOC, headers, footnotes, and DC summary
********************************************************************************;
proc cimport library=work infile=edc_ref;
run;

*- Create a macro flag variable indicating if a pediatric datamart -*;
data _null_;
     set pediatric_datamarts end=eof;
     retain pedyes 0;
     if datamartid="&dmid" then pedyes=1;
     if eof then do;
        call symputx('_pedyes',pedyes);
     end;
run;

********************************************************************************;
* Create row variable for ease of merging with exceptions
********************************************************************************;
proc sort data=dc_summary(rename=(data_check_num=row));
     by edc_table_s_ row;
run;

********************************************************************************;
*- Create dataset & format of 61 most recent month/year values, for charts -*;
********************************************************************************;
data chart_months;
     format xdate date9.;
     do yr = year("&refresh_max"d)-5 to year("&refresh_max"d);
        do mon = 1 to 12;
           xdate=mdy(mon,1,yr);
           output;
        end;
    end;
run;

proc sort data=chart_months;
     by descending xdate;
     where xdate<="&refresh_max"d;
run;

*- Limit data to five years + 1 month -*;
data chart_months;
     set chart_months;
     by descending xdate;
     if _n_<=61;
run;

proc sort data=chart_months;
     by xdate;
run;

*- Create variables for formatting x-axis -*;
data chart_months;
     length label $8;
     set chart_months end=eof;
     by xdate;

     fmtname="CHART";
     start=_n_;
     if (start=1 or mod(start,6)=1) then
        label=put(month(xdate),mnth.) || " " || put(year(xdate),4.);
run;

*- Create format for x-axis -*;
proc format cntlin=chart_months;
run;

********************************************************************************;
*- Macro to remove working datasets after each query result -*;
********************************************************************************;
%macro clean(savedsn);
     proc datasets  noprint;
          save dc_normal toc headers footers chart_months dc_summary 
               required_structure q2_stat_dlg_loinc tbl_ivi_ref pediatric_datamarts 
               lab_volume_ref specimen_source_category &savedsn / memtype=data;
          save formats / memtype=catalog;
     quit;
%mend clean;

********************************************************************************;
* Bring in NORMALIZED data and compress it
********************************************************************************;
data dc_normal(compress=yes);
     set normdata.&dmid._&r_daten._dc_norm;
run;

********************************************************************************;
* TABLE OF CONTENTS report
********************************************************************************;
%global ets_inst;
%macro print_toc;

*- Create macro variable for output name -*;
%let fname=tbl_toc;
%let _ftn=TOC;
%let _hdr=TOC;

*- Get titles and footnotes -;
%ttl_ftn;

data _null_;
     set dc_normal;
     if dc_name="XTBL_L3_METADATA" and 
        variable in ("LOW_CELL_CNT" "QUERY_PACKAGE" "REFRESH_MAX" "RESPONSE_DATE" 
                     "LOOKBACK_DATE" "CDM_VERSION" "SAS_ETS" "SAS_ETS_INSTALLED");

     call symput("r_date",put(today(),yymmdd10.));
     if variable="LOW_CELL_CNT" then call symput("low_cell",strip(resultc));
     else if variable="QUERY_PACKAGE" then call symput("qpackage",strip(resultc));
     else if variable="REFRESH_MAX" then call symput("rmax",strip(resultc));
     else if variable="RESPONSE_DATE" then call symput("q_date",strip(resultc));
     else if variable="LOOKBACK_DATE" then call symput("lb_date",strip(resultc));
     else if variable="CDM_VERSION" then do;
        call symput("cdm_ver",strip(resultc));
        if resultc="Values outside of CDM specifications" then call symputx("ver_out",1);
        else call symputx("ver_out",0);
     end;
     else if variable="SAS_ETS" then call symput("ets",strip(resultc));
     else if variable="SAS_ETS_INSTALLED" then call symput("ets_inst",strip(resultc));
run;

*- Produce output -;
ods listing close;
ods path sashelp.tmplmst(read) work.templat(read);
ods escapechar = '~';
ods pdf file = "&qpath.drnoc/%upcase(&dmid)_&r_daten._EDCRPT.pdf" 
        style=pcornet_dctl nogtitle nogfootnote pdftoc=1;

ods proclabel "&_ttl1";
title1 justify=left "&_ttl1";
title2 justify=left h=2.5 "&_ttl2";
title3 justify=left h=2.5 "Report Run Date:  &r_date";
title4 justify=left h=2.5 "Query Run Date:  &q_date";
title5 justify=left h=2.5 "Maximum Table Refresh Date:  &rmax";
title6 justify=left h=2.5 %if &low_cell^=0 %then %do; color=orange %end; "Low Cell Count Threshold:  &low_cell";
title7 justify=left h=2.5 "Query Package:  &qpackage";
title8 justify=left h=2.5 "Lookback Date:  &lb_date";
title9 justify=left h=2.5 "CDM Version: " %if &ver_out=1 %then %do; color=red %end; "&cdm_ver";
title10 justify=left h=2.5 "SAS_ETS licensed:  &ets / SAS_ETS installed: &ets_inst";
footnote1 justify=left "&_fnote";

proc report data=toc split='|' style(header)=[backgroundcolor=CXCCCCCC] contents=' ';
     column pg section table table_description data_check_s_;
     where pg=1;
    
     define pg                /order noprint;
     define section           /display flow "Section" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
     define table             /display flow "Table" style(header)=[just=left cellwidth=8%] style(column)=[just=left];
     define table_description /display flow "Table Description" style(header)=[just=left cellwidth=45%] style(column)=[just=left];
     define data_check_s_     /display flow "Data Check(s)" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
run;    

*- Determine how many page in report -*;
proc means data=toc noprint;
     var pg;
     output out=_toc_pg max=max_pg;
run;
        
data _null_;
     set _toc_pg;
     call symputx("_nopg",compress(put(max_pg,8.)));
run;

%do _pg = 2 %to &_nopg;
    ods listing close;
    ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
    title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=toc split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg section table table_description data_check_s_;
         where pg=&_pg;
    
         define pg                /order noprint;
         define section           /display flow "Section" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
         define table             /display flow "Table" style(header)=[just=left cellwidth=8%] style(column)=[just=left];
         define table_description /display flow "Table Description" style(header)=[just=left cellwidth=45%] style(column)=[just=left];
         define data_check_s_     /display flow "Data Check(s)" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
    run;    

    ods listing;
%end;

%mend print_toc;
%print_toc;

********************************************************************************;
* TABLE IA DEMOGRAPHIC SUMMARY
********************************************************************************;

*- Create table formats -*;
proc format;
     value rowfmt
        0='Patients'
        1='Age'
        2='Age group'
        3='Hispanic'
        4='Sex'
        5='Race'
        6='Race among patients with at least 1 encounter after December 2011'
        7='Gender Identity'
        8='Sexual Orientation'
         ;

     value dc_name
        0='DEM_L3_N'
        1='DEM_L3_AGEYRSDIST1'
        2='DEM_L3_AGEYRSDIST2'
        3='DEM_L3_HISPDIST'
        4='DEM_L3_SEXDIST'
        5='DEM_L3_RACEDIST'
        6='XTBL_L3_RACE_ENC'
        7='DEM_L3_GENDERDIST'
        8='DEM_L3_ORIENTDIST'
         ;

     value $agegroup
        "<0 yrs"='1'
        "0-1 yrs"='1'
        "2-4 yrs"='1'
        "5-9 yrs"='2'
        "10-14 yrs"='2'
        "15-18 yrs"='3'
        "19-21 yrs"='3'
        "22-44 yrs"='4'
        "45-64 yrs"='4'
        "65-74 yrs"='5'
        "75-89 yrs"='5'
        ">89 yrs"='5'
        "NULL or missing"='6'
        ;
run;

*- Create macro based upon row type -*;
%macro stat(dcn=,stat=,cat=,ord=,ord2=,col1=);
     if dc_name="&dcn" and statistic="&stat" then do;
        ord=&ord;
        ord2=&ord2;
        col1="&col1";
        output;
     end;
%mend stat;

%macro cat(dcn=,stat=,cat=,ord=,ord2=,col1=);
     if dc_name="&dcn" and category in (&cat) then do;
        ord=&ord;
        ord2=&ord2;
        col1="&col1";
        output;
     end;
%mend cat;

*- Create dummy row dataset -*;
data dummy;
     length col1 col4 $200;
     do ord = 0 to 8;
        ord2=0;
        col1=put(ord,rowfmt.);
        col4=put(ord,dc_name.);
        output;
     end;
run;

data demog newcat;
     set dc_normal(where=(datamartid=%upcase("&dmid") and scan(dc_name,1,'_') in ("DEM" "XTBL")));
     if dc_name in ("DEM_L3_N" "DEM_L3_AGEYRSDIST1") then output demog;
     else output newcat;
run;

*- Special re-categorizing -*;
data newcat;
     length cat $50;
     set newcat;
     if dc_name="XTBL_L3_RACE_ENC" and statistic="DISTINCT_PATID_N" then delete; 

     if dc_name="DEM_L3_AGEYRSDIST2" then cat=strip(put(category,$agegroup.));
     else if category in ("NI" "UN" "OT" "NULL or missing") or 
          (dc_name="DEM_L3_HISPDIST" and category="R") or
          (dc_name="DEM_L3_SEXDIST" and category="A") or
          (dc_name="DEM_L3_GENDERDIST" and category="DC") or
          (dc_name="DEM_L3_ORIENTDIST" and category="DC") or
          (dc_name in ("DEM_L3_RACEDIST" "XTBL_L3_RACE_ENC") and category="07") then cat="Missing, NI, UN or OT";
     else if dc_name in ("DEM_L3_RACEDIST"  "XTBL_L3_RACE_ENC") and category in ('01', '02', '03', '04', '06') 
        then cat="Non-White";
     else if dc_name="DEM_L3_GENDERDIST" and category in ('MU' 'SE' 'TF' 'TM') 
        then cat="MU, SE, TF, TM";
     else if dc_name="DEM_L3_ORIENTDIST" and category in ('AS' 'MU' 'SE' 'QS') 
        then cat="AS, MU, SE, QS";
     else cat=category;

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep dc_name statistic cat resultn bt_flag;
run;

*- Recalculate count and percent -*;
proc means data=newcat nway noprint missing;
     class dc_name statistic cat bt_flag;
     var resultn;
     output out=newcat_sum sum=sum;
run;

*- Reconfigure to match original normalized data -*;
data newcat_sum(keep=dc_name statistic category resultn bt_flag);
     length category $255;
     merge newcat_sum(rename=(bt_flag=flag) where=(flag=" "))
           newcat_sum(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name statistic cat;

     resultn=sum;
     category=cat;
run;

*- Create rows -*;
data tbl(keep=ord ord2 col1 resultc dc_name statistic);
     length col1 resultc $200;
     set demog newcat_sum;

     if statistic^="RECORD_PCT" then do;
         if resultn^=.t then resultc=strip(put(resultn,comma16.)||strip(bt_flag));
         else resultc=strip(put(resultn,threshold.));
     end;
     else if statistic="RECORD_PCT" then resultc=strip(put(resultn,5.1));

     %stat(dcn=DEM_L3_N,stat=DISTINCT_N,ord=0,ord2=0,col1=Patients)
     %stat(dcn=DEM_L3_AGEYRSDIST1,stat=MEAN,ord=1,ord2=1,col1=Mean)
     %stat(dcn=DEM_L3_AGEYRSDIST1,stat=MEDIAN,ord=1,ord2=2,col1=Median)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='1',ord=2,ord2=1,col1=0-4)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='2',ord=2,ord2=2,col1=5-14)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='3',ord=2,ord2=3,col1=15-21)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='4',ord=2,ord2=4,col1=22-64)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='5',ord=2,ord2=5,col1=65+)
     %cat(dcn=DEM_L3_AGEYRSDIST2,cat='6',ord=2,ord2=6,col1=%str(Missing))
     %cat(dcn=DEM_L3_HISPDIST,cat='N',ord=3,ord2=1,col1=N (No))
     %cat(dcn=DEM_L3_HISPDIST,cat='Y',ord=3,ord2=2,col1=Y (Yes))
     %cat(dcn=DEM_L3_HISPDIST,cat=%str('Missing, NI, UN or OT'),ord=3,ord2=3,col1=%str(Missing or Refused))
     %cat(dcn=DEM_L3_SEXDIST,cat='F',ord=4,ord2=1,col1=F (Female))
     %cat(dcn=DEM_L3_SEXDIST,cat='M',ord=4,ord2=2,col1=M (Male))
     %cat(dcn=DEM_L3_SEXDIST,cat=%str('Missing, NI, UN or OT'),ord=4,ord2=3,col1=%str(Missing or Ambiguous))
     %cat(dcn=DEM_L3_RACEDIST,cat='05',ord=5,ord2=1,col1=White)
     %cat(dcn=DEM_L3_RACEDIST,cat='Non-White',ord=5,ord2=2,col1=Non-White)
     %cat(dcn=DEM_L3_RACEDIST,cat=%str('Missing, NI, UN or OT'),ord=5,ord2=3,col1=%str(Missing or Refused))
     %cat(dcn=XTBL_L3_RACE_ENC,cat='05',ord=6,ord2=1,col1=White)
     %cat(dcn=XTBL_L3_RACE_ENC,cat='Non-White',ord=6,ord2=2,col1=Non-White)
     %cat(dcn=XTBL_L3_RACE_ENC,cat=%str('Missing, NI, UN or OT'),ord=6,ord2=3,col1=%str(Missing or Refused))
     %cat(dcn=DEM_L3_GENDERDIST,cat='GQ',ord=7,ord2=1,col1=%str(GQ (Genderqueer/Non-Binary)))
     %cat(dcn=DEM_L3_GENDERDIST,cat='M',ord=7,ord2=2,col1=%str(M (Man)))
     %cat(dcn=DEM_L3_GENDERDIST,cat='F',ord=7,ord2=3,col1=%str(W (Woman)))
     %cat(dcn=DEM_L3_GENDERDIST,cat=%str('MU, SE, TF, TM'),ord=7,ord2=4,col1=%str(~mMU (Multiple gender categories), SE (Something else), ~-2nTF (Transgender female/Trans woman/Male-to-female), ~-2nor TM (Transgender male/Trans man/Female-to-male)))
     %cat(dcn=DEM_L3_GENDERDIST,cat=%str('Missing, NI, UN or OT'),ord=7,ord2=5,col1=%str(Missing or Refused))
     %cat(dcn=DEM_L3_ORIENTDIST,cat='BI',ord=8,ord2=1,col1=Bisexual)
     %cat(dcn=DEM_L3_ORIENTDIST,cat='GA',ord=8,ord2=2,col1=Gay)
     %cat(dcn=DEM_L3_ORIENTDIST,cat='LE',ord=8,ord2=3,col1=Lesbian)
     %cat(dcn=DEM_L3_ORIENTDIST,cat='QU',ord=8,ord2=4,col1=Queer)
     %cat(dcn=DEM_L3_ORIENTDIST,cat='ST',ord=8,ord2=5,col1=Straight)
     %cat(dcn=DEM_L3_ORIENTDIST,cat=%str('AS, MU, SE, QS'),ord=8,ord2=6,col1=%str(~mAS (Asexual), MU (Multiple sexual orientations), ~-2nSE (Something else), QS (Questioning)))
     %cat(dcn=DEM_L3_ORIENTDIST,cat=%str('Missing, NI, UN or OT'),ord=8,ord2=7,col1=%str(Missing or Refused))
run;

proc sort data=tbl;
     by ord ord2;
run;

*- Bring everything together - separate count and percent records -*;
data print_ia;
     merge dummy
           tbl(where=(statistic^="RECORD_PCT") rename=(resultc=col2))
           tbl(where=(statistic="RECORD_PCT") rename=(resultc=col3))
     ;
     by ord ord2;

     if ord<=5 then pg=1;
     else pg=2;
run;

*- Produce output -*;
%macro print_ia;

    *- Create macro variable for output name -*;
    %let _ftn=IA;
    %let _hdr=Table IA;

    *- Get titles and footnotes -*;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4 ord2;
         where pg=1;

         define pg       /order noprint;
         define ord2     /display noprint;
         define col1     /display flow "" style(header)=[just=left cellwidth=40%];
         define col2     /display flow "N" style(column)=[just=center cellwidth=14%];
         define col3     /display flow "%" style(column)=[just=center cellwidth=14%];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=30%] style(column)=[just=left];

         compute ord2;
            if ord2^=0 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4 ord2;
         where pg>1;

         define pg       /order noprint;
         define ord2     /display noprint;
         define col1     /display flow "" style(header)=[just=left cellwidth=40%] style(column)=[just=left];
         define col2     /display flow "N" style(column)=[just=center cellwidth=14%];
         define col3     /display flow "%" style(column)=[just=center cellwidth=14%];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         break after pg / page;

         compute ord2;
            if ord2^=0 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
    run;

    ods listing;
%mend print_ia;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IB POTENTIAL POOLS OF PATIENTS
********************************************************************************;

*- Create table formats -*;
proc format;
     value metric
        1='Potential pool of patients for observational studies'
        2='Potential pool of patients for trials'
        3='Potential pool of patients for studies requiring data on diagnoses, vital measures and (a) medications or (b) medications and lab results'
        4=' '
        5=' '
        6='Patients with diagnosis data'
        7='Patients with procedure data'
        ;

     value met_desc
        1='Number of unique patients with at least 1 ED, EI, IP, OS, TH, or AV encounter within the past 5 years'
        2='Number of unique patients with at least 1 ED, EI, IP, OS, TH, or AV encounter within the past 1 year'
        3='Number of unique patients with at least 1 encounter and DIAGNOSIS and VITAL records within the past 5 years'
        4='Number of unique patients with at least 1 encounter and DIAGNOSIS, VITAL, and PRESCRIBING or DISPENSING records within the past 5 years'
        5='Number of unique patients with at least 1 encounter and DIAGNOSIS, VITAL, PRESCRIBING or DISPENSING, and LAB_RESULT_CM records within the past 5 years'
        6='Percentage of patients with encounters who have at least 1 diagnosis'
        7='Percentage of patients with encounters who have at least 1 procedure'
        ;
run;

*- Create macro based upon row type -*;
%macro stat(dcn=,var=,stat=,cat=,ord=);
    data row&ord;
         length col3-col4 $200;
         set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="&dcn" and category="&cat"));

         ord=&ord;
         if resultn^=.t then col3=strip(put(resultn,comma16.));
         else col3=strip(put(resultn,threshold.));
         col4=dc_name;
         keep ord: col:;
    run;
        
    proc append base=tbl data=row&ord;
    run;

%mend stat;
%stat(dcn=ENC_L3_DASH2,cat=5 yrs,ord=1)
%stat(dcn=ENC_L3_DASH2,cat=1 yr,ord=2)
%stat(dcn=XTBL_L3_DASH1,cat=5 yrs,ord=3)
%stat(dcn=XTBL_L3_DASH2,cat=5 yrs,ord=4)
%stat(dcn=XTBL_L3_DASH3,cat=5 yrs,ord=5)

*- Percentage rows -*;
data prows;
     length col3-col4 $200;
     if _n_=1 then set dc_normal(where=(dc_name="ENC_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     set dc_normal(in=d where=(dc_name="DIA_L3_N" and variable="PATID" and statistic="DISTINCT_N"))
         dc_normal(in=p where=(dc_name="PRO_L3_N" and variable="PATID" and statistic="DISTINCT_N"))
     ;
     if d then ord=6;
     else if p then ord=7;

     if resultn^=.t then do;
        col3=strip(put((resultn/denom)*100,comma16.))|| "%";
        if .<(resultn/denom)*100<50.0 then threshold=1;
     end;
     else col3=strip(put(resultn,threshold.));
     col4="ENC_L3_N; " || strip(dc_name);
    
     keep ord: col: threshold;
run;

*- Bring everything together -*;
data print_ib;
     length col1-col2 $200;
     set tbl prows;
     by ord;
     col1=put(ord,metric.);
     col2=put(ord,met_desc.);
run;

*- Keep records resulting in an exception -*;
data dc_summary_ib;
     length edc_table_s_ $10;
     set print_ib(keep=ord threshold);
     if threshold=1;
     if ord=6 then row=3.04;
     else if ord=7 then row=3.05;
     exception=1;
     edc_table_s_="Table IB";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ib nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ib;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ib;

    *- Create macro variable for output name -*;
    %let _ftn=IB;
    %let _hdr=Table IB;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ib split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 threshold col3 col4;

         define col1     /display flow "Metric" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col2     /display flow "Metric Description" style(header)=[just=left cellwidth=45%] style(column)=[just=left];
         define threshold/display noprint;
         define col3     /display flow "Result" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         compute col3;
            if threshold=1 then call define(_col_, "style", "style=[color=red]");
         endcomp;
    run;    

    ods listing;
%mend print_ib;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IC HEIGHT, WEIGHT, AND BMI
********************************************************************************;

*- Create table formats -*;
proc format;
     value rowfmt
        1='Height measurements'
        2='Weight measurements'
        3='Body Mass Index (BMI) measurements'
        ;

     value dc_name
        1='VIT_L3_HT_DIST'
        2='VIT_L3_WT_DIST'
        3='VIT_L3_BMI'
         ;

     value $bmigroup
        '0-1'='BMI <=25'
        '2-5'='BMI <=25'
        '6-10'='BMI <=25'
        '11-15'='BMI <=25'
        '16-20'='BMI <=25'
        '21-25'='BMI <=25'
        '26-30'='BMI 26-30'
        '31-35'='BMI >=31'
        '36-40'='BMI >=31'
        '41-45'='BMI >=31'
        '46-50'='BMI >=31'
        '>50'='BMI >=31'
        ;
run;

*- Create dummy row dataset -*;
data dummy;
     length col1 col4 $200;
     do ord = 1 to 3;
        ord2=0;
        col1=put(ord,rowfmt.);
        col4=put(ord,dc_name.);
        output;
     end;
run;

*- Re-categorize BMI and SMOKING -*;
data bmi_smoke;
     set dc_normal;
     if datamartid=%upcase("&dmid") and statistic="RECORD_N" and
        dc_name in ("VIT_L3_BMI");

     if (dc_name="VIT_L3_BMI" and category in ("<0" "NULL or missing"))
        then delete;

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep dc_name category resultn bt_flag;
run;

%macro recat(dcn,dcname,fmt);
    proc means data=bmi_smoke completetypes nway missing noprint;
         class dc_name bt_flag;
         class category/preloadfmt;
         var resultn;
         output out=&dcn._sum sum=sum;
         format category &fmt.;
         where dc_name="&dcname";
    run;

    *- Create a total records record -*;
    data &dcn._total;
         merge &dcn._sum(rename=(bt_flag=flag) where=(flag=" " and sum^=.))
               &dcn._sum(in=bt rename=(sum=bt_sum) where=(bt_flag="*" and bt_sum^=.)) end=eof
         ;
         by dc_name category;
         output;
         retain total 0;
         total=total+sum;
         if eof then do;
            category="TOTAL";
            output;
         end;
    run;
%mend recat;
%recat(dcn=bmi,dcname=VIT_L3_BMI,fmt=$bmigroup.);

*- Create macro based upon row type -*;
%macro stat(dcn=,var=,stat=,cat=,ord=,ord2=,type=,col1=,dsn=);
    data row&ord;
         length col1-col3 $50;
         %if &type=1 %then %do;
             set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="&dcn"
                                    and variable="&var" and statistic="&stat"));
                 if resultn^=.t then col2=strip(put(resultn,comma16.));
                 else col2=strip(put(resultn,threshold.));
                 col3=" ";
         %end;
         %else %if &type=2 %then %do; 
             if _n_=1 then set &dsn._total(rename=(total=denom) where=(category="TOTAL"));
             set &dsn._total(where=(category="&cat"));
                 col2=strip(put(&var,comma16.)||strip(bt_flag));
                 %if &var=sum %then %do;
                    if denom>0 then col3=strip(put((&var/denom)*100,5.1));
                 %end;
                 %else %do;
                    col3=" ";
                 %end;
         %end;
         ord=&ord;
         ord2=&ord2;
         col1="&col1";
         keep ord: col:;
    run;

    proc append base=tbl data=row&ord;
    run;

%mend stat;
%stat(dcn=VIT_L3_HT_DIST,var=HEIGHT,stat=N,ord=1,ord2=1,type=1,col1=Records)
%stat(dcn=VIT_L3_HT_DIST,var=HEIGHT,stat=MEAN,ord=1,ord2=2,type=1,col1=%str(Height (inches), mean))
%stat(dcn=VIT_L3_HT_DIST,var=HEIGHT,stat=MEDIAN,ord=1,ord2=3,type=1,col1=%str(Height (inches), median))
%stat(dcn=VIT_L3_WT_DIST,var=WEIGHT,stat=N,ord=2,ord2=1,type=1,col1=Records)
%stat(dcn=VIT_L3_WT_DIST,var=WEIGHT,stat=MEAN,ord=2,ord2=2,type=1,col1=%str(Weight (lbs.), mean))
%stat(dcn=VIT_L3_WT_DIST,var=WEIGHT,stat=MEDIAN,ord=2,ord2=3,type=1,col1=%str(Weight (lbs.), median))
%stat(dcn=VIT_L3_BMI,var=total,cat=TOTAL,ord=3,ord2=1,type=2,col1=Records,dsn=bmi)
%stat(dcn=VIT_L3_BMI,var=sum,cat=0-1,ord=3,ord2=2,type=2,col1=BMI <=25,dsn=bmi)
%stat(dcn=VIT_L3_BMI,var=sum,cat=26-30,ord=3,ord2=3,type=2,col1=BMI 26-30,dsn=bmi)
%stat(dcn=VIT_L3_BMI,var=sum,cat=31-35,ord=3,ord2=4,type=2,col1=BMI >=31,dsn=bmi)

*- Bring everything together -*;
data print_ic;
     merge dummy tbl;
     by ord ord2;
run;

*- Produce output -*;
%macro print_ic;
    %let _ftn=IC;
    %let _hdr=Table IC;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ic split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 col3 col4 ord2;

         define ord2     /display noprint;
         define col1     /display flow " " style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         define col2     /display flow "Result" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /display flow "%" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=43%] style(column)=[just=left];

         compute ord2;
            if ord2^=0 then CALL DEFINE('_c1_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
    run;

    ods listing;
%mend print_ic;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* CHART IA VITAL MEASUREMENTS
********************************************************************************;
%macro chart_ia;

*- Create macro variable for output name -*;
%let _hdr=Chart IA;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="VIT_L3_MDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c1data',compress(put(sum,16.)));
     call symput('_c1std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c1data^= and &_c1data^=0 and &_c1std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_1a;
         if _n_=1 then set standard(keep=mean std);
         set data ;
    
         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_1a nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_1a_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
    run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("MEASURE_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_1a;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_1a_anno*/;
         format start chart.;
    run;
    quit;
%end;
%mend chart_ia;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE ID RECORDS, PATIENTS, ENCOUNTERS, AND DATE RANGES BY TABLE
********************************************************************************;

*- Create table formats -*;
proc format;
     invalue rowfmt
       'DEMOGRAPHIC'=1
       'ENROLLMENT'=2
       'DEATH'=6
       'ENCOUNTER'=3
       'DIAGNOSIS'=4
       'PROCEDURES'=5
       'VITAL'=7
       'LAB_RESULT_CM'=8
       'PRESCRIBING'=9
       'DISPENSING'=10
       'CONDITION'=11
       'DEATH_CAUSE'=12
       'PRO_CM'=13
       'PROVIDER'=14
       'MED_ADMIN'=15
       'OBS_CLIN'=16
       'OBS_GEN'=17
       'PCORNET_TRIAL'=18
       'HASH_TOKEN'=19
       'LDS_ADDRESS_HISTORY'=20
       'IMMUNIZATION'=21
       'LAB_HISTORY'=22
        ;
run;

*- Subset and Re-categorize encounter type -*;
data data;
     set dc_normal(where=(datamartid=%upcase("&dmid") and scan(dc_name,3,'_')="N" and variable in 
                  ("PATID" "ENCOUNTERID" "ENROLLID" "DIAGNOSISID" "PROCEDURESID" "VITALID" "LAB_RESULT_CM_ID" "PRESCRIBINGID" "DISPENSINGID" 
                   "CONDITIONID" "DEATHCID" "PRO_CM_ID" "PROVIDERID" "MEDADMNID" "OBSCLINID" "OBSGENID" "IMMUNIZATIONID"
                   "LABHISTORYID")));
     if (table="DISPENSING" and variable="PRESCRIBINGID") or (table in ("DIAGNOSIS" "PROCEDURES") and variable="PROVIDERID") or 
         dc_name in ("LAB_L3_N_5Y") or (table="IMMUNIZATION" and variable="PROCEDURESID") then delete;

     if variable not in ("PATID" "ENCOUNTERID") then variable="ID";

     if statistic in ("ALL_N" "NULL_N") then stat=1;
     else stat=2;

     if resultn=. then resultn=0;
     if table="TRIAL" then table="PCORNET_TRIAL";
     else if table="HASH" then table="HASH_TOKEN";
     else if table="LDSADRS" then table="LDS_ADDRESS_HISTORY";
     else if table="LABHIST" then table="LAB_HISTORY";

     keep table variable stat resultn dc_name;
run;

*- Recalculate count and percent -*;
proc means data=data nway noprint;
     class variable stat table dc_name;
     var resultn;
     output out=stats sum=sum;
run;

proc sort data=dc_normal(keep=datamartid dc_name table statistic result: variable)
     out=cross(drop=datamartid dc_name);
     by table;
     where datamartid=%upcase("&dmid") and dc_name="XTBL_L3_DATES" and 
        variable not in ("DISCHARGE_DATE" "ENR_END_DATE" "DX_DATE" "PX_DATE" 
           "RESOLVE_DATE" "SPECIMEN_DATE" "LAB_ORDER_DATE" "RX_END_DATE" 
           "RX_START_DATE" "ONSET_DATE" "MEDADMIN_STOP_DATE" "ADDRESS_PERIOD_START"
           "ADDRESS_PERIOD_END" "VX_ADMIN_DATE" "VX_EXP_DATE" "OBSCLIN_STOP_DATE"
           "OBSGEN_STOP_DATE");
run;

data cross;
     set cross;
     if table="LDSADRS" then table="LDS_ADDRESS_HISTORY";
run;

*- Add number of records -*;
proc sort data=normdata.datamart_all(keep=memname nobs) 
     out=datamart_all(rename=(memname=table)) nodupkey;
     by memname;
run;

*- Bring everything together -*;
data print_id;
     length col0-col8 $50;
     merge stats(in=t where=(variable="ID" and stat=1 and table not in ("DEMOGRAPHIC" "DEATH" "ENCOUNTER")) rename=(sum=records))
           stats(in=t where=(variable="PATID" and stat=1 and table in ("DEMOGRAPHIC" "DEATH" "HASH_TOKEN" "LDS_ADDRESS_HISTORY" 
                        "PCORNET_TRIAL")) rename=(sum=records))
           stats(in=t where=(variable="ENCOUNTERID" and stat=1 and table in ("ENCOUNTER")) rename=(sum=records))
           stats(in=p where=(variable="PATID" and stat=2) rename=(sum=patients))
           stats(in=e where=(variable="ENCOUNTERID" and stat=2) rename=(sum=encounters))
           cross(in=c where=(statistic="P5") rename=(resultc=p5))
           cross(in=c where=(statistic="P95") rename=(resultc=p95))
           datamart_all
           dc_normal(keep=datamartid dc_name resultc table rename=(resultc=refresh_dtc)
                     where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_REFRESH_DATE"))
     ;
     by table;
     if t or p or e or c;

     if variable in ("DISCHARGE_DATE" "ENR_END_DATE" "PX_DATE" "SPECIMEN_DATE" 
                     "LAB_ORDER_DATE" "RX_END_DATE" "RX_START_DATE" "ONSET_DATE"
                     "RESOLVE_DATE" "MEDADMIN_STOP_DATE" "ADDRESS_PERIOD_START" 
                     "ADDRESS_PERIOD_END" "VX_ADMIN_DATE" "VX_EXP_DATE") then delete;

     ord=input(table,rowfmt.);
     col0=table;
     if nobs^="BT" then nobsn=input(nobs,16.);
     else if nobs="BT" then nobsn=.t;
     if nobsn^=.t then col1=strip(put(nobsn,comma16.));
     else col1=strip(put(nobsn,threshold.));
     col2=strip(refresh_dtc);
     if col2="N/A" then threshold=1;
     if records^=.t then col3=strip(put(records,comma16.));
     else col3=strip(put(records,threshold.));
     if p and patients^=.t then col4=strip(put(patients,comma16.));
     else if p then col4=strip(put(patients,threshold.));
     else col4="---";
     if encounters^=. then col5=strip(put(encounters,comma16.));
     else if e then col5=strip(put(.t,threshold.));
     else col5="---";
     if table^ in ('DEATH_CAUSE' 'HASH_TOKEN' 'LDS_ADDRESS_HISTORY' 'PROVIDER' 'PCORNET_TRIAL' 'LAB_HISTORY') then do;
        col6=variable;
        if strip(col6)="ENR_START_DATE" then col6="ENR_START_DATE^";
        if p5^=" " then col7=scan(p5,1,'_') || "_" || put(input(scan(p5,2,'_'),2.),mnth.);
        if p95^=" " then col8=scan(p95,1,'_') || "_" || put(input(scan(p95,2,'_'),2.),mnth.);
        col9=strip(trim(dc_name) || "; XTBL_L3_DATES");
     end;
     else if table in ('DEATH_CAUSE' 'HASH_TOKEN' 'LDS_ADDRESS_HISTORY' 'PROVIDER' 'PCORNET_TRIAL' 'LAB_HISTORY') then do;
        col6="---";
        col7="---";
        col8="---";
        col9=strip(trim(dc_name));
     end;

     if ord<=9 then pg=1;
     else if ord<=18 then pg=2;
     else pg=3;
    
     keep pg ord col: threshold;
run;

proc sort data=print_id;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_id;
     length edc_table_s_ $10;
     set print_id(keep=threshold);
     if threshold=1;
     row=1.18;
     exception=1;
     edc_table_s_="Table ID";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_id nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_id;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_id;
    %let _ftn=ID;
    %let _hdr=Table ID;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_id split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column threshold pg col0 ('All Data|___________________' col1 col2)
                ('Characterized Data|_____________________________________' col3 col4 col5)
                ('Characterized Data Range|______________________________________' col6 col7 col8) 
                col9;
         where pg=1;

         define threshold     /display noprint;
         define pg            /order noprint;
         define col0          /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col1          /display flow "Records" style(header)=[just=center cellwidth=8.0%] style(column)=[just=center];
         define col2          /display flow "Refresh|Date" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col3          /display flow "Records" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col4          /display flow "Patients" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col5          /display flow "Encounters" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col6          /display flow "Field name" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col7          /display flow "5th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col8          /display flow "95th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col9          /display flow "Source Tables" style(header)=[just=left cellwidth=14%] style(column)=[just=left];
         compute col2;
            if threshold=1 then call define(_col_, "style", "style=[color=red]");
         endcomp;
    run;

    *- Determine how many page in report -*;
    proc means data=print_id noprint;
         var pg;
         output out=_id_pg max=max_pg;
    run;
        
    data _null_;
         set _id_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
    
        proc report data=print_id split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column threshold pg col0 ('All Data|___________________' col1 col2)
                ('Characterized Data|_____________________________________' col3 col4 col5)
                ('Characterized Data Range|______________________________________' col6 col7 col8) 
                col9;
             where pg=&_pg;

             define threshold     /display noprint;
             define pg            /order noprint;
             define col0          /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
             define col1          /display flow "Records" style(header)=[just=center cellwidth=8.0%] style(column)=[just=center];
             define col2          /display flow "Refresh|Date" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
             define col3          /display flow "Records" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
             define col4          /display flow "Patients" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
             define col5          /display flow "Encounters" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
             define col6          /display flow "Field name" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
             define col7          /display flow "5th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
             define col8          /display flow "95th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
             define col9          /display flow "Source Tables" style(header)=[just=left cellwidth=14%] style(column)=[just=left];
             compute col2;
                if threshold=1 then call define(_col_, "style", "style=[color=red]");
             endcomp;
        run;
    %end;

    ods listing;
%mend print_id;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IE RECORDS PER TABLE BY ENCOUNTER TYPE 
********************************************************************************;

*- Subset and Re-categorize encounter type -*;
data data;
     length cat $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N"
         and dc_name in ("DIA_L3_ENCTYPE" "PRO_L3_ENCTYPE" "ENC_L3_ENCTYPE")
         and category^="Values outside of CDM specifications"));

     * re-categorize encounter type *;
     if category in ("NI" "UN" "OT" "NULL or missing") then cat="Missing, NI, UN or OT";
     else cat=category;

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;
     output;
     cat="TOTAL";
     output;
     keep dc_name cat resultn bt_flag;
run;

*- Summarize counts on new encounter type -*;
proc means data=data missing nway noprint;
     class dc_name cat bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

proc sort data=stats;
     by dc_name;
run;

data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name cat;
run;

data stats;
     merge stats
           stats(keep=dc_name cat sum rename=(cat=totcat sum=denom) where=(totcat="TOTAL"))
     ;
     by dc_name;
run;

proc sort data=stats;
     by cat;
run;

*- Bring everything together -*;
data print_ie(keep=ord col: enc_fl);
     length col1-col7 $50;
     if _n_=1 then set stats(keep=dc_name cat sum where=(scan(dc_name,1,'_')="ENC" and cat_ei='EI') 
                             rename=(sum=encounter_ei cat=cat_ei));
     merge stats(where=(scan(dc_name,1,'_')="DIA") rename=(sum=diagnosis denom=dia_den bt_flag=bt_dia))
           stats(where=(scan(dc_name,1,'_')="PRO") rename=(sum=procedure denom=pro_den bt_flag=bt_pro))
           stats(where=(scan(dc_name,1,'_')="ENC") rename=(sum=encounter denom=enc_den bt_flag=bt_enc))
     ;
     by cat;

     ord=put(cat,$svar.);
     col1=strip(put(cat,$etype.));

     if encounter=. and bt_enc="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(encounter,comma16.));
     if cat^="TOTAL" and encounter>0 and enc_den>0 then col3=strip(put((encounter/enc_den)*100,16.1));

     if diagnosis=. and bt_dia="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(diagnosis,comma16.));
     if cat^="TOTAL" and diagnosis>0 and dia_den>0 then col5=strip(put((diagnosis/dia_den)*100,16.1));

     if procedure=. and bt_pro="*" then col6=strip(put(.t,threshold.));
     else col6=strip(put(procedure,comma16.));
     if cat^="TOTAL" and procedure>0 and pro_den>0 then col7=strip(put((procedure/pro_den)*100,16.1));

     if (ord='1' and .<encounter<=0) or (ord in ('2' '4') and .<encounter<=0 and .<encounter_ei<=0) then enc_fl=1;

     output;
     if cat="TOTAL" then do;
        ord="9";
        col1="Source table";
        col2="ENC_L3_ENCTYPE";
        col3=" ";
        col4="DIA_L3_ENCTYPE";
        col5=" ";
        col6="PRO_L3_ENCTTPE";
        col7=" ";
        output;
     end;
run;

proc sort data=print_ie;
     by ord;
run;

*- Produce output -*;
%macro print_ie;
    %let _ftn=IE;
    %let _hdr=Table IE;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ie split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column enc_fl col1 
                ("ENCOUNTER|_______________________________" col2 col3) 
                ("DIAGNOSIS|_______________________________" col4 col5)
                ("PROCEDURES|______________________________" col6 col7);

         define enc_fl   /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=32%] style(column)=[just=left];
         define col2     /display flow "N" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /display flow "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col4     /display flow "N" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col5     /display flow "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col6     /display flow "N" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col7     /display flow "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
    run;    

    ods listing;
%mend print_ie;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IB TREND IN ENCOUNTERS BY ADMIT DATE AND ENCOUNTER TYPE
********************************************************************************;
%macro chart_ib_cat(cat1,cat2,cat3,c1,c2,c3,gcont);

%let _hdr=Chart IB;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="ENC_L3_ENCTYPE_ADATE_YM" and category in ("&cat1" "&cat2" "&cat3")
         and cross_category^="NULL or missing"));

     if resultn=.t then resultn=0;

     xdate=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));
    
     keep category xdate resultn;
run;

proc sort data=data;
     by category xdate;
run;

data chart_months_enc;
     length category $255;
     set chart_months(keep=xdate);
     by xdate;
     do category = "&cat1", "&cat2", "&cat3";
        output;
     end;
run;

proc sort data=chart_months_enc;
     by category xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months_enc(in=a) data;
     by category xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     class category;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard end=eof;
     retain totalsum 0;
     totalsum=totalsum+sum;
     if eof then do;
         call symput('_c2data',compress(put(totalsum,16.)));
         call symput('_c2std',compress(put(std,best.)));
     end;
run;

*- If non-zero results exists, produce chart -*;
%macro c2data;
%if &_c2data^= and &_c2data^=0 and &_c2std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ib;
         merge data standard(keep=category mean std);
         by category;
    
         retain count;
         if first.category then count=0;
         count= count+1;
    
         if std>0 and resultn^=. and mean^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ib nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
/*  data chart_ib_anno;
         length text $50;
         set standard;
         by category;

         * stats *;
         function='label'; xsys='3'; ysys='3'; position='6'; size=1.2; color="black"; x=10;

         if mean>0 then text=strip(category) || " COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2));
         else text=strip(category) || " COV = not calculable";

         if category in ("AV" "ED") then y=95;
         else if category in ("OA" "EI") then y=92;
         else if category in ("IS" "IP") then y=89;
         else if category in ("IC") then y=86;
         else if category in ("OS") then y=83;
         output;
    run;
*/
    *- Produce output -*;
    goptions reset=all dev=png300 rotate=landscape gsfmode=replace htext=0.9 
             ftext='Albany AMT' hsize=9 vsize=5.5;

    ods listing close;
    ods proclabel "&_ttl1 &gcont";
    title1 justify=left "&_ttl1 &gcont";
    title2 justify=left h=2.5 "&_ttl2";
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("ADMIT_DATE")
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    legend1 label=none noframe;

    symbol1  c=&c1 line=1 v=NONE interpol=join;
    symbol2  c=&c2 line=1 v=NONE interpol=join;
    symbol3  c=&c3 line=1 v=NONE interpol=join;

    proc gplot data=chart_ib;
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid /*annotate=chart_ib_anno*/;
         format category $cat. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c2data;
%c2data;

%mend chart_ib_cat;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IC TREND IN INSTITUTIONAL ENCOUNTERS BY DISCHARGE DATE AND ENCOUNTER TYPE
********************************************************************************;
%macro chart_ic_cat(cat1,cat2,cat3,c1,c2,c3);

%let _hdr=Chart IC;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Create formats -*;
/*proc format;
     value $cat
        'EI'='EI (ED to IP Stay)'
        'IP'='IP (Inpatient Hospital Stay)'
        'IS'='IS (Non-acute Institutional Stay)'
        ;
run;*/

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="ENC_L3_ENCTYPE_DDATE_YM" and category in ("&cat1", "&cat2", "&cat3")
         and cross_category^="NULL or missing"));

     if resultn=.t then resultn=0;

     xdate=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));

     keep category xdate resultn;
run;
    
proc sort data=data;
     by category xdate;
run;

data chart_months_enc;
     length category $255;
     set chart_months(keep=xdate);
     by xdate;
     do category = "&cat1", "&cat2", "&cat3";
        output;
     end;
run;

proc sort data=chart_months_enc;
     by category xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months_enc(in=a) data;
     by category xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     class category;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard end=eof;
     retain totalsum 0;
     totalsum=totalsum+sum;
     if eof then do;
         call symput('_c3data',compress(put(totalsum,16.)));
         call symput('_c3std',compress(put(std,best.)));
     end;
run;

*- If non-zero results exists, produce chart -*;
%macro c3data;
%if &_c3data^= and &_c3data^=0 and &_c3std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ic;
         merge data standard(keep=category mean std);
         by category;
    
         retain count;
         if first.category then count=0;
         count= count+1;
    
         if std>0 and resultn^=. and mean^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ic nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ic_anno;
         length text $50;
         set standard;
         by category;

         * stats *;
         function='label'; xsys='3'; ysys='3'; position='6'; size=1.2; color="black"; 
         x=10; y=95;

         if mean>0 then text=strip(category) || " COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2));
         else text=strip(category) || " COV = not calculable";

         if category in ("EI") then y=95;
         else if category in ("IP") then y=92;
         else if category in ("IS") then y=89;
         output;
    run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("DISCHARGE_DATE")
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    legend1 label=none noframe;

    symbol1  c=&c1 line=1 v=NONE interpol=join;
    symbol2  c=&c2 line=1 v=NONE interpol=join;
    symbol3  c=&c3 line=1 v=NONE interpol=join;

    proc gplot data=chart_ic;
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid /*annotate=chart_ic_anno*/;
         format category $cat. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c3data;
%c3data;

%mend chart_ic_cat;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IF DATE OBFUSCATION OR IMPUTATION
********************************************************************************;

proc format;
    value $dmgmt
      '01'='01 (No imputation or obfuscation)'
      '02'='02 (Imputation for incomplete dates)'
      '03'='03 (Date obfuscation)'
      '04'='04 (Both imputation and obfuscation)'
      'NI'='NI (No information)'
      'UN'='UN (Unknown)'
      'OT'='OT (Other)'
      'NULL or missing display'='NULL or missing'
      ;
run;

*- Subset -*;
data print_if(keep=col:);
     length col1 col2 col3c col4 $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_METADATA"
         and variable in ("BIRTH_DATE_MGMT" "ENR_START_DATE_MGMT" "ENR_END_DATE_MGMT"
         "ADMIT_DATE_MGMT" "DISCHARGE_DATE_MGMT" "PX_DATE_MGMT" "MEASURE_DATE_MGMT"
         "RX_ORDER_DATE_MGMT" "RX_START_DATE_MGMT" "RX_END_DATE_MGMT" "DISPENSE_DATE_MGMT"
         "LAB_ORDER_DATE_MGMT" "SPECIMEN_DATE_MGMT" "RESULT_DATE_MGMT" "ONSET_DATE_MGMT"
         "REPORT_DATE_MGMT" "RESOLVE_DATE_MGMT" "PRO_DATE_MGMT" "DEATH_DATE_MGMT" 
         "MEDADMIN_START_DATE_MGMT" "MEDADMIN_STOP_DATE_MGMT" "OBSCLIN_START_DATE_MGMT" 
         "OBSCLIN_STOP_DATE_MGMT" "OBSGEN_START_DATE_MGMT" "OBSGEN_STOP_DATE_MGMT" 
         "DX_DATE_MGMT" "ADDRESS_PERIOD_START_MGMT" "ADDRESS_PERIOD_END_MGMT" 
         "VX_RECORD_DATE_MGMT" "VX_ADMIN_DATE_MGMT" "VX_EXP_DATE_MGMT")));

     col1="HARVEST";
     col2=variable;
     if resultc in ('03','04') then col3flag=1;
     else col3flag=0;
     col3c=put(resultc,$dmgmt.);
     col4=dc_name;
run;

*- Produce output -*;
%macro print_if;
    %let _ftn=IF;
    %let _hdr=Table IF;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_if split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col3c col3flag col1 col2 col3 col4;

         define col3c    /display noprint;
         define col3flag /display noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col3     /computed flow "DATE_MGMT" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
         compute col3 / char length=50;
            if col3flag=1 then do;
                col3=col3c;
                call define(_col_, "style", "style=[color=orange]");
            end;
            else do;
                col3=col3c;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_if;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IG LAB RESULT RECORDS FOR COMMON LAB MEASURES
********************************************************************************;

*- Subset and create ordering variable -*;
data data include_edc(keep=ord);
     length ord $200;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("LAB_L3_DCGROUP")
                and statistic in ("DISTINCT_PATID_N" "INCLUDE_EDC")
                and category^='Unassigned'));

     * convert slash, space, and dash to underbar for merging with ref file *;
     ord=upcase(translate(strip(category),"_","/- "));
     ord=translate(strip(ord),"_"," ");

     keep ord dc_name category statistic resultn;
     if statistic^="INCLUDE_EDC" then output data;
     else if statistic="INCLUDE_EDC" and resultn=1 then output include_edc;
run;

proc means data=data nway noprint;
     class ord category statistic;
     var resultn;
     output out=stats sum=sum;
run;

proc sort data=include_edc;
     by ord;
run;

*- Macro to prevent open code -*;
%macro _plat;
    proc sort data=lab_volume_ref(keep=pedi dc_lab_group lower_bound)
         out=lower_volume(rename=(dc_lab_group=ord) drop=pedi);
         by dc_lab_group;
         %if &_pedyes=1 %then %do;
             where pedi=1;
         %end;
         %else %do;
             where pedi=0;
         %end;        
    run;
%mend _plat;
%_plat;

*- Bring everything together and restrict to those parameters to be included in the edc -*;
data print_ig;
     length col1 col2 col3 col4c col5 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     merge stats(where=(statistic in ("DISTINCT_PATID_N")) rename=(sum=dist_patid))
           include_edc(in=i)
           lower_volume
     ;
     by ord;
     if i;

     * track BT values but set missing to zero *;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;
    
     * create column variables and output *;
     col1=strip(category);
     if bt_dist_patid="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(dist_patid,comma16.));
     if dist_patid>0 and denom>0 then do;
        col3n=(dist_patid/denom)*100;
        col3=strip(put(col3n,5.1));
     end;
     else col3="0";
     if dist_patid=0 or (lower_bound^=. and col3n<lower_bound) then do;
        col4flag=1;
        col4c="Yes";
     end;
     else do;
        col4flag=0;
        col4c="No";
     end;
     col5="LAB_L3_DCGROUP;ENC_L3_N";
    
     keep ord col:;
run;

data print_ig;
     set print_ig;
     by ord;
    
     if _n_<=23 then pg=1;
     else if 24<=_n_<=46 then pg=2;
     else pg=3;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ig;
     length edc_table_s_ $10;
     set print_ig(keep=col4flag);
     if col4flag=1;
     row=3.13;
     exception=1;
     edc_table_s_="Table IG";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ig nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ig;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ig;
    %let _ftn=IG;
    %let _hdr=Table IG;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ig split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col4c col4flag col1 col2 col3 col4 col5;
         where pg=1;

         define pg       /order noprint;
         define col4c    /display noprint;
         define col4flag /display noprint;
         define col1     /display flow "DC_LAB_GROUP" style(header)=[just=left cellwidth=23%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /computed flow "Data Check 3.13 Exception"  style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col5     /display flow "Source tables" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         compute col4 / char length=3;
            if col4flag=1 then do;
                col4=col4c;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4c;
            end;
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ig split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col4c col4flag col1 col2 col3 col4 col5 ;
         where pg>1;

         define pg       /order noprint;
         define col4c    /display noprint;
         define col4flag /display noprint;
         define col1     /display flow "DC_LAB_GROUP" style(header)=[just=left cellwidth=23%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /computed flow "Data Check 3.13 Exception"  style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col5     /display flow "Source tables" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         compute col4 / char length=3;
            if col4flag=1 then do;
                col4=col4c;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4c;
            end;
         endcomp;

         break after pg / page;
    run;

    ods listing;
%mend print_ig;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IH DIAGNOSIS RECORDS FOR COMMON GROUPS
********************************************************************************;

*- Subset and create ordering variable -*;
data data;
     length ord $200;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("DIA_L3_DCGROUP")
                and statistic in ("DISTINCT_PATID_N")
                and category^='Unassigned'));

     ord=upcase(category);

     keep ord dc_name category statistic resultn;
run;

proc means data=data nway noprint;
     class ord category statistic;
     var resultn;
     output out=stats sum=sum;
run;

*- Bring everything together and restrict to those parameters to be included in the edc -*;
data print_ih;
     length col1 $100 col2-col4 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     set stats(where=(statistic in ("DISTINCT_PATID_N")) rename=(sum=dist_patid));
     by ord;

     * track BT values but set missing to zero *;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;
    
     * create column variables and output *;
     col1=strip(category);
     if bt_dist_patid="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(dist_patid,comma16.));
     if dist_patid>0 and denom>0 then col3=strip(put((dist_patid/denom)*100,5.1));
     col4="DIA_L3_DCGROUP;ENC_L3_N";
    
     keep ord col:;
run;

data print_ih;
     set print_ih;
     by ord;
    
     if _n_<=23 then pg=1;
     else if 24<=_n_<=46 then pg=2;
     else pg=3;
run;

*- Produce output -*;
%macro print_ih;
    %let _ftn=IH;
    %let _hdr=Table IH;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ih split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4;
         where pg=1;

         define pg       /order noprint;
         define col1     /display flow "DC_DX_GROUP" style(header)=[just=left cellwidth=43%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ih split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4;
         where pg>1;

         define pg       /order noprint;
         define col1     /display flow "DC_DX_GROUP" style(header)=[just=left cellwidth=43%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];

         break after pg / page;
    run;

    ods listing;
%mend print_ih;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE II PROCEDURES RECORDS FOR COMMON GROUPS
********************************************************************************;

*- Subset and create ordering variable -*;
data data;
     length ord $200;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("PRO_L3_DCGROUP")
                and statistic in ("DISTINCT_PATID_N")
                and category^='Unassigned'));

     ord=upcase(category);

     keep ord dc_name category statistic resultn;
run;

proc means data=data nway noprint;
     class ord category statistic;
     var resultn;
     output out=stats sum=sum;
run;

*- Bring everything together and restrict to those parameters to be included in the edc -*;
data print_ii;
     length col1 $100 col2-col4 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     set stats(where=(statistic in ("DISTINCT_PATID_N")) rename=(sum=dist_patid));
     by ord;

     * track BT values but set missing to zero *;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;
    
     * create column variables and output *;
     col1=strip(category);
     if bt_dist_patid="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(dist_patid,comma16.));
     if dist_patid>0 and denom>0 then col3=strip(put((dist_patid/denom)*100,5.1));
     col4="PRO_L3_DCGROUP;ENC_L3_N";
    
     keep ord col:;
run;

data print_ii;
     set print_ii;
     by ord;
    
     if _n_<=23 then pg=1;
     else if 24<=_n_<=46 then pg=2;
     else pg=3;
run;

*- Produce output -*;
%macro print_ii;
    %let _ftn=II;
    %let _hdr=Table II;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ii split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4;
         where pg=1;

         define pg       /order noprint;
         define col1     /display flow "DC_PX_GROUP" style(header)=[just=left cellwidth=43%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ii split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4;
         where pg>1;

         define pg       /order noprint;
         define col1     /display flow "DC_PX_GROUP" style(header)=[just=left cellwidth=43%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] 
                                    style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];

         break after pg / page;
    run;

    ods listing;
%mend print_ii;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART ID TREND IN LAB RESULTS BY RESULT DATE, PAST 5 YEARS
********************************************************************************;

%macro chart_id;

*- Create macro variable for output name -*;
%let _hdr=Chart ID;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="LAB_L3_RDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

proc sort data=chart_months(keep=xdate) out=chart_months_lab;
     by xdate;
run;

*- Check to see if any obs after subset -*;
proc sql noprint;
     select count(xdate) into :nobs from data;
quit;

*- If obs, continue, else stop -*;
%if &nobs^=0 %then %do;

    *- Bring five year data and actual data together -*;
    data data;
         format xdate date9.;
         merge chart_months_lab(in=a) data;
         by xdate;
         if a;
    run;

    *- Calculate mean and std dev -*;
    proc means data=data nway noprint;
         var resultn;
         output out=standard mean=mean std=std sum=sum;
    run;

    *- Create macro variable to test if any non-zero results exist in data -*;
    data _null_;
         set standard end=eof;
         retain totalsum 0;
         totalsum=totalsum+sum;
         if eof then do;
             call symput('_c4data',compress(put(totalsum,16.)));
             call symput('_c4std',compress(put(std,best.)));
         end;
    run;

    *- If non-zero results exists, produce chart -*;
    %macro c4data;
    %if &_c4data^= and &_c4data^=0 and &_c4std^= %then %do;

        *- Calculate deviations from the mean -*;
        data chart_id;
             if _n_=1 then set standard(keep=mean std);
             set data;
    
             retain count 0;
             count=count+1;
    
             if std>0 and resultn^=. and mean^=. then deviations=(resultn-mean)/std;
             else if std=0 then deviations=0;

             if mean<0 and std<0 then delete;

             if deviations^=. then v_deviations=abs(ceil(deviations))+1;
        run;

        *- Determine the maximum value for the vertical axis -*;
        proc means data=chart_id nway noprint;
             var v_deviations;
             output out=vertaxis max=max;
        run;

        data _null_;
             set vertaxis;
             call symput('lvaxis',compress(put(max*-1,8.)));
             call symput('uvaxis',compress(put(max,8.)));
        run;

        *- Create annotate dataset for COV -*;
        data chart_id_anno;
             length text $50;
             set standard;

             * stats *;
             function='label'; xsys='3'; ysys='3'; position='6'; size=1.2; color="black"; x=10; y=95;

             if mean>0 then text=" COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2));
             else text=" COV = not calculable";

             output;
        run;

        *- Produce output -*;
        ods listing close;
        ods proclabel "&_ttl1";
        title1 justify=left "&_ttl1";
        title2 justify=left h=2.5 "&_ttl2";
        symbol1  c=red line=1 v=NONE interpol=join;
        symbol2  c=blue line=1 v=NONE interpol=join;
        symbol3  c=green line=1 v=NONE interpol=join;
        footnote " ";

        axis1 order=(1 to 61 by 6) label=("RESULT_DATE" h=1.5)
              minor=none
              offset=(0.5,1);

        axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
              minor=none
              offset=(0.5,.75);

        proc gplot data=chart_id;
             plot deviations*count / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_id_anno*/;
             format count chart.;
             where count^=. and deviations^=.;
        run;
        quit;
    %end;
    %mend c4data;
    %c4data;
%end;    

%mend chart_id;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IE TREND IN PRESCRIBED MEDICATIONS BY RX START DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ie;

*- Create macro variable for output name -*;
%let _hdr=Chart IE;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="PRES_L3_ODATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c5data',compress(put(sum,16.)));
     call symput('_c5std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c5data^= and &_c5data^=0 and &_c5std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ie;
         if _n_=1 then set standard(keep=mean std);
         set data ;
    
         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ie nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ie_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
    run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("RX_ORDER_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ie;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_ie_anno*/;
         format start chart.;
         where start^=. and deviations^=.;
    run;
    quit;
%end;
%mend chart_ie;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IF TREND IN DISPENSED MEDICATIONS BY DISPENSE DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_if;

*- Create macro variable for output name -*;
%let _hdr=Chart IF;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="DISP_L3_DDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c6data',compress(put(sum,16.)));
     call symput('_c6std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c6data^= and &_c6data^=0 and &_c6std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_if;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_if nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_if_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("DISPENSE_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_if;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_if_anno*/;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_if;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IG TREND IN MEDICATION ADMINISTRATION RECORDS BY START DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ig;

*- Create macro variable for output name -*;
%let _hdr=Chart IG;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="MEDADM_L3_SDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c7data',compress(put(sum,16.)));
     call symput('_c7std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c7data^= and &_c7data^=0 and &_c7std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ig;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ig nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ig_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("START_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ig;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_ig_anno*/;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_ig;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IH TREND IN CONDITION RECORDS BY REPORT DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ih;

*- Create macro variable for output name -*;
%let _hdr=Chart IH;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="COND_L3_RDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c8data',compress(put(sum,16.)));
     call symput('_c8std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c8data^= and &_c8data^=0 and &_c8std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ih;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ih nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ih_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("REPORT_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ih;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_ih_anno*/;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_ih;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART II TREND IN DEATH RECORDS BY REPORT DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ii_cat(cat1,cat2,cat3,cat4,cat5,c1,c2,c3,c4,c5);

*- Create macro variable for output name -*;
%let _hdr=Chart II;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Create formats -*;
proc format;
     value $cat_dth
        'L'='Other, locally defined'
        'N'='National Death Index'
        'D'='Social Security'
        'S'='State Death files'
        'T'='Tumor data'
        ;
run;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="DEATH_L3_SOURCE_YM" and cross_category in ("&cat1" "&cat2" "&cat3" "&cat4" "&cat5")
         and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep cross_category xdate resultn;
run;

proc sort data=data(rename=(cross_category=category));
     by category xdate;
run;

data chart_months_enc;
     length category $255;
     set chart_months(keep=xdate);
     by xdate;
     do category = "&cat1", "&cat2", "&cat3", "&cat4", "&cat5";
        output;
     end;
run;

proc sort data=chart_months_enc;
     by category xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months_enc(in=a) data;
     by category xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     class category;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard end=eof;
     retain totalsum 0;
     totalsum=totalsum+sum;
     if eof then do;
         call symput('_c9data',compress(put(totalsum,16.)));
     end;
run;

*- If non-zero results exists, produce chart -*;
%macro c9data;
%if &_c9data^= and &_c9data^=0 %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ii;
         merge data standard(keep=category mean std);
         by category;
    
         retain count;
         if first.category then count=0;
         count= count+1;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ii nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ii_anno;
         length text $50;
         set standard;
         by category;

         * stats *;
         function='label'; xsys='3'; ysys='3'; position='6'; size=1.2; color="black"; x=10;

         if mean>0 then text=strip(put(category,$cat_dth.)) || " COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2));
         else text=strip(put(category,$cat_dth.)) || " COV = not calculable";

         if category in ("L") then y=95;
         else if category in ("N") then y=92;
         else if category in ("D") then y=89;
         else if category in ("S") then y=86;
         else if category in ("T") then y=83;
         output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("DEATH_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ii;
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid /*annotate=chart_ii_anno*/;
         format category $cat_dth. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c9data;
%c9data;

%mend chart_ii_cat;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IJ TREND IN IMMUNIZATION RECORDS BY REPORT DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ij;

*- Create macro variable for output name -*;
%let _hdr=Chart IJ;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="IMMUNE_L3_RDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c10data',compress(put(sum,16.)));
     call symput('_c10std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c10data^= and &_c10data^=0 and &_c10std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ij;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ij nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ij_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("VX_RECORD_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ij;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid /*annotate=chart_ij_anno*/;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_ij;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IK TREND IN CLINICAL OBSERVATION RECORDS BY REPORT DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_ik;

*- Create macro variable for output name -*;
%let _hdr=Chart IK;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="OBSCLIN_L3_SDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c11data',compress(put(sum,16.)));
     call symput('_c11std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c11data^= and &_c11std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_ik;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_ik nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_ik_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("OBSCLIN_START_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_ik;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid ;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_ik;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* CHART IL TREND IN GENERAL OBSERVATION RECORDS BY REPORT DATE, PAST 5 YEARS
********************************************************************************;
%macro chart_il;

*- Create macro variable for output name -*;
%let _hdr=Chart IL;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="OBSGEN_L3_SDATE_YM" and category^="NULL or missing"));

     * set below threshold to zero *;
     if resultn=.t then resultn=0;

     * create SAS data variable *;
     xdate=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     keep xdate resultn;
run;

proc sort data=data;
     by xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months(in=a keep=xdate start) data;
     by xdate;
     if a;
run;

*- Calculate mean and std dev -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard mean=mean std=std sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data -*;
data _null_;
     set standard;
     call symput('_c12data',compress(put(sum,16.)));
     call symput('_c12std',compress(put(std,best.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_c12data^= and &_c12std^= %then %do;

    *- Calculate deviations from the mean -*;
    data chart_il;
         if _n_=1 then set standard(keep=mean std);
         set data ;

         if std^=0 and resultn^=. then deviations=(resultn-mean)/std;
         else if std=0 then deviations=0;

         if mean<=0 and std<=0 then delete;

         if deviations^=. then v_deviations=abs(ceil(deviations))+1;
    run;

    *- Determine the maximum value for the vertical axis -*;
    proc means data=chart_il nway noprint;
         var v_deviations;
         output out=vertaxis max=max;
    run;

    data _null_;
         set vertaxis;
         call symput('lvaxis',compress(put(max*-1,8.)));
         call symput('uvaxis',compress(put(max,8.)));
    run;

    *- Create annotate dataset for COV -*;
    data chart_il_anno;
         length text $50;
         set standard;

         * stats *;
         function='label'; xsys='3'; ysys='3'; x=10; y=95; position='6'; size=1.2; color="black"; 
            text="COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2)); output;
  run;

    *- Produce output -*;
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    symbol1  c=red line=1 v=NONE interpol=join;
    footnote " ";

    axis1 order=(1 to 61 by 6) label=("OBSGEN_START_DATE" h=1.5)
          minor=none
          offset=(0.5,1);

    axis2 order=&lvaxis to &uvaxis label=(angle=90 "z score")
          minor=none
          offset=(0.5,.75);

    proc gplot data=chart_il;
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid ;
         format start chart.;
         where start^=. and deviations^=.;
    run;
quit;
%end;
%mend chart_il;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IIA PRIMARY KEY DEFINITIONS
********************************************************************************;

*- Create table formats -*;
proc format;
     value pkeys
       1='PATID is unique'
       2='ENROLLID (concatenation of PATID + ENR_START_DATE+ENR_BASIS) is unique'
       3='DEATHID (concatenation of PATID and DEATH_SOURCE) is unique'
       4='ENCOUNTERID is unique'
       5='DIAGNOSISID is unique'
       6='PROCEDURESID is unique'
       7='VITALID is unique'
       8='PRESCRIBINGID is unique'
       9='DISPENSINGID is unique'
      10='LAB_RESULT_CM_ID is unique'
      11='NETWORKID+DATAMARTID is unique'
      12='CONDITIONID is unique'
      13='DEATHCID (concatenation of PATID + DEATH_CAUSE + DEATH_CAUSE_CODE + DEATH_CAUSE_TYPE + DEATH_CAUSE_SOURCE) is unique'
      14='TRIAL_KEY (concatenation of PATID + TRIALID + PARTICIPANTID ) is unique'
      15='PRO_CM_ID is unique'
      16='PROVIDERID is unique'
      17='MEDADMINID is unique'
      18='OBSCLINID is unique'
      19='OBSGENID is unique'
      20='PATID is unique'
      21='ADDRESSID is unique'
      22='IMMUNIZATIONID is unique'
      23='LABHISTORYID is unique'
        ;
run;

*- Macro for each EHR based data row -*;
%macro tbl(dcn=,ord=,var=);
    data tbl&ord(keep=ord col:);
         length col1 col3 col4 $50 col2 $150;
         if _n_=1 then set dc_normal(drop=table where=(datamartid=%upcase("&dmid") and variable="&var"
                and statistic="DISTINCT_N" and dc_name="&dcn") rename=(resultn=distinct));
         set dc_normal(where=(datamartid=%upcase("&dmid") and variable="&var"
                and statistic="ALL_N" and dc_name="&dcn") rename=(resultn=all));

         if table='TRIAL' then table='PCORNET_TRIAL';
         else if table='IMMUNE' then table='IMMUNIZATION';
         else if table='LDSADRS' then table='LDS_ADDRESS_HISTORY';

         ord=&ord;
         col1=strip(table);
         col2=strip(put(ord,pkeys.));
         if distinct=all then col3="No";
         else if distinct^=all then col3="Yes";
         col4=strip(dc_name);
    run;

    proc append base=tbl data=tbl&ord;
    run;
%mend;
%tbl(dcn=DEM_L3_N,ord=1,var=PATID)
%tbl(dcn=ENR_L3_N,ord=2,var=ENROLLID)
%tbl(dcn=DEATH_L3_N,ord=3,var=DEATHID)
%tbl(dcn=ENC_L3_N,ord=4,var=ENCOUNTERID)
%tbl(dcn=DIA_L3_N,ord=5,var=DIAGNOSISID)
%tbl(dcn=PRO_L3_N,ord=6,var=PROCEDURESID)
%tbl(dcn=VIT_L3_N,ord=7,var=VITALID)
%tbl(dcn=PRES_L3_N,ord=8,var=PRESCRIBINGID)
%tbl(dcn=DISP_L3_N,ord=9,var=DISPENSINGID)
%tbl(dcn=LAB_L3_N,ord=10,var=LAB_RESULT_CM_ID)
%tbl(dcn=COND_L3_N,ord=12,var=CONDITIONID)
%tbl(dcn=DEATHC_L3_N,ord=13,var=DEATHCID)
%tbl(dcn=TRIAL_L3_N,ord=14,var=TRIAL_KEY)
%tbl(dcn=PROCM_L3_N,ord=15,var=PRO_CM_ID)
%tbl(dcn=PROV_L3_N,ord=16,var=PROVIDERID)
%tbl(dcn=MEDADM_L3_N,ord=17,var=MEDADMINID)
%tbl(dcn=OBSCLIN_L3_N,ord=18,var=OBSCLINID)
%tbl(dcn=OBSGEN_L3_N,ord=19,var=OBSGENID)
%tbl(dcn=HASH_L3_N,ord=20,var=PATID)
%tbl(dcn=LDSADRS_L3_N,ord=21,var=ADDRESSID)
%tbl(dcn=IMMUNE_L3_N,ord=22,var=IMMUNIZATIONID)
%tbl(dcn=LABHIST_L3_N,ord=23,var=LABHISTORYID)

*- Unique structure for HARVEST data -*;
data harvest(keep=ord col:);
     length col1 col3 col4 $50 col2 $150;
     set dc_normal(where=(datamartid=%upcase("&dmid") and variable="NETWORKID"
                 and dc_name="XTBL_L3_METADATA")) end=eof;
     if eof then do;
        ord=11;
        col1="HARVEST";
        col2=strip(put(ord,pkeys.));
        if _n_=1 then col3="No";
        else col3="Yes";
        col4=strip(dc_name);
        output;
     end;
run;    

*- Bring everything together -*;
data print_iia;
     set tbl harvest;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iia;
     length edc_table_s_ $10;
     set print_iia(keep=col3);
     if col3="Yes";
     row=1.05;
     exception=1;
     edc_table_s_="Table IIA";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iia nodupkey;
     by edc_table_s_ row;
run;

*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iia;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iia;
    %let _ftn=IIA;
    %let _hdr=Table IIA;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 col3 col4;

         define col1     /display flow "Table" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "CDM specifications for primary keys" style(header)=[just=left cellwidth=39%] style(column)=[just=left];
         define col3     /display flow "Exception to specifications" style(header)=[just=center cellwidth=22%] style(column)=[just=center];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=19%] style(column)=[just=left];
         compute col3;
            if col3="Yes" then call define(_col_, "style", "style=[color=red]");
         endcomp;
    run;    

    ods listing;
%mend print_iia;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IIB VALUES OUTSIDE OF CDM SPECIFICATIONS
********************************************************************************;

*- Macro for each type of data row -*;
%macro stat(dcn=,ord=,ord2=,var=);
     if dc_name="&dcn" and variable="&var" and statistic="RECORD_N" and
                category="Values outside of CDM specifications" then do;
        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if resultc^="BT" then col3=put(input(strip(resultc),16.),comma16.);
        else col3=strip(resultc);
        col4=strip(dc_name);
        output;
     end;
%mend stat;

%macro harvest(dcn=,ord=,ord2=,var=);
     if dc_name="&dcn" and variable="&var" and statistic="VALUE" then do;

        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if substr(resultc,1,21)="Values outside of CDM" then col3="1";
        else col3="0";
        col4=strip(dc_name);
        output;
     end;
%mend harvest;

%macro cross(dcn=,ord=,ord2=,var=,cross=);

    proc means data=dc_normal noprint nway;
         class dc_name;
         var resultn;
         output out=cross&ord sum=sum;
         where datamartid=%upcase("&dmid") and dc_name="&dcn" and &cross.variable="&var" 
             and statistic="RECORD_N" and &cross.category="Values outside of CDM specifications";
    run;

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(dc_name) into :nobs from cross&ord;
    quit;

    *- If obs, continue, else create a zero record -*;
    %if &nobs^=0 %then %do;
        data cross&ord(keep=ord: col:);
             length col1-col4 $50;
             set cross&ord;
                 ord=&ord;
                 ord2=&ord2;
                 col1=strip(put(ord,tbl_row.));
                 col2=strip("&var");
                 if sum>=0 then col3=strip(put(sum,comma16.));
                 else if sum=.t then col3=strip(put(sum,threshold.));
                 col4=strip(dc_name);
        run;
    %end;
    %else %do;
        data cross&ord(keep=ord: col:);
             length col1-col4 $50;
             ord=&ord;
             ord2=&ord2;
             col1=strip(put(ord,tbl_row.));
             col2=strip("&var");
             col3="0";
             col4="&dcn";
             output;
        run;
    %end;
    
    proc append base=cross data=cross&ord;
    run;

%mend cross;

*- Non-cross table queries -*;
data tbl(keep=ord: col:);
     length col1-col4 $50;
     set dc_normal(where=(datamartid=%upcase("&dmid")));

     %stat(dcn=DEM_L3_SEXDIST,ord=1,ord2=1,var=SEX)
     %stat(dcn=DEM_L3_HISPDIST,ord=1,ord2=2,var=HISPANIC)
     %stat(dcn=DEM_L3_RACEDIST,ord=1,ord2=3,var=RACE)
     %stat(dcn=DEM_L3_PATPREFLANG,ord=1,ord2=4,var=PAT_PREF_LANGUAGE_SPOKEN)
     %stat(dcn=DEM_L3_GENDERDIST,ord=1,ord2=5,var=GENDER_IDENTITY)
     %stat(dcn=DEM_L3_ORIENTDIST,ord=1,ord2=6,var=SEXUAL_ORIENTATION)
     %stat(dcn=ENR_L3_BASEDIST,ord=2,ord2=1,var=ENR_BASIS)
     %stat(dcn=ENR_L3_CHART,ord=2,ord2=2,var=CHART)
     %stat(dcn=DEATH_L3_IMPUTE,ord=3,ord2=1,var=DEATH_DATE_IMPUTE)
     %stat(dcn=DEATH_L3_SOURCE,ord=3,ord2=2,var=DEATH_SOURCE)
     %stat(dcn=DEATH_L3_MATCH,ord=3,ord2=3,var=DEATH_MATCH_CONFIDENCE)
     %stat(dcn=ENC_L3_ENCTYPE,ord=4,ord2=1,var=ENC_TYPE)
     %stat(dcn=ENC_L3_DISDISP,ord=4,ord2=2,var=DISCHARGE_DISPOSITION)
     %stat(dcn=ENC_L3_DISSTAT,ord=4,ord2=3,var=DISCHARGE_STATUS)
     %stat(dcn=ENC_L3_DRG_TYPE,ord=4,ord2=4,var=DRG_TYPE)
     %stat(dcn=ENC_L3_ADMSRC,ord=4,ord2=5,var=ADMITTING_SOURCE)
     %stat(dcn=DIA_L3_ENCTYPE,ord=5,ord2=1,var=ENC_TYPE)
     %stat(dcn=DIA_L3_DXSOURCE,ord=5,ord2=3,var=DX_SOURCE)
     %stat(dcn=DIA_L3_PDX,ord=5,ord2=4,var=PDX)
     %stat(dcn=DIA_L3_DXPOA,ord=5,ord2=5,var=DX_POA)
     %stat(dcn=PRO_L3_ENCTYPE,ord=6,ord2=1,var=ENC_TYPE)
     %stat(dcn=PRO_L3_PXSOURCE,ord=6,ord2=3,var=PX_SOURCE)
     %stat(dcn=PRO_L3_PPX,ord=6,ord2=4,var=PPX)
     %stat(dcn=VIT_L3_VITAL_SOURCE,ord=7,ord2=1,var=VITAL_SOURCE)
     %stat(dcn=VIT_L3_BP_POSITION_TYPE,ord=7,ord2=2,var=BP_POSITION)
     %stat(dcn=VIT_L3_SMOKING,ord=7,ord2=3,var=SMOKING)
     %stat(dcn=VIT_L3_TOBACCO,ord=7,ord2=4,var=TOBACCO)
     %stat(dcn=VIT_L3_TOBACCO_TYPE,ord=7,ord2=5,var=TOBACCO_TYPE)
     %stat(dcn=PRES_L3_BASIS,ord=8,ord2=1,var=RX_BASIS)
     %stat(dcn=PRES_L3_FREQ,ord=8,ord2=2,var=RX_FREQUENCY)
     %stat(dcn=PRES_L3_RXDOSEFORM,ord=8,ord2=3,var=RX_DOSE_FORM)
     %stat(dcn=PRES_L3_RXDOSEODR_DIST,ord=8,ord2=4,var=RX_DOSE_ORDERED)
     %stat(dcn=PRES_L3_RXDOSEODRUNIT,ord=8,ord2=5,var=RX_DOSE_ORDERED_UNIT)
     %stat(dcn=PRES_L3_PRNFLAG,ord=8,ord2=6,var=RX_PRN_FLAG)
     %stat(dcn=PRES_L3_ROUTE,ord=8,ord2=7,var=RX_ROUTE)
     %stat(dcn=PRES_L3_SOURCE,ord=8,ord2=8,var=RX_SOURCE)
     %stat(dcn=PRES_L3_DISPASWRTN,ord=8,ord2=9,var=RX_DISPENSE_AS_WRITTEN)
     %stat(dcn=DISP_L3_DOSEUNIT,ord=9,ord2=2,var=DISPENSE_DOSE_DISP_UNIT)
     %stat(dcn=DISP_L3_ROUTE,ord=9,ord2=3,var=DISPENSE_ROUTE)
     %stat(dcn=DISP_L3_SOURCE,ord=9,ord2=4,var=DISPENSE_SOURCE)
     %stat(dcn=LAB_L3_LSOURCE,ord=10,ord2=0,var=LAB_LOINC_SOURCE)
     %stat(dcn=LAB_L3_RSOURCE,ord=10,ord2=1,var=LAB_RESULT_SOURCE)
     %stat(dcn=LAB_L3_SOURCE,ord=10,ord2=2,var=SPECIMEN_SOURCE)
     %stat(dcn=LAB_L3_PRIORITY,ord=10,ord2=3,var=PRIORITY)
     %stat(dcn=LAB_L3_LOC,ord=10,ord2=4,var=RESULT_LOC)
     %stat(dcn=LAB_L3_PX_TYPE,ord=10,ord2=5,var=LAB_PX_TYPE)
     %stat(dcn=LAB_L3_QUAL,ord=10,ord2=6,var=RESULT_QUAL)
     %stat(dcn=LAB_L3_MOD,ord=10,ord2=7,var=RESULT_MODIFIER)
     %stat(dcn=LAB_L3_LOW,ord=10,ord2=8,var=NORM_MODIFIER_LOW)
     %stat(dcn=LAB_L3_HIGH,ord=10,ord2=9,var=NORM_MODIFIER_HIGH)
     %stat(dcn=LAB_L3_ABN,ord=10,ord2=10,var=ABN_IND)
     %stat(dcn=LAB_L3_UNIT,ord=10,ord2=11,var=RESULT_UNIT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=1,var=DATAMART_PLATFORM)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=2,var=DATAMART_CLAIMS)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=3,var=DATAMART_EHR)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=4,var=BIRTH_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=5,var=ENR_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=6,var=ENR_END_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=7,var=ADMIT_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=8,var=DISCHARGE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=9,var=PX_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=10,var=DX_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=11,var=RX_ORDER_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=12,var=RX_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=13,var=RX_END_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=14,var=DISPENSE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=15,var=LAB_ORDER_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=16,var=SPECIMEN_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=17,var=RESULT_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=18,var=MEASURE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=19,var=ONSET_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=20,var=REPORT_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=21,var=RESOLVE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=22,var=PRO_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=23,var=DEATH_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=24,var=MEDADMIN_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=25,var=MEDADMIN_STOP_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=26,var=OBSCLIN_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=27,var=OBSGEN_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=28,var=ADDRESS_PERIOD_START_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=29,var=ADDRESS_PERIOD_END_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=30,var=VX_RECORD_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=31,var=VX_ADMIN_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=32,var=VX_EXP_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=33,var=OBSCLIN_STOP_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=34,var=OBSGEN_STOP_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=35,var=CDM_VERSION)
     %stat(dcn=COND_L3_STATUS,ord=12,ord2=1,var=CONDITION_STATUS)
     %stat(dcn=COND_L3_TYPE,ord=12,ord2=2,var=CONDITION_TYPE)
     %stat(dcn=COND_L3_SOURCE,ord=12,ord2=3,var=CONDITION_SOURCE)
     %stat(dcn=DEATHC_L3_CODE,ord=13,ord2=1,var=DEATH_CAUSE_CODE)
     %stat(dcn=DEATHC_L3_SOURCE,ord=13,ord2=2,var=DEATH_CAUSE_SOURCE)
     %stat(dcn=DEATHC_L3_TYPE,ord=13,ord2=3,var=DEATH_CAUSE_TYPE)
     %stat(dcn=DEATHC_L3_CONF,ord=13,ord2=4,var=DEATH_CAUSE_CONFIDENCE)
     %stat(dcn=PROCM_L3_TYPE,ord=14,ord2=1,var=PRO_TYPE)
     %stat(dcn=PROCM_L3_ITEMNM,ord=14,ord2=2,var=PRO_ITEM_NAME)
     %stat(dcn=PROCM_L3_ITEMFULLNAME,ord=14,ord2=3,var=PRO_ITEM_FULLNAME)
     %stat(dcn=PROCM_L3_MEASURE_FULLNAME,ord=14,ord2=4,var=PRO_MEASURE_NAME)
     %stat(dcn=PROCM_L3_MEASURENM,ord=14,ord2=5,var=PRO_MEASURE_FULLNAME)
     %stat(dcn=PROCM_L3_METHOD,ord=14,ord2=6,var=PRO_METHOD)
     %stat(dcn=PROCM_L3_MODE,ord=14,ord2=7,var=PRO_MODE)
     %stat(dcn=PROCM_L3_CAT,ord=14,ord2=8,var=PRO_CAT)
     %stat(dcn=PROCM_L3_SOURCE,ord=14,ord2=9,var=PRO_SOURCE)
     %stat(dcn=PROV_L3_SPECIALTY,ord=16,ord2=1,var=PROVIDER_SPECIALTY_PRIMARY)
     %stat(dcn=PROV_L3_SEX,ord=16,ord2=2,var=PROVIDER_SEX)
     %stat(dcn=PROV_L3_NPIFLAG,ord=16,ord2=3,var=PROVIDER_NPI_FLAG)
     %stat(dcn=MEDADM_L3_ROUTE,ord=17,ord2=2,var=MEDADMIN_ROUTE)
     %stat(dcn=MEDADM_L3_SOURCE,ord=17,ord2=3,var=MEDADMIN_SOURCE)
     %stat(dcn=MEDADM_L3_TYPE,ord=17,ord2=4,var=MEDADMIN_TYPE)
     %stat(dcn=OBSCLIN_L3_MOD,ord=18,ord2=1,var=OBSCLIN_RESULT_MODIFIER)
     %stat(dcn=OBSCLIN_L3_QUAL,ord=18,ord2=2,var=OBSCLIN_RESULT_QUAL)
     %stat(dcn=OBSCLIN_L3_RUNIT,ord=18,ord2=3,var=OBSCLIN_RESULT_UNIT)
     %stat(dcn=OBSCLIN_L3_TYPE,ord=18,ord2=4,var=OBSCLIN_TYPE)
     %stat(dcn=OBSCLIN_L3_SOURCE,ord=18,ord2=5,var=OBSCLIN_SOURCE)
     %stat(dcn=OBSCLIN_L3_ABN,ord=18,ord2=6,var=OBSCLIN_ABN_IND)
     %stat(dcn=OBSGEN_L3_MOD,ord=19,ord2=1,var=OBSGEN_RESULT_MODIFIER)
     %stat(dcn=OBSGEN_L3_TMOD,ord=19,ord2=2,var=OBSGEN_TABLE_MODIFIED)
     %stat(dcn=OBSGEN_L3_QUAL,ord=19,ord2=3,var=OBSGEN_RESULT_QUAL)
     %stat(dcn=OBSGEN_L3_RUNIT,ord=19,ord2=4,var=OBSGEN_RESULT_UNIT)
     %stat(dcn=OBSGEN_L3_TYPE,ord=19,ord2=5,var=OBSGEN_TYPE)
     %stat(dcn=OBSGEN_L3_SOURCE,ord=19,ord2=6,var=OBSGEN_SOURCE)
     %stat(dcn=OBSGEN_L3_ABN,ord=19,ord2=7,var=OBSGEN_ABN_IND)
     %stat(dcn=LDSADRS_L3_ADRSUSE,ord=21,ord2=1,var=ADDRESS_USE)
     %stat(dcn=LDSADRS_L3_ADRSTYPE,ord=21,ord2=2,var=ADDRESS_TYPE)
     %stat(dcn=LDSADRS_L3_ADRSPREF,ord=21,ord2=3,var=ADDRESS_PREFERRED)
     %stat(dcn=LDSADRS_L3_ADRSSTATE,ord=21,ord2=4,var=ADDRESS_STATE)
     %stat(dcn=IMMUNE_L3_STATUS,ord=22,ord2=2,var=VX_STATUS)
     %stat(dcn=IMMUNE_L3_STATUSREASON,ord=22,ord2=3,var=VX_STATUS_REASON)
     %stat(dcn=IMMUNE_L3_SOURCE,ord=22,ord2=4,var=VX_SOURCE)
     %stat(dcn=IMMUNE_L3_DOSEUNIT,ord=22,ord2=5,var=VX_DOSE_UNIT)
     %stat(dcn=IMMUNE_L3_ROUTE,ord=22,ord2=6,var=VX_ROUTE)
     %stat(dcn=IMMUNE_L3_BODYSITE,ord=22,ord2=7,var=VX_BODY_SITE)
     %stat(dcn=IMMUNE_L3_MANUFACTURER,ord=22,ord2=8,var=VX_MANUFACTURER)
     %stat(dcn=LABHIST_L3_SEXDIST,ord=23,ord2=1,var=SEX)
     %stat(dcn=LABHIST_L3_RACEDIST,ord=23,ord2=2,var=RACE)
     %stat(dcn=LABHIST_L3_UNIT,ord=23,ord2=3,var=RESULT_UNIT)
     %stat(dcn=LABHIST_L3_LOW,ord=23,ord2=4,var=NORM_MODIFIER_LOW)
     %stat(dcn=LABHIST_L3_HIGH,ord=23,ord2=5,var=NORM_MODIFIER_HIGH)
run;

proc sort data=tbl;
     by ord ord2;
run;

*- Cross table queries -*;
%cross(dcn=ENC_L3_PAYERTYPE1,ord=4,ord2=6,var=PAYER_TYPE_PRIMARY,cross=cross_)
%cross(dcn=ENC_L3_PAYERTYPE2,ord=4,ord2=7,var=PAYER_TYPE_SECONDARY,cross=cross_)
%cross(dcn=ENC_L3_FACILITYTYPE,ord=4,ord2=8,var=FACILITY_TYPE,cross=cross_)
%cross(dcn=DIA_L3_DXTYPE_DXSOURCE,ord=5,ord2=2,var=DX_TYPE,cross=)
%cross(dcn=PRO_L3_PXTYPE_ENCTYPE,ord=6,ord2=2,var=PX_TYPE,cross=cross_)
%cross(dcn=MEDADM_L3_DOSEADMUNIT,ord=17,ord2=1,var=MEDADMIN_DOSE_ADMIN_UNIT)
%cross(dcn=IMMUNE_L3_CODE_CODETYPE,ord=22,ord2=1,var=VX_CODE_TYPE,cross=cross_)

proc sort data=cross;
     by ord ord2;
run;

*- NDC -*;
data ndc(keep=ord: col:);
     length col1-col4 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DISP_L3_N" and 
                                        variable="NDC" and statistic="ALL_N") rename=(resultn=alln));
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DISP_L3_N" and 
                          variable="NDC" and statistic="VALID_N") rename=(resultn=validn));
     ord=9;
     ord2=1;
     col1=strip(put(ord,tbl_row.));
     col2=strip(variable);
     if alln>=0 and validn>=0 then col3=put(alln-validn,comma16.);
     else if alln=.t or validn=.t then col3=put(.t,threshold.);
     col4=strip(dc_name);
run;

*- Bring everything together -*;
data print_iib(drop=col3);
     set tbl cross ndc;
     by ord ord2;

     * do not want to highlight these records *;
     if strip(col3)^="0" then col3flag=1;
     else col3flag=0;
     col3c=col3;

     if col1 in ("DEMOGRAPHIC" "ENROLLMENT" "DEATH" "ENCOUNTER" "DIAGNOSIS"
                  "PROCEDURES" ) then pg=1;
     else if col1 in ("VITAL" "PRESCRIBING" "DISPENSING" "LAB_RESULT_CM") then pg=2;
     else if col1 in ("HARVEST") and ord2<=27 then pg=3;
     else if col1 in ("HARVEST") and ord2>27 or 
             col1 in ("CONDITION" "DEATH_CAUSE" "PRO_CM" "PROVIDER" "MED_ADMIN") then pg=4;
     else pg=5;
run;

*- Check to see if any exceptions -*;
proc sql noprint;
     select sum(col3flag) into :_exceptions_iib from print_iib;
quit;

*- Macro to prevent open code -*;
%macro _openb;
    * if no exceptions, no need to print all variables with zeroes *;
    %if &_exceptions_iib=0 %then %do;
        data print_iib;
            col3flag=0;
            pg=1;
            array _c $40 col1 col2 col3 col4 col3c;
            col1="All fields conform to specifications";
            output;
        run;
    %end;
%mend _openb;
%_openb;

*- Keep records resulting in an exception -*;
data dc_summary_iib;
     length edc_table_s_ $10;
     set print_iib(keep=col3flag);
     if col3flag=1;
     row=1.06;
     exception=1;
     edc_table_s_="Table IIB";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iib nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iib;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iib;
    %let _ftn=IIB;
    %let _hdr=Table IIB;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iib split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col3c col3flag pg col1 col2 col3 col4;
         where pg=1;

         define col3c    /display noprint;
         define col3flag /display noprint;
         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
         define col3     /computed flow "Number of records with values|outside of specifications" style(header)=[just=center cellwidth=27%] 
                                        style(column)=[just=center];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
         compute col3 / char length=20;
            if col3flag=1 then do;
                col3=col3c;
                call define(_col_, "style", "style=[color=red]");
            end;
            else do;
                col3=col3c;
            end;
         endcomp;
    run;

    *- Determine how many page in report -*;
    proc means data=print_iib noprint;
         var pg;
         output out=_iib_pg max=max_pg;
    run;
        
    data _null_;
         set _iib_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";

        proc report data=print_iib split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column col3c col3flag pg col1 col2 col3 col4;
             where pg=&_pg;

             define col3flag /display noprint;
             define col3c    /display noprint;
             define pg       /order noprint;
             define col1     /display flow "Table" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
             define col2     /display flow "Field" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
             define col3     /computed flow "Number of records with values|outside of specifications" style(header)=[just=center cellwidth=27%]
                                            style(column)=[just=center];
             define col4     /display flow "Source table" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
    
             break after pg / page;
             compute col3 / char length=20;
                if col3flag=1 then do;
                    col3=col3c;
                    call define(_col_, "style", "style=[color=red]");
                end;
                else do;
                    col3=col3c;
                end;
             endcomp;
        run;    

        ods listing;
    %end;
%mend print_iib;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IIC NON-PERMISSABLE MISSING VALUES
********************************************************************************;

*- Macro for each type of data row -*;
%macro stat(dcn=,ord=,ord2=,cat=,var=,stat=,tbl=);
     if dc_name="&dcn" and category="&cat" and variable="&var" and statistic="&stat" &tbl then do;
        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if resultn^=.t then col3=strip(put(resultn,comma16.));
        else col3=strip(put(resultn,threshold.));
        if resultn=0 then _obs3=0;
        else _obs3=1;
        col4=strip(dc_name);
        output;
     end;
%mend stat;

%macro harvest(dcn=,ord=,ord2=,var=);
     if dc_name="&dcn" and variable="&var" and statistic="VALUE" then do;

        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if resultc^=" " then do; col3="0"; _obs3=0; end;
        else do; col3="1"; _obs3=1; end;
        col4=strip(dc_name);
        output;
     end;
%mend harvest;

%macro mult(dcn=,ord=,ord2=,var=);

    proc means data=dc_normal noprint nway;
         class dc_name;
         var resultn;
         output out=mult&ord sum=sum;
         where datamartid=%upcase("&dmid") and dc_name="&dcn" and variable="&var" 
             and statistic="RECORD_N" and category in ("NULL or missing");
    run;
    
    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(dc_name) into :nobs from mult&ord;
    quit;

    *- If obs, continue, else create a zero record -*;
    %if &nobs^=0 %then %do;
        data mult&ord(keep=ord: col: _obs3);
             length col1-col4 $50;
             set mult&ord;
                 ord=&ord;
                 ord2=&ord2;
                 col1=strip(put(ord,tbl_row.));
                 col2=strip("&var");
                 col3=strip(put(sum,comma16.));
                 _obs3=1;
                 col4=strip(dc_name);
        run;
    %end;
    %else %do;
        data mult&ord(keep=ord: col: _obs3);
             length col1-col4 $50;
             ord=&ord;
             ord2=&ord2;
             col1=strip(put(ord,tbl_row.));
             col2=strip("&var");
             col3="0";
             _obs3=0;
             col4="&dcn";
             output;
        run;
    %end;

    proc append base=mult data=mult&ord;
    run;

%mend mult;

%macro cross(dcn=,ord=,ord2=,var=);

    proc means data=dc_normal noprint nway;
         class dc_name;
         var resultn;
         output out=cross&ord sum=sum;
         where datamartid=%upcase("&dmid") and dc_name="&dcn" and cross_variable="&var" 
             and statistic="RECORD_N" and cross_category="NULL or missing";
    run;
    
    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(dc_name) into :nobs from cross&ord;
    quit;

    *- If obs, continue, else create a zero record -*;
    %if &nobs^=0 %then %do;
        data cross&ord(keep=ord: col: _obs3);
             length col1-col4 $50;
             set cross&ord;
                 ord=&ord;
                 ord2=&ord2;
                 col1=strip(put(ord,tbl_row.));
                 col2=strip("&var");
                 col3=strip(put(sum,comma16.));
                 if sum>0 then _obs3=1;
                 else _obs3=0;
                 col4=strip(dc_name);
        run;
    %end;
    %else %do;
        data cross&ord(keep=ord: col: _obs3);
             length col1-col4 $50;
             ord=&ord;
             ord2=&ord2;
             col1=strip(put(ord,tbl_row.));
             col2=strip("&var");
             col3="0";
             _obs3=0;
             col4="&dcn";
             output;
        run;
    %end;

    proc append base=cross data=cross&ord;
    run;

%mend cross;

*- Non-cross table queries -*;
data tbl(keep=ord: col: _obs3);
     length col1-col4 $50;
     set dc_normal(where=(datamartid=%upcase("&dmid")));

     %stat(dcn=DEM_L3_N,ord=1,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=ENR_L3_N,ord=2,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=2,ord2=2,var=ENR_START_DATE,stat=NMISS)
     %stat(dcn=ENR_L3_BASEDIST,ord=2,ord2=3,cat=NULL or missing,var=ENR_BASIS,stat=RECORD_N)
     %stat(dcn=DEATH_L3_N,ord=3,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=DEATH_L3_SOURCE,ord=3,ord2=2,cat=NULL or missing,var=DEATH_SOURCE,stat=RECORD_N)
     %stat(dcn=ENC_L3_N,ord=4,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=ENC_L3_N,ord=4,ord2=2,var=ENCOUNTERID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=4,ord2=3,var=ADMIT_DATE,stat=NMISS,tbl=and table="ENCOUNTER")
     %stat(dcn=ENC_L3_ENCTYPE,ord=4,ord2=4,cat=NULL or missing,var=ENC_TYPE,stat=RECORD_N)
     %stat(dcn=DIA_L3_N,ord=5,ord2=1,var=DIAGNOSISID,stat=NULL_N)
     %stat(dcn=DIA_L3_N,ord=5,ord2=2,var=PATID,stat=NULL_N)
     %stat(dcn=DIA_L3_DX,ord=5,ord2=3,cat=NULL or missing,var=DX,stat=RECORD_N)
     %stat(dcn=DIA_L3_DXSOURCE,ord=5,ord2=5,cat=NULL or missing,var=DX_SOURCE,stat=RECORD_N)
     %stat(dcn=PRO_L3_N,ord=6,ord2=1,var=PROCEDURESID,stat=NULL_N)
     %stat(dcn=PRO_L3_N,ord=6,ord2=2,var=PATID,stat=NULL_N)
     %stat(dcn=PRO_L3_PX,ord=6,ord2=3,cat=NULL or missing,var=PX,stat=RECORD_N)
     %stat(dcn=VIT_L3_N,ord=7,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=VIT_L3_N,ord=7,ord2=2,var=VITALID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=7,ord2=3,var=MEASURE_DATE,stat=NMISS)
     %stat(dcn=VIT_L3_VITAL_SOURCE,ord=7,ord2=4,cat=NULL or missing,var=VITAL_SOURCE,stat=RECORD_N)
     %stat(dcn=PRES_L3_N,ord=8,ord2=1,var=PRESCRIBINGID,stat=NULL_N)
     %stat(dcn=PRES_L3_N,ord=8,ord2=2,var=PATID,stat=NULL_N)
     %stat(dcn=DISP_L3_N,ord=9,ord2=1,var=DISPENSINGID,stat=NULL_N)
     %stat(dcn=DISP_L3_N,ord=9,ord2=2,var=PATID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=9,ord2=3,var=DISPENSE_DATE,stat=NMISS)
     %stat(dcn=DISP_L3_NDC,ord=9,ord2=4,cat=NULL or missing,var=NDC,stat=RECORD_N)
     %stat(dcn=LAB_L3_N,ord=10,ord2=1,var=LAB_RESULT_CM_ID,stat=NULL_N)
     %stat(dcn=LAB_L3_N,ord=10,ord2=2,var=PATID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=3,var=RESULT_DATE,stat=NMISS)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=1,var=NETWORKID)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=2,var=DATAMARTID)
     %stat(dcn=COND_L3_N,ord=12,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=COND_L3_N,ord=12,ord2=2,var=CONDITIONID,stat=NULL_N)
     %stat(dcn=COND_L3_N,ord=12,ord2=3,var=CONDITION,stat=NULL_N)
     %stat(dcn=COND_L3_TYPE,ord=12,ord2=4,cat=NULL or missing,var=CONDITION_TYPE,stat=RECORD_N)
     %stat(dcn=COND_L3_SOURCE,ord=12,ord2=5,cat=NULL or missing,var=CONDITION_SOURCE,stat=RECORD_N)
     %stat(dcn=DEATHC_L3_N,ord=13,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=DEATHC_L3_N,ord=13,ord2=2,var=DEATH_CAUSE,stat=NULL_N)
     %stat(dcn=DEATHC_L3_CODE,ord=13,ord2=3,cat=NULL or missing,var=DEATH_CAUSE_CODE,stat=RECORD_N)
     %stat(dcn=DEATHC_L3_TYPE,ord=13,ord2=4,cat=NULL or missing,var=DEATH_CAUSE_TYPE,stat=RECORD_N)
     %stat(dcn=DEATHC_L3_SOURCE,ord=13,ord2=5,cat=NULL or missing,var=DEATH_CAUSE_SOURCE,stat=RECORD_N)
     %stat(dcn=PROCM_L3_N,ord=14,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=PROCM_L3_N,ord=14,ord2=2,var=PRO_CM_ID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=14,ord2=3,var=PRO_DATE,stat=NMISS)
     %stat(dcn=TRIAL_L3_N,ord=15,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=TRIAL_L3_N,ord=15,ord2=2,var=TRIALID,stat=NULL_N)
     %stat(dcn=TRIAL_L3_N,ord=15,ord2=3,var=PARTICIPANTID,stat=NULL_N)
     %stat(dcn=PROV_L3_N,ord=16,ord2=1,var=PROVIDERID,stat=NULL_N)
     %stat(dcn=MEDADM_L3_N,ord=17,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=MEDADM_L3_N,ord=17,ord2=2,var=MEDADMINID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=17,ord2=3,var=MEDADMIN_START_DATE,stat=NMISS)
     %stat(dcn=OBSCLIN_L3_N,ord=18,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=OBSCLIN_L3_N,ord=18,ord2=2,var=OBSCLINID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=18,ord2=3,var=OBSCLIN_START_DATE,stat=NMISS)
     %stat(dcn=OBSGEN_L3_N,ord=19,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=OBSGEN_L3_N,ord=19,ord2=2,var=OBSGENID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=19,ord2=3,var=OBSGEN_START_DATE,stat=NMISS)
     %stat(dcn=HASH_L3_N,ord=20,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=LDSADRS_L3_N,ord=21,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=LDSADRS_L3_N,ord=21,ord2=2,var=ADDRESSID,stat=NULL_N)
     %stat(dcn=LDSADRS_L3_ADRSUSE,ord=21,ord2=3,cat=NULL or missing,var=ADDRESS_USE,stat=RECORD_N)
     %stat(dcn=LDSADRS_L3_ADRSTYPE,ord=21,ord2=4,cat=NULL or missing,var=ADDRESS_TYPE,stat=RECORD_N)
     %stat(dcn=LDSADRS_L3_ADRSPREF,ord=21,ord2=5,cat=NULL or missing,var=ADDRESS_PREFERRED,stat=RECORD_N)
     %stat(dcn=IMMUNE_L3_N,ord=22,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=IMMUNE_L3_N,ord=22,ord2=2,var=IMMUNIZATIONID,stat=NULL_N)
     %stat(dcn=IMMUNE_L3_STATUS,ord=22,ord2=5,cat=NULL or missing,var=VX_STATUS,stat=RECORD_N)
     %stat(dcn=LABHIST_L3_N,ord=23,ord2=1,var=LABHISTORYID,stat=NULL_N)
     %stat(dcn=LABHIST_L3_LOINC,ord=23,ord2=2,cat=NULL or missing,var=LAB_LOINC,stat=RECORD_N)
run;

proc sort data=tbl;
     by ord ord2;
run;

*- Multi row queries -*;
%mult(dcn=IMMUNE_L3_CODE_CODETYPE,ord=22,ord2=3,var=VX_CODE)

proc sort data=mult;
     by ord ord2;
run;

*- Cross table queries -*;
%cross(dcn=DIA_L3_DXTYPE_ENCTYPE,ord=5,ord2=4,var=DX_TYPE)
%cross(dcn=PRO_L3_PXTYPE_ENCTYPE,ord=6,ord2=4,var=PX_TYPE)
%cross(dcn=IMMUNE_L3_CODE_CODETYPE,ord=22,ord2=4,var=VX_CODE_TYPE)

proc sort data=cross;
     by ord ord2;
run;

*- Bring everything together -*;
data print_iic;
     set tbl mult cross;
     by ord ord2;
     if col1 in ("DEMOGRAPHIC" "ENROLLMENT" "DEATH" "ENCOUNTER" "DIAGNOSIS"
                  "PROCEDURES" "VITAL" "PRESCRIBING") then pg=1;
     else if col1 in ("DISPENSING" "LAB_RESULT_CM" "HARVEST" "CONDITION"  
                      "DEATH_CAUSE" "PRO_CM" "PCORNET_TRIAL" "PROVIDER" "MED_ADMIN"
                      "HASH_TOKEN") then pg=2;
     else pg=3;
run;

*- Check to see if any exceptions -*;
proc sql noprint;
     select sum(_obs3) into :_exceptions_iic from print_iic
        where _obs3^=0;
quit;

*- Macro to prevent open code -*;
%macro _openc; 
    * if no exceptions, no need to print all variables with zeroes *;
    %if &_exceptions_iic<=0 %then %do;
        data print_iic;
            pg=1;
            _obs3=0;
            array _c $40 col1 col2 col3 col4;
            col1="All fields conform to specifications";
            output;
        run;
    %end;
%mend _openc;
%_openc;
    
*- Keep records resulting in an exception -*;
data dc_summary_iic;
     length edc_table_s_ $10;
     set print_iic(keep=_obs3);
     if _obs3^=0;
     row=1.07;
     exception=1;
     edc_table_s_="Table IIC";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iic nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iic;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iic;
    %let _ftn=IIC;
    %let _hdr=Table IIC;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iic split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4;
         where pg=1;

         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
         define col3     /display flow "Number of records with missing values" style(header)=[just=center cellwidth=30%] style(column)=[just=center];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         compute col3;
            if col3^="0" then call define(_col_, "style", "style=[color=red]");
         endcomp;
    run;    

    *- Determine how many page in report -*;
    proc means data=print_iic noprint;
         var pg;
         output out=_iic_pg max=max_pg;
    run;
        
    data _null_;
         set _iic_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";

        proc report data=print_iic split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column pg col1 col2 col3 col4;
             where pg=&_pg;

             define pg       /order noprint;
             define col1     /display flow "Table" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
             define col2     /display flow "Field" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
             define col3     /display flow "Number of records with missing values" style(header)=[just=center cellwidth=30%] style(column)=[just=center];
             define col4     /display flow "Source table" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
             compute col3;
                if col3^="0" then call define(_col_, "style", "style=[color=red]");
             endcomp;
             break after pg / page;
        run;

        ods listing;
    %end;
%mend print_iic;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IID DIAGNOSTIC ERRORS
********************************************************************************;

proc format;
     value $ord
        "DEMOGRAPHIC"="01"
        "ENROLLMENT"="02"
        "ENCOUNTER"="03"
        "DIAGNOSIS"="04"
        "PROCEDURES"="05"
        "VITAL"="06"
        "DISPENSING"="07"
        "LAB_RESULT_CM"="08"
        "CONDITION"="09"
        "PRO_CM"="10"
        "PRESCRIBING"="11"
        "PCORNET_TRIAL"="12"
        "DEATH"="13"
        "DEATH_CAUSE"="14"
        "HARVEST"="15"
        "PROVIDER"="16"
        "MED_ADMIN"="17"
        "OBS_CLIN"="18"
        "OBS_GEN"="19"
        "HASH_TOKEN"="20"
        "LDS_ADDRESS_HISTORY"="21"
        "IMMUNIZATION"="22"
        "LAB_HISTORY"="23"        
        other=" "
        ;

     value dcheck
        1 = "1.01"
        2 = "1.02"
        3,4 = "1.03"
        5,6,7 = "1.04"
        8 = "1.15"
        9 = "1.17"
        ;

     value $dcdesc
        '1.01'='Required tables are not present'
        '1.02'='Expected tables are not populated'
        '1.03'='Required fields are not present'
        '1.04'='Required fields do not conform to data model specifications for data type, length, or name'
        '1.15'='Fields with undefined lengths that are present in more than one table do not have harmonized field lengths'
        '1.17'='Zip codes in the ENCOUNTER or LDS_ADDRESS_HISTORY table do not conform to expected values'
        ;

    value except
        1='Required table is not present.  All tables must be present in an instantiation of the CDM'
        2='Table expected to be populated (DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, and HARVEST) is not populated'
        3='Required numeric field is not present'
        4='Required character field is not present'
        5='Required character field is numeric'
        6='Required numeric field is character'
        7='Required field is present but of unexpected length'
        8='Field lengths are not harmonized for one or more of the following fields: PATID, ENCOUNTERID, PRESCRIBINGID, PROCEDURESID, PROVIDERID, MEDADMIN_PROVIDERID, OBSGEN_PROVIDERID, OBSCLIN_PROVIDERID, RX_PROVIDERID, and VX_PROVIDERID.'
        9='LDS_ADDRESS_HISTORY.ADDRESS_ZIP5, LDS_ADDRESS_HISTORY.ADDRESS_ZIP9 or ENCOUNTER.FACILITY_LOCATION contains alphabetical characters or does not have the expected number of digits.'
        ;
run;

*- Get meta-data from DataMart data structures and create ordering variable -*;
data datamart_all;
     set normdata.datamart_all;
     memname=upcase(memname);
     name=upcase(name);
     ord=put(upcase(memname),$ord.);
     if nobs not in (" " "BT") then nobsn=input(nobs,16.);
     if memname="HARVEST" and nobs="BT" then nobsn=1;

     * subset on required data structures *;
     if ord^=' ';
run;

*- For field comparison -*;
proc sort data=datamart_all(keep=ord memname name type length nobsn) out=datamart;
     by ord memname name;
run;

*- For table comparison -*;
proc sort data=datamart(keep=ord memname nobsn) out=datamart_tbl nodupkey;
     by ord memname;
run;

*- Create ordering variable for output using import transport file -*;
data required_structure;
     set required_structure;
     ord=put(upcase(memname),$ord.);
run;

*- For field comparison -*;
proc sort data=required_structure nodupkey;
     by ord memname name;
run;

*- For table comparison -*;
proc sort data=required_structure(keep=ord memname current_version) 
     out=required_structure_tbl nodupkey;
     by ord memname;
run;

*- Table comparison -*;
data compare_tbl(keep=miss_table not_pop)
     compare_tbl_var(keep=ord memname miss_tablen);
     length miss_table not_pop $200;
     merge required_structure_tbl(in=r)
           datamart_tbl(in=dm) end=eof
     ;
     by ord memname;
        
     * keep if required *;
     if r;

     retain miss_table not_pop;
     if not dm then do;
        miss_tablen=1;
        if miss_table=" " then miss_table=memname;
        else if miss_table^=" " then miss_table=strip(miss_table) || "; " || strip(memname);
     end;
     if .<=nobsn<=0 and upcase(current_version)="YES" then do;
        if not_pop=" " then not_pop=memname;
        else if not_pop^=" " then not_pop=strip(not_pop) || "; " || strip(memname);
     end;
     output compare_tbl_var;
     if eof then output compare_tbl;
run;

*- Field comparison -*;
data compare_var;
     merge required_structure(in=r) datamart(in=dm);
     by ord memname name;

     * keep if present in required, but not datamart or field type/length is mismatched *;
     if (r and not dm) or (r and dm and r_type^=type) or 
        (r and dm and indexw(name,'DATE')=0 and indexw(name,'TIME')=0 and r_length^=. and length^=r_length);
     
     * label condition *;
     if r and not dm and r_type=2 then row=4;
     else if r and not dm and r_type=1 then row=3;
     else if r and dm and r_type^=type and r_type=2 then row=5;
     else if r and dm and r_type^=type and r_type=1 then row=6;
     else if r and dm and r_type=type and r_length^=. and length^=r_length then row=7;

     * assign sorting variable in case of multiple variables for each condition *;
     subrow=_n_;
    
     keep ord memname name row subrow;
run;

*- Test for comparable lengths of variables of interest -*;
data var_si;
     set datamart(keep=name length);
     if name in ('MEDADMIN_PROVIDERID' 'OBSGEN_PROVIDERID' 'OBSCLIN_PROVIDERID' 
                 'RX_PROVIDERID' 'VX_PROVIDERID') then name='PROVIDERID';
     if name in ('PATID' 'ENCOUNTERID' 'PRESCRIBINGID' 'PROCEDURESID' 'PROVIDERID');
run;

*- Macro to test lengths of variables of special interest -*;
%macro varlen(var);

    proc means data=var_si nway noprint;
         class name;
         var length;
         output out=&var min=min max=max;
         where name="&var";
    run;

    data &var._length;
         set &var;
         if min^=max;
         ord='99';
         keep ord;
   run;
%mend varlen;
%varlen(var=PATID)
%varlen(var=ENCOUNTERID)
%varlen(var=PRESCRIBINGID)
%varlen(var=PROCEDURESID)
%varlen(var=PROVIDERID)

data row8;
     length memname name $32;
     merge patid_length(in=pt)
           encounterid_length(in=enc)
           prescribingid_length(in=pres)
           proceduresid_length(in=proc)
           providerid_length(in=prov)
     ;
     by ord;

     memname=" ";
     row=8;
     subrow=1;

     retain name;
     array l pt enc pres proc prov;
     array lt $15 ptt enct prest proct provt ('PATID' 'ENCOUNTERID' 'PRESCRIBINGID' 'PROCEDURESID' 'PROVIDERID');
        do i = 1 to dim(l);
           if name=" " and l{i}=1 then name=lt{i};
           else if name^=" " and l{i}=1 then name=strip(name)||","||lt{i};
        end;

     keep ord memname row subrow name;
run;

data row9;
     length memname name $32;
     merge dc_normal(in=fl keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="ALL_N"
                   and dc_name='ENC_L3_N' and variable='FACILITY_LOCATION')
                   rename=(resultn=fl_all))
           dc_normal(in=fl keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="VALID_N"
                   and dc_name='ENC_L3_N' and variable='FACILITY_LOCATION')
                   rename=(resultn=fl_valid))
           dc_normal(in=z5 keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="ALL_N" 
                   and dc_name='LDSADRS_L3_N' and variable='ADDRESS_ZIP5')
                   rename=(resultn=z5_all))
           dc_normal(in=z5 keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="VALID_N" 
                   and dc_name='LDSADRS_L3_N' and variable='ADDRESS_ZIP5')
                   rename=(resultn=z5_valid))
           dc_normal(in=z9 keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="ALL_N" 
                   and dc_name='LDSADRS_L3_N' and variable='ADDRESS_ZIP9')
                   rename=(resultn=z9_all))
           dc_normal(in=z9 keep=datamartid statistic dc_name variable resultn table 
                   where=(datamartid=%upcase("&dmid") and statistic="VALID_N" 
                   and dc_name='LDSADRS_L3_N' and variable='ADDRESS_ZIP9')
                   rename=(resultn=z9_valid))
     ;
     by datamartid;

     ord='99';
     row=9;
     subrow=1;

     if fl_valid<fl_all then flr=1;
     else flr=0;
     if z5_valid<z5_all then z5r=1;
     else z5r=0;
     if z9_valid<z9_all then z9r=1;
     else z9r=0;

     if flr=1 then do;
        if (z5r=1 and z9r=1) then do;
            memname="ENCOUNTER, LDS_ADDRESS_HISTORY";
            name="FACLITY_LOCATION, ADDRESS_ZIP5, ADDRESS_ZIP9";
        end;
        else if z5r=1 then do;
            memname="ENCOUNTER, LDS_ADDRESS_HISTORY";
            name="FACLITY_LOCATION, ADDRESS_ZIP5";
        end;
        else if z9r=1 then do;
            memname="ENCOUNTER, LDS_ADDRESS_HISTORY";
            name="FACLITY_LOCATION, ADDRESS_ZIP9";
        end;
        else do;
            memname="ENCOUNTER";
            name="FACLITY_LOCATION";
        end;

     end;
     else if z5r=1 then do;
        memname="LDS_ADDRESS_HISTORY";
        if z9r=1 then name="ADDRESS_ZIP5, ADDRESS_ZIP9";
        else name="ADDRESS_ZIP5";
     end;
     else if z9r=1 then do;
        memname="LDS_ADDRESS_HISTORY";
        name="ADDRESS_ZIP9";
     end;
     else do;
        memname=" ";
        name="None";
     end;

     keep ord memname row subrow name;
run;

*- Remove records from field comparison if no comparative table/obs present -*;
data compare_var;
     merge compare_var(in=dm)
           compare_tbl_var(in=mdsn where=(miss_tablen=1))
           row8
           row9
     ;
     by ord memname;
     if mdsn then delete;
     drop miss_tablen;
run;

proc sort data=compare_var;
     by row subrow;
run;

data table;
     do row = 1 to 9;
        output;
     end;
run;

data final;
     length col1-col6 $250;
     if _n_=1 then set compare_tbl;
     merge table compare_var;
     by row;
    
     col1=put(row,dcheck.);
     col2=put(col1,$dcdesc.);
     col3=put(row,except.);

     if row not in (1,2,8) then do;
        if memname^=" " then col4=memname;
        else col4="None";
     end;
     else if row=1 then do;
         if miss_table^=" " then col4=miss_table;
         else col4="None";
     end;
     else if row=2 then do;
        if not_pop^=" " then col4=not_pop;
         else col4="None";
     end;

     if row not in (1,2) then do;
        if name^=" " then col5=name;
        else col5="None";
        if name="FACLITY_LOCATION, ADDRESS_ZIP5, " then 
           col5="FACLITY_LOCATION, ADDRESS_ZIP5, ADDRESS_ZIP9";
     end;
     else if row in (1,2) then col5="n/a";

     if row^=9 then col6="DATAMART_ALL";
     else if row=9 then col6="ENC_L3_N; LDSADRS_L3_N";
run;

data print_iid;
     set final;
     by row;

     if col4 not in ("None" "n/a" " ") then do;
        if row^=9 then c4=1;
        else if row=9 then c4=2;
     end;
     if col5 not in ("None" "n/a") then do;
        if row^=9 then c5=1;
        else if row=9 then c5=2;
     end;

     * count records for page break *;
     retain pg 1 cnt 0;
     cnt=cnt+1;
     if first.row then do;
         if row in (1) then cnt=cnt+min(2,countc(col4,';'));
         else if row in (2) then cnt=cnt+4;
         else if row in (5) then cnt=cnt+2;
     end;
    
     if (first.row and cnt>30) or cnt>30 then do;
        pg=pg+1;
        cnt=2;
     end;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iid;
     length edc_table_s_ $10;
     set print_iid(keep=c4 c5 col1);
     if c4^=. or c5^=.;
     row=input(col1,8.2);
     exception=1;
     edc_table_s_="Table IID";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iid nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iid;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iid;
    %let _ftn=IID;
    %let _hdr=Table IID;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iid split='|' style(header)=[backgroundcolor=CXCCCCCC] missing;
         column pg row col1 col2 col3 c4 c5 col4 col5 col6;
         where pg=1;

         define pg       /order noprint;
         define row      /order noprint;
         define col1     /order order=data flow "Data|Check" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col2     /order order=data flow "Data Check Description" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
         define col3     /order order=data flow "Exception" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
         define c4       /display noprint;
         define c5       /display noprint;
         define col4     /display flow "Table(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col5     /display flow "Field(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=14.25%] style(column)=[just=left];
         compute col4;
            if c4=1 then call define(_col_, "style", "style=[color=red]");
            else if c4=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if c5=1 then call define(_col_, "style", "style=[color=red]");
            else if c5=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_iid split='|' style(header)=[backgroundcolor=CXCCCCCC] missing;
         column pg row col1 col2 col3 c4 c5 col4 col5 col6;
         where pg>1;

         define pg       /order noprint;
         define row      /order noprint;
         define col1     /order order=data flow "Data|Check" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col2     /order order=data flow "Data Check Description" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
         define col3     /order order=data flow "Exception" style(header)=[just=left cellwidth=27%] style(column)=[just=left];
         define c4       /display noprint;
         define c5       /display noprint;
         define col4     /display flow "Table(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col5     /display flow "Field(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=14.25%] style(column)=[just=left];
         compute col4;
            if c4=1 then call define(_col_, "style", "style=[color=red]");
            else if c4=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if c5=1 then call define(_col_, "style", "style=[color=red]");
            else if c5=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         break after pg / page;
    run;
    ods listing;
%mend print_iid;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IIE ORHPAN RECORDS
********************************************************************************;

proc format;
     value $ord
        "DEMOGRAPHIC"="01"
        "ENROLLMENT"="02"
        "ENCOUNTER"="03"
        "DIAGNOSIS"="04"
        "PROCEDURES"="05"
        "VITAL"="06"
        "DISPENSING"="07"
        "LAB_RESULT_CM"="08"
        "CONDITION"="09"
        "PRO_CM"="10"
        "PRESCRIBING"="11"
        "PCORNET_TRIAL"="12"
        "DEATH"="13"
        "DEATH_CAUSE"="14"
        "HARVEST"="15"
        "MED_ADM"="16"
        "OBS_CLIN"="17"
        "OBS_GEN"="18"
        "LDS_ADDRESS_HISTORY"="19"
        "IMMUNIZATION"="20"
        "HASH_TOKEN"="21"
        other=" "
        ;

     value dcheck
        8.01 = "1.08"
        9.01 = "1.09"
       10.01 = "1.10"
       11.01 = "1.11"
       12.01 = "1.12"
       14.01 = "1.14"
       19.01 = "1.19"
        other = ' '
        ;

     value dcdesc
         8.01='Orphan PATIDs'
         9.01='Orphan ENCOUNTERIDs'
        10.01='Replication errors'
        11.01='More than 5% of encounters'
        11.02='are assigned to more than'
        11.03='one patient'
        12.01='Orphan PROVIDERIDs'
        14.01='Orphan PATIDs'
        19.01='More than 10% of hash'
        19.02='tokens are assigned to'
        19.03='multiple patients'
        other = ' '
        ;

    value except
         8.01='Orphan PATID(S) in the'
         8.02='CONDITION, DIAGNOSIS, DEATH,'
         8.03='DEATH_CAUSE, DISPENSING,'
         8.04='ENCOUNTER, ENROLLMENT,'
         8.05='HASH_TOKEN, IMMUNIZATION,'
         8.06='LAB_RESULT_CM,'
         8.07='LDS_ADDRESS_HISTORY,'
         8.08='MED_ADMIN, OBS_CLIN, OBS_GEN,'
         8.09='PCORNET_TRIAL, PRESCRIBING,'
         8.10='PROCEDURES, PRO_CM, or'
         8.11='VITAL table'
         9.01='Orphan ENCOUNTERID(S) for more' 
         9.02='than 5% of the records in the'
         9.03='CONDITION, DIAGNOSIS,'
         9.04='IMMUNIZATION, LAB_RESULT_CM,'
         9.05='MED_ADMIN, OBS_CLIN, OBS_GEN,'
         9.06='PRESCRIBING, PROCEDURES,'
         9.07='PRO_CM, or VITAL table'
        10.01='Replication error(s) in ENC_TYPE'
        10.02='or ADMIT_DATE in the DIAGNOSIS'
        10.03='or PROCEDURES table'
        11.01='An ENCOUNTERID in the'
        11.02='CONDITION, DIAGNOSIS,'
        11.03='ENCOUNTER, IMMUNIZATION,'
        11.04='LAB_RESULT_CM, MED_ADMIN,'
        11.05='OBS_CLIN, OBS_GEN, PRESCRIBING,'
        11.06='PROCEDURES, PRO_CM, or VITAL'
        11.07='table is associated with more'
        11.08='than 1 PATID in the same table'
        12.01='Orphan PROVIDERID(S) in the'
        12.02='ENCOUNTER, DIAGNOSIS,'
        12.03='IMMUNIZATION, MED_ADMIN,'
        12.04='OBS_CLIN, OBS_GEN, PRESCRIBING,'
        12.05='or PROCEDURES table.'
        14.01='A PATID in the DEMOGRAPHIC'
        14.02='table is not included in the'
        14.03='HASH_TOKEN table'
        19.01='More than 10% of the values in'
        19.02='any hash token field (TOKEN_01,'
        19.03='TOKEN_02, TOKEN_03, TOKEN_04,'
        19.04='TOKEN_05, or TOKEN_16) are' 
        19.05='assigned to multiple patients'
        other = ' '
        ;
    
     value source
         8.01='XTBL_L3_MISMATCH;'
         8.02='ENR_L3_N; ENC_L3_N;'
         8.03='DIA_L3_N; PRO_L3_N;'
         8.04='VIT_L3_N; LAB_L3_N;'
         8.05='PRES_L3_N; DISP_L3_N;'
         8.06='DEATH_L3_N;'
         8.07='DEATHC_L3_N;'
         8.08='COND_L3_N;'
         8.09='PROCM_L3_N;'
         8.10='TRIAL_L3_N;'
         8.11='OBSCLIN_L3_N;'
         8.12='OBSGEN_L3_N;'
         8.13='MEDADM_L3_N;'
         8.14='HASH_L3_N;'
         8.15='IMMUNE_L3_N;'
         8.16='LDSADRS_L3_N'
         9.01='XTBL_L3_MISMATCH;'
         9.02='DIA_L3_N; PRO_L3_N;'
         9.03='VIT_L3_N; PRES_L3_N;'
         9.04='LAB_L3_N;'
         9.05='PROCM_L3_N;'
         9.06='COND_L3_N;'
         9.07='OBSCLIN_L3_N;'
         9.08='OBSGEN_L3_N;'
         9.09='MEDADM_L3_N;'
         9.10='IMMUNE_L3_N'
        10.01='XTBL_L3_MISMATCH'
        11.01='XTBL_L3_NON_UNIQUE;'
        11.02='COND_L3_N; DIA_L3_N;'
        11.03='ENC_L3_N: LAB_L3_N;'
        11.04='PRES_L3_N; PRO_L3_N;'
        11.05='VIT_L3_N;'
        11.06='PROCM_L3_N;'
        11.07='OBSCLIN_L3_N;'
        11.08='OBSGEN_L3_N;'
        11.09='MEDADM_L3_N;'
        11.10='IMMUNE_L3_N'
        12.01='XTBL_L3_MISMATCH;'
        12.02='DIA_L3_N; ENC_L3_N;'
        12.03='MEDADM_L3_N;'
        12.04='OBSCLIN_L3_N;'
        12.05='OBSGEN_L3_N;'
        12.06='PRES_L3_N; PRO_L3_N;'
        12.07='IMMUNE_L3_N'
        14.01='XTBL_L3_MISMATCH'
        19.01='HASH_L3_N'
        other = ' '
        ;
run;

*- Create denominators -*;
data denom;
     merge dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_enc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="VIT_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_vit))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="LAB_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_lab))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N")
                        rename=(resultn=enc_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PROCM_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N")
                        rename=(resultn=enc_procm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="COND_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_cond))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N")
                        rename=(resultn=enc_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N")
                        rename=(resultn=enc_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="IMMUNE_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") 
                        rename=(resultn=enc_immune))

           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DEM_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_dem))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_enc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENR_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_enr))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="VIT_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_vit))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="LAB_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_lab))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="PATID" and statistic="DISTINCT_N")
                        rename=(resultn=pat_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DISP_L3_N" and variable="PATID" and statistic="DISTINCT_N")
                        rename=(resultn=pat_disp))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DEATH_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_death))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DEATHC_L3_N" and variable="PATID" and statistic="DISTINCT_N")
                        rename=(resultn=pat_deathc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="COND_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_cond))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PROCM_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_procm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="TRIAL_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_trial))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="PATID" and statistic="DISTINCT_N")
                        rename=(resultn=pat_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="PATID" and statistic="DISTINCT_N")
                        rename=(resultn=pat_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="IMMUNE_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_immune))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="LDSADRS_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_ldsadrs))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="HASH_L3_N" and variable="PATID" and statistic="DISTINCT_N") 
                        rename=(resultn=pat_hash))

           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="MEDADMIN_PROVIDERID" and 
                        statistic="DISTINCT_N") rename=(resultn=prov_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="OBSCLIN_PROVIDERID" and 
                        statistic="DISTINCT_N") rename=(resultn=prov_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="OBSGEN_PROVIDERID" and 
                        statistic="DISTINCT_N") rename=(resultn=prov_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="RX_PROVIDERID" and statistic="DISTINCT_N") 
                        rename=(resultn=prov_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") 
                        rename=(resultn=prov_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") 
                        rename=(resultn=prov_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") 
                        rename=(resultn=prov_enc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="IMMUNE_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") 
                        rename=(resultn=prov_immune))
     ;
     by datamartid;
     keep enc_: pat_: prov_:;
run; 

*- Get orphan and foreign key data -*;
data orphan;
     if _n_=1 then do;
         set denom;
     end;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_MISMATCH"
                          and statistic="DISTINCT_N"))
         dc_normal(in=nu where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_NON_UNIQUE"
                          and statistic="DISTINCT_N") rename=(resultn=nonunique));

     if nu then row=11;
     else if scan(table,1,' ')="HASH_TOKEN" then row=14;
     else if scan(variable,1,' ')="PROVIDERID" then row=12;
     else if scan(variable,1,' ')="ENCOUNTERID" then row=9;
     else if scan(variable,1,' ')="PATID" then row=8;
     else if scan(variable,2,' ')="Mismatch" then row=10;
    
     if resultn=0 or nonunique=0 or 
        (row=9 and scan(table,3,' ')^ in ("DIAGNOSIS" "PROCEDURES" "VITAL" "PRESCRIBING" "LAB_RESULT_CM" "CONDITION" "PRO_CM"
                                          "OBS_CLIN" "OBS_GEN" "MED_ADMIN" "IMMUNIZATION" "LDS_ADDRESS_HISTORY")) then delete;

     keep row table variable resultn enc: pat: prov: nonunique;
run;

proc sort data=orphan;
     by row;
run;

data orphan;
     set orphan;
     by row;
     
     retain subrow 0;
     if first.row then subrow = 0;
     subrow=subrow+1;
run;

data token;
     merge dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="HASH_L3_N" and variable^="PATID" and statistic="VALID_DISTINCT_N")
                     rename=(resultn=valid_distinct_n))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="HASH_L3_N" and variable^="PATID" and statistic="VALID_N")
                     rename=(resultn=valid_n))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="HASH_L3_N" and variable^="PATID" and statistic="VALID_DISTINCT_N_PCT")
                     rename=(resultn=valid_distinct_n_pct))
     ;
     by variable;
     row=19;
     subrow=_n_;
     resultn=valid_n-valid_distinct_n;
     nondistinct_n_pct=100-valid_distinct_n_pct;
     if nondistinct_n_pct>=10.0;
     keep row subrow table variable resultn nondistinct_n_pct;
run;

data table;
     do row = 8;
        do subrow = 1 to 16;
            output;
        end;
     end;
     do row = 9;
        do subrow = 1 to 10;
            output;
        end;
     end;
     do row = 10;
        do subrow = 1 to 3;
            output;
        end;
     end;
     do row = 11;
        do subrow = 1 to 10;
            output;
        end;
     end;
     do row = 12;
        do subrow = 1 to 7;
            output;
        end;
     end;
     do row = 14;
        do subrow = 1 to 3;
            output;
        end;
     end;
     do row = 19;
        do subrow = 1 to 5;
            output;
        end;
     end;
run;

data final;
     length col1-col8 $200;
     merge table(in=t) orphan(in=o) token;
     by row subrow;

     row_dec=row+subrow/100;

     col1=put(row_dec,dcheck.);
     col2=put(row_dec,dcdesc.);
     col3=put(row_dec,except.);

     if table^=" " and row in (8,9,10,12,14) then col4=scan(table,3,' ');
     else if table^=" " and row in (11,19) then col4=strip(table);
     else if o then col4=" ";

     if variable^=' ' then col5=scan(variable,1,' ');
     else if subrow=1 then col5="None";

     if row in (8,9,10,12,14,19) then do;
        col6=strip(put(resultn,threshold.));
        col6n=resultn;
     end;
     else if row=11 then do;
        col6=strip(put(nonunique,threshold.));
        col6n=nonunique;
     end;

     if row=8 and resultn>=0 then do;
        if col4="ENROLLMENT" and pat_enr>0 then col7=strip(put((resultn/pat_enr)*100,16.1));
        else if col4="ENCOUNTER" and pat_enc>0 then col7=strip(put((resultn/pat_enc)*100,16.1));
        else if col4="DIAGNOSIS" and pat_dia>0 then col7=strip(put((resultn/pat_dia)*100,16.1));
        else if col4="PROCEDURES" and pat_pro>0 then col7=strip(put((resultn/pat_pro)*100,16.1));
        else if col4="VITAL" and pat_vit>0 then col7=strip(put((resultn/pat_vit)*100,16.1));
        else if col4="DISPENSING" and pat_disp>0 then col7=strip(put((resultn/pat_disp)*100,16.1));
        else if col4="LAB_RESULT_CM" and pat_lab>0 then col7=strip(put((resultn/pat_lab)*100,16.1));
        else if col4="PRESCRIBING" and pat_pres>0 then col7=strip(put((resultn/pat_pres)*100,16.1));
        else if col4="DEATH" and pat_death>0 then col7=strip(put((resultn/pat_death)*100,16.1));
        else if col4="DEATHC" and pat_deathc>0 then col7=strip(put((resultn/pat_deathc)*100,16.1));
        else if col4="CONDITION" and pat_cond>0 then col7=strip(put((resultn/pat_cond)*100,16.1));
        else if col4="PRO_CM" and pat_procm>0 then col7=strip(put((resultn/pat_procm)*100,16.1));
        else if col4="PCORNET_TRIAL" and pat_trial>0 then col7=strip(put((resultn/pat_trial)*100,16.1));
        else if col4="MED_ADMIN" and pat_medadm>0 then col7=strip(put((resultn/pat_medadm)*100,16.1));
        else if col4="OBS_CLIN" and pat_obsclin>0 then col7=strip(put((resultn/pat_obsclin)*100,16.1));
        else if col4="OBS_GEN" and pat_obsgen>0 then col7=strip(put((resultn/pat_obsgen)*100,16.1));
        else if col4="HASH_TOKEN" and pat_hash>0 then col7=strip(put((resultn/pat_hash)*100,16.1));
        else if col4="IMMUNIZATION" and pat_immune>0 then col7=strip(put((resultn/pat_immune)*100,16.1));
        else if col4="LDS_ADDRESS_HISTORY" and pat_ldsadrs>0 then col7=strip(put((resultn/pat_ldsadrs)*100,16.1));
     end;
     else if row=9 and resultn>=0 then do;
        if col4="DIAGNOSIS" and enc_dia>0 then col7=strip(put((resultn/enc_dia)*100,16.1));
        else if col4="PROCEDURES" and enc_pro>0 then col7=strip(put((resultn/enc_pro)*100,16.1));
        else if col4="VITAL" and enc_vit>0 then col7=strip(put((resultn/enc_vit)*100,16.1));
        else if col4="LAB_RESULT_CM" and enc_lab>0 then col7=strip(put((resultn/enc_lab)*100,16.1));
        else if col4="PRESCRIBING" and enc_pres>0 then col7=strip(put((resultn/enc_pres)*100,16.1));
        else if col4="PRO_CM" and enc_procm>0 then col7=strip(put((resultn/enc_procm)*100,16.1));
        else if col4="CONDITION" and enc_cond>0 then col7=strip(put((resultn/enc_cond)*100,16.1));
        else if col4="MED_ADMIN" and enc_medadm>0 then col7=strip(put((resultn/enc_medadm)*100,16.1));
        else if col4="OBS_CLIN" and enc_obsclin>0 then col7=strip(put((resultn/enc_obsclin)*100,16.1));
        else if col4="OBS_GEN" and enc_obsgen>0 then col7=strip(put((resultn/enc_obsgen)*100,16.1));
        else if col4="IMMUNIZATION" and enc_immune>0 then col7=strip(put((resultn/enc_immune)*100,16.1));
     end;
     else if row=10 then do;
        col7="n/a";
     end;
     else if row=11 and nonunique>=0  then do;
        if col4="CONDITION" and enc_cond>0 then col7=strip(put((nonunique/enc_cond)*100,16.1));
        else if col4="DIAGNOSIS" and enc_dia>0 then col7=strip(put((nonunique/enc_dia)*100,16.1));
        else if col4="ENCOUNTER" and enc_enc>0 then col7=strip(put((nonunique/enc_enc)*100,16.1));
        else if col4="LAB_RESULT_CM" and enc_lab>0 then col7=strip(put((nonunique/enc_lab)*100,16.1));
        else if col4="PRESCRIBING" and enc_pres>0 then col7=strip(put((nonunique/enc_pres)*100,16.1));
        else if col4="PROCEDURES" and enc_pro>0 then col7=strip(put((nonunique/enc_pro)*100,16.1));
        else if col4="VITAL" and enc_vit>0 then col7=strip(put((nonunique/enc_vit)*100,16.1));
        else if col4="PRO_CM" and enc_procm>0 then col7=strip(put((nonunique/enc_procm)*100,16.1));
        else if col4="MED_ADMIN" and enc_medadm>0 then col7=strip(put((nonunique/enc_medadm)*100,16.1));
        else if col4="OBS_CLIN" and enc_obsclin>0 then col7=strip(put((nonunique/enc_obsclin)*100,16.1));
        else if col4="OBS_GEN" and enc_obsgen>0 then col7=strip(put((nonunique/enc_obsgen)*100,16.1));
        else if col4="IMMUNIZATION" and enc_immune>0 then col7=strip(put((nonunique/enc_immune)*100,16.1));
     end;
     else if row=12 and resultn>=0 then do;
        if col4="DIAGNOSIS" and prov_dia>0 then col7=strip(put((resultn/prov_dia)*100,16.1));
        else if col4="ENCOUNTER" and prov_enc>0 then col7=strip(put((resultn/prov_enc)*100,16.1));
        else if col4="PROCEDURES" and prov_pro>0 then col7=strip(put((resultn/prov_pro)*100,16.1));
        else if col4="PRESCRIBING" and prov_pres>0 then col7=strip(put((resultn/prov_pres)*100,16.1));
        else if col4="MED_ADMIN" and prov_medadm>0 then col7=strip(put((resultn/prov_medadm)*100,16.1));
        else if col4="OBS_CLIN" and prov_obsclin>0 then col7=strip(put((resultn/prov_obsclin)*100,16.1));
        else if col4="OBS_GEN" and prov_obsgen>0 then col7=strip(put((resultn/prov_obsgen)*100,16.1));
        else if col4="IMMUNIZATION" and prov_immune>0 then col7=strip(put((resultn/prov_immune)*100,16.1));
     end;
     else if row=14 and resultn>=0 then do;
        if col4="DEMOGRAPHIC" and pat_dem>0 then col7=strip(put((resultn/pat_dem)*100,16.1));
     end;
     else if row=19 and resultn>=0 then do;
        col7=strip(put(nondistinct_n_pct,16.1));
     end;

     col8=put(row_dec,source.);
run;

data print_iie;
     set final;
     by row subrow;

     if row in (8,10,12,14) then do;
         if col6n>=.t then c6=1;
         else c6=.;
     end;
     else if row in (9,11) then do;
         if input(col7,16.1)>5.0 then c7=2;
         else c7=.;
     end;
     else if row in (19) then do;
         if input(col7,16.1)>10.0 then c7=2;
         else c7=.;
     end;

     * count records for page break *;
     if row=8 then pg=1;
     else if row in (9,10) then pg=2;
     else if row in (11, 12, 14) then pg=3;
     else pg=4;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iie;     
     length edc_table_s_ $10;
     set print_iie(keep=c6 c7 row_dec row rename=(row=row_int));
     if c6^=. or c7^=.;
     if row_int=14 then row=1.14;
     else row=1+(int(row_dec)/100);
     exception=1;
     edc_table_s_="Table IIE";
     keep edc_table_s_ row exception row_dec;
run;

proc sort data=dc_summary_iie nodupkey;
     by edc_table_s_ row;
run;

*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iie;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iie;
    %let _ftn=IIE;
    %let _hdr=Table IIE;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iie split='|' style(header)=[backgroundcolor=CXCCCCCC] missing;
         column pg row row_dec col1 col2 col3 c6 c7 col4 col5 col6 col7 col8;
         where pg=1;

         define pg       /order order=data noprint;
         define row      /order order=data noprint;
         define row_dec  /order order=data noprint;
         define col1     /display flow "Data|Check" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col2     /display flow "Data Check Description" style(header)=[just=left cellwidth=16%] style(column)=[just=left];
         define col3     /display "Exception" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
         define c6       /display noprint;
         define c7       /display noprint;
         define col4     /display flow "Table(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col5     /display flow "Field(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col6     /display flow "Count" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col7     /display flow "%" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col8     /display flow "Source table(s)" style(header)=[just=left cellwidth=16.25%] style(column)=[just=left];

         compute col6;
            if c6=1 and row^=14 then call define(_col_, "style", "style=[color=red]");
            if c6=1 and row=14 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if c7=2 and row^=19 then call define(_col_, "style", "style=[color=red]");
            if c7=2 and row=19 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    *- Determine how many pages in report -*;
    proc means data=print_iie noprint;
         var pg;
         output out=_iie_pg max=max_pg;
    run;
        
    data _null_;
         set _iie_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_iie split='|' style(header)=[backgroundcolor=CXCCCCCC] missing;
             column pg row_dec col1 col2 col3 row c6 c7 col4 col5 col6 col7 col8;
             where pg=&_pg;

             define pg       /order order=data noprint;
             define row_dec  /order order=data noprint;
             define col1     /display flow "Data|Check" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col2     /display flow "Data Check Description" style(header)=[just=left cellwidth=16%] style(column)=[just=left];
             define col3     /display flow "Exception" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
             define row      /display noprint;
             define c6       /display noprint;
             define c7       /display noprint;
             define col4     /display flow "Table(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
             define col5     /display flow "Field(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
             define col6     /display flow "Count" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col7     /display flow "%" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col8     /display flow "Source table(s)" style(header)=[just=left cellwidth=16.25%] style(column)=[just=left];

             compute col6;
                if c6=1 and row^=14 then call define(_col_, "style", "style=[color=red]");
                if c6=1 and row=14 then call define(_col_, "style", "style=[color=blue]");
             endcomp;
             compute col7;
                if c7=2 and row^=19 then call define(_col_, "style", "style=[color=red]");
                if c7=2 and row=19 then call define(_col_, "style", "style=[color=blue]");
             endcomp;
             break after pg / page;
        run;
        ods listing;
    %end;
%mend print_iie;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IIF CODE ERRORS
********************************************************************************;

proc sort data=dc_normal(keep=datamartid dc_name table variable category 
                              statistic resultn)
     out=print_iif;
     by dc_name table variable category;
     where datamartid=%upcase("&dmid") and dc_name="CODE_SUMMARY";
run;

data print_iif;
     merge print_iif(where=(statistic="RECORDS") rename=(resultn=records))
           print_iif(where=(statistic="CODES") rename=(resultn=codes))
           print_iif(where=(statistic="BAD_RECORDS") rename=(resultn=bad_records))
           print_iif(where=(statistic="BAD_CODES") rename=(resultn=bad_codes))
           print_iif(where=(statistic="MISPLACED_RECORDS") rename=(resultn=misplaced_records))
           print_iif(where=(statistic="MISPLACED_CODES") rename=(resultn=misplaced_cods))
           print_iif(where=(statistic="BAD_RECORD_PCT") rename=(resultn=bad_record_pct))
           print_iif(where=(statistic="MISPLACED_RECORD_PCT") rename=(resultn=misplaced_record_pct))
     ;
     by dc_name table variable category;
     rename category=code_type;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iif;
     length edc_table_s_ $10;
     set print_iif(keep=bad_record_pct misplaced_record_pct);
     exception=1;
     edc_table_s_="Table IIF";

     if bad_record_pct>5.00 then do;
         row=1.13;
         output;
     end;
     else if misplaced_record_pct>0 then do;
         row=1.16;
         output;
     end;
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iif nodupkey;
     by edc_table_s_ row;
run;

*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iif;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iif;
    %let _ftn=IIF;
    %let _hdr=Table IIF;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iif split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column table code_type records codes bad_records bad_record_pct misplaced_records misplaced_record_pct;
    
         define table         /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define code_type     /display flow "Code Type" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define records       /display flow "Records" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define codes         /display flow "Codes" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define bad_records   /display flow "Records with Code Type Errors" style(header)=[just=center cellwidth=12%] 
                                            style(column)=[just=center];
         define bad_record_pct/display format=8.2 flow "Percent of Records with Code Type Errors (Data Check 1.13)" 
                                            style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define misplaced_records/display flow "Records in the Wrong Table" style(header)=[just=center cellwidth=12%] 
                                            style(column)=[just=center];
         define misplaced_record_pct/display format=8.2 flow "Percentage of Records in the Wrong Table (Data Check 1.16)" 
                                            style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         compute bad_record_pct;
             if bad_record_pct>5.0 then call define(_col_, "style", "style=[color=red]");
         endcomp;
         compute misplaced_record_pct;
             if misplaced_record_pct>0 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;    
%mend print_iif;

*- Clear working directory -*;
%clean(savedsn=print:);

********************************************************************************;
* TABLE IIIA FUTURE DATES
********************************************************************************;

*- Macro for each type of data row -*;
%macro stat(dcn=,ord=,ord2=,tbl=,var=,stat=);
     if dc_name="&dcn" and table="&tbl" and variable="&var" and statistic="&stat" then do;
        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if 0<resultn<=llc then resultn=.t;
        if resultn^=.t then col3=strip(put(resultn,comma16.));
        else col3=strip(put(resultn,threshold.));
        col4=" ";
        output;
     end;
%mend stat;

%macro denom(dcn=,ord=,ord2=,tbl=,var=);
     if dc_name="&dcn" and table="&tbl" and variable="&var" and statistic="N" then do;
        ord=&ord;
        ord2=&ord2;
        if 0<resultn<=llc then resultn=.t;
        if resultn^=.t then denom=resultn;
        output;
     end;
%mend denom;

%macro harvest(dcn=,ord=,ord2=,var=);
     if dc_name="&dcn" and variable="&var" and statistic="VALUE" then do;
        ord=&ord;
        ord2=&ord2;
        col1=strip(put(ord,tbl_row.));
        col2=strip("&var");
        if input(resultc,yymmdd10.)>today() then col3="1";
        else col3="0";
        col4=" ";
        output;
     end;
%mend harvest;

*- Non-cross table queries -*;
data tbl(keep=ord: col: dc_name resultn);
     length col1-col4 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid" and dc_name="XTBL_L3_METADATA" and variable="LOW_CELL_CNT")) rename=(resultn=llc));
     set dc_normal(where=(datamartid=%upcase("&dmid")));

     %stat(dcn=XTBL_L3_DATES,ord=1,ord2=1,tbl=DEMOGRAPHIC,var=BIRTH_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=2,ord2=2,tbl=ENROLLMENT,var=ENR_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=3,ord2=4,tbl=DEATH,var=DEATH_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=4,ord2=5,tbl=ENCOUNTER,var=ADMIT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=4,ord2=6,tbl=ENCOUNTER,var=DISCHARGE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=5,ord2=7,tbl=DIAGNOSIS,var=ADMIT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=5,ord2=8,tbl=DIAGNOSIS,var=DX_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=6,ord2=9,tbl=PROCEDURES,var=ADMIT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=6,ord2=10,tbl=PROCEDURES,var=PX_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=7,ord2=11,tbl=VITAL,var=MEASURE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=12,tbl=PRESCRIBING,var=RX_ORDER_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=13,tbl=PRESCRIBING,var=RX_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=14,tbl=PRESCRIBING,var=RX_END_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=9,ord2=15,tbl=DISPENSING,var=DISPENSE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=16,tbl=LAB_RESULT_CM,var=LAB_ORDER_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=17,tbl=LAB_RESULT_CM,var=SPECIMEN_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=18,tbl=LAB_RESULT_CM,var=RESULT_DATE,stat=FUTURE_DT_N)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=25,var=REFRESH_MAX)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=26,tbl=CONDITION,var=REPORT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=27,tbl=CONDITION,var=RESOLVE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=28,tbl=CONDITION,var=ONSET_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=14,ord2=29,tbl=PRO_CM,var=PRO_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=17,ord2=30,tbl=MED_ADMIN,var=MEDADMIN_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=17,ord2=31,tbl=MED_ADMIN,var=MEDADMIN_STOP_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=18,ord2=32,tbl=OBS_CLIN,var=OBSCLIN_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=18,ord2=33,tbl=OBS_CLIN,var=OBSCLIN_STOP_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=19,ord2=34,tbl=OBS_GEN,var=OBSGEN_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=19,ord2=35,tbl=OBS_GEN,var=OBSGEN_STOP_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=21,ord2=36,tbl=LDS_ADDRESS_HISTORY,var=ADDRESS_PERIOD_START,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=21,ord2=37,tbl=LDS_ADDRESS_HISTORY,var=ADDRESS_PERIOD_END,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=22,ord2=38,tbl=IMMUNIZATION,var=VX_RECORD_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=22,ord2=39,tbl=IMMUNIZATION,var=VX_ADMIN_DATE,stat=FUTURE_DT_N)
run;

proc sort data=tbl;
     by ord ord2;
run;

*- Denominators for rows requiring summation -*;
data den(keep=ord: denom);
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid" and dc_name="XTBL_L3_METADATA" and variable="LOW_CELL_CNT")) rename=(resultn=llc));
     set dc_normal(where=(datamartid=%upcase("&dmid")));

     %denom(dcn=XTBL_L3_DATES,ord=1,ord2=1,tbl=DEMOGRAPHIC,var=BIRTH_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=2,ord2=2,tbl=ENROLLMENT,var=ENR_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=3,ord2=4,tbl=DEATH,var=DEATH_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=4,ord2=5,tbl=ENCOUNTER,var=ADMIT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=4,ord2=6,tbl=ENCOUNTER,var=DISCHARGE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=5,ord2=7,tbl=DIAGNOSIS,var=ADMIT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=5,ord2=8,tbl=DIAGNOSIS,var=DX_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=6,ord2=9,tbl=PROCEDURES,var=ADMIT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=6,ord2=10,tbl=PROCEDURES,var=PX_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=7,ord2=11,tbl=VITAL,var=MEASURE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=12,tbl=PRESCRIBING,var=RX_ORDER_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=13,tbl=PRESCRIBING,var=RX_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=14,tbl=PRESCRIBING,var=RX_END_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=9,ord2=15,tbl=DISPENSING,var=DISPENSE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=16,tbl=LAB_RESULT_CM,var=LAB_ORDER_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=17,tbl=LAB_RESULT_CM,var=SPECIMEN_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=18,tbl=LAB_RESULT_CM,var=RESULT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=26,tbl=CONDITION,var=REPORT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=27,tbl=CONDITION,var=RESOLVE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=28,tbl=CONDITION,var=ONSET_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=14,ord2=29,tbl=PRO_CM,var=PRO_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=17,ord2=30,tbl=MED_ADMIN,var=MEDADMIN_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=17,ord2=31,tbl=MED_ADMIN,var=MEDADMIN_STOP_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=18,ord2=32,tbl=OBS_CLIN,var=OBSCLIN_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=18,ord2=33,tbl=OBS_CLIN,var=OBSCLIN_STOP_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=19,ord2=34,tbl=OBS_GEN,var=OBSGEN_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=19,ord2=35,tbl=OBS_GEN,var=OBSGEN_STOP_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=21,ord2=36,tbl=LDS_ADDRESS_HISTORY,var=ADDRESS_PERIOD_START)
     %denom(dcn=XTBL_L3_DATES,ord=21,ord2=37,tbl=LDS_ADDRESS_HISTORY,var=ADDRESS_PERIOD_END)
     %denom(dcn=XTBL_L3_DATES,ord=22,ord2=38,tbl=IMMUNIZATION,var=VX_RECORD_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=22,ord2=39,tbl=IMMUNIZATION,var=VX_ADMIN_DATE)
run;

proc sort data=den;
     by ord ord2;
run;

*- Bring everything together -*;
data print_iiia(drop=col3);
     length col4 col6 $50;
     merge tbl den(in=d);
     by ord ord2;

     if col3 not in (" " "BT") then col3n=input(col3,comma16.);
     else if col3 in ("BT") then col3n=.t;
     col4=strip(put(denom,comma16.));
     if denom>0 and resultn>=0 then col5=strip(put((resultn/denom)*100,6.2));
     col6=strip(dc_name);

     if col1^ in ("OBS_CLIN" "OBS_GEN" "LDS_ADDRESS_HISTORY" "IMMUNIZATION") 
        then pg=1;
     else pg=2;
     
run;

*- Keep records resulting in an exception -*;
data dc_summary_iiia;
     length edc_table_s_ $10;
     set print_iiia(keep=ord col3n col5);
     if (ord=11 and col3n>0) or (input(col5,5.2)>5.00);
     row=2.01;
     exception=1;
     edc_table_s_="Table IIIA";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iiia nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iiia;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iiia;
    %let _ftn=IIIA;
    %let _hdr=Table IIIA;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col3n ord col1 col2 
                ("Records with future dates|____________________________________________" col3 col4 col5) 
                col6;
         where pg=1;

         define pg       /display noprint;
         define col3n    /display noprint;
         define ord      /display noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col3     /computed format=thresholdc. "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "%" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         compute col3;
            if ord=11 and col3n>0 then do;
                col3=col3n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3n;
            end;
         endcomp;
         compute col5;
            if input(col5,5.2)>5.00 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;    

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_iiia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col3n ord col1 col2 
                ("Records with future dates|____________________________________________" col3 col4 col5) 
                col6;
         where pg>1;

         define pg       /order noprint;
         define col3n    /display noprint;
         define ord      /display noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col3     /computed format=thresholdc. "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "%" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
        
         break after pg / page;
         compute col3;
            if ord=11 and col3n>0 then do;
                col3=col3n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3n;
            end;
         endcomp;
         compute col5;
            if input(col5,5.2)>5.00 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;    

    ods listing;
%mend print_iiia;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IIIB RECORDS WITH EXTREME VALUES
********************************************************************************;

*- Create table formats -*;
proc format;
     value field
        1='AGE (derived from BIRTH_DATE)'
        2='HT'
        3='WT'
        4='DIASTOLIC'
        5='SYSTOLIC'
        6='DISPENSE_SUP_GROUP'
        ;

     value low
        1='<0 yrs.'
        2='<0 inches'
        3='<0 lbs.'
        4='<40 mgHg'
        5='<40 mgHg'
        6='<1 day'
        ;

     value high
        1='>89 yrs.'
        2='>=95 inches'
        3='>350 lbs.'
        4='>120 mgHg'
        5='>210 mgHg'
        6='>90 days'
        ;
    
     value median
       .='n/a'
       ;
run;

*- Macro for each type of data row -*;
%macro stat(dcn=,ord=,bound=,cat=,stat=);
     if dc_name="&dcn" and category="&cat" and statistic="&stat" then do;
        ord=&ord;
        bound="&bound";
        col1=strip(table);
        if resultn^=.t then result=strip(put(resultn,comma16.));
        else result=strip(put(resultn,threshold.));
        col12=strip(dc_name);
        output;
     end;
%mend stat;

*- Non-cross table queries -*;
data tbl(keep=ord bound statistic result: col:);
     length col1 col12 result $50;
     set dc_normal(where=(datamartid=%upcase("&dmid")));

     %stat(dcn=DEM_L3_AGEYRSDIST2,ord=1,bound=L,cat=<0 yrs,stat=RECORD_N)
     %stat(dcn=DEM_L3_AGEYRSDIST2,ord=1,bound=H,cat=>89 yrs,stat=RECORD_N)

     %stat(dcn=VIT_L3_HT,ord=2,bound=L,cat=<0,stat=RECORD_N)
     %stat(dcn=VIT_L3_HT,ord=2,bound=H,cat=>=95,stat=RECORD_N)
    
     %stat(dcn=VIT_L3_WT,ord=3,bound=L,cat=<0,stat=RECORD_N)
     %stat(dcn=VIT_L3_WT,ord=3,bound=H,cat=>350,stat=RECORD_N)
    
     %stat(dcn=VIT_L3_DIASTOLIC,ord=4,bound=L,cat=<40,stat=RECORD_N)
     %stat(dcn=VIT_L3_DIASTOLIC,ord=4,bound=H,cat=>120,stat=RECORD_N)
    
     %stat(dcn=VIT_L3_SYSTOLIC,ord=5,bound=L,cat=<40,stat=RECORD_N)
     %stat(dcn=VIT_L3_SYSTOLIC,ord=5,bound=H,cat=>210,stat=RECORD_N)

     %stat(dcn=DISP_L3_SUPDIST2,ord=6,bound=L,cat=<1 day,stat=RECORD_N)
     %stat(dcn=DISP_L3_SUPDIST2,ord=6,bound=H,cat=>90 days,stat=RECORD_N)
run;

proc sort data=tbl;
     by ord;
run;

*- Denominators for rows requiring summation -*;
data denom;
     set dc_normal;
     if statistic="RECORD_N" and category^="NULL or missing" and 
        dc_name in ("DEM_L3_AGEYRSDIST2" "VIT_L3_HT" "VIT_L3_WT" "VIT_L3_DIASTOLIC"
                    "VIT_L3_SYSTOLIC" "DISP_L3_SUPDIST2");

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep dc_name resultn bt_flag;
run;

proc means data=denom nway missing noprint;
     class dc_name bt_flag;
     var resultn;
     output out=xdenom sum=denom;
run;

data xdenom;
     length col6 $50;
     merge xdenom(rename=(bt_flag=flag) where=(flag=" "))
           xdenom(in=bt drop=denom where=(bt_flag="*"))
     ;
     by dc_name;

     col6=strip(put(denom,comma16.))||strip(bt_flag);
     if dc_name="DEM_L3_AGEYRSDIST2" then ord=1;
     else if dc_name="VIT_L3_HT" then ord=2;
     else if dc_name="VIT_L3_WT" then ord=3;
     else if dc_name="VIT_L3_DIASTOLIC" then ord=4;
     else if dc_name="VIT_L3_SYSTOLIC" then ord=5;
     else if dc_name="DISP_L3_SUPDIST2" then ord=6;
run;

proc sort data=xdenom;
     by ord;
run;

*- Get median distribution of height and weight -*;
data median;
     set dc_normal;
     if statistic="MEDIAN" and dc_name in ("VIT_L3_HT_DIST" "VIT_L3_WT_DIST" "DEM_L3_AGEYRSDIST1");
    
     if variable="AGE" then ord=1;
     else if variable="HEIGHT" then ord=2;
     else if variable="WEIGHT" then ord=3;
     rename resultn=median dc_name=added_source;
     keep ord resultn dc_name;
run;

proc sort data=median;
     by ord;
run;

*- Bring everything together -*;
data print_iiib(keep=ord col: );
     length col1-col4 col6-col12 $50;
     merge tbl(where=(statistic="RECORD_N" and bound="L") rename=(result=col7 resultn=col7n))
           tbl(where=(statistic="RECORD_N" and bound="H") rename=(result=col9 resultn=col9n))
           xdenom
           median
     ;
     by ord;
        
     col2=strip(put(ord,field.));
     col3=strip(put(ord,low.));
     col4=strip(put(ord,high.));
     if denom>0 and col7n>.t then col8=strip(put((col7n/denom)*100,16.1));
     if denom>0 and col9n>.t then col10=strip(put((col9n/denom)*100,16.1));
     col11=strip(put(median,median.));
     if ord<=3 then col12=strip(col12) || "; " || strip(added_source);
run;

*- Keep records resulting in an exception -*;
data dc_summary_iiib;
     length edc_table_s_ $10;
     set print_iiib(keep=col8 col10);
     if input(col8,8.2)>10.00 or input(col10,8.2)>10.00;
     row=2.02;
     exception=1;
     edc_table_s_="Table IIIB";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iiib nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iiib;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iiib;
    %let _ftn=IIIB;
    %let _hdr=Table IIIB;

    *- Get titles and footnotes -;
    %ttl_ftn;
    
    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiib split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 ("Data Check| Parameters|______________________" col3 col4) col6
                   ("Records with values in the lowest category|________________" col7 col8)
                   ("Records with values in the highest category|________________" col9 col10) col11 col12;

         define col1     /display flow "Table" style(header)=[just=left] style(column)=[just=left cellwidth=11%];
         define col2     /display flow "Field" style(header)=[just=left] style(column)=[just=left cellwidth=18%];
         define col3     /display flow "Low" style(header)=[just=center] style(column)=[just=center cellwidth=7.5%];
         define col4     /display flow "High" style(header)=[just=center] style(column)=[just=center cellwidth=7.5%];
         define col6     /display flow "Records" style(header)=[just=center] style(column)=[just=center cellwidth=8.5%];
         define col7     /display flow "N" style(header)=[just=center] style(column)=[just=center cellwidth=7.5%];
         define col8     /display flow "%" style(header)=[just=center] style(column)=[just=center cellwidth=3.5%];
         define col9     /display flow "N" style(header)=[just=center] style(column)=[just=center cellwidth=7.5%];
         define col10    /display flow "%" style(header)=[just=center] style(column)=[just=center cellwidth=3.5%];
         define col11    /display flow "Median" style(header)=[just=center] style(column)=[just=center cellwidth=5%];
         define col12    /display flow "Source table" style(header)=[just=left] style(column)=[just=left cellwidth=16%];
         compute col8;
            if input(col8,8.2)>10.00 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col10;
            if input(col10,8.2)>10.00 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods listing;
%mend print_iiib;

*- Clear working directory -*;
%clean(savedsn=print: chart:);


********************************************************************************;
* TABLE IIIC ILLOGICAL DATES
********************************************************************************;

proc format;
     invalue rowfmt
        'ADMIT_DATE < BIRTH_DATE'=1
        'DISCHARGE_DATE < BIRTH_DATE'=2
        'PX_DATE < BIRTH_DATE'=3
        'DX_DATE < BIRTH_DATE'=4
        'MEASURE_DATE < BIRTH_DATE'=5
        'DISPENSE_DATE < BIRTH_DATE'=6
        'RX_START_DATE < BIRTH_DATE'=7
        'RESULT_DATE < BIRTH_DATE'=8
        'DEATH_DATE < BIRTH_DATE'=9
        'MEDADMIN_START_DATE < BIRTH_DATE'=10
        'OBSCLIN_START_DATE < BIRTH_DATE'=11
        'OBSGEN_START_DATE < BIRTH_DATE'=12
        'VX_RECORD_DATE < BIRTH_DATE'=13
        'ADMIT_DATE > DEATH_DATE'=14
        'DISCHARGE_DATE > DEATH_DATE'=15
        'PX_DATE > DEATH_DATE'=16
        'DX_DATE > DEATH_DATE'=17
        'MEASURE_DATE > DEATH_DATE'=18
        'DISPENSE_DATE > DEATH_DATE'=19
        'RX_START_DATE > DEATH_DATE'=20
        'RESULT_DATE > DEATH_DATE'=21
        'MEDADMIN_START_DATE > DEATH_DATE'=22
        'OBSCLIN_START_DATE > DEATH_DATE'=23
        'OBSGEN_START_DATE > DEATH_DATE'=24
        'VX_RECORD_DATE > DEATH_DATE'=25
        'ADMIT_DATE > DISCHARGE_DATE'=26
        'PX_DATE < ADMIT_DATE'=27
        'PX_DATE > DISCHARGE_DATE'=28
        'DX_DATE < ADMIT_DATE'=29
        'DX_DATE > DISCHARGE_DATE'=30
        'OBSCLIN_START_DATE > OBSCLIN_STOP_DATE'=31
        'OBSGEN_START_DATE > OBSGEN_STOP_DATE'=32
    ;

*- Bring everything together -*;
data print_iiic;
     length col1 col2 col4 $60;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("XTBL_L3_DATE_LOGIC")
           and statistic in ("DISTINCT_PATID_N")) rename=(resultn=dist_patid));

     ord=input(category,rowfmt.);

     * track BT values but set missing to zero *;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;

     * create column variables and output *;
     if ord<27 or ord>30 then col1=strip(category);
     else if ord=27 then col1='PX_DATE is More Than 5 Days Prior To The ADMIT_DATE';
     else if ord=28 then col1='PX_DATE is More Than 5 Days After The DISCHARGE_DATE';
     else if ord=29 then col1='DX_DATE is More Than 5 Days Prior To The ADMIT_DATE';
     else if ord=30 then col1='DX_DATE is More Than 5 Days After The DISCHARGE_DATE';

     if bt_dist_patid="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(dist_patid,comma16.));
     if dist_patid>.t and denom>0 then col3p=(dist_patid/denom)*100;
     col4="XTBL_L3_DATE_LOGIC;ENC_L3_N";

     if ord<=23 then pg=1;
     else pg=2;

     keep pg ord col: ;
run;

proc sort data=print_iiic;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iiic;
     length edc_table_s_ $10;
     set print_iiic(keep=col3p);
     if col3p>5.0;
     row=2.03;
     exception=1;
     edc_table_s_="Table IIIC";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iiic nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iiic;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iiic;
    %let _ftn=IIIC;
    %let _hdr=Table IIIC;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiic split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col3p col1 col2 col3 col4;
         where pg=1;

         define pg       /order noprint;
         define col3p    /display noprint;
         define col1     /display flow "DATE_COMPARISON" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /computed flow format=5.1 "Percentage of|total patients in|the ENCOUNTER table" style(header)=[just=center cellwidth=22%] 
                                   style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
         compute col3;
            if col3p>5.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_iiic split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col3p col1 col2 col3 col4;
         where pg>1;

         define pg       /order noprint;
         define col3p    /display noprint;
         define col1     /display flow "DATE_COMPARISON" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /computed flow format=5.1 "Percentage of|total patients in|the ENCOUNTER table" style(header)=[just=center cellwidth=22%] 
                                   style(column)=[just=center];
         define col4     /display flow "Source tables" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
         compute col3;
            if col3p>5.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
    run;

    ods listing;
%mend print_iiic;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IIID ENCOUNTERS PER VISIT AND PER PATIENT
********************************************************************************;

*- Subset -*;
proc sort data=dc_normal(keep=datamartid dc_name statistic category resultn)
     out=data nodupkey;
     by category statistic;
     where datamartid=%upcase("&dmid") and dc_name in ("ENC_L3_ENCTYPE")
           and statistic in ("RECORD_N" "ELIG_RECORD_N" "DISTINCT_PATID_N" "DISTINCT_VISIT_N")
           and category^="Values outside of CDM specifications";
run;

*- Subset and Re-categorize encounter type -*;
data data;
     length cat $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("ENC_L3_ENCTYPE")
         and statistic in ("RECORD_N" "ELIG_RECORD_N" "DISTINCT_PATID_N" "DISTINCT_VISIT_N")
         and category^="Values outside of CDM specifications"));

     * re-categorize encounter type *;
     if category in ("NI" "UN" "OT" "NULL or missing") then cat="Missing, NI, UN or OT";
     else cat=category;

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep statistic cat resultn bt_flag;
run;

*- Summarize counts on new encounter type -*;
proc means data=data missing nway noprint;
     class statistic cat bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

proc sort data=stats;
     by statistic;
run;

data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by statistic cat;
run;

proc sort data=stats;
     by cat;
run;

*- Bring everything together -*;
data print_iiid;
     length col1-col6 col8 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     merge stats(where=(statistic in ("RECORD_N")) rename=(sum=record bt_flag=bt_record))
           stats(where=(statistic in ("ELIG_RECORD_N")) rename=(sum=elig_record bt_flag=bt_elig))
           stats(where=(statistic in ("DISTINCT_VISIT_N")) rename=(sum=dist_visit bt_flag=bt_visit))
           stats(where=(statistic in ("DISTINCT_PATID_N")) rename=(sum=dist_patid bt_flag=bt_patid))
     ;
     by cat;

     ord=put(cat,$svar.);
     col1=strip(put(cat,$etype.));

     if record=. and bt_record="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(record,comma16.));
     if dist_patid=. and bt_patid="*" then col3=strip(put(.t,threshold.));
     else col3=strip(put(dist_patid,comma16.));
     if record>0 and dist_patid>0 then col4=strip(put((record/dist_patid),16.1));

     if elig_record=. and bt_elig="*" then col5=strip(put(.t,threshold.));
     else col5=strip(put(elig_record,comma16.));
     if dist_visit=. and bt_visit="*" then col6=strip(put(.t,threshold.));
     else col6=strip(put(dist_visit,comma16.));
     if elig_record>0 and dist_visit>0 then col7p=(elig_record/dist_visit);

     col8="ENC_L3_ENCTYPE";
    
     keep ord col:;
run;

proc sort data=print_iiid;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iiid;
     length edc_table_s_ $10;
     set print_iiid(keep=col1 col7p);
     if col1 in ('ED (Emergency Dept)' 'EI (ED to IP Stay)' 'IP (Inpatient Hospital Stay)') and col7p>2.0;
     row=2.04;
     exception=1;
     edc_table_s_="Table IIID";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iiid nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iiid;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iiid;
    %let _ftn=IIID;
    %let _hdr=Table IIID;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiid split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col7p col1 col2 col3 col4 col5 col6 col7 col8;

         define col7p    /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
         define col2     /display flow "Encounters" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col3     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col4     /display flow "Encounters|per Patient" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /display flow "Encounters|with known|PROVIDERID" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col6     /display flow "Visit (unique combinations of|PATID, ENC_TYPE, ADMIT_DATE, and PROVIDERID)" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col7     /computed flow format=8.2 "Encounters|with known|PROVIDERID|per visit" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         compute col7;
            if col1 in ('ED (Emergency Dept)' 'EI (ED to IP Stay)' 'IP (Inpatient Hospital Stay)') and col7p>2.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
    run;

    ods listing;
%mend print_iiid;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IIIE SPECIMEN SOURCE DISCREPANCIES
********************************************************************************;

*- Create table formats -*;
proc format;
     value field
        1='Blood, Plasma, Platelet Poor Plasma, Serum, or Serum/Plasma'
        2='Cerebrospinal Fluid'
        3='Urine'
        4='Body Fluid'
        5='Stool'
        6='Cervix/Vagina'
        7='Tissue'
        ;
    
     value spec_ref
        1='Match'
        2='Other'
        ;
run;

*- Subset and Re-categorize source -*;
data data;
     length specimen_source specimen_source2 $26;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("LAB_L3_LOINC_SOURCE")
         and statistic in ("RECORD_N") and category not in ("NI" "UN" "OT" "NULL or missing")));
     specimen_source=cross_category;
     specimen_source2=cross_category2;
     keep specimen_source: category resultn;
run;

proc sort data=data;
     by specimen_source;
run;

proc sort data=specimen_source_category;
     by specimen_source;
run;

*- Add category for cross catagory -*;
data data;
     merge data(in=d) specimen_source_category(in=s);
     by specimen_source;
     if d;
     source_cat=specimen_source_category_num;
run;
    
proc sort data=data;
     by specimen_source2;
run;

*- Add category for cross catagory2-*;
data data;
     merge data(in=d) 
           specimen_source_category(in=s keep=specimen_source specimen_source_category_num
                                         rename=(specimen_source=specimen_source2
                                                 specimen_source_category_num=source_cat2));
     by specimen_source2;
     if d;

     * if record fit a category, check if category2 matched *;
     if source_cat^=. then do;
        if source_cat=source_cat2 then exp_cat=1;
        else exp_cat=2;
     end;

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep source_cat exp_cat category resultn bt_flag;
run;
    
*- Summarize counts at specimen source and LOINC level -*;
proc means data=data missing completetypes nway noprint;
     class source_cat exp_cat/preloadfmt;
     class category bt_flag;
     var resultn;
     output out=stats sum=sum;
     format source_cat field. exp_cat spec_ref.;
     where source_cat^=. and exp_cat^=.;
run;

*- Summarize at specimen source level -*;
data counts(keep=source_cat exp_cat dist_loinc record bt_flag bt_flag_exp)
     denom(keep=record_tot);
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*")) end=eof
     ;
     by source_cat exp_cat category;

     if sum=. then sum=0;

     retain dist_loinc record record_tot 0;
     retain bt_flag_exp;
     if first.exp_cat then do;
        dist_loinc=0; record=0; bt_flag_exp=" ";
     end;
     if sum>0 then dist_loinc=dist_loinc+1;
     record=record+sum;
     if bt_flag="*" and _freq_>=1 then bt_flag_exp="*";
     record_tot=record_tot+sum;
    
     if last.exp_cat then output counts;
     if eof then output denom;
run;

*- Bring everything together -*;
data print_iiie;
     length col1 $300 col2-col4 col6 $75;
     if _n_=1 then set denom;
     set counts end=eof;
     by source_cat exp_cat;

     retain any 0 bt;
     any=any+record;
     if bt_flag_exp="*" then bt="*";

     if first.source_cat then col1=strip(put(source_cat,field.));
     else col1=" ";
     if exp_cat=1 then col2=(put(source_cat,field.));
     else col2=strip(put(exp_cat,spec_ref.));

     col3=strip(put(dist_loinc,comma16.));
     if bt_flag_exp="*" and record=0 then col4=strip(put(.t,threshold.));
     else col4=strip(put(record,comma16.))||strip(bt_flag_exp);
     if record>0 and record_tot>0 then col5p=(record/record_tot)*100;

     col6="LAB_L3_LOINC_SOURCE";
    
     keep col: source_cat exp_cat any bt;

     * output and create Any row *;
     output;
     if eof then do;
        col1="Any of the above";
        col2=" ";
        col3=" ";
        col4=strip(put(any,comma16.))||strip(bt);
        col5p=.;
        output;
     end;
run;

data print_iiie;
     set print_iiie;
     pg=1;
run;
        
*- Keep records resulting in an exception -*;
data dc_summary_iiie;
     length edc_table_s_ $10;
     set print_iiie(keep=source_cat exp_cat col5p);
     if exp_cat=2 and col5p>5.0;
     row=2.05;
     exception=1;
     edc_table_s_="Table IIIE";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iiie nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iiie;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iiie;
    %let _ftn=IIIE;
    %let _hdr=Table IIIE;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiie split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg source_cat exp_cat col5p col1 col2 col3 col4 col5 col6;

         define pg        /order noprint;
         define source_cat/display noprint;
         define exp_cat   /display noprint;
         define col5p     /display noprint;
         define col1      /display flow "Actual Specimen Source Category" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col2      /display flow "Reference Specimen Source Category" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
         define col3      /display flow "LAB_LOINC codes" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col4      /display flow "Records" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5      /computed flow format=8.2 "Percent of records" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6      /display flow "Source table" style(header)=[just=left cellwidth=17%] style(column)=[just=left];
         compute col5;
            if exp_cat=2 and col5p>5.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         break after pg / page;
    run;

    ods listing;
%mend print_iiie;

*- Clear working directory -*;
%clean(savedsn=print: chart:);


********************************************************************************;
* TABLE IIIF LAB RESULT OUTLIERS, SELECTED TESTS
********************************************************************************;

*- Test to see if LAB_RESULT_CM data exist, if not, skip table -*;
%global _ylab_result_cm;

proc sql noprint;
     select count(*) into :_ylab_result_cm from dc_normal
            where table="LAB_RESULT_CM";
quit;

%macro _labexist;
    %if &_ylab_result_cm>0 %then %do;

    %macro _quant;
 
    *- Get appropriate quantitative results -*;
    proc sort data=q2_stat_dlg_loinc out=_quant_loinc;
         by lab_loinc;
         %if &_pedyes=1 %then %do;
             where pedi="Y";
         %end;
         %else %do;
             where pedi="N";
         %end;
    run;

    *- Subset and Re-categorize encounter type -*;
    data data;
         length lab_loinc $50;
         merge dc_normal(in=n where=(datamartid=%upcase("&dmid") and statistic="N" and
                         number>=500 and dc_name in ("LAB_L3_LOINC_RESULT_NUM")) 
                        rename=(resultn=number))
            dc_normal(where=(datamartid=%upcase("&dmid") and statistic="MEDIAN" and 
                         dc_name in ("LAB_L3_LOINC_RESULT_NUM")) rename=(resultn=median))
               dc_normal(where=(datamartid=%upcase("&dmid") and statistic="N" and 
                         dc_name in ("LAB_L3_LOINC_RESULT_NUM")) rename=(resultn=count))
         ;
         by category;
         if n;

         lab_loinc=category;
    
         keep lab_loinc count median;
    run;

    *- Bring everything together -*;
    data print_iiif(keep=col: thresholdflag) anyout(keep=col0);
         length col0-col10 $50;
         merge data(in=d)
               _quant_loinc(in=q)           
         ;
         by lab_loinc;
         if d and q;

         col0=strip(dc_lab_group);
         col1=strip(lab_loinc);
         col2=strip(ex_ucum_units);
         if norm_min^=. then col3=strip(put(norm_min,16.1));
         if norm_max^=. then col4=strip(put(norm_max,16.1));
         col5=strip(put(loinc_median,16.1));
         col6=strip(put(outlier_low,16.1));
         col7=strip(put(outlier_high,16.1));
         col8=strip(put(count,16.));
         col9=strip(put(median,16.1));
         array fl thresholdflag;
         if (median<norm_min and median<outlier_low) or (median>norm_max and median>outlier_high) then do;
            col10="Yes";
            thresholdflag=1;
         end;
         else do;
            col10="No";
            thresholdflag=0;
         end;

         output print_iiif;
         if thresholdflag=1 then output anyout;
    run;

    proc sort data=print_iiif;
         by col0;
    run;

    *- Restrict report to lab groups with at least one outlier -*;
    proc sort data=anyout nodupkey;
         by col0;
    run;

    data print_iiif;
         merge print_iiif anyout(in=a);
         by col0;
         if a;
    run;

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(col1) into :nobs from print_iiif;
    quit;

    *- If obs, continue, else stop -*;
    %if &nobs^=0 %then %do;
        data print_iiif;
             set print_iiif;
             by col0;

             retain cnt pg 0;
             cnt=cnt+1;
             if cnt>11 then do;
                pg=pg+1;
                cnt=1;
             end;
        run;

        *- Keep records resulting in an exception -*;
        data dc_summary_iiif;
             length edc_table_s_ $10;
             set print_iiif(keep=thresholdflag);
             if thresholdflag=1;
             row=2.06;
             exception=1;
             edc_table_s_="Table IIIF";
             keep edc_table_s_ row exception;
        run;

        proc sort data=dc_summary_iiif nodupkey;
             by edc_table_s_ row;
        run;
    
        *- Append exceptions to master dataset -*;
        data dc_summary;
             merge dc_summary dc_summary_iiif;
             by edc_table_s_ row;
        run;
    %end;
    %else %do;
        data print_iiif;
            array _char_ $50 col0-col10;
            pg=1;
            thresholdflag=0;
            col0="No Lab Group contains a Data Check Exception";
            output;
        run;
    %end;

    %mend _quant;
    %_quant;

    *- Produce output -*;
    %macro print_iiif;
        %let _ftn=IIIF;
        %let _hdr=Table IIIF;

        *- Get titles and footnotes -;
        %ttl_ftn;

        ods listing close;
        ods proclabel "&_ttl1";
        title1 justify=left "&_ttl1";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_iiif split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column pg thresholdflag col0 col1 col2 
                ("Data Curation|Normal Range|____________________" col3 col4)
                ("PCORnet Results|______________________________" col5 col6 col7)
                ("DataMart Results|______________________________" col8 col9 col10);

         define pg       /order noprint;
         define thresholdflag /display noprint;
         define col0     /display flow "Data Curation|Lab Group" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col1     /display flow "LAB|LOINC" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col2     /display flow "EX_UCUM_UNITS" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col3     /display flow "Clinical|Lower|Bound" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col4     /display flow "Clinical|Upper|Bound" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         define col5     /display flow "Median" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col6     /display flow "Statistical|Lower|Bound" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col7     /display flow "Statistical|Upper|Bound" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col8     /display flow "N" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col9     /display flow "Median" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col10    /display flow "Outlier" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         compute col10;
            if thresholdflag=1 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
        
         break after pg / page;
        run;    

        ods listing;
    %mend print_iiif;

    *- Clear working directory -*;
    %clean(savedsn=print: chart:);

    %end;
%mend _labexist;
%_labexist;

********************************************************************************;
* TABLE IIIG MONTHLY OUTLIERS
********************************************************************************;
%global nback nsd exp_vital exp_prescribing exp_lab_result_cm;
%put _global_;

*- Macro variables for number of lookback periods and number of std deviations -*;
%let nback=12;
%let nsd=-7;
    
*- Macro to prevent open code -*;
%macro etst;

%if %upcase(&ets_inst)=YES %then %do;

*- Get P5 date values for each table -*;
data p5values;
     format p5_date date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name='XTBL_L3_DATES' and statistic="P5" and 
         ((table in ("ENCOUNTER" "DIAGNOSIS" "PROCEDURES") and variable="ADMIT_DATE") or 
          (table in ("PRESCRIBING") and variable="RX_ORDER_DATE") or 
          (table in ("LAB_RESULT_CM") and variable="RESULT_DATE") or 
          (table in ("VITAL") and variable="MEASURE_DATE"))));

     p5_date=mdy(input(scan(resultc,2,'_'),8.),1,input(scan(resultc,1,'_'),8.));

     keep table p5_date;
run;

proc sort data=p5values;
     by table;
run;

*- Determine Month 11 -*;
data month11;
     format month11_date date9.;
     set dc_normal(keep=datamartid dc_name variable resultc 
                   where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     month11_date=intnx('month',input(resultc,yymmdd10.),-11);

     keep month11_date;
run;

*- Get monthly number of observations -*;
data data;
     format month date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         ((dc_name in ('ENC_L3_ENCTYPE_ADATE_YM' 'DIA_L3_ENCTYPE_ADATE_YM' 'PRO_L3_ENCTYPE_ADATE_YM') and 
          cross_category ne "NULL or missing" and category in ("AV", "ED", "IP", "EI")) or
         (dc_name in ('VIT_L3_MDATE_YM' 'PRES_L3_ODATE_YM' 'LAB_L3_RDATE_YM') and category ne "NULL or missing"))))
     ;

     * set below threshold values to zero *;
     if resultn=.t then resultn=0;
    
     * create a month variable *;
     if dc_name in ('ENC_L3_ENCTYPE_ADATE_YM' 'DIA_L3_ENCTYPE_ADATE_YM' 'PRO_L3_ENCTYPE_ADATE_YM') then 
            month=mdy(input(scan(cross_category,2,'_'),8.),1,input(scan(cross_category,1,'_'),8.));
     else if dc_name in ('VIT_L3_MDATE_YM' 'PRES_L3_ODATE_YM' 'LAB_L3_RDATE_YM') then 
            month=mdy(input(scan(category,2,'_'),8.),1,input(scan(category,1,'_'),8.));

     keep dc_name table month category resultn;
run;


*- Determine average and standard deviation based on a rolling average -*;
%macro exp(dsn=,cat=1);

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(table) into :enobs from data
            where table="&dsn";
    quit;
    
    *- If obs, continue, else stop -*;
    %if &enobs^=0 %then %do;
        proc expand data=data(where=(table="&dsn")) out=expand_&dsn;
             convert resultn = lag_avg / method = none transformout = (movave &nback lag 1);
             convert resultn = lag_std / method = none transformout = (movstd &nback lag 1);
             by dc_name %if &cat=1 %then %do; category %end; ;
        run;
        %let exp_&dsn=YES;
    %end;
    %else %do;
        data expand_&dsn;
             length table $50;
             table="&dsn";
             output;
             call symput("exp_&dsn","NO");
        run;
    %end;

%mend exp;
%exp(dsn=ENCOUNTER);
%exp(dsn=DIAGNOSIS);
%exp(dsn=PROCEDURES);
%exp(dsn=VITAL,cat=0);
%exp(dsn=PRESCRIBING,cat=0);
%exp(dsn=LAB_RESULT_CM,cat=0);

*- Bring everything together -*;
data print_iiig;
     length col1-col9 $100;
     if _n_=1 then set month11;
     merge expand_encounter(in=c1) 
           expand_diagnosis(in=c2) 
           expand_procedures (in=c3)
           expand_vital(in=nc1)
           expand_prescribing(in=nc2)
           expand_lab_result_cm(in=nc3)
           p5values
     ;
     by table;

     * drop data outside limits of interest *;    
     if lag_avg<500 or lag_std<=0 or .<month<=p5_date or month>=month11_date or
        month in ('01MAR2020'd '01APR2020'd) then delete;
    
     * determine the number of standard deviations *;
     dsd=(resultn-lag_avg)/lag_std;

     * keep only months where no obs or beyond the limit of std deviations *;
     if resultn=0 or dsd<&nsd;
    
     array o c1 c2 c3 nc1 nc2 nc3;
        do oi = 1 to dim(o);
          if o{oi}=1 then ord = oi;
        end;

     col1=strip(table);
     if month(month11_date)>1 then col2=put(month(p5_date),mnth.)||", "||put(year(p5_date),4.)||" - "||
          put(month(month11_date)-1,mnth.)||", "||put(year(month11_date),4.);
     else if month(month11_date)=1 then col2=put(month(p5_date),mnth.)||", "||put(year(p5_date),4.)||
          " - Dec, "||put(year(month11_date)-1,4.);
     if c1 or c2 or c3 then col3=category;
     else col3="n/a";
     col4=put(year(month),4.)||"-"||put(month(month),mnth.);
     col5=strip(put(resultn,8.));
     col6=strip(put(lag_avg,8.));
     col7=strip(put(lag_std,8.));
     col8=strip(put(dsd,8.2));
     col9=strip(dc_name)||"; XTBL_L3_DATES";
    
     keep ord col: dsd resultn month;
run;

proc sort data=print_iiig;
     by ord col1 col3 month;
run;

*- Check to see if any obs after subset -*;
proc sql noprint;
     select count(col1) into :nobs from print_iiig;
quit;

%macro check;

*- If obs, continue, else stop -*;
%if &nobs^=0 %then %do;
    *- Keep records resulting in an exception -*;
    data dc_summary_iiig;
         length edc_table_s_ $10;
         set print_iiig;
         row=2.08;
         exception=1;
         edc_table_s_="Table IIIG";
         keep edc_table_s_ row exception;
    run;

    proc sort data=dc_summary_iiig nodupkey;
         by edc_table_s_ row;
    run;
    
    *- Append exceptions to master dataset -*;
    data dc_summary;
         merge dc_summary dc_summary_iiig;
         by edc_table_s_ row;
    run;
%end;
%else %do;
    data print_iiig;
        array _char_ $50 col1-col9;
        resultn=.;
        dsd=.;
        pg=1;
        col2="No Data Table contains a Data Check Exception";
        output;
    run;
%end;
%mend check;
%check;

%end;
%else %do;
    *- If no ETS, create dummy data set to satisfy PROC EXPAND usage later on -*;
    data expand;
    run;

    *- Create output stating no ETS -*;
    data print_iiig;
        array _char_ $50 col1-col9;
        resultn=.; 
        dsd=&nsd;
        pg=1;
        col2="Not eligible (SAS_ETS is required)";
        output;
    run;
%end;

%mend etst;
%etst;

*- Produce output -*;
%macro print_iiig;
    %let _ftn=IIIG;
    %let _hdr=Table IIIG;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iiig split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column resultn dsd col1 col2 col3 col4 col5 col6 col7 col8 col9;

         define resultn /display noprint;
         define dsd     /display noprint;
         define col1    /display flow "Table" style(header)=[just=left cellwidth=11%] style(column)=[just=left];
         define col2    /display flow "Evaluation Window^" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col3    /display flow "Encounter|Type" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col4    /display flow "Exception|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col5    /display flow "Records" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6    /display flow "Average Records in the Previous 12 Months" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col7    /display flow "Standard Deviation in the Previous 12 Months" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col8    /display flow "Difference|Ratio^^" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col9    /display flow "Source table" style(header)=[just=left cellwidth=21%] style(column)=[just=left];
         compute col5;
            if resultn=0 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col8;
            if dsd<&nsd then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;    

    ods listing;
%mend print_iiig;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* CHART IIIA/B/C SIGNIFICANT DECREASES IN ENCOUNTER VOLUMES PER MONTH (CATEGORY)
********************************************************************************;
*- Macro to prevent open code -*;
%macro etsca;

%if %upcase(&_sasets)=YES %then %do;

%macro chart_iii_cat(chart,tbl,cat1,cat2,cat3,cat4,c1,c2,c3,c4,gcont);

*- Create macro variable for output name -*;
%let _hdr=Chart III&chart;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Determine Month 1 and create a record for the 60 preceding months -*;
data chart_months_&tbl;
     format xdate date9.;
     set dc_normal(keep=dc_name variable resultc 
         where=(dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     start_date=intnx('month',input(resultc,yymmdd10.),-11);
     fmtname="CHARTIII";

     do i = -1 to -61 by -1;
        xdate=intnx('month',start_date,i);
        start=i+62;
        if (start=1 or mod(start,6)=1) then
            label=put(month(xdate),mnth.) || " " || put(year(xdate),4.);
        else label=" ";
        output;
     end;

     keep xdate start label fmtname;
run;

*- Create format for x-axis -*;
proc format cntlin=chart_months_&tbl;
run;

proc sort data=chart_months_&tbl;
     by xdate;
run;

data chart_months_&tbl;
     length category $255;
     set chart_months_&tbl;
     by xdate;

     do category = "&cat1", "&cat2", "&cat3", "&cat4";
        output;
     end;
run;

proc sort data=chart_months_&tbl;
     by category xdate;
run;

proc sort data=expand_&tbl(keep=month category resultn lag: rename=(month=xdate));
     by category xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     format xdate date9.;
     merge chart_months_&tbl(in=a) expand_&tbl;
     by category xdate;
     if a;
run;

*- Calculate min and max -*;
proc means data=data nway noprint;
     var resultn;
     output out=standard max=max sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data and to set axis range -*;
data _null;
     set standard;

     d=int(log10(max));
     f=ceil(max/(10**d));
     r=f*(10**d);

     call symput('_ch_iii',compress(put(sum,16.)));
     call symput('uvaxis',compress(put(r,16.)));
     call symput('_interval',compress(put((10**d)/2,16.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_ch_iii^= and &_ch_iii^=0 %then %do;

    *- Calculate deviations from the mean -*;
    data chart_iii&chart;
         set data;
         by category;
    
         retain count;
         if first.category then count=0;
         count= count+1;
    run;

        *- Create annotate dataset for exceptions -*;
    data chart_iii&chart._anno;
         set chart_iii&chart;

         * drop data outside limits of interest *;    
         if lag_avg<500 or lag_std<=0 or xdate in ('01MAR2020'd '01APR2020'd) 
            then delete;
        
         if (resultn-lag_avg)/lag_std<&nsd then do;
             xsys='2'; ysys='2'; function='label'; y=resultn; x=count; size=1.5; 
             color="black"; style='special'; text='J';
             output;
         end;
    run;

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(x) into :nobs from chart_iii&chart._anno;
    quit;

    *- If obs, continue, else stop -*;
    %if &nobs^=0 %then %do;

        *- Produce output -*;
        ods listing close;
        ods proclabel "&_ttl1";
        title1 justify=left "&_ttl1";
        title2 justify=left h=2.5 "&_ttl2";
        symbol1  c=red line=1 v=NONE interpol=join;
        footnote " ";

        axis1 order=(1 to 61 by 6) label=("ADMIT_DATE" h=1.5)
              minor=none
              offset=(0.5,1);

        axis2 order=0 to &uvaxis by &_interval label=(angle=90 "Number of records")
              minor=none
              offset=(0.5,.75);
    
        legend1 label=none noframe;

        symbol1  c=&c1 line=1 v=NONE interpol=join;
        symbol2  c=&c2 line=1 v=NONE interpol=join;
        symbol3  c=&c3 line=1 v=NONE interpol=join;
        symbol4  c=&c4 line=1 v=NONE interpol=join;

        proc gplot data=chart_iii&chart;
             plot resultn*count=category / haxis=axis1 vaxis=axis2 legend=legend1 grid annotate=chart_iii&chart._anno;
             format category $cat. count chartiii.;
        run;
        quit;
    %end;
%end;
%mend chart_iii_cat;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

%end;

%mend etsca;
%etsca;

********************************************************************************;
* CHART IIID/E/F SIGNIFICANT DECREASES IN ENCOUNTER VOLUMES PER MONTH (NON-CAT)
********************************************************************************;
*- Macro to prevent open code -*;
%macro etscd;

%if %upcase(&_sasets)=YES %then %do;

%macro chart_iii_noncat(chart,tbl,xlbl);

*- Create macro variable for output name -*;
%let _hdr=Chart III&chart;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Determine Month 1 and create a record for the 60 preceding months -*;
data chart_months_&tbl;
     format xdate date9.;
     set dc_normal(keep=dc_name variable resultc 
         where=(dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     start_date=intnx('month',input(resultc,yymmdd10.),-11);
     fmtname="CHARTIII";

     do i = -1 to -61 by -1;
        xdate=intnx('month',start_date,i);
        start=i+62;
        if (start=1 or mod(start,6)=1) then
            label=put(month(xdate),mnth.) || " " || put(year(xdate),4.);
        else label=" ";
        output;
     end;

     keep xdate start label fmtname;
run;

*- Create format for x-axis -*;
proc format cntlin=chart_months_&tbl;
run;

proc sort data=chart_months_&tbl;
     by xdate;
run;

proc sort data=expand_&tbl(keep=month resultn lag: rename=(month=xdate));
     by xdate;
run;

*- Bring five year data and actual data together -*;
data chart_iii&chart;
     format xdate date9.;
     merge chart_months_&tbl(in=a) expand_&tbl;
     by xdate;
     if a;
run;

*- Calculate min and max -*;
proc means data=chart_iii&chart nway noprint;
     var resultn;
     output out=standard min=min max=max sum=sum;
run;

*- Create macro variable to test if any non-zero results exist in data and to set axis range -*;
data _null_;
     set standard;
     d=int(log10(max));
     f=ceil(max/(10**d));
     r=f*(10**d);

     call symput('_ch_iii',compress(put(sum,16.)));
     call symput('uvaxis',compress(put(r,16.)));
     call symput('_interval',compress(put((10**d)/2,16.)));
run;

*- If non-zero results exists, produce chart -*;
%if &_ch_iii^= and &_ch_iii^=0 %then %do;

    *- Create annotate dataset for exceptions -*;
    data chart_iii&chart._anno;
         set chart_iii&chart;

         * drop data outside limits of interest *;    
         if lag_avg<500 or lag_std<=0 or xdate in ('01MAR2020'd '01APR2020'd) 
            then delete;
        
         if (resultn-lag_avg)/lag_std<&nsd then do;
             xsys='2'; ysys='2'; function='label'; y=resultn; x=start; size=1.2; 
             color="black"; style='special'; text='J';
             output;
         end;
    run;

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(x) into :nobs from chart_iii&chart._anno;
    quit;

    *- If obs, continue, else stop -*;
    %if &nobs^=0 %then %do;

        *- Produce output -*;
        ods listing close;
        ods proclabel "&_ttl1";
        title1 justify=left "&_ttl1";
        title2 justify=left h=2.5 "&_ttl2";
        symbol1  c=red line=1 v=NONE interpol=join;
        footnote " ";

        axis1 order=(1 to 61 by 6) label=("&xlbl" h=1.5)
              minor=none
              offset=(0.5,1);

        axis2 order=0 to &uvaxis by &_interval label=(angle=90 "Number of records")
              minor=none
              offset=(0.5,.75);
    
        proc gplot data=chart_iii&chart;
             plot resultn*start / haxis=axis1 vaxis=axis2 nolegend grid annotate=chart_iii&chart._anno;
             format start chartiii.;
        run;
        quit;
    %end;
%end;
%mend chart_iii_noncat;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

%end;

%mend etscd;
%etscd;

********************************************************************************;
* TABLE IVA DIAGNOSIS RECORDS PER ENCOUNTER, OVERALL AND BY ENCOUNTER TYPE
********************************************************************************;

*- Subset and Re-categorize encounter type -*;
data data;
     length cat $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         category^="Values outside of CDM specifications" and 
         (dc_name in ("DIA_L3_ENCTYPE" "ENC_L3_ENCTYPE")) or
         (dc_name="DIA_L3_DXTYPE_ENCTYPE" and category^ in ("Values outside of CDM specifications") and
            cross_category^ in ("NI" "UN" "OT" "NULL or missing" "Values outside of CDM specifications")))) 
     ;

     * re-categorize encounter type *;
     if category in ("NI" "UN" "OT" "NULL or missing") then cat="Missing, NI, UN or OT";
     else cat=category;
    
     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;
     output;
     cat="TOTAL";
     output;
     keep dc_name cat resultn bt_flag;
run;

*- Summarize counts on new encounter type -*;
proc means data=data missing nway noprint;
     class dc_name cat bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name cat;
run;

proc sort data=stats;
     by cat;
run;

*- Bring everything together -*;
data print_iva(keep=ord col: perc thresholdflag);
     length col1-col5 col7 $100;
     merge stats(where=(scan(dc_name,1,'_')="ENC") rename=(sum=encounter bt_flag=bt_enc))
           stats(where=(dc_name="DIA_L3_ENCTYPE") rename=(sum=diagnosis bt_flag=bt_dia))
           stats(where=(dc_name="DIA_L3_DXTYPE_ENCTYPE") rename=(sum=diagnosis_known bt_flag=bt_dia_known))
     ;
     by cat;

     ord=put(cat,$svar.);
     col1=strip(put(cat,$etype.));
     if diagnosis=. and bt_dia="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(diagnosis,comma16.))||strip(bt_dia);
     if diagnosis_known=. and bt_dia_known="*" then col3=strip(put(.t,threshold.));
     else col3=strip(put(diagnosis_known,comma16.))||strip(bt_dia_known);
     if encounter=. and bt_enc="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(encounter,comma16.))||strip(bt_enc);
     if encounter>0 then col5=strip(put((diagnosis/encounter),16.2));
     if encounter>0 and diagnosis_known^=. then perc=diagnosis_known/encounter;
     col7=strip("DIA_L3_ENCTYPE; DIA_L3_DXTYPE_ENCTYPE; ENC_L3_ENCTYPE");

     if cat in ('AV' 'ED' 'EI' 'IP' 'TH') and .<perc<1.00 then thresholdflag=1;
run;

proc sort data=print_iva;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iva;
     length edc_table_s_ $10;
     set print_iva(keep=thresholdflag);
     if thresholdflag=1;
     row=3.01;
     exception=1;
     edc_table_s_="Table IVA";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_iva nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_iva;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_iva;
    %let _ftn=IVA;
    %let _hdr=Table IVA;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_iva split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column thresholdflag perc col1 col2 col3 col4 col5 col6 col7;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=14%] style(column)=[just=left];
         define col2     /display flow "DIAGNOSIS|records" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col3     /display flow "DIAGNOSIS|records with|known DX_TYPE" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "ENCOUNTER|records" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "Diagnosis|records|per encounter" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /computed format=8.2 "Diagnosis|records with|known DX_TYPE|per encounter" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col7     /display flow "Source table" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
         compute col6;
            if thresholdflag=1 then do;
               col6=perc;
               call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
               col6=perc;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_iva;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* CHART IVA DIAGNOSIS RECORDS PER ENCOUNTER BY ADMIT DATE AND BY ENCOUNTER TYPE
********************************************************************************;
%macro chart_iva(cat1,cat2,cat3,cat4,cat5,cat6,cat7,cat8);
    
%let _hdr=Chart IVA;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name in ("DIA_L3_ENCTYPE_ADATE_YM" "ENC_L3_ENCTYPE_ADATE_YM") and
         category in ("ED" "EI" "IP" "IS" "OA" "AV" "IC" "OS") and cross_category^="NULL or missing"));

     if resultn=.t then resultn=0;
     xdate=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));
    
     keep dc_name category xdate resultn;
run;
    
proc sort data=data;
     by category xdate;
run;

*- Bring five year data and actual data together -*;
data data;
     merge data( where=(dc_name="DIA_L3_ENCTYPE_ADATE_YM") rename=(resultn=diagnosis))
           data( where=(dc_name="ENC_L3_ENCTYPE_ADATE_YM") rename=(resultn=encounter))
     ;
     by category xdate;
run;

data chart_months_enc;
     length category $255;
     set chart_months(keep=xdate);
     by xdate;
     do category = "&cat1", "&cat2", "&cat3", "&cat4", "&cat5", "&cat6", "&cat7", "&cat8";
        output;
     end;
run;

proc sort data=chart_months_enc;
     by category xdate;
run;

data data;
     merge chart_months_enc(in=a keep=category xdate) data;
     by category xdate;
     if a;
     if diagnosis<=0 or encounter<=0 then rate=.;
     else rate=diagnosis/encounter;
run;

*- Create a count variable to represent each month -*;
data final;
     set data;
     by category;
    
     retain count;
     if first.category then count=0;
     count= count+1;
run;

*- Determine the maximum value for the vertical axis -*;
proc means data=data nway noprint;
     var rate;
     output out=vertaxis max=max;
run;

data _null_;
     set vertaxis;
     if max>=250 then do;
        vertaxis=max-mod(max,25)+25;
        tick=25;
     end;
     else if max>=100 then do;
        vertaxis=max-mod(max,10)+10;
        tick=10;
     end;
     else if max>=10 then do;
        vertaxis=max-mod(max,5)+5;
        tick=5;
     end;
     else if max<10 then do;
        vertaxis=10;
        tick=1;
     end;
     call symput('vertaxis',compress(put(vertaxis,8.)));
     call symput('bytick',compress(put(tick,8.)));
run;

*- Produce output -*;
ods listing close;
ods proclabel "&_ttl1";
title1 justify=left "&_ttl1";
title2 justify=left h=2.5 "&_ttl2";
footnote " ";

symbol1  c=red line=1 v=NONE interpol=join;
symbol2  c=blue line=1 v=NONE interpol=join;
symbol3  c=green line=1 v=NONE interpol=join;
symbol4  c=purple line=1 v=NONE interpol=join;
symbol5  c=olive line=1 v=NONE interpol=join;
symbol6  c=brown line=1 v=NONE interpol=join;
symbol7  c=bipb line=1 v=NONE interpol=join;
symbol8  c=black line=1 v=NONE interpol=join;

axis1 order=(1 to 61 by 6) label=("ADMIT_DATE" h=1.5)
      minor=none
      offset=(0.5,1);

axis2 order=0 to &vertaxis by &bytick label=(angle=90 "Diagnosis Records per Encounter" h=1.5)
      minor=(n=1)
      offset=(0.5,.75);

legend1 label=none noframe;

proc gplot data=final;
     plot rate*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid;
     format count chart.;
     where rate^=. and count^=.;
run;
quit;

%mend chart_iva;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVB DIAGNOSIS RECORDS PER ENCOUNTER, OVERALL AND BY ENCOUNTER TYPE
********************************************************************************;

*- Subset and Re-categorize encounter type -*;
data data;
     length cat $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         category^="Values outside of CDM specifications" and 
         (dc_name in ("ENC_L3_ENCTYPE" "PRO_L3_ENCTYPE")) or
         (dc_name="PRO_L3_PXTYPE_ENCTYPE" and category^ in ("Values outside of CDM specifications") and
            cross_category^ in ("NI" "UN" "OT" "NULL or missing" "Values outside of CDM specifications"))))
     ;

     * re-categorize encounter type *;
     if category in ("NI" "UN" "OT" "NULL or missing") then cat="Missing, NI, UN or OT";
     else cat=category;

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;
     output;
     cat="TOTAL";
     output;
     keep dc_name cat resultn bt_flag;
run;

*- Summarize counts on new encounter type -*;
proc means data=data missing nway noprint;
     class dc_name cat bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

data stats;
     merge stats(in=s rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name cat;
     if bt and not s and sum=. then sum=0;
run;

proc sort;
     by cat;
run;

*- Bring everything together -*;
data print_ivb(keep=ord col: perc thresholdflag);
     length col1-col5 col7 $100;
     merge stats(where=(scan(dc_name,1,'_')="ENC") rename=(sum=encounter bt_flag=bt_enc))
           stats(where=(dc_name="PRO_L3_ENCTYPE") rename=(sum=procedure bt_flag=bt_pro))
           stats(where=(dc_name="PRO_L3_PXTYPE_ENCTYPE") rename=(sum=procedure_known bt_flag=bt_pro_known))
     ;
     by cat;

     ord=put(cat,$svar.);
     col1=strip(put(cat,$etype.));
     if procedure=. and bt_pro="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(procedure,comma16.))||strip(bt_pro);
     if procedure_known=. and bt_pro_known="*" then col3=strip(put(.t,threshold.));
     else col3=strip(put(procedure_known,comma16.))||strip(bt_pro_known);
     if encounter=. and bt_enc="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(encounter,comma16.))||strip(bt_enc);
     if encounter>0 then col5=strip(put((procedure/encounter),16.2));
     if encounter>0 and procedure_known^=. then perc=procedure_known/encounter;
     col7=strip("PRO_L3_ENCTYPE; PRO_L3_PXTYPE_ENCTYPE; ENC_L3_ENCTYPE");

     if (cat in ('AV' 'ED') and .<perc<0.75) or
        (cat in ('EI' 'IP') and .<perc<1.00) then thresholdflag=1;
run;

proc sort data=print_ivb;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivb;
     length edc_table_s_ $10;
     set print_ivb(keep=thresholdflag);
     if thresholdflag=1;
     row=3.02;
     exception=1;
     edc_table_s_="Table IVB";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivb nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivb;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivb;
    %let _ftn=IVB;
    %let _hdr=Table IVB;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivb split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column thresholdflag perc col1 col2 col3 col4 col5 col6 col7;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=14%] style(column)=[just=left];
         define col2     /display flow "PROCEDURES|records" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col3     /display flow "PROCEDURES|records with|known PX_TYPE" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "ENCOUNTER|records" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "Procedures|records|per encounter" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /computed format=8.2 "Procedures|records with|known PX_TYPE|per encounter" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col7     /display flow "Source table" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
         compute col6;
            if thresholdflag=1 then do;
               col6=perc;
               call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
               col6=perc;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_ivb;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* CHART IVB PROCEDURE RECORDS PER ENCOUNTER BY ADMIT DATE AND BY ENCOUNTER TYPE
********************************************************************************;
%macro chart_ivb(cat1,cat2,cat3,cat4,cat5,cat6,cat7,cat8);

%let _hdr=Chart IVB;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name in ("PRO_L3_ENCTYPE_ADATE_YM" "ENC_L3_ENCTYPE_ADATE_YM") and
         category in ("ED" "EI" "IP" "IS" "OA" "AV" "IC" "OS") and cross_category^="NULL or missing"));

     if resultn=.t then resultn=0;
     xdate=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));
    
     keep dc_name category xdate resultn;
run;
    
proc sort data=data;
     by category xdate;
run;

*- Bring dummy and actual data together -*;
data data;
     merge data( where=(dc_name="PRO_L3_ENCTYPE_ADATE_YM") rename=(resultn=procedure))
           data( where=(dc_name="ENC_L3_ENCTYPE_ADATE_YM") rename=(resultn=encounter))
     ;
     by category xdate;
run;

data chart_months_enc;
     length category $255;
     set chart_months(keep=xdate);
     by xdate;
     do category = "&cat1", "&cat2", "&cat3", "&cat4", "&cat5", "&cat6", "&cat7", "&cat8";
        output;
     end;
run;

proc sort data=chart_months_enc;
     by category xdate;
run;

data data;
     merge chart_months_enc(in=a keep=category xdate) data ;
     by category xdate;
     if a;
     if procedure<=0 or encounter<=0 then rate=.;
     else rate=procedure/encounter;
run;
    
*- Create a count variable to represent each month -*;
data final;
     set data;
     by category;
    
     retain count;
     if first.category then count=0;
     count= count+1;
run;

*- Determine the maximum value for the vertical axis -*;
proc means data=data nway noprint;
     var rate;
     output out=vertaxis max=max;
run;

data _null_;
     set vertaxis;
     if max>=250 then do;
        vertaxis=max-mod(max,25)+25;
        tick=25;
     end;
     else if max>=100 then do;
        vertaxis=max-mod(max,10)+10;
        tick=10;
     end;
     else if max>=10 then do;
        vertaxis=max-mod(max,5)+5;
        tick=5;
     end;
     else if max<10 then do;
        vertaxis=10;
        tick=1;
     end;
     call symput('vertaxis',compress(put(vertaxis,8.)));
     call symput('bytick',compress(put(tick,8.)));
run;

*- Produce output -*;
ods listing close;
ods proclabel "&_ttl1";
title1 justify=left "&_ttl1";
title2 justify=left h=2.5 "&_ttl2";
footnote " ";

symbol1  c=red line=1 v=NONE interpol=join;
symbol2  c=blue line=1 v=NONE interpol=join;
symbol3  c=green line=1 v=NONE interpol=join;
symbol4  c=purple line=1 v=NONE interpol=join;
symbol5  c=olive line=1 v=NONE interpol=join;
symbol6  c=brown line=1 v=NONE interpol=join;
symbol7  c=bipb line=1 v=NONE interpol=join;
symbol8  c=black line=1 v=NONE interpol=join;

axis1 order=(1 to 61 by 6) label=("ADMIT_DATE" h=1.5)
      minor=none
      offset=(0.5,1);

axis2 order=0 to &vertaxis by &bytick label=(angle=90 "Procedure Records per Encounter" h=1.5)
      minor=(n=1)
      offset=(0.5,.75);

legend1 label=none noframe;

proc gplot data=final;
     plot rate*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid;
     format count chart.;
     where rate^=. and count^=.;
run;
quit;
%mend chart_ivb;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVC MISSING OR UNKNOWN VALUES, REQUIRED TABLES
********************************************************************************;

*- Create table formats -*;
proc format cntlout=r_field;
     value $r_field
        "BIRTH_DATE"="01"
        "BIRTH_TIME"="02"
        "SEX"="03"
        "HISPANIC"="04"
        "RACE"="05"
        "GENDER_IDENTITY"="06"
        "SEXUAL_ORIENTATION"="07"
        "PAT_PREF_LANGUAGE_SPOKEN"="08"
        "ENR_END_DATE"="09"
        "CHART"="10"
        "ADMIT_TIME"="11"
        "DISCHARGE_DATE"="12"
        "DISCHARGE_TIME"="13"
        "ENC_TYPE"="14"
        "PROVIDERID"="15"
        "FACILITYID"="16"
        "DISCHARGE_DISPOSITION"="17"
        "DISCHARGE_STATUS"="18"
        "DRG"="19"
        "ADMITTING_SOURCE"="20"
        "PAYER_TYPE_PRIMARY"="21"
        "PAYER_TYPE_SECONDARY"="22"
        "FACILITY_TYPE"="23"
        "FACILITY_LOCATION"="24"
        "DX_DATE"="25"
        "DX_TYPE"="26"
        "DX_SOURCE"="27"
        "DX_ORIGIN"="28"
        "PDX"="29"
        "DIA_ENCOUNTERID"="30"
        "DIA_ADMIT_DATE"="31"
        "DIA_ENC_TYPE"="32"
        "DX_POA"="33"
        "DIA_PROVIDERID"="34"
        "PX_DATE"="35"
        "PX_TYPE"="36"
        "PX_SOURCE"="37"
        "PRO_ENCOUNTERID"="38"
        "PPX"="39"
        "PRO_PROVIDERID"="40"
        "PRO_ADMIT_DATE"="41"
        "PRO_ENC_TYPE"="42"
        ;

     value $r_source
        "01"="XTBL_L3_DATES"
        "02"="XTBL_L3_TIMES"
        "03"="DEM_L3_SEXDIST"
        "04"="DEM_L3_HISPDIST"
        "05"="DEM_L3_RACE"
        "06"="DEM_L3_GENDERDIST"
        "07"="DEM_L3_ORIENTDIST"
        "08"="DEM_L3_PATPREFLANG"
        "09"="XTBL_L3_DATES"
        "10"="ENR_L3_CHART"
        "11"="XTBL_L3_TIMES"
        "12"="ENC_L3_ENCTYPE_DDATE_YM"
        "13"="XTBL_L3_TIMES"
        "14"="ENC_L3_ENCTYPE"
        "15"="ENC_L3_N"
        "16"="ENC_L3_N"
        "17"="ENC_L3_ENCTYPE_DISDISP"
        "18"="ENC_L3_ENCTYPE_DISSTAT"
        "19"="ENC_L3_ENCTYPE_DRG"
        "20"="ENC_L3_ENCTYPE_ADMSRC"
        "21"="ENC_L3_PAYERTYPE1"
        "22"="ENC_L3_PAYERTYPE2"
        "23"="ENC_L3_FACILITYTYPE"
        "24"="ENC_L3_FACILITYLOC"
        "25"="XTBL_L3_DATES"
        "26"="DIA_L3_DXTYPE_DXSOURCE"
        "27"="DIA_L3_DXSOURCE"
        "28"="DIA_L3_ORIGIN"
        "29"="DIA_L3_PDX_ENCTYPE"
        "30"="DIA_L3_N"
        "31"="XTBL_L3_DATES"
        "32"="DIA_L3_ENCTYPE"
        "33"="DIA_L3_DXPOA"
        "34"="DIA_L3_N"
        "35"="XTBL_L3_DATES"
        "36"="PRO_L3_PXTYPE_ENCTYPE"
        "37"="PRO_L3_PXSOURCE"
        "38"="PRO_L3_N"
        "39"="PRO_L3_PPX"
        "40"="PRO_L3_N"
        "41"="XTBL_L3_DATES"
        "42"="PRO_L3_ENCTYPE"
        ;
run;

*- Create a dummy dataset to ensure every variable appears in the table -*;
data dum_field;
     length ord $2 var table $50 dc_name $100;
     set r_field;
     if fmtname="R_FIELD";
     var=strip(start);
     ord=strip(label);
     ordn=input(ord,2.);
     if 1<=ordn<=8 then table="DEMOGRAPHIC";
     else if 9<=ordn<=10 then table="ENROLLMENT";
     else if 11<=ordn<=24 then table="ENCOUNTER";
     else if 25<=ordn<=34 then table="DIAGNOSIS";
     else if 35<=ordn<=42 then table="PROCEDURES";
     dc_name=put(ord,$r_source.);
    
     keep table var ord dc_name;
run;

proc sort data=dum_field;
     by table var;
run;

*- Denominators -*;
data denominators;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
                (dc_name in ("DEM_L3_N" "ENR_L3_N" "ENC_L3_N" "DIA_L3_N" "PRO_L3_N") and 
                    statistic in ("ALL_N" "NULL_N") and variable in ("PATID")) or
                (dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" 
                             "ENC_L3_ENCTYPE_DISSTAT"  "ENC_L3_ENCTYPE_DRG" 
                             "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE") 
                and category in ("IP" "EI") and statistic="RECORD_N")));

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep table dc_name resultn bt_flag;
run;

proc means data=denominators nway missing noprint;
     class table dc_name bt_flag;
     var resultn;
     output out=denom sum=denom;
run;

data denom;
     length mvar $50;
     merge denom(rename=(bt_flag=flag) where=(flag=" "))
           denom(in=bt drop=denom where=(bt_flag="*"))
     ;
     by table dc_name;

     if dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
        "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE") then mvar=dc_name;
     else mvar=" ";
run;

proc sort data=denom;
     by table mvar;
run;

*- Numerators -*;
data numerators;
     set dc_normal(where=(datamartid=%upcase("&dmid")));
     if (dc_name in ("DEM_L3_SEXDIST" "DEM_L3_HISPDIST" "DEM_L3_RACEDIST" "DEM_L3_GENDERDIST"
                     "DEM_L3_ORIENTDIST" "DEM_L3_PATPREFLANG" "ENC_L3_ENCTYPE" "ENR_L3_CHART" 
                     "ENC_L3_PAYERTYPE1" "ENC_L3_PAYERTYPE2" "ENC_L3_FACILITYTYPE" "ENC_L3_FACILITYLOC"
                     "DIA_L3_DXSOURCE" "DIA_L3_ORIGIN" "DIA_L3_DXPOA" "DIA_L3_ENCTYPE" "PRO_L3_PXSOURCE" 
                     "PRO_L3_PPX" "PRO_L3_ENCTYPE") and
                     category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
                     "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE")  and category in ("IP" "EI") and 
                     cross_category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("DIA_L3_DXTYPE_DXSOURCE") and category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("PRO_L3_PXTYPE_ENCTYPE") and cross_category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("ENC_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "FACILITYID")) or
        (dc_name in ("DIA_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "ENCOUNTERID")) or
        (dc_name in ("PRO_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "ENCOUNTERID")) or
        (dc_name in ("XTBL_L3_DATES") and statistic="NMISS" and variable in ("BIRTH_DATE" "ENR_END_DATE" "PX_DATE" "DX_DATE")) or
        (dc_name in ("XTBL_L3_DATES") and statistic="NMISS" and table in ("DIAGNOSIS" "PROCEDURES") and variable in ("ADMIT_DATE")) or
        (dc_name in ("XTBL_L3_TIMES") and statistic="NMISS" and variable in ("BIRTH_TIME" "ADMIT_TIME" "DISCHARGE_TIME"))
        ;

     if dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
                    "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE" "PRO_L3_PXTYPE_ENCTYPE"
                    "ENC_L3_PAYERTYPE1" "ENC_L3_PAYERTYPE2" "ENC_L3_FACILITYTYPE") then var=cross_variable;
     else if dc_name="DIA_L3_N" and variable="ENCOUNTERID" then var="DIA_ENCOUNTERID";
     else if dc_name="PRO_L3_N" and variable="ENCOUNTERID" then var="PRO_ENCOUNTERID";
     else if dc_name="DIA_L3_N" and variable="PROVIDERID" then var="DIA_PROVIDERID";
     else if dc_name="PRO_L3_N" and variable="PROVIDERID" then var="PRO_PROVIDERID";
     else if dc_name="PRO_L3_ENCTYPE" and variable="ENC_TYPE" then var="PRO_ENC_TYPE";
     else if dc_name="DIA_L3_ENCTYPE" and variable="ENC_TYPE" then var="DIA_ENC_TYPE";
     else if table="DIAGNOSIS" and variable="ADMIT_DATE" then var="DIA_ADMIT_DATE";
     else if table="PROCEDURES" and variable="ADMIT_DATE" then var="PRO_ADMIT_DATE";
     else var=variable;

     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep table resultn dc_name var bt_flag;
run;

proc sort data=numerators;
     by table var;
run;

data numerators;
     merge dum_field(in=d) numerators;
     by table var;
     if d;
     if resultn=. then resultn=0;
run;

*- Recalculate count and percent -*;
proc means data=numerators nway missing noprint;
     class table dc_name var bt_flag;
     var resultn;
     output out=num sum=num;
run;

data num;
     length mvar $50;
     merge num(rename=(bt_flag=flag) where=(flag=" "))
           num(in=bt drop=num where=(bt_flag="*"))
     ;
     by table dc_name var;

     if dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
        "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE") then mvar=dc_name;
     else mvar=" ";
run;

proc sort data=num;
     by table mvar;
run;

*- Create dummy record in case no missing records exist -*;
data enctype_ddate_ym;
     length table var $50 dc_name mvar $100;
     table="ENCOUNTER";
     dc_name="ENC_L3_ENCTYPE_DDATE_YM";
     mvar="ENC_L3_ENCTYPE_DDATE_YM";
     var="DISCHARGE_DATE";
     num=0;
     output;
run;

*- Bring everything together -*;
data print_ivc(keep=ord col: perc thresholdflag);
     length col1-col5 col8 $50;
     merge enctype_ddate_ym 
           num 
           denom(keep=table mvar denom bt_flag rename=(bt_flag=bt_denom));
     by table mvar;

     ord=put(var,$r_field.);
     col1=table;
     if var in ("DIA_ENCOUNTERID" "PRO_ENCOUNTERID") then col2="ENCOUNTERID";
     else if var in ("DIA_PROVIDERID" "PRO_PROVIDERID") then col2="PROVIDERID";
     else if var in ("DIA_ENC_TYPE" "PRO_ENC_TYPE") then col2="ENC_TYPE";
     else if var in ("DIA_ADMIT_DATE" "PRO_ADMIT_DATE") then col2="ADMIT_DATE";
     else col2=var;
     if var in ("DISCHARGE_DATE" "DISCHARGE_DISPOSITION" "DISCHARGE_STATUS" "DRG" 
                "ADMITTING_SOURCE" "PDX") then col3="IP or EI";
     else col3=" ";
     if num=. and bt_flag="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(num,comma16.))||strip(bt_flag);
     if denom=. and bt_denom="*" then col5=strip(put(.t,threshold.));
     else col5=strip(put(denom,comma16.))||strip(bt_denom);
     if num>0 and denom>0 then perc=num/denom*100;
     if var in ("BIRTH_DATE" "SEX" "DISCHARGE_DATE" "DISCHARGE_DISPOSITION" 
                "DX_SOURCE" "DX_ORIGIN" "DIA_ENCOUNTERID" "PX_DATE" "PX_SOURCE" 
                "PRO_ENCOUNTERID") 
                and perc>=10.00 then thresholdflag=1;
     col8=dc_name;
run;

proc sort data=print_ivc;
     by ord;
run;

data print_ivc;
     set print_ivc;
     by ord;
     if _n_<=18 then pg=1;
     else if _n_<=40 then pg=2;
     else pg=3;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivc;
     length edc_table_s_ $10;
     set print_ivc(keep=thresholdflag);
     if thresholdflag=1;
     row=3.03;
     exception=1;
     edc_table_s_="Table IVC";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivc nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivc;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivc;
    %let _ftn=IVC;
    %let _hdr=Table IVC;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivc split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column thresholdflag perc pg col1 col2 col3 
                ("Records with missing, NI, UN, or OT values|______________________________________" col4 col5 col6)
                (" |  " col8);
         where pg=1;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=20.5%] style(column)=[just=left];
         define col3     /display flow "Encounter Type|Constraint" style(header)=[just=center cellwidth=9.5%] style(column)=[just=center];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /display flow "Denominator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=21%] style(column)=[just=left];

         break after pg / page;

         compute col6;
            if thresholdflag=1 then do;
                col6=perc;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col6=perc;
            end;
        endcomp;
    run; 

    *- Determine how many page in report -*;
    proc means data=print_ivc noprint;
         var pg;
         output out=_ivc_pg max=max_pg;
    run;
        
    data _null_;
         set _ivc_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_ivc split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column thresholdflag perc pg col1 col2 col3 
                ("Records with missing, NI, UN, or OT values|________________________________________" col4 col5 col6)
                (" |  " col8);
             where pg=&_pg;

             define thresholdflag /display noprint;
             define perc     /display noprint;
             define pg       /order noprint;
             define col1     /display flow "Table" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
             define col2     /display flow "Field" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
             define col3     /display flow "Encounter Type|Constraint" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
             define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
             define col8     /display flow "Source table" style(header)=[just=left cellwidth=21%] style(column)=[just=left];

             break after pg / page;

             compute col6;
                if thresholdflag=1 then do;
                    col6=perc;
                    call define(_col_, "style", "style=[color=blue]");
                end;
                else do;
                    col6=perc;
                end;
            endcomp;
        run; 

        ods listing;
    %end;
%mend print_ivc;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVD MISSING OR UNKNOWN VALUES, OPTIONAL TABLES
********************************************************************************;
*- Create table formats -*;
proc format cntlout=o_field;
     value $o_field
        "VITAL_ENCOUNTERID"="00"
        "VITAL_SOURCE"="01"
        "MEASURE_TIME"="02"
        "DEATH_DATE_IMPUTE"="03"
        "DEATH_MATCH_CONFIDENCE"="04"
        "DEATH_DATE"="05"
        "DEATH_SOURCE"="06"
        "ENCOUNTERID"="07"
        "SPECIMEN_SOURCE"="08"
        "LAB_LOINC"="09"
        "PRIORITY"="10"
        "RESULT_LOC"="11"
        "LAB_PX_TYPE"="12"
        "LAB_PX"="13"
        "LAB_ORDER_DATE"="14"
        "SPECIMEN_DATE"="15"
        "SPECIMEN_TIME"="16"
        "RESULT_TIME"="17"
        "RESULT_QUAL"="18"
        "RESULT_NUM"="19"
        "RESULT_MODIFIER"="20"
        "RESULT_UNIT"="21"
        "NORM_MODIFIER_LOW"="22"
        "NORM_MODIFIER_HIGH"="23"
        "ABN_IND"="24"
        "LAB_LOINC_SOURCE"="25"
        "LAB_RESULT_SOURCE"="26"
        "RX_ENCOUNTERID"="27"
        "RX_PROVIDERID"="28"
        "RX_ORDER_DATE"="29"
        "RX_ORDER_TIME"="30"
        "RX_START_DATE"="31"
        "RX_END_DATE"="32"
        "RX_DAYS_SUPPLY_GROUP"="33"
        "RX_FREQUENCY"="34"
        "RX_BASIS"="35"
        "RXNORM_CUI"="36"        
        "RX_QUANTITY"="37"
        "RX_REFILLS"="38"
        "RX_DOSE_ORDERED"="39"
        "RX_DOSE_ORDERED_UNIT"="40"
        "RX_DOSE_FORM"="41"
        "RX_PRN_FLAG"="42"
        "RX_ROUTE"="43"
        "RX_SOURCE"="44"
        "RX_DISPENSE_AS_WRITTEN"="45"
        "PRESCRIBINGID"="46"
        "DISPENSE_SUP_GROUP"="47"
        "DISPENSE_DOSE_DISP"="48"
        "DISPENSE_DOSE_DISP_UNIT"="49"
        "DISPENSE_ROUTE"="50"
        "DISPENSE_SOURCE"="51"
        "DISPENSE_AMT"="52"
        "COND_ENCOUNTERID"="53"
        "REPORT_DATE"="54"
        "RESOLVE_DATE"="55"
        "ONSET_DATE"="56"
        "CONDITION_STATUS"="57"
        "CONDITION_SOURCE"="58"
        "CONDITION_TYPE"="59"
        "DEATH_CAUSE_CONFIDENCE"="60"
        "DEATH_CAUSE_SOURCE"="61"
        "DEATH_CAUSE_TYPE"="62"
        "DEATH_CAUSE_CODE"="63"
        "PROVIDER_NPI"="76"
        "PROVIDER_NPI_FLAG"="77"
        "PROVIDER_SPECIALTY_PRIMARY"="78"
        "PROVIDER_SEX"="79"
        "MEDADMIN_ENCOUNTERID"="80"
        "MEDADMIN_PRESCRIBINGID"="81"
        "MEDADMIN_PROVIDERID"="82"
        "MEDADMIN_DOSE_ADMIN"="83"
        "MEDADMIN_DOSE_ADMIN_UNIT"="84"
        "MEDADMIN_ROUTE"="85"
        "MEDADMIN_SOURCE"="86"
        "MEDADMIN_CODE"="87"
        "MEDADMIN_TYPE"="88"
        "OBSCLIN_ENCOUNTERID"="89"
        "OBSCLIN_PROVIDERID"="90"
        "OBSCLIN_START_TIME"="92"
        "OBSCLIN_STOP_DATE"="93"
        "OBSCLIN_STOP_TIME"="94"
        "OBSCLIN_CODE"="95"
        "OBSCLIN_TYPE"="96"
        "OBSCLIN_RESULT_QUAL"="97"
        "OBSCLIN_RESULT_MODIFIER"="98"
        "OBSCLIN_RESULT_UNIT"="99"
        "OBSCLIN_ABN_IND"="100"
        "OBSGEN_ENCOUNTERID"="101"
        "OBSGEN_PROVIDERID"="102"
        "OBSGEN_START_TIME"="104"
        "OBSGEN_STOP_DATE"="105"
        "OBSGEN_STOP_TIME"="106"
        "OBSGEN_CODE"="107"
        "OBSGEN_TYPE"="108"
        "OBSGEN_RESULT_QUAL"="109"
        "OBSGEN_RESULT_MODIFIER"="110"
        "OBSGEN_RESULT_UNIT"="111"  
        "OBSGEN_ABN_IND"="112"
        "ADDRESS_USE"="113"
        "ADDRESS_TYPE"="114"
        "ADDRESS_PREFERRED"="115"
        "ADDRESS_CITY"="116"
        "ADDRESS_STATE"="117"
        "ADDRESS_ZIP5"="118"
        "ADDRESS_ZIP9"="119"
        "ADDRESS_PERIOD_START"="120"
        "ADDRESS_PERIOD_END"="121"
        "IMMUNE_ENCOUNTERID"="122"
        "PROCEDURESID"="123"
        "VX_PROVIDERID"="124"
        "VX_RECORD_DATE"="125"
        "VX_ADMIN_DATE"="126"
        "VX_CODE_TYPE"="127"
        "VX_STATUS"="128"
        "VX_STATUS_REASON"="129"
        "VX_SOURCE"="130"
        "VX_DOSE"="131"
        "VX_DOSE_UNIT"="132"
        "VX_ROUTE"="133"
        "VX_BODY_SITE"="134"
        "VX_MANUFACTURER"="135"
        "VX_LOT_NUM"="136"
        "VX_EXP_DATE"="137"
        "LAB_FACILITYID"="138"
        "SEX"="139"
        "RACE"="140"
        "AGE_MIN_WKS"="141"
        "AGE_MAX_WKS"="142"
        "LH_RESULT_UNIT"="143"
        "NORM_RANGE_LOW"="144"
        "LH_NORM_MODIFIER_LOW"="145"
        "NORM_RANGE_HIGH"="146"
        "LH_NORM_MODIFIER_HIGH"="147"
        "PERIOD_START"="148"
        "PERIOD_END"="149"
        ;
    
     value $o_source
        "00"="VIT_L3_N"
        "01"="VIT_L3_VITAL_SOURCE"
        "02"="XTBL_L3_TIMES"
        "03"="DEATH_L3_IMPUTE"
        "04"="DEATH_L3_MATCH"
        "05"="XTBL_L3_DATES"
        "06"="DEATH_L3_SOURCE"        
        "07"="LAB_L3_N"
        "08"="LAB_L3_SOURCE"
        "09"="LAB_L3_LOINC"
        "10"="LAB_L3_PRIORITY"
        "11"="LAB_L3_LOC"
        "12"="LAB_L3_PX_TYPE"
        "13"="LAB_L3_PX_PXTYPE"
        "14"="XTBL_L3_DATES"
        "15"="XTBL_L3_DATES"
        "16"="XTBL_L3_TIMES"
        "17"="XTBL_L3_TIMES"
        "18"="LAB_L3_QUAL"
        "19"="LAB_L3_LOINC_RESULT_NUM"
        "20"="LAB_L3_MOD"
        "21"="LAB_L3_UNIT"
        "22"="LAB_L3_LOW"
        "23"="LAB_L3_HIGH"
        "24"="LAB_L3_ABN"
        "25"="LAB_L3_LSOURCE"
        "26"="LAB_L3_RSOURCE"
        "27"="PRES_L3_N"
        "28"="PRES_L3_N"
        "29"="XTBL_L3_DATES"
        "30"="XTBL_L3_TIMES"
        "31"="XTBL_L3_DATES"
        "32"="XTBL_L3_DATES"
        "33"="PRES_L3_SUPDIST2"
        "34"="PRES_L3_FREQ"
        "35"="PRES_L3_BASIS"
        "36"="PRES_L3_RXCUI"
        "37"="PRES_L3_RXQTY_DIST"
        "38"="PRES_L3_RXREFILL_DIST"
        "39"="PRES_L3_RXDOSEODR_DIST"
        "40"="PRES_L3_RXDOSEODRUNIT"
        "41"="PRES_L3_RXDOSEFORM"
        "42"="PRES_L3_PRNFLAG"
        "43"="PRES_L3_ROUTE"
        "44"="PRES_L3_SOURCE"
        "45"="PRES_L3_DISPASWRTN"
        "46"="DISP_L3_N"
        "47"="DISP_L3_SUPDIST2"
        "48"="DISP_L3_DOSE_DIST"
        "49"="DISP_L3_DOSE_UNIT"
        "50"="DISP_L3_ROUTE"
        "51"="DISP_L3_SOURCE"
        "52"="DISP_L3_DISPAMT_DIST"

        "53"="CONDITION_L3_N"
        "54"="XTBL_L3_DATES"
        "55"="XTBL_L3_DATES"
        "56"="XTBL_L3_DATES"
        "57"="COND_L3_STATUS"
        "58"="COND_L3_SOURCE"
        "59"="COND_L3_TYPE"
        "60"="DEATHC_L3_CONF"
        "61"="DEATHC_L3_SOURCE"
        "62"="DEATHC_L3_TYPE"
        "63"="DEATHC_L3_CODE"

        "76"="PROV_L3_N"
        "77"="PROV_L3_NPIFLAG"
        "78"="PROV_L3_SPECIALTY"
        "79"="PROV_L3_SEX"
        "80"="MEDADM_L3_N"
        "81"="MEDADM_L3_N"
        "82"="MEDADM_L3_N"
        "83"="MEDADM_L3_DOSEADM"
        "84"="MEDADM_L3_DOSEADMUNIT"
        "85"="MEDADM_L3_ROUTE"
        "86"="MEDADM_L3_SOURCE"
        "87"="MEDADM_L3_CODE_TYPE"
        "88"="MEDADM_L3_TYPE"
        "89"="OBSCLIN_L3_N"
        "90"="OBSCLIN_L3_N"
        "92"="XTBL_L3_TIMES"
        "93"="XTBL_L3_DATES"
        "94"="XTBL_L3_TIMES"
        "95"="OBSCLIN_L3_CODE_TYPE"
        "96"="OBSCLIN_L3_TYPE"
        "97"="OBSCLIN_L3_QUAL"
        "98"="OBSCLIN_L3_MOD"
        "99"="OBSCLIN_L3_UNIT"
       "100"="OBSCLIN_L3_ABN"
       "101"="OBSGEN_L3_N"
       "102"="OBSGEN_L3_N"
       "104"="XTBL_L3_TIMES"
       "105"="XTBL_L3_DATES"
       "106"="XTBL_L3_TIMES"
       "107"="OBSGEN_L3_CODE_TYPE"
       "108"="OBSGEN_L3_TYPE"
       "109"="OBSGEN_L3_QUAL"
       "110"="OBSGEN_L3_MOD"
       "111"="OBSGEN_L3_UNIT"
       "112"="OBSGEN_L3_ABN"
       "113"="LDSADRS_L3_ADRSUSE"
       "114"="LDSADRS_L3_ADRSTYPE"
       "115"="LDSADRS_L3_ADRSPREF"
       "116"="LDSADRS_L3_ADRSCITY"
       "117"="LDSADRS_L3_ADRSSTATE"
       "118"="LDSADRS_L3_ADRSZIP5"
       "119"="LDSADRS_L3_ADRSZIP9"
       "120"="XTBL_L3_DATES"
       "121"="XTBL_L3_DATES"
       "122"="IMMUNE_L3_N"
       "123"="IMMUNE_L3_N"
       "124"="IMMUNE_L3_N"
       "125"="XTBL_L3_DATES"
       "126"="XTBL_L3_DATES"
       "127"="IMMUNE_L3_CODETYPE"
       "128"="IMMUNE_L3_STATUS"
       "129"="IMMUNE_L3_STATUSREASON"
       "130"="IMMUNE_L3_SOURCE"
       "131"="IMMUNE_L3_DOSE_DIST"
       "132"="IMMUNE_L3_DOSEUNIT"
       "133"="IMMUNE_L3_ROUTE"
       "134"="IMMUNE_L3_BODYSITE"
       "135"="IMMUNE_L3_MANUFACTURER"
       "136"="IMMUNE_L3_LOTNUM"
       "137"="XTBL_L3_DATES"
       "138"="LABHIST_L3_N"
       "139"="LABHIST_L3_SEXDIST"
       "140"="LABHIST_L3_RACEDIST"
       "141"="LABHIST_L3_MIN_WKS"
       "142"="LABHIST_L3_MAX_WKS"
       "143"="LABHIST_L3_UNIT"
       "144"="LABHIST_L3_RLOW_DIST"
       "145"="LABHIST_L3_LOW"
       "146"="LABHIST_L3_RHIGH_DIST"
       "147"="LABHIST_L3_HIGH"
       "148"="LABHIST_L3_PDSTART_Y"
       "149"="LABHIST_L3_PDEND_Y"
        ;
run;

*- Create a dummy dataset to ensure every variable appears in the table -*;
data dum_field;
     length ord $3 var table $50 dc_name $100;
     set o_field;
     if fmtname="O_FIELD";
     var=strip(start);
     ord=strip(label);
     ordn=input(strip(ord),8.);
     if 0<=ordn<=2 then table="VITAL";
     else if 3<=ordn<=6 then table="DEATH";
     else if 7<=ordn<=26 then table="LAB_RESULT_CM";
     else if 27<=ordn<=45 then table="PRESCRIBING";
     else if 46<=ordn<=52 then table="DISPENSING";
     else if 53<=ordn<=59 then table="CONDITION";
     else if 60<=ordn<=63 then table="DEATH_CAUSE";
     else if 76<=ordn<=79 then table="PROVIDER";
     else if 80<=ordn<=88 then table="MED_ADMIN";
     else if 89<=ordn<=100 then table="OBS_CLIN";
     else if 101<=ordn<=112 then table="OBS_GEN";
     else if 113<=ordn<=121 then table="LDS_ADDRESS";
     else if 122<=ordn<=137 then table="IMMUNIZATION";
     else if 138<=ordn<=149 then table="LAB_HISTORY";
     dc_name=put(ord,$o_source.);
    
     keep table var ord: dc_name;
run;

proc sort data=dum_field;
     by table var;
run;

*- Denominators -*;
data denominators;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
                ((dc_name in ("VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "DISP_L3_N" 
                              "DEATH_L3_N" "COND_L3_N" "DEATHC_L3_N" "MEDADM_L3_N"
                              "OBSCLIN_L3_N" "OBSGEN_L3_N" "LDSADRS_L3_N" 
                              "IMMUNE_L3_N") and
                    statistic in ("ALL_N" "NULL_N") and variable in ("PATID")) or
                (dc_name in ("PROV_L3_N") and 
                 statistic in ("ALL_N" "NULL_N") and variable in ("PROVIDERID"))) or
                (dc_name in ("LABHIST_L3_N") and statistic in ("ALL_N" "NULL_N") and 
                    variable in ("LAB_FACILITYID"))));

     if table="LABHIST" then table="LAB_HISTORY";
     else if table="LDS_ADDRESS_HISTORY" then table="LDS_ADDRESS";
     else table=table;
    
     * flag BT records *;
     if resultn=.t and statistic="RECORD_N" then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep table dc_name resultn bt_flag;
run;

proc means data=denominators nway missing noprint;
     class table dc_name bt_flag;
     var resultn;
     output out=denom sum=denom;
run;

data denom;
     merge denom(rename=(bt_flag=flag) where=(flag=" "))
           denom(in=bt drop=denom where=(bt_flag="*"))
     ;
     by table dc_name;
run;

proc sort data=denom;
     by table;
run;

data numerators;
     length var $50;
     set dc_normal(where=(datamartid=%upcase("&dmid")));
     if (dc_name in ("VIT_L3_VITAL_SOURCE" "DEATH_L3_IMPUTE" "DEATH_L3_MATCH" "DEATH_L3_SOURCE"
                     "LAB_L3_NAME" "LAB_L3_SOURCE" "LAB_L3_LOINC" "LAB_L3_PRIORITY" 
                     "LAB_L3_LOC" "LAB_L3_PX_TYPE" "LAB_L3_QUAL" "LAB_L3_MOD" "LAB_L3_UNIT"
                     "LAB_L3_LOW" "LAB_L3_HIGH" "LAB_L3_ABN" "LAB_L3_LSOURCE" "LAB_L3_RSOURCE"
                     "PRES_L3_SUPDIST2" "PRES_L3_FREQ" "PRES_L3_BASIS" "PRES_L3_RXCUI" 
                     "PRES_L3_RXQTY_DIST" "PRES_L3_RXREFILL_DIST" "PRES_L3_RXDOSEFORM" "PRES_L3_PRNFLAG" 
                     "PRES_L3_ROUTE" "PRES_L3_SOURCE" "PRES_L3_DISPASWRTN" "PRES_L3_RXDOSEODRUNIT"
                     "DISP_L3_SUPDIST2" "DISP_L3_DOSEUNIT" "DISP_L3_ROUTE" "DISP_L3_SOURCE"
                     "COND_L3_STATUS" "COND_L3_SOURCE" "COND_L3_TYPE" 
                     "DEATHC_L3_CONF" "DEATHC_L3_SOURCE" "DEATHC_L3_TYPE" "DEATHC_L3_CODE" 
                     "PROV_L3_NPIFLAG" "PROV_L3_SPECIALTY" "PROV_L3_SEX"
                     "MEDADM_L3_DOSEADMUNIT" "MEDADM_L3_ROUTE" "MEDADM_L3_SOURCE" "MEDADM_L3_CODE_TYPE" "MEDADM_L3_TYPE"
                     "OBSCLIN_L3_CODE_TYPE" "OBSCLIN_L3_TYPE" "OBSCLIN_L3_QUAL" "OBSCLIN_L3_MOD" "OBSCLIN_L3_RUNIT" "OBSCLIN_L3_ABN"
                     "OBSGEN_L3_CODE_TYPE" "OBSGEN_L3_TYPE" "OBSGEN_L3_QUAL" "OBSGEN_L3_MOD" "OBSGEN_L3_RUNIT" "OBSGEN_L3_ABN"
                     "LDSADRS_L3_ADRSUSE" "LDSADRS_L3_ADRSTYPE" "LDSADRS_L3_ADRSPREF" "LDSADRS_L3_ADRSCITY" 
                     "LDSADRS_L3_ADRSSTATE" "LDSADRS_L3_ADRSZIP5" "LDSADRS_L3_ADRSZIP9"
                     "IMMUNE_L3_CODETYPE" "IMMUNE_L3_STATUS" "IMMUNE_L3_STATUSREASON" "IMMUNE_L3_SOURCE"
                     "IMMUNE_L3_DOSEUNIT" "IMMUNE_L3_ROUTE" "IMMUNE_L3_BODYSITE" "IMMUNE_L3_MANUFACTURER"
                     "IMMUNE_L3_LOTNUM" "LABHIST_L3_SEXDIST" "LABHIST_L3_RACEDIST" "LABHIST_L3_UNIT" "LABHIST_L3_LOW"
                     "LABHIST_L3_HIGH" "LABHIST_L3_PDSTART_Y" "LABHIST_L3_PDEND_Y") and 
                     category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("LAB_L3_PX_PXTYPE") and cross_category in ("NULL or missing") and statistic="RECORD_N") or
        (dc_name in ("VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "COND_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID")) or
        (dc_name in ("PRES_L3_N") and statistic="NULL_N" and variable in ("RX_PROVIDERID")) or
        (dc_name in ("DISP_L3_N") and statistic="NULL_N" and variable in ("PRESCRIBINGID")) or
        (dc_name in ("PROV_L3_N") and statistic="NULL_N" and variable in ("PROVIDER_NPI")) or
        (dc_name in ("MEDADM_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "PRESCRIBINGID" "MEDADMIN_PROVIDERID")) or
        (dc_name in ("OBSCLIN_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "OBSCLIN_PROVIDERID")) or
        (dc_name in ("OBSGEN_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "OBSGEN_PROVIDERID")) or
        (dc_name in ("IMMUNE_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "PROCEDURESID" "VX_PROVIDERID")) or
        (dc_name in ("LAB_L3_LOINC_RESULT_NUM") and statistic="NMISS" and variable in ("LAB_LOINC")) or
        (dc_name in ("LABHIST_L3_N") and statistic="NULL_N" and variable in ("FACILITYID")) or
        (dc_name in ("XTBL_L3_DATES") and statistic="NMISS" and variable in ("LAB_ORDER_DATE" "SPECIMEN_DATE" "DEATH_DATE"
                     "RX_ORDER_DATE" "RX_START_DATE" "RX_END_DATE" "REPORT_DATE" "RESOLVE_DATE" "ONSET_DATE"  
                     "OBSCLIN_STOP_DATE" "OBSGEN_STOP_DATE" "ADDRESS_PERIOD_START" "ADDRESS_PERIOD_END" "VX_RECORD_DATE"
                     "VX_ADMIN_DATE" "VX_EXP_DATE")) or
        (dc_name in ("XTBL_L3_TIMES") and statistic="NMISS" and variable in ("MEASURE_TIME" "SPECIMEN_TIME" "RESULT_TIME" 
                     "RX_ORDER_TIME" "OBSCLIN_START_TIME" "OBSCLIN_STOP_TIME" "OBSGEN_START_TIME" "OBSGEN_STOP_TIME")) or
        (dc_name in ("PRES_L3_RXQTY_DIST" "PRES_L3_RXREFILL_DIST" "PRES_L3_RXDOSEODR_DIST" "MEDADM_L3_DOSEADM" "DISP_L3_DOSE_DIST" "DISP_L3_DISPAMT_DIST"
                     "IMMUNE_L3_DOSE_DIST" "LABHIST_L3_MIN_WKS" "LABHIST_L3_MAX_WKS" "LABHIST_L3_RLOW_DIST" "LABHIST_L3_RHIGH_DIST") and 
                        statistic="NULL or missing")
        ;

     if table="LABHIST" then table="LAB_HISTORY";
     else if table="LDS_ADDRESS_HISTORY" then table="LDS_ADDRESS";
     else table=table;

     if dc_name in ("LAB_L3_PX_PXTYPE") then var=cross_variable;
     else if dc_name="PRES_L3_N" and variable="ENCOUNTERID" then var="RX_ENCOUNTERID";
     else if dc_name="VIT_L3_N" and variable="ENCOUNTERID" then var="VITAL_ENCOUNTERID";
     else if dc_name="COND_L3_N" and variable="ENCOUNTERID" then var="COND_ENCOUNTERID";
     else if dc_name="MEDADM_L3_N" and variable="ENCOUNTERID" then var="MEDADMIN_ENCOUNTERID";
     else if dc_name="OBSCLIN_L3_N" and variable="ENCOUNTERID" then var="OBSCLIN_ENCOUNTERID";
     else if dc_name="OBSGEN_L3_N" and variable="ENCOUNTERID" then var="OBSGEN_ENCOUNTERID";
     else if dc_name="IMMUNE_L3_N" and variable="ENCOUNTERID" then var="IMMUNE_ENCOUNTERID";
     else if dc_name="MEDADM_L3_N" and variable="PRESCRIBINGID" then var="MEDADMIN_PRESCRIBINGID";
     else if dc_name="LAB_L3_LOINC_RESULT_NUM" and variable="LAB_LOINC" then var="RESULT_NUM";
     else if dc_name="LABHIST_L3_UNIT" and variable="RESULT_UNIT" then var="LH_RESULT_UNIT";
     else if dc_name="LABHIST_L3_LOW" and variable="NORM_MODIFIER_LOW" then var="LH_NORM_MODIFIER_LOW";
     else if dc_name="LABHIST_L3_HIGH" and variable="NORM_MODIFIER_HIGH" then var="LH_NORM_MODIFIER_HIGH";
     else var=variable;

     * flag BT records *;
     if resultn=.t and statistic in ("RECORD_N" "NULL_N" "NMISS") then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep table resultn dc_name var bt_flag;
run;

proc sort data=numerators;
     by table var;
run;

data numerators;
     merge dum_field(in=d) numerators;
     by table var;
     if d;
     if resultn=. then resultn=0;
run;


*- Recalculate count and percent -*;
proc means data=numerators nway missing noprint;
     class table dc_name var bt_flag;
     var resultn;
     output out=num sum=num;
run;

*- Create a dummy record for LOINC, since it is the only non-guaranteed record -*;
data dum_loinc;
     length dc_name $100 table var $50;
     dc_name="LAB_L3_LOINC";
     table="LAB_RESULT_CM";
     var="LAB_LOINC";
     num=0;
     output;
run;

data num;
     merge dum_loinc
           num(rename=(bt_flag=flag) where=(flag=" "))
           num(in=bt drop=num where=(bt_flag="*"))
     ;
     by table dc_name var;
run;

proc sort data=num;
     by table;
run;

*- Bring everything together -*;
data print_ivd(keep=ord: col: perc thresholdflag);
     length ord col1-col5 col8 $50;
     merge num(in=n) denom(in=d keep=table denom bt_flag rename=(bt_flag=bt_denom));
     by table;
     if n and d;

     ord=put(var,$o_field.);
     ordn=input(strip(ord),8.);
     col1=table;
     if var in ("VITAL_ENCOUNTERID" "RX_ENCOUNTERID" "COND_ENCOUNTERID" "PROCM_ENCOUNTERID"
                "MEDADMIN_ENCOUNTERID" "OBSCLIN_ENCOUNTERID" "OBSGEN_ENCOUNTERID" "IMMUNE_ENCOUNTERID") then col2="ENCOUNTERID";
     else if var in ("MEDADMIN_PRESCRIBINGID") then col2="PRESCRIBINGID";
     else if var="RX_DAYS_SUPPLY_GROUP" then col2="RX_DAYS_SUPPLY";
     else if var="DISPENSE_SUP_GROUP" then col2="DISPENSE_SUP";
     else if var="LH_NORM_MODIFIER_HIGH" then col2="NORM_MODIFIER_HIGH";
     else if var="LH_NORM_MODIFIER_LOW" then col2="NORM_MODIFIER_LOW";
     else if var="LH_RESULT_UNIT" then col2="RESULT_UNIT";
     else col2=var;
     col3=" ";
     if num=. and bt_flag="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(num,comma16.))||strip(bt_flag);
     if denom=. and bt_denom="*" then col5=strip(put(.t,threshold.));
     else col5=strip(put(denom,comma16.))||strip(bt_denom);
     if num>0 and denom>0 then perc=num/denom*100;

     * threshold values for this table *;
     if var in ("VITAL_SOURCE" "DEATH_SOURCE" "RX_ORDER_DATE" "RX_SOURCE" 
                "DISPENSE_SUP_GROUP" "CONDITION_SOURCE" "MEDADMIN_ENCOUNTERID" 
                "MEDADMIN_SOURCE" "MEDADMIN_CODE" "MEDADMIN_TYPE" "OBSCLIN_CODE" 
                "OBSCLIN_TYPE" "OBSGEN_CODE" "OBSGEN_TYPE" "VITAL_ENCOUNTERID" 
                "ENCOUNTERID" "VX_RECORD_DATE" "VX_SOURCE" "DISPENSE_SOURCE" 
                "LAB_RESULT_SOURCE" "DEATH_CAUSE_CODE" "CONDITION_TYPE" 
                "VX_CODE_TYPE" "DEATH_CAUSE_SOURCE")
                and perc>=10.00 then thresholdflag=1;

     col8=dc_name;
run;

proc sort data=print_ivd;
     by ordn;
run;

data print_ivd;
     set print_ivd;
     by ordn;
     if _n_<=18 then pg=1;
     else if _n_<=36 then pg=2;
     else if _n_<=54 then pg=3;
     else if _n_<=72 then pg=4;
     else if _n_<=90 then pg=5;
     else if _n_<=108 then pg=6;
     else if _n_<=126 then pg=7;
     else if _n_<=144 then pg=8;
     else pg=9;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivd;
     length edc_table_s_ $10;
     set print_ivd(keep=thresholdflag);
     if thresholdflag=1;
     row=3.03;
     exception=1;
     edc_table_s_="Table IVD";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivd nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivd;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivd;
    %let _ftn=IVD;
    %let _hdr=Table IVD;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivd split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column thresholdflag perc pg col1 col2 
                ("Records with missing, NI, UN, or OT values|________________________________________" col4 col5 col6)
                (" |  " col8);
         where pg=1;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=22%] style(column)=[just=left];

         compute col6;
            if thresholdflag=1 then do;
                col6=perc;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col6=perc;
            end;
        endcomp;
    run;

    *- Determine how many page in report -*;
    proc means data=print_ivd noprint;
         var pg;
         output out=_ivd_pg max=max_pg;
    run;
        
    data _null_;
         set _ivd_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_ivd split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column thresholdflag perc pg col1 col2 
                    ("Records with missing, NI, UN, or OT values|________________________________________" col4 col5 col6)
                    (" |  " col8);
             where pg=&_pg;

             define thresholdflag /display noprint;
             define perc     /display noprint;
             define pg       /order noprint;
             define col1     /display flow "Table" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
             define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
             define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
             define col8     /display flow "Source table" style(header)=[just=left cellwidth=22%] style(column)=[just=left];

             break after pg / page;

             compute col6;
                if thresholdflag=1 then do;
                    col6=perc;
                    call define(_col_, "style", "style=[color=blue]");
                end;
                else do;
                    col6=perc;
                end;
            endcomp;
        run; 

        ods listing;
    %end;
%mend print_ivd;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVE PRINCIPAL DIAGNOSES FOR INSTITUTIONAL ENCOUNTERS
********************************************************************************;

proc format;
     value $stable
        'BI'='DIA_L3_PDX_ENCTYPE'
        'CL'='DIA_L3_PDXGRP_ENCTYPE'
        'DR'=' '
        'OD'=' '
        ;

data data;
     length diag $2;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_PDX_ENCTYPE"
                          and category in ("EI" "IP" "IS" "OS") 
                          and cross_category2 in ("OD" "BI" "CL" "DR")
                          and statistic in ("RECORD_N" "DISTINCT_ENCID_N")));
     if cross_category^="P" then diag="NP";
     else diag="P";

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep dc_name category cross_category2 statistic diag resultn bt_flag;
run;

*- Summary diagnosis -*;
proc means data=data nway noprint missing;
     class dc_name category cross_category2 statistic diag bt_flag;
     var resultn;
     output out=sum sum=sum;
run;

*- Bring all of the diagnosis sums together -*;
data print_ive;
     merge sum(where=(statistic="DISTINCT_ENCID_N" and diag="NP" and btflag_pat_np=" ") rename=(sum=pat_np bt_flag=btflag_pat_np))
           sum(where=(statistic="DISTINCT_ENCID_N" and diag="P" and btflag_pat_p=" ") rename=(sum=pat_p bt_flag=btflag_pat_p))
           sum(where=(statistic="RECORD_N" and diag="P" and btflag_rec_p=" ") rename=(sum=rec_p bt_flag=btflag_rec_p))
           sum(where=(statistic="RECORD_N" and diag="NP" and btflag_rec_np=" ") rename=(sum=rec_np bt_flag=btflag_rec_np))

           sum(where=(statistic="DISTINCT_ENCID_N" and diag="NP" and btflag_pat_np="*") drop=sum rename=(bt_flag=btflag_pat_np))
           sum(where=(statistic="DISTINCT_ENCID_N" and diag="P" and btflag_pat_p="*") drop=sum rename=(bt_flag=btflag_pat_p))
           sum(where=(statistic="RECORD_N" and diag="P" and btflag_rec_p="*") drop=sum rename=(bt_flag=btflag_rec_p))
           sum(where=(statistic="RECORD_N" and diag="NP" and btflag_rec_np="*") drop=sum rename=(bt_flag=btflag_rec_np))
     ;
     by category cross_category2;
     rename dc_name=dc_name1;
run;

*- Bring everything together by adding encounters without principal diagnosis -*;
data print_ive(keep=pg col: perc: thresholdflag: exception:);
     length col0-col3 col5-col7 $50;
     merge print_ive
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_PDXGRP_ENCTYPE"
                     and cross_category="P" and category in ("EI" "IP" "IS" "OS")
                     and statistic in ("DISTINCT_ENCID_N") and cross_category2 in ("OD" "BI" "CL" "DR")) 
                     rename=(resultn=col3num))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_PDXGRP_ENCTYPE"
                     and cross_category="U" and category in ("EI" "IP" "IS" "OS")
                     and statistic in ("DISTINCT_ENCID_N") and cross_category2 in ("OD" "BI" "CL" "DR")));
     ;
     by category cross_category2;

     if first.category then col0=put(category,$etype.);
     else col0=" ";

     col1=put(cross_category2,$origin.);

     if btflag_pat_p^="*" then col2=strip(put(pat_p,comma16.));
     else col2="BT";

     if col3num>.t then col3=strip(put(col3num,comma16.));
     else col3=strip(put(col3num,threshold.));

     if category in ("EI" "IP") and (compress(col2) in ("0" "BT")) and resultn>0 then exceptionflag=1;

     if btflag_rec_p^="*" then col5=strip(put(rec_p,comma16.));
     else col5="BT";

     if resultn>.t and pat_p>.t then do;
        if sum(resultn,pat_p)>0 then do;
            perc=(resultn/(resultn+pat_p))*100;
            if category in ("EI" "IP") and perc>10.0 then thresholdflag=1;
        end;
     end;

     if rec_p>0 and pat_p>0 then do;
        col6=strip(put((rec_p/pat_p),16.1));
        if category in ("EI" "IP") and rec_p/pat_p > 2.0 then exceptionrate=1;
     end;

     col7=put(cross_category2,$stable.);

     pg=1;        
run;

*- Keep records resulting in an exception -*;
data dc_summary_ive;
     length edc_table_s_ $10;
     set print_ive(keep=thresholdflag exception:);
     if thresholdflag=1 or exceptionflag=1 or exceptionrate=1;
     exception=1;
     edc_table_s_="Table IVE";
     if thresholdflag=1 or exceptionflag=1 then do;
        row=3.06; output;
     end;
     if exceptionrate=1 then do;
        row=2.07; output;
     end;
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ive nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ive;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ive;
    %let _ftn=IVE;
    %let _hdr=Table IVE;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    ods listing close;
    proc report data=print_ive split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg thresholdflag exceptionflag exceptionrate perc col0 col1 col2 col3 col4 col5 col6 col7;
         where pg=1;

         define pg       /display noprint;
         define thresholdflag /display noprint;
         define exceptionflag /display noprint;
         define exceptionrate /display noprint;
         define perc     /display noprint;
         define col0     /display flow "Encounter Type" style(header)=[just=left cellwidth=19.5%] style(column)=[just=left];
         define col1     /display flow "DX_ORIGIN" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col2     /display flow "Distinct encounter|IDs with a|principal diagnosis" style(header)=[just=center cellwidth=11%] 
                            style(column)=[just=center];
         define col3     /display flow "Distinct encounter|IDs without a|principal diagnosis" style(header)=[just=center cellwidth=11%] 
                            style(column)=[just=center];
         define col4     /computed format=8.1 "% of encounters|without a principal|diagnosis" style(header)=[just=center cellwidth=9%] 
                            style(column)=[just=center];
         define col5     /display flow "Principal|diagnoses" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Principal diagnoses|per encounter with any|principal diagnosis" style(header)=[just=center cellwidth=10%] 
                            style(column)=[just=center];
         define col7     /display flow "Source table" style(header)=[just=left cellwidth=19%] style(column)=[just=left];

         compute col2;
            if exceptionflag=1 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col4;
            if thresholdflag=1 then do;
                col4=perc;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=perc;
            end;
        endcomp;
        compute col6;
            if exceptionrate=1 then call define(_col_, "style", "style=[color=blue]");
        endcomp;
    run; 

    ods listing;
%mend print_ive;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVF DATA LATENCY - ENCOUNTER, DIAGNOSIS, AND PROCEDURES
********************************************************************************;

*- Determine Month 0 and create a record for the 24 preceding months -*;
data months;
     set dc_normal(keep=dc_name variable resultc 
         where=(dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     do i = 0 to -23 by -1;
        date=intnx('month',input(resultc,yymmdd10.),i);
        ord=abs(i);
        do dc_name = "ENC_L3_ENCTYPE_ADATE_YM", "DIA_L3_ENCTYPE_ADATE_YM", 
                     "PRO_L3_ENCTYPE_ADATE_YM";
           output;
        end;
     end;

     keep dc_name date ord;
run;

proc sort data=months;
     by date dc_name;
run;

proc sort data=months(keep=ord date) out=calendar nodupkey;
     by ord;
run;

*- Subset data -*;
data data;
     set dc_normal(keep=dc_name statistic category resultn cross_category 
                 where=(category in ("AV" "ED" "EI" "IP" "TH") and resultn>=0 and 
                 cross_category not in ("NULL or missing" "Values outside of CDM specifications") and 
                 (statistic="RECORD_N" and dc_name in ("ENC_L3_ENCTYPE_ADATE_YM" "DIA_L3_ENCTYPE_ADATE_YM" 
                                                       "PRO_L3_ENCTYPE_ADATE_YM"))));

     date=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;

     keep dc_name date resultn bt_flag;
run;

proc sort data=data;
     by date dc_name;
run;

*- Merge 24 month data with table data -*;
data data;
     merge data months(in=m);
     by date dc_name;
     if m;

     if resultn=. then resultn=0;
run;

*- Create benchmark records -*;
data data;
     set data;

     output;

     * create benchmark records *;
     if 12<=ord<=23 then do;
        ord=24;
        output;
     end;
run;

*- Determine counts for all months -*;
proc means data=data nway noprint missing;
     class ord dc_name bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

proc sort;
     by dc_name ord;
run;

*- Flag records if BT -*;
data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name ord;
run;

*- Add denominator -*;
data stats;
     merge stats
           stats(keep=dc_name ord sum rename=(ord=bord sum=denom) where=(bord=24))
     ;
     by dc_name;
run;

proc sort data=stats;
     by ord;
run;

*- Bring everything together -*;
data print_ivf;
     length col1 col1a col2 col4 col6 $35;
     merge stats(where=(dc_name="ENC_L3_ENCTYPE_ADATE_YM") rename=(sum=enc denom=enc_den bt_flag=bt_enc))
           stats(where=(dc_name="DIA_L3_ENCTYPE_ADATE_YM") rename=(sum=dia denom=dia_den bt_flag=bt_dia))
           stats(where=(dc_name="PRO_L3_ENCTYPE_ADATE_YM") rename=(sum=pro denom=pro_den bt_flag=bt_pro))
           calendar
     ;
     by ord;

     if ord<24 then do;
         if enc=. and bt_enc="*" then col2=strip(put(.t,threshold.));
         else col2=strip(put(enc,comma16.))||strip(bt_enc);
         if enc>0 and enc_den>0 then col3p=(enc/round((enc_den/12),1))*100;

         if dia=. and bt_dia="*" then col4=strip(put(.t,threshold.));
         else col4=strip(put(dia,comma16.))||strip(bt_dia);
         if dia>0 and dia_den>0 then col5p=(dia/round((dia_den/12),1))*100;

         if pro=. and bt_pro="*" then col6=strip(put(.t,threshold.));
         else col6=strip(put(pro,comma16.))||strip(bt_pro);
         if pro>0 and pro_den>0 then col7p=(pro/round((pro_den/12),1))*100;

         col1="Month -"||compress(put(ord,2.));
         col1a=put(date,mmyys7.);
         output;
     end;
     else if ord=24 then do;
         col1="Benchmark average";
         col1a=" ";
         if enc=. and bt_enc="*" then col2=strip(put(.t,threshold.));
         else col2=strip(put(enc/12,comma16.))||strip(bt_enc);

         if dia=. and bt_dia="*" then col4=strip(put(.t,threshold.));
         else col4=strip(put(dia/12,comma16.))||strip(bt_dia);

         if pro=. and bt_pro="*" then col6=strip(put(.t,threshold.));
         else col6=strip(put(pro/12,comma16.))||strip(bt_pro);

         output;
         ord=11.5;
         col1="Benchmark Period";
         col1a=" ";
         col2=" ";
         col3=" ";
         col4=" ";
         col5=" ";
         col6=" ";
         col7=" ";
         output;
         ord=25;
         col1="Source table";
         col1a=" ";
         col2="ENC_L3_ ~n ENCTYPE_ ~n ADATE_YM";
         col3=" ";
         col4="DIA_L3_ ~n ENCTYPE_ ~n ADATE_YM";
         col5=" ";
         col6="PRO_L3_ ~n ENCTTPE_ ~n ADATE_YM";
         col7=" ";
         output;
     end;
run;

proc sort data=print_ivf;
     by ord;
run;

*- Create page number -*;
data print_ivf;
     set print_ivf;
     if ord<=11 then pg=1;
     else pg=2;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivf;
     length edc_table_s_ $10;
     set print_ivf(keep=ord col2 col3p col4 col5p col6 col7p);
     if ord in (3) and ((compress(col2) in ("0" "BT") or .<col3p<75.0) or 
                        (compress(col4) in ("0" "BT") or .<col5p<75.0) or 
                        (compress(col6) in ("0" "BT") or .<col7p<75.0));
     row=3.07;
     exception=1;
     edc_table_s_="Table IVF";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivf nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivf;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivf;
    %let _ftn=IVF;
    %let _hdr=Table IVF;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivf split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("Ambulatory, Teleheath, ED, Inpatient or ED-to-Inpatient encounters|_________________________________" col2 col3)
                ("Ambulatory, Teleheath, ED, Inpatient or ED-to-Inpatient diagnoses|_________________________________" col4 col5)
                ("Ambulatory, Teleheath, ED, Inpatient or ED-to-Inpatient procedures|_________________________________" col6 col7);
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=center cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ivf split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient encounters|_________________________________" col2 col3)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient diagnoses|_________________________________" col4 col5)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient procedures|_________________________________" col6 col7);
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
         break after pg / page;
    run;

    ods listing;
%mend print_ivf;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);


********************************************************************************;
* TABLE IVG DATA LATENCY - VITAL, PRESCRIBING, AND LAB
********************************************************************************;

*- Determine Month 0 and create a record for the 24 preceding months -*;
data months;
     set dc_normal(keep=dc_name variable resultc 
         where=(dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     do i = 0 to -23 by -1;
        date=intnx('month',input(resultc,yymmdd10.),i);
        ord=abs(i);
        do dc_name = "VIT_L3_MDATE_YM", "PRES_L3_ODATE_YM", 
                     "LAB_L3_RDATE_YM";
           output;
        end;
     end;

     keep dc_name date ord;
run;

proc sort data=months;
     by date dc_name;
run;

proc sort data=months(keep=ord date) out=calendar nodupkey;
     by ord;
run;

*- Subset data -*;
data data;
     set dc_normal(keep=dc_name statistic category resultn cross_category 
                 where=(resultn>=0 and category not in ("NULL or missing" "Values outside of CDM specifications") 
                  and statistic="RECORD_N" and dc_name in ("VIT_L3_MDATE_YM" "PRES_L3_ODATE_YM" "LAB_L3_RDATE_YM")));

     date=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;

     keep dc_name date resultn bt_flag;
run;

proc sort data=data;
     by date dc_name;
run;

*- Determine if table is not present -*;
proc sql noprint;
     select count(dc_name) into :_vit_mdate from data
         where dc_name="VIT_L3_MDATE_YM";
     select count(dc_name) into :_pres_odate from data
         where dc_name="PRES_L3_ODATE_YM";
     select count(dc_name) into :_lab_rdate from data
         where dc_name="LAB_L3_RDATE_YM";
quit;

*- Merge 24 month data with table data -*;
data data;
     merge data months(in=m);
     by date dc_name;
     if m;

     if resultn=. then resultn=0;
run;

*- Create benchmark records -*;
data data;
     set data;

     output;

     * create benchmark records *;
     if 12<=ord<=23 then do;
        ord=24;
        output;
     end;
run;

*- Determine counts for all months -*;
proc means data=data nway noprint missing;
     class ord dc_name bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

proc sort;
     by dc_name ord;
run;

*- Flag records if BT -*;
data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name ord;
run;

*- Add denominator -*;
data stats;
     merge stats
           stats(keep=dc_name ord sum rename=(ord=bord sum=denom) where=(bord=24))
     ;
     by dc_name;
run;

proc sort data=stats;
     by ord;
run;

*- Bring everything together -*;
data print_ivg;
     length col1 col1a col2 col4 col6 $35;
     merge stats(where=(dc_name="VIT_L3_MDATE_YM") rename=(sum=vit denom=vit_den bt_flag=bt_vit))
           stats(where=(dc_name="PRES_L3_ODATE_YM") rename=(sum=pres denom=pres_den bt_flag=bt_pres))
           stats(where=(dc_name="LAB_L3_RDATE_YM") rename=(sum=lab denom=lab_den bt_flag=bt_lab))
           calendar
     ;
     by ord;

     vitpresent=&_vit_mdate;
     prespresent=&_pres_odate;
     labpresent=&_lab_rdate;
    
     if ord<24 then do;
         if vit=. and bt_vit="*" then col2=strip(put(.t,threshold.));
         else if vitpresent=0 then col2="N/A";
         else col2=strip(put(vit,comma16.))||strip(bt_vit);
         if vit>0 and vit_den>0 then col3p=(vit/round((vit_den/12),1))*100;

         if pres=. and bt_pres="*" then col4=strip(put(.t,threshold.));
         else if prespresent=0 then col4="N/A";
         else col4=strip(put(pres,comma16.))||strip(bt_pres);
         if pres>0 and pres_den>0 then col5p=(pres/round((pres_den/12),1))*100;

         if lab=. and bt_lab="*" then col6=strip(put(.t,threshold.));
         else if labpresent=0 then col6="N/A";
         else col6=strip(put(lab,comma16.))||strip(bt_lab);
         if lab>0 and lab_den>0 then col7p=(lab/round((lab_den/12),1))*100;

         col1="Month -"||compress(put(ord,2.));
         col1a=put(date,mmyys7.);

         output;
     end;
     else if ord=24 then do;
         col1="Benchmark average";
         col1a=" ";
         if vit=. and bt_vit="*" then col2=strip(put(.t,threshold.));
         else if vitpresent=0 then col2="N/A";
         else col2=strip(put(vit/12,comma16.))||strip(bt_vit);

         if pres=. and bt_pres="*" then col4=strip(put(.t,threshold.));
         else if prespresent=0 then col4="N/A";
         else col4=strip(put(pres/12,comma16.))||strip(bt_pres);

         if lab=. and bt_lab="*" then col6=strip(put(.t,threshold.));
         else if labpresent=0 then col6="N/A";
         else col6=strip(put(lab/12,comma16.))||strip(bt_lab);

         output;
         ord=11.5;
         col1="Benchmark Period";
         col1a=" ";
         col2=" ";
         col3=" ";
         col4=" ";
         col5=" ";
         col6=" ";
         col7=" ";
         output;
         ord=25;
         col1="Source table";
         col1a=" ";
         col2="VIT_L3_ ~n MDATE_YM";
         col3=" ";
         col4="PRES_L3_ ~n ODATE_YM";
         col5=" ";
         col6="LAB_L3_ ~n  RDATE_YM";
         col7=" ";
         output;
     end;
run;

proc sort data=print_ivg;
     by ord;
run;

*- Create page number -*;
data print_ivg;
     set print_ivg;
     if ord<=11 then pg=1;
     else pg=2;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivg;
     length edc_table_s_ $10;
     set print_ivg(keep=ord col2 col3p col4 col5p col6 col7p);
     if ord in (3) and ((compress(col2) in ("0" "BT") or .<col3p<75.0) or 
                        (compress(col4) in ("0" "BT") or .<col5p<75.0) or 
                        (compress(col6) in ("0" "BT") or .<col7p<75.0));
     row=3.11;
     exception=1;
     edc_table_s_="Table IVG";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivg nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivg;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivg;
    %let _ftn=IVG;
    %let _hdr=Table IVG;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivg split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("Vitals|_________________________________" col2 col3)
                ("Prescriptions|_________________________________" col4 col5)
                ("Labs|_________________________________" col6 col7);
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=center cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ivg split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("Vitals|_________________________________" col2 col3)
                ("Prescriptions|_________________________________" col4 col5)
                ("Labs|_________________________________" col6 col7);
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
         break after pg / page;
    run;

    ods listing;
%mend print_ivg;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);


********************************************************************************;
* TABLE IVH RXNORM TERM TYPE MAPPING
********************************************************************************;

proc format;
     value tier_desc
       1 = 'RXNORM_CUI encodes ingredient(s), strength and dose form'
     1.5 = 'RXNORM_CUI encodes ingredient(s), strength, dose form and brand'
       2 = 'RXNORM_CUI encodes ingredient(s) and potentially strength or dose form.  Can still represent medications with multiple ingredients with a single RXCUI.'
       3 = 'Requires more than one RXNORM_CUI to represent medications with multiple ingredients.'
       4 = 'RXNORM_CUI does not encode any ingredient information.'
       5 = 'RXNORM_CUI was not populated or could not be matched to the reference table'
       6 = 'RXNORM_CUI encodes ingredient(s), strength and dose form'
       7 = 'MEDADMIN_CODE encodes ingredient(s), strength and dose form'
       8 = 'MEDADMIN_CODE encodes ingredient(s) and potentially strength or dose form.  Can still represent medications with multiple ingredients with a single RXCUI.'
       9 = 'Requires more than one MEDADMIN_CODE to represent medications with multiple ingredients.'
      10 = 'MEDADMIN_CODE does not encode any ingredient information.'
      11 = 'MEDADMIN_CODE was not populated or could not be matched to the reference table'
      12 = 'MEDADMIN_CODE encodes ingredient(s), strength and dose form'
       ;

     value tier_type
       1,7 = 'SCD, SBD, BPCK, and GPCK'
       1.5 = 'SBD, BPCK'
       2,8 = 'SBDF, SCDF, SBDG, SCDG, SBDC, BN,and MIN'
       3,9 = 'SCDC, PIN, and IN'
       4,10 = 'DF and DFG'
       5,11 = 'n/a'
       6,12 = 'SCD, SBD, BPCK, and GPCK'
       ;
    
     value period
       1 = 'All years'
       6 = 'Most Recent Five Years'
       7 = 'All years'
      12 = 'Most Recent Five Years'
   other = ' '
       ;
    
     value table
       1 = 'PRESCRIBING'
       7 = 'MED_ADMIN'
   other = ' '
       ;
run;

*- Subset and re-categorize -*;
data data_cui;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("PRES_L3_RXCUI") and 
                          cross_category^="NULL or missing" and statistic="RECORD_N")) end=eof;
     retain num denom 0;
     if cross_category in ("SCD" "SBD" "BPCK" "GPCK") and resultn>.t then denom=denom+resultn;
     if cross_category in ("SBD" "BPCK") and resultn>.t then num=num+resultn;

     if eof then do;
        ord=1.5;
        category="Tier 1 brand";
        if num>=0 and denom>0 then col5p=(num/denom)*100;
        else col5p=.;
        output;
        keep ord category num col5p;
    end;
run;

data data;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
                  (dc_name in ("PRES_L3_RXCUI_TIER" "MEDADM_L3_RXCUI_TIER") or 
                  (dc_name in ("PRES_L3_RXCUI_TIER_5Y" "MEDADM_L3_RXCUI_TIER_5Y") and 
                   category="Tier 1"))));

     if category="NULL or missing" then do;
        category="Unknown";
        if scan(dc_name,1,'_')='PRES' then ord=5;
        else if scan(dc_name,1,'_')='MEDADM' then ord=11;
     end;
     else if dc_name="PRES_L3_RXCUI_TIER" then ord=input(scan(category,2),1.);
     else if dc_name="PRES_L3_RXCUI_TIER_5Y" then ord=6;
     else if dc_name="MEDADM_L3_RXCUI_TIER" then ord=input(scan(category,2),1.)+6;
     else if dc_name="MEDADM_L3_RXCUI_TIER_5Y" then ord=12;


     keep ord category statistic resultn;
run;

proc sort data=data;
     by ord;
run;

*- Bring everything together -*;
data print_ivh;
     length col0 col1 col3 col4 col6 $45 col2 $200;
     merge data(where=(statistic="RECORD_N"))
           data(where=(statistic="RECORD_PCT") rename=(resultn=col5p))
           data_cui(in=c rename=(num=resultn))
     ;
     by ord;

     col_1=put(ord,table.);
     col0=put(ord,period.);
     col1=strip(category);
     col2=strip(put(ord,tier_desc.));
     col3=strip(put(ord,tier_type.));

     if resultn=.t then col4=strip(put(resultn,threshold.));
     else if resultn=. then col4=strip(put(0,threshold.));
     else col4=strip(put(resultn,comma16.));

     if not c then do;
        if ord<=5 then col6="PRES_L3_RXCUI_TIER";
        else if ord=6 then col6="PRES_L3_RXCUI_TIER_5Y";
        else if ord<=11 then col6="MEDADM_L3_RXCUI_TIER";
        else if ord=12 then col6="MEDADM_L3_RXCUI_TIER_5Y";
     end;   
     else if c then col6="PRES_L3_RXCUI";

     if ord<=6 then pg=1;
     else pg=2;
    
     keep pg ord col:;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivh;
     length edc_table_s_ $10;
     set print_ivh(keep=ord col4 col5p);
     if ord in (1,6,7,12) and (col5p<80.0 or compress(col4) in ("0" "BT"));
     if ord in (1,6) then row=3.08;
     else if ord in (7,12) then row=3.15;
     exception=1;
     edc_table_s_="Table IVH";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivh nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivh;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivh;
    %let _ftn=IVH;
    %let _hdr=Table IVH;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivh split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col_1 col0 col1 col2 col3 ord col4 col5p col5 col6;
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col_1    /display flow "Table"  style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col0     /display flow "Period" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col1     /display flow "Term Type Tier" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col2     /display flow "Term Type Tier Description" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col3     /display flow "Term Types" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /computed flow format=8.2 "Percentage" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table" style(header)=[just=left cellwidth=9%] style(column)=[just=left];
         define col5p    /display noprint;
         compute ord;
            if ord=1.5 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col5;
            if ord in (1,6,7,12) and col5p<80.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col4;
            if ord in (1,6,7,12) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ivh split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col_1 col0 col1 col2 col3 ord col4 col5p col5 col6;
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col_1    /display flow "Table"  style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col0     /display flow "Period" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
         define col1     /display flow "Term Type Tier" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col2     /display flow "Term Type Tier Description" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col3     /display flow "Term Types" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /computed flow format=8.2 "Percentage" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table" style(header)=[just=left cellwidth=9%] style(column)=[just=left];
         define col5p    /display noprint;
         compute ord;
            if ord=1.5 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col5;
            if ord in (1,6,7,12) and col5p<80.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col4;
            if ord in (1,6,7,12) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods listing;
%mend print_ivh;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);


********************************************************************************;
* TABLE IVI LABORATORY RESULT DATA COMPLETENESS
********************************************************************************;
%macro _ivi;

proc format;
     value known_desc
       1 = 'Number of distinct LAB_LOINCs'
       2 = 'Results mapped to a known LAB_LOINC'
       3 = 'Results mapped to a known LAB_LOINC with a known result'
       4 = 'Quantitative results'
       7 = '~mQuantitative results which fully specify the normal range'
       8 = '~mQuantitative results which specify the result unit'
      10 = '~mQuantitative results which fully specify the normal range'
      11 = '~mQuantitative results which specify the result unit'
       ;

     value _data_check
       1 = 'n/a'
       2 = 'n/a'
       3 = '3.09'
       4 = 'n/a'
       7 = '3.10'
       8 = '3.12'
      10 = '3.10'
      11 = '3.12'
       ;

     value period
       1 = 'All years'
      10 = 'Most Recent Five Years'
   other = ' '
       ;
run;

*- Subset and re-categorize -*;
data data;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
         (dc_name in ("LAB_L3_RECORDC" "LAB_L3_RECORDC_5Y") and statistic="ALL_N") or
         (dc_name in ("LAB_L3_N" "LAB_L3_N_5Y") and variable="LAB_RESULT_CM_ID" and 
          statistic in ("ALL_N" "NULL_N"))));

     keep variable resultn dc_name;
run;

*- Check to see if record exist - not required -*;
proc sql noprint;
     select count(*) into :_recordc_5y from data
         where dc_name="LAB_L3_RECORDC_5Y" and variable="KNOWN_TEST_RESULT_NUM";
quit;
%put &_recordc_5y;

proc means data=data nway noprint;
     class variable dc_name;
     var resultn;
     output out=stats sum=sum;
     where variable="LAB_RESULT_CM_ID";
run;

proc means data=dc_normal nway noprint;
     class dc_name;
     var resultn;
     output out=loinc n=resultn;
     where datamartid=%upcase("&dmid") and dc_name="LAB_L3_LOINC" and statistic="RECORD_N" and 
           category not in ("NULL or missing" "Values outside of CDM specifications");
run;

*- Bring everything together -*;
data print_ivi;
     length col00 $25 col0 $10 col1 $130 col3 col4 col6 $30;
     if _n_=1 then set stats(where=(variable="LAB_RESULT_CM_ID" and dc_name="LAB_L3_N") rename=(sum=denom_1));
     if _n_=1 then set data(where=(dc_name="LAB_L3_RECORDC" and variable="KNOWN_TEST") rename=(resultn=denom_2));
     if _n_=1 then set data(where=(dc_name="LAB_L3_RECORDC" and variable="KNOWN_TEST_RESULT_NUM") rename=(resultn=denom_8));
     %if &_recordc_5y=1 %then %do;
         if _n_=1 then set data(where=(dc_name="LAB_L3_RECORDC_5Y" and variable="KNOWN_TEST_RESULT_NUM") rename=(resultn=denom_85));
     %end;
     set loinc(in=l)
         data(where=(variable not in ("LAB_RESULT_CM_ID" "KNOWN_TEST_RESULT_NUM_SRCE_UNIT" "KNOWN_TEST_RESULT_NUM_SOURCE" "KNOWN_TEST_RESULT_PLAUSIBLE")));

     if l then do;
        ord=1;
        col4=" ";
        col5p=.;
        col6="LAB_L3_LOINC";
     end;
     else if variable="KNOWN_TEST" then do;
        if dc_name="LAB_L3_RECORDC" then ord=2;
        else if dc_name="LAB_L3_RECORDC_5Y" then delete;
        col4=strip(put(denom_1,comma16.));
        if resultn>=0 and denom_1>0 then col5p=(resultn/denom_1)*100;
        else col5p=.;
        col6=strip(dc_name) || "; LAB_L3_N";
     end;
     else if variable="KNOWN_TEST_RESULT" then do;
        if dc_name="LAB_L3_RECORDC" then ord=3;
        else if dc_name="LAB_L3_RECORDC_5Y" then delete;
        col4=strip(put(denom_1,comma16.));
        if resultn>=0 and denom_1>0 then col5p=(resultn/denom_1)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM" then do;
        if dc_name="LAB_L3_RECORDC" then ord=4;
        else if dc_name="LAB_L3_RECORDC_5Y" then delete;
        col4=" ";
        col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_RANGE" then do;
        if dc_name="LAB_L3_RECORDC" then do;
           ord=7;
           col4=strip(put(denom_8,comma16.));
           if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
           else col5p=.;
        end;
     %if &_recordc_5y=1 %then %do;
        else if dc_name="LAB_L3_RECORDC_5Y" then do;
           ord=10;
           col4=strip(put(denom_85,comma16.));
           if resultn>=0 and denom_85>0 then col5p=(resultn/denom_85)*100;
           else col5p=.;
         end;
        col6=dc_name;
     %end;
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_UNIT" then do;
        if dc_name="LAB_L3_RECORDC" then do;
           ord=8;
           col4=strip(put(denom_8,comma16.));
           if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
           else col5p=.;
        end;
     %if &_recordc_5y=1 %then %do;
        else if dc_name="LAB_L3_RECORDC_5Y" then do;
           ord=11;
           col4=strip(put(denom_85,comma16.));
           if resultn>=0 and denom_85>0 then col5p=(resultn/denom_85)*100;
           else col5p=.;
        end;
        col6=dc_name; 
     %end;
     end;

     col00=strip(put(ord,period.));
     col0=strip(put(ord,_data_check.));
     col1=strip(put(ord,known_desc.));
     if resultn=.t then col3=strip(put(resultn,threshold.));
     else col3=strip(put(resultn,comma16.));
    
     pg=1;

    keep pg ord col:;
run;

%if &_recordc_5y^=1 %then %do;
    data recordc_5y;
         length col00 $25 col0 $10 col1 $130 col3 col4 col6 $30;
         col3="N/A";
         col4="N/A";
         col5p=.;
         col6="LAB_L3_RECORDC_5Y";
         pg=1;

         ord=10;
         col00=strip(put(ord,period.));
         col0=strip(put(ord,_data_check.));
         col1=strip(put(ord,known_desc.));
         output;
         ord=11;
         col00=strip(put(ord,period.));
         col0=strip(put(ord,_data_check.));
         col1=strip(put(ord,known_desc.));
         output;
    run;
    
    data print_ivi;
         set print_ivi recordc_5y;
    run;
%end;

    
proc sort data=print_ivi;
     by ord;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivi;
     length edc_table_s_ $10;
     set print_ivi(keep=ord col5p col3);
     if ord in (3,7,8,10,11) and (.<col5p<80.0 or compress(col3) in ("0" "BT"));
     if ord in (3) then row=3.09;
     else if ord in (7,10) then row=3.10;
     else if ord in (8,11) then row=3.12;
     exception=1;
     edc_table_s_="Table IVI";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivi nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivi;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivi;
    %let _ftn=IVI;
    %let _hdr=Table IVI;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivi split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col00 col0 col1 ord col3 col4 col5p col5 col6;
         where pg=1;
    
         define pg       /order noprint;
         define ord      /display noprint;
         define col00    /display flow "Period" style(header)=[just=left cellwidth=8%] style(column)=[just=left];
         define col0     /display flow "Data Check" style(header)=[just=center cellwidth=4.5%] style(column)=[just=center];
         define col1     /display flow "Description" style(header)=[just=left cellwidth=40%] style(column)=[just=left];
         define col3     /display flow "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /computed flow format=8.2 "Percentage" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table" style(header)=[just=left cellwidth=14.5%] style(column)=[just=left];
         define col5p    /display noprint;
         compute ord;
            if ord>4 then CALL DEFINE('_c4_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col5;
            if ord in (3,7,8,9,10,11) and .<col5p<80.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col3;
            if ord in (3,7,8,9,10,11) and compress(col3) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods listing;
%mend print_ivi;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);
%mend _ivi;
%_ivi;

********************************************************************************;
* TABLE IVI REFERENCE
********************************************************************************;

*- Produce output -*;
%macro print_ivi_ref;
    %let _ftn=IVI_Ref;
    %let _hdr=Table IVI_Ref;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote " ";

    proc report data=tbl_ivi_ref split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column description source_table_row lab_loinc_criteria result_num_criteria
                result_modifier_criteria other_criteria;
    
         define description              /display flow "Description" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define source_table_row         /display flow "Source Table Row" style(header)=[just=center cellwidth=12%] style(column)=[just=left];
         define lab_loinc_criteria       /display flow "LAB_LOINC criteria" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define result_num_criteria      /display flow "RESULT_NUM criteria" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define result_modifier_criteria /display flow "RESULT_MODIFIER criteria" style(header)=[just=center cellwidth=17%] style(column)=[just=center];
         define other_criteria           /display flow "Other criteria" style(header)=[just=left cellwidth=38%] style(column)=[just=left];
    run;

    ods listing;
%mend print_ivi_ref;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE IVJ DATA LATENCY - MED_ADMIN, DISPENSING, AND OBS_CLIN
********************************************************************************;

*- Determine Month 0 and create a record for the 24 preceding months -*;
data months;
     set dc_normal(keep=dc_name variable resultc 
         where=(dc_name="XTBL_L3_METADATA" and variable="RESPONSE_DATE"));

     do i = 0 to -23 by -1;
        date=intnx('month',input(resultc,yymmdd10.),i);
        ord=abs(i);
        do dc_name = "MEDADM_L3_SDATE_YM", "DISP_L3_DDATE_YM", 
                     "OBSCLIN_L3_SDATE_YM";
           output;
        end;
     end;

     keep dc_name date ord;
run;

proc sort data=months;
     by date dc_name;
run;

proc sort data=months(keep=ord date) out=calendar nodupkey;
     by ord;
run;

*- Subset data -*;
data data;
     set dc_normal(keep=dc_name statistic category resultn cross_category 
                 where=(resultn>=0 and category not in ("NULL or missing" "Values outside of CDM specifications") 
                  and statistic="RECORD_N" and dc_name in ("MEDADM_L3_SDATE_YM", "DISP_L3_DDATE_YM", "OBSCLIN_L3_SDATE_YM")));

     date=mdy(input(scan(category,2,'_'),2.),1,input(scan(category,1,'_'),4.));

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;

     keep dc_name date resultn bt_flag;
run;

proc sort data=data;
     by date dc_name;
run;

*- Determine if table is not present -*;
proc sql noprint;
     select count(dc_name) into :_med_sdate from data
         where dc_name="MEDADM_L3_SDATE_YM";
     select count(dc_name) into :_disp_ddate from data
         where dc_name="DISP_L3_DDATE_YM";
     select count(dc_name) into :_obsc_sdate from data
         where dc_name="OBSCLIN_L3_SDATE_YM";
quit;

*- Merge 24 month data with table data -*;
data data;
     merge data months(in=m);
     by date dc_name;
     if m;

     if resultn=. then resultn=0;
run;

*- Create benchmark records -*;
data data;
     set data;

     output;

     * create benchmark records *;
     if 12<=ord<=23 then do;
        ord=24;
        output;
     end;
run;

*- Determine counts for all months -*;
proc means data=data nway noprint missing;
     class ord dc_name bt_flag;
     var resultn;
     output out=stats sum=sum;
run;

proc sort;
     by dc_name ord;
run;

*- Flag records if BT -*;
data stats;
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name ord;
run;

*- Add denominator -*;
data stats;
     merge stats
           stats(keep=dc_name ord sum rename=(ord=bord sum=denom) where=(bord=24))
     ;
     by dc_name;
run;

proc sort data=stats;
     by ord;
run;

*- Bring everything together -*;
data print_ivj;
     length col1 col1a col2 col4 col6 $35;
     merge stats(where=(dc_name="MEDADM_L3_SDATE_YM") rename=(sum=med denom=med_den bt_flag=bt_med))
           stats(where=(dc_name="DISP_L3_DDATE_YM") rename=(sum=disp denom=disp_den bt_flag=bt_disp))
           stats(where=(dc_name="OBSCLIN_L3_SDATE_YM") rename=(sum=obsc denom=obsc_den bt_flag=bt_obsc))
           calendar
     ;
     by ord;

     medpresent=&_med_sdate;
     disppresent=&_disp_ddate;
     obscpresent=&_obsc_sdate;
    
     if ord<24 then do;
         if med=. and bt_med="*" then col2=strip(put(.t,threshold.));
         else if medpresent=0 then col2="N/A";
         else col2=strip(put(med,comma16.))||strip(bt_med);
         if med>0 and med_den>0 then col3p=(med/round((med_den/12),1))*100;

         if disp=. and bt_disp="*" then col4=strip(put(.t,threshold.));
         else if disppresent=0 then col4="N/A";
         else col4=strip(put(disp,comma16.))||strip(bt_disp);
         if disp>0 and disp_den>0 then col5p=(disp/round((disp_den/12),1))*100;

         if obsc=. and bt_obsc="*" then col6=strip(put(.t,threshold.));
         else if obscpresent=0 then col6="N/A";
         else col6=strip(put(obsc,comma16.))||strip(bt_obsc);
         if obsc>0 and obsc_den>0 then col7p=(obsc/round((obsc_den/12),1))*100;

         col1="Month -"||compress(put(ord,2.));
         col1a=put(date,mmyys7.);

         output;
     end;
     else if ord=24 then do;
         col1="Benchmark average";
         col1a=" ";
         if med=. and bt_med="*" then col2=strip(put(.t,threshold.));
         else if medpresent=0 then col2="N/A";
         else col2=strip(put(med/12,comma16.))||strip(bt_med);

         if disp=. and bt_disp="*" then col4=strip(put(.t,threshold.));
         else if disppresent=0 then col4="N/A";
         else col4=strip(put(disp/12,comma16.))||strip(bt_disp);

         if obsc=. and bt_obsc="*" then col6=strip(put(.t,threshold.));
         else if obscpresent=0 then col6="N/A";
         else col6=strip(put(obsc/12,comma16.))||strip(bt_obsc);

         output;
         ord=11.5;
         col1="Benchmark Period";
         col1a=" ";
         col2=" ";
         col3=" ";
         col4=" ";
         col5=" ";
         col6=" ";
         col7=" ";
         output;
         ord=25;
         col1="Source table";
         col1a=" ";
         col2="MEDADM_L3_ ~n SDATE_YM";
         col3=" ";
         col4="DISP_L3_ ~n DDATE_YM";
         col5=" ";
         col6="OBSCLIN_L3_ ~n  SDATE_YM";
         col7=" ";
         output;
     end;
run;

proc sort data=print_ivj;
     by ord;
run;

*- Create page number -*;
data print_ivj;
     set print_ivj;
     if ord<=11 then pg=1;
     else pg=2;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivj;
     length edc_table_s_ $10;
     set print_ivj(keep=ord col2 col3p col4 col5p col6 col7p);
     if ord in (3) and ((compress(col2) in ("0" "BT") or .<col3p<75.0) or 
                        (compress(col4) in ("0" "BT") or .<col5p<75.0) or 
                        (compress(col6) in ("0" "BT") or .<col7p<75.0));
     row=3.14;
     exception=1;
     edc_table_s_="Table IVJ";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_ivj nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_ivj;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_ivj;
    %let _ftn=IVJ;
    %let _hdr=Table IVJ;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_ivj split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("MED_ADMN|_________________________________" col2 col3)
                ("DISPENSING|_________________________________" col4 col5)
                ("OBS_CLIN|_________________________________" col6 col7);
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=center cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ivj split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg ord col3p col5p col7p col1 col1a
                ("MED_ADMIN|_________________________________" col2 col3)
                ("DISPENSING|_________________________________" col4 col5)
                ("OBS_CLIN|_________________________________" col6 col7);
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col1a    /display flow "Calendar|Month" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         compute col2;
            if ord in (3) and compress(col2) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col3;
            if ord in (3) and col3p<75.0 then do;
                col3=col3p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col3=col3p;
            end;
         endcomp;
         compute col4;
            if ord in (3) and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col5;
            if ord in (3) and col5p<75.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col6;
            if ord in (3) and compress(col6) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col7;
            if ord in (3) and col7p<75.0 then do;
                col7=col7p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7p;
            end;
         endcomp;
         break after pg / page;
    run;

    ods listing;
%mend print_ivj;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);


********************************************************************************;
* TABLE VA CHANGES IN TABLES
********************************************************************************;

*- Create table formats -*;
proc format;
     invalue tableord
       'DEMOGRAPHIC'=1
       'ENROLLMENT'=2
       'ENCOUNTER'=3
       'DIAGNOSIS'=4
       'PROCEDURES'=5
       'VITAL'=6
       'DEATH'=7
       'PRESCRIBING'=8
       'DISPENSING'=9
       'LAB_RESULT_CM'=10
       'CONDITION'=11
       'DEATH_CAUSE'=12
       'PRO_CM'=13
       'PROVIDER'=14
       'MED_ADMIN'=15
       'OBS_CLIN'=16
       'OBS_GEN'=17
       'HASH_TOKEN'=18
       'IMMUNIZATION'=19
       'LDS_ADDRESS_HISTORY'=20
       'LAB_HISTORY'=21
        ;
run;

*- Bring current table -*;
data current(keep=ord table dc_name statistic resultn);
     set dc_normal(where=(datamartid=%upcase("&dmid") and scan(dc_name,3,"_")="N" and 
                          (variable="PATID" and statistic in ("ALL_N" "DISTINCT_N")) or
                          (table="LABHIST" and variable="LAB_FACILITYID" and statistic in ("ALL_N")) or
                          (table="PROVIDER" and variable="PROVIDERID")));
     if dc_name="TRIAL_L3_N" or (dc_name="PROV_L3_N" and statistic="DISTINCT_N") or 
        dc_name="LAB_L3_N_5Y" then delete;
     if table="LABHIST" then table="LAB_HISTORY";
     ord=input(table,tableord.);
run;

proc sort data=current;
     by ord;
run;

*- Determine if previous data set exists -*;
proc contents data=prevdata._all_ out=_prevdata noprint;
run;
    
proc sql noprint;
     select unique(memname) into :_prevdata separated by '|'  from _prevdata
         where memname in ("DEM_L3_N" "ENR_L3_N" "ENC_L3_N" "DIA_L3_N" "PRO_L3_N" 
             "DEATH_L3_N" "VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "DISP_L3_N" "COND_L3_N"
             "DEATHC_L3_N" "PROCM_L3_N" "PROV_L3_N" "MEDADM_L3_N" "OBSCLIN_L3_N" 
             "OBSGEN_L3_N" "HASH_L3_N" "IMMUNE_L3_N" "LDSADRS_L3_N" "LABHIST_L3_N");
     select count(unique memname) into :prevdata_count from _prevdata
         where memname in ("DEM_L3_N" "ENR_L3_N" "ENC_L3_N" "DIA_L3_N" "PRO_L3_N" 
             "DEATH_L3_N" "VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "DISP_L3_N" "COND_L3_N"
             "DEATHC_L3_N" "PROCM_L3_N" "PROV_L3_N" "MEDADM_L3_N" "OBSCLIN_L3_N" 
             "OBSGEN_L3_N" "HASH_L3_N" "IMMUNE_L3_N" "LDSADRS_L3_N" "LABHIST_L3_N");
quit;

*- Stack existing previous data -*;
%macro prevdataloop;
    %do p = 1 %to &prevdata_count;
        %let pdata=%scan(&_prevdata,&p,'|');
        * Using data step instead of append due to varying variable lengths across datasets *;
        data previous;
            length dc_name $100;
            %if &p=1 %then %do;
                set prevdata.&pdata(keep=dataset tag all_n distinct_n);
                    dc_name="&pdata";
            %end;
            %else %if &p>1 %then %do; 
                set previous
                    prevdata.&pdata(in=c keep=dataset tag all_n distinct_n);
                if c then dc_name="&pdata";
            %end;
        run;
    %end;
%mend prevdataloop;
%prevdataloop;

*- Create table variables for previous data -*;
data previous(keep=ord dataset dc_name col: rename=(dataset=table));
     set previous;
     if tag="PATID" or (dataset="PROVIDER" and tag="PROVIDERID");
     ord=input(dataset,tableord.);
     if all_n^="BT" then col2n=input(all_n,comma16.);
     else if all_n="BT" then col2n=.t;
     if tag^="PROVIDERID" then do;
        if distinct_n^="BT" then col5n=input(distinct_n,comma16.);
        else if distinct_n="BT" then col5n=.t;
     end;
     else col5n=.;
run;

proc sort data=previous;
     by ord;
run;

*- Bring everything together -*;
data print_va;
     length col1-col3 col5-col6 col8 $50;
     merge current(where=(statistic="ALL_N") rename=(resultn=col3n))
           current(where=(statistic="DISTINCT_N") rename=(resultn=col6n))
           previous(in=p)
     ;
     by ord;

     col1=strip(table);
     if col2n^=. then col2=strip(put(col2n,thresholdc.));
     else col2="---";
     if col3n^=. then col3=strip(put(col3n,comma16.));
     else do; col3n=0; col3="---"; end;
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
     if col5n^=. then col5=strip(put(col5n,thresholdc.));
     else col5="---";
     if col6n^=. then col6=strip(put(col6n,comma16.));
     else do; col6n=0; col6="---"; end;
     if col5n>0 and col6n>=0 then col7n=((col6n-col5n)/col5n)*100;
     col8=strip(dc_name);

     pg=1;
run;

*- Keep records resulting in an exception -*;
data dc_summary_va;
     length edc_table_s_ $10;
     set print_va(keep=ord col2n col3n col4n col5n col6n col7n);
     if (col3n=0 and col2n>0) or (col6n=0 and col5n>0) or (.<col4n<-5.00) or (.<col7n<-5.00);
     row=4.01;
     exception=1;
     edc_table_s_="Table VA";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_va nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_va;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_va;
    %let _ftn=VA;
    %let _hdr=Table VA;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_va split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col2n col3n col4n col5n col6n col7n ord col1  
                ("Records|______________________________________________" col2 col3 col4) 
                ("Patients|______________________________________________" col5 col6 col7) 
                col8;

         define pg       /display noprint;
         define col2n    /display noprint;
         define col3n    /display noprint;
         define col4n    /display noprint;
         define col5n    /display noprint;
         define col6n    /display noprint;
         define col7n    /display noprint;
         define ord      /display noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col2     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col3     /display flow "Current|Refresh" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /display flow "Current|Refresh" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col7     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=14%] style(column)=[just=left];
         compute col4;
            if (col3n in (.,0) and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
         compute col7;
            if (col6n in (.,0) and col5n>0) or (.<col7n<-5.00) then do;
                col7=col7n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7n;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_va;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE VB CHANGES IN SELECTED ENCOUNTER TYPES AND DOMAINS
********************************************************************************;

*- Create table formats -*;
proc format;
     invalue enctype_vb
        "DIA_L3_ENCTYPE"=1
        "PRO_L3_ENCTYPE"=2
        "XTBL_L3_LAB_ENCTYPE"=3
        "XTBL_L3_PRES_ENCTYPE"=4
        ;

     value $row_vb
        "AV"='Ambulatory Visit (AV)'
        "ED"='Emergency Department (ED)'
        "IP"='Inpatient (IP)'
        "OA"='Other Ambulatory (OA)'
        "TH"='Telehealth (TH) encounters'
        ;

     value enc_vb
        1='Diagnoses'
        2='Procedures'
        3='Labs'
        4='Prescriptions'
        ;
run;

*- Bring current table -*;
data current(keep=ord dc_name statistic category resultn);
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name IN ("PRO_L3_ENCTYPE"
                    "DIA_L3_ENCTYPE" "XTBL_L3_LAB_ENCTYPE" "XTBL_L3_PRES_ENCTYPE") and 
                     category in ("AV" "ED" "IP" "OA" "TH") and 
                     statistic in ("RECORD_N" "DISTINCT_PATID_N")));
     ord=input(dc_name,enctype_vb.);
run;

proc sort data=current;
     by category ord;
run;

*- Determine if previous data set exists -*;
proc contents data=prevdata._all_ out=_prevdata noprint;
run;
    
proc sql noprint;
     select unique(memname) into :_prevdata separated by '|'  from _prevdata
         where memname in ("PRO_L3_ENCTYPE" "DIA_L3_ENCTYPE" "XTBL_L3_LAB_ENCTYPE" 
             "XTBL_L3_PRES_ENCTYPE");
     select count(unique memname) into :prevdata_count from _prevdata
         where memname in ("PRO_L3_ENCTYPE" "DIA_L3_ENCTYPE" "XTBL_L3_LAB_ENCTYPE" 
             "XTBL_L3_PRES_ENCTYPE");
quit;

*- Stack existing previous data -*;
%macro prevdataloop;
    %do p = 1 %to &prevdata_count;
        %let pdata=%scan(&_prevdata,&p,'|');
        data &pdata;
             length enc_type $36 record_n distinct_patid_n $20;
             set prevdata.&pdata(drop=datamartid response_date query_package);
             array _c enc_type record_n distinct_patid_n;
             ord=&p;
        run;

        proc append base=previous data=&pdata;
        run;
    %end;
%mend prevdataloop;
%prevdataloop;

*- Bring previous table data -*;
data previous(keep=ord enc_type col: rename=(enc_type=category));
     set previous;
     if enc_type in ("AV" "ED" "IP" "OA" "TH");

     if record_n not in (" " "BT") then col2n=input(record_n,comma16.);
     if distinct_patid_n not in (" " "BT") then col5n=input(distinct_patid_n,comma16.);
run;

proc sort data=previous;
     by category ord;
run;

*- Create label rows -*;
data label;
     do category = "AV", "ED", "IP", "OA", "TH";
        ord=0;
        output;
     end;
run;

*- Bring everything together -*;
data print_vb;
     length col1-col3 col5-col6 col8 $50;
     merge current(where=(statistic="RECORD_N") rename=(resultn=col3n))
           current(where=(statistic="DISTINCT_PATID_N") rename=(resultn=col6n))
           previous
           label
     ;
     by category ord;

     if ord=0 then col1=strip(put(category,row_vb.));
     else col1=strip(put(ord,enc_vb.));
     if col2n^=. then col2=strip(put(col2n,comma16.));
     else if ord>0 then col2="---";
     if col3n^=. then col3=strip(put(col3n,comma16.));
     else do; col3n=0; col3="---"; end;
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
     if col5n^=. then col5=strip(put(col5n,comma16.));
     else if ord>0 then col5="---";
     if col6n^=. then col6=strip(put(col6n,comma16.));
     else do; col6n=0; col6="---"; end;
     if col5n>0 and col6n>=0 then col7n=((col6n-col5n)/col5n)*100;
     col8=strip(dc_name);

     pg=1;
run;

*- Keep records resulting in an exception -*;
data dc_summary_vb;
     length edc_table_s_ $10;
     set print_vb(keep=ord col2n col3n col4n col5n col6n col7n);
     if (col3n=0 and col2n>0) or (col6n=0 and col5n>0) or (.<col4n<-5.00) or (.<col7n<-5.00);
     row=4.02;
     exception=1;
     edc_table_s_="Table VB";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_vb nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_vb;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_vb;
    %let _ftn=VB;
    %let _hdr=Table VB;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_vb split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2n col3n col4n col5n col6n col7n ord
                ("Records|___________________________________________" col2 col3 col4) 
                ("Patients|___________________________________________" col5 col6 col7) 
                col8;

         define pg       /order noprint;
         define ord      /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col3     /display flow "Current|Refresh" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col4     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         define col5     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /display flow "Current|Refresh" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col7     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=17%] style(column)=[just=left];
         define col2n    /display noprint;
         define col3n    /display noprint;
         define col4n    /display noprint;
         define col5n    /display noprint;
         define col6n    /display noprint;
         define col7n    /display noprint;
         compute ord;
            if ord^=0 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col4;
            if (col3n in (.,0) and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
         compute col7;
            if (col6n in (.,0) and col5n>0) or (.<col7n<-5.00) then do;
                col7=col7n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7n;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_vb;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
* TABLE VC CHANGES IN SELECTED CODE TYPES
********************************************************************************;

*- Create table formats -*;
proc format;
     invalue type_vc
        "DIA_L3_DX_DXTYPE"=1
        "PRO_L3_PX_PXTYPE"=2
        ;

     value row_vc
        1='DIAGNOSIS table'
        2='PROCEDURES table'
        ;

     value code_vc
        1='09'
        2='10'
        3='CH'
        ;
run;

data dc_normal;
     set dc_normal;
     var=1;
run;

proc means data=dc_normal nway noprint;
     class dc_name category;
     var resultn;
     output out=current sum=col3n n=n;
     where datamartid=%upcase("&dmid") and dc_name IN ("PRO_L3_PX_PXTYPE" "DIA_L3_DX_DXTYPE") and 
                   category in ("09" "10" "CH") and statistic in ("RECORD_N");
run;

data current;
     set current;
     by dc_name category;
     ord=input(dc_name,type_vc.);
     col6n=_freq_;
     keep dc_name category ord col:;
run;

proc sort data=current;
     by ord category;
run;

*- Determine if previous data set exists -*;
proc contents data=prevdata._all_ out=_prevdata noprint;
run;
    
proc sql noprint;
     select unique(memname) into :_prevdata separated by '|'  from _prevdata
         where memname in ("PRO_L3_PX_PXTYPE" "DIA_L3_DX_DXTYPE");
     select count(unique memname) into :prevdata_count from _prevdata
         where memname in ("PRO_L3_PX_PXTYPE" "DIA_L3_DX_DXTYPE");
quit;

*- Summarize previous data -*;
%macro prevdataloop;
    %do p = 1 %to &prevdata_count;
        %let pdata=%scan(&_prevdata,&p,'|');
        data &pdata;
             length statistic $50;
             set prevdata.&pdata;
             dc_name="&pdata";

             %if &pdata=DIA_L3_DX_DXTYPE %then %do;
                category=dx_type;
             %end;
             %else %if &pdata=PRO_L3_PX_PXTYPE %then %do;
                category=px_type;
             %end;

             * create character and numeric result variables *;
             if record_n not in ("BT" " ") then do;
                if indexc(upcase(record_n),"ABCDEFGHIJKLMNOPQRSTUVWXYZ_():")=0 then resultn=input(record_n,best.);
                else resultn=.;
             end;
             else if record_n in ("BT") then resultn=.t;
             else if record_n in (" ") then resultn=.;
             statistic="RECORD_N";

             keep dc_name category statistic resultn;
        run;

        * stats for previous data *;    
        proc means data=&pdata nway noprint;
             class dc_name category statistic;
             var resultn;
             output out=prev_&pdata sum=col2n;
             where category in ("09" "10" "CH");
        run;
    %end;
%mend prevdataloop;
%prevdataloop;

*- Set procedure and diagnosis -*;
data previous(keep=dc_name ord category col:);
     set prev_pro_l3_px_pxtype
         prev_dia_l3_dx_dxtype
     ;

     ord=input(dc_name,type_vc.);
     col5n=_freq_;
run;

proc sort data=previous;
     by ord category;
run;

*- Merge previous and current data -*;
data data;
     length col2-col3 col5-col6 $50;
     merge current previous;
     by ord category;

     * derive percentage for records *;
     if col2n^=. then col2=strip(put(col2n,comma16.));
     else if category^="00" then col2="---";
     if col3n^=. then col3=strip(put(col3n,comma16.));
     else do; col3n=0; col3="---"; end;
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
    
     * derive percentage for codes *;
     if col5n^=. then col5=strip(put(col5n,comma16.));
     else if category^="00" then col5="---";
     if col6n^=. then col6=strip(put(col6n,comma16.));
     else do; col3n=0; col6="---"; end;
     if col5n>0 and col6n>=0 then col7n=((col6n-col5n)/col5n)*100;
run;

*- Create label rows -*;
data label;
     do ord = 1;
        do category = "00", "09", "10";
           output;
        end;
     end;
     do ord = 2;
        do category = "00", "09", "10", "CH";
           output;
        end;
     end;
run;

*- Bring everything together -*;
data print_vc;
     merge data label;
     by ord category;

     if category="00" then col1=strip(put(ord,row_vc.));
     else col1=category;

     col8=strip(dc_name);
     pg=1;
run;

*- Keep records resulting in an exception -*;
data dc_summary_vc;
     length edc_table_s_ $10;
     set print_vc(keep=ord col2n col3n col4n col5n col6n col7n);
     if ((col3n=0 and col2n>0) or (.<col4n<-5.00)) or ((col6n in (.,0) and col5n>0) or (.<col7n<-5.00));
     row=4.03;
     exception=1;
     edc_table_s_="Table VC";
     keep edc_table_s_ row exception;
run;

proc sort data=dc_summary_vc nodupkey;
     by edc_table_s_ row;
run;
    
*- Append exceptions to master dataset -*;
data dc_summary;
     merge dc_summary dc_summary_vc;
     by edc_table_s_ row;
run;

*- Produce output -*;
%macro print_vc;
    %let _ftn=VC;
    %let _hdr=Table VC;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=print_vc split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2n col3n col4n col5n col6n col7n category 
                ("Records|________________________________________________" col2 col3 col4) 
                ("Distinct Codes|________________________________________________" col5 col6 col7) 
                col8;

         define pg       /order noprint;
         define category /display noprint;
         define col1     /display flow " " style(header)=[just=left cellwidth=14%] style(column)=[just=left];
         define col2     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col3     /display flow "Current|Refresh" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col4     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col5     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col6     /display flow "Current|Refresh" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col7     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=11.5%] style(column)=[just=center];
         define col8     /display flow "Source table" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col2n    /display noprint;
         define col3n    /display noprint;
         define col4n    /display noprint;
         define col5n    /display noprint;
         define col6n    /display noprint;
         define col7n    /display noprint;
         compute category;
            if category^="00" then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col4;
            if (col3n in (.,0) and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
         compute col7;
            if (col6n in (.,0) and col5n>0) or (.<col7n<-5.00) then do;
                col7=col7n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col7=col7n;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_vc;

*- Clear working directory -*;
%clean(savedsn=print: chart: expand:);

********************************************************************************;
*  DC SUMMARY OF EXCEPTIONS
********************************************************************************;

proc format;
     value no_except
     . = "---"
     ;
run;

proc sort data=dc_summary;
     by row edc_table_s_;
run;

data dc_summary;
     length exceptions $3 tblnum $10;
     set dc_summary;
     by row edc_table_s_;

     * for exception report *;
     tblnum=scan(edc_table_s_,2);

     * create color code variable *;
     if exception=1 then do;
        exceptions="Yes";
        if row in (1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.10, 1.11, 1.12, 1.13, 1.15, 1.18, 3.04, 3.05) then color = 1;
        else color = 2;
     end;
     else exceptions="No";
        
     if _n_<=18 then pg=1;
     else if _n_<=23 then pg=2;
     else if _n_<=30 then pg=3;
     else pg=4;
run;

*- Produce output -*;
%macro print_dcsummary;
    %let _ftn=DC Summary;
    %let _hdr=DC Summary;

    *- Get titles and footnotes -;
    %ttl_ftn;

    ods listing close;
    ods proclabel "&_ttl1";
    title1 justify=left "&_ttl1";
    title2 justify=left h=2.5 "&_ttl2";
    footnote1 justify=left "&_fnote";

    proc report data=dc_summary split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg data_check edc_table_s_ data_check_description category type color exceptions __of_datamarts_with_exceptions;
         where pg=1;
    
         define pg                /order order=data noprint;
         define data_check        /display flow "Data Check" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define edc_table_s_      /display flow "EDC Table" style(header)=[just=left cellwidth=7%] style(column)=[just=left];
         define data_check_description /display flow "Data Check Description" style(header)=[just=left cellwidth=42%] style(column)=[just=left];
         define category          /display flow "Category" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define type              /display flow "Type" style(header)=[just=left cellwidth=8%] style(column)=[just=left];
         define color             /display noprint;
         define exceptions        /display flow "Exception?" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         define __of_datamarts_with_exceptions /display flow format=no_except. "% DataMarts w/ exceptions" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         compute exceptions;
             if color=1 then call define(_col_, "style", "style=[color=red]");
             else if color=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;    

    *- Determine how many page in report -*;
    proc means data=dc_summary noprint;
         var pg;
         output out=_dc_summary_pg max=max_pg;
    run;
        
    data _null_;
         set _dc_summary_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=dc_summary split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column pg data_check edc_table_s_ data_check_description category type color exceptions __of_datamarts_with_exceptions;
             where pg=&_pg;
    
             define pg                /order order=data noprint;
             define data_check        /display flow "Data Check" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
             define edc_table_s_      /display flow "EDC Table" style(header)=[just=left cellwidth=7%] style(column)=[just=left];
             define data_check_description /display flow "Data Check Description" style(header)=[just=left cellwidth=42%] style(column)=[just=left];
             define category          /display flow "Category" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
             define type              /display flow "Type" style(header)=[just=left cellwidth=8%] style(column)=[just=left];
             define color             /display noprint;
             define exceptions        /display flow "Exception?" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
             define __of_datamarts_with_exceptions /display flow format=no_except. "% DataMarts w/ exceptions" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
             compute exceptions;
                 if color=1 then call define(_col_, "style", "style=[color=red]");
                 else if color=2 then call define(_col_, "style", "style=[color=blue]");
             endcomp;
        run;    

        ods listing;
    %end;
%mend print_dcsummary;

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;
********************************************************************************;

*******************************************************************************;
* Print each table sequentially
*******************************************************************************;

*- Section I -*;
%print_dcsummary;
%print_ia;
%print_ib;
%print_ic;
%print_id;
%print_ie;
%print_if;
%print_ig;
%print_ih;
%print_ii;
%chart_ia;
%chart_ib_cat(cat1=AV,cat2=TH,cat3=OA,c1=red,c2=blue,c3=green);
%chart_ib_cat(cat1=ED,cat2=EI,cat3=IP,c1=purple,c2=olive,c3=black,gcont=(continued));
%chart_ib_cat(cat1=IS,cat2=IC,cat3=OS,c1=bipb,c2=brown,c3=grey,gcont=(continued));
%chart_ic_cat(cat1=EI,cat2=IP,cat3=IS,c1=olive,c2=black,c3=bipb);
%chart_id;
%chart_ie;
%chart_if;
%chart_ig;
%chart_ih;
%chart_ii_cat(cat1=L,cat2=N,cat3=D,cat4=S,cat5=T,c1=red,c2=blue,c3=green,c4=purple,c5=olive);
%chart_ij;
%chart_ik;
%chart_il;

*- Section II -*;
%print_iia;
%print_iib;
%print_iic;
%print_iid;
%print_iie;
%print_iif;

*- Section III -*;
%print_iiia;
%print_iiib;
%print_iiic;
%print_iiid;
%print_iiie;
    * macro to prevent open code *;
    %macro _labprnt;
        %if &_ylab_result_cm>0 %then %do;
            %print_iiif;
        %end;
    %mend _labprnt;
    %_labprnt;

%print_iiig;
    * macro to prevent open code *;
    %macro etsprnt;
        %if %upcase(&_sasets)=YES %then %do;
            %chart_iii_cat(chart=A,tbl=encounter,cat1=AV,cat2=ED,cat3=EI,cat4=IP,c1=red,c2=blue,c3=green,c4=purple);
            %chart_iii_cat(chart=B,tbl=diagnosis,cat1=AV,cat2=ED,cat3=EI,cat4=IP,c1=red,c2=blue,c3=green,c4=purple);
            %chart_iii_cat(chart=C,tbl=procedures,cat1=AV,cat2=ED,cat3=EI,cat4=IP,c1=red,c2=blue,c3=green,c4=purple);
            %if %upcase(&exp_vital)=YES %then %do;
                %chart_iii_noncat(chart=D,tbl=vital,xlbl=MEASURE_DATE);
            %end;
            %if %upcase(&exp_prescribing)=YES %then %do;
                %chart_iii_noncat(chart=E,tbl=prescribing,xlbl=RX_ORDER_DATE);
            %end;
            %if %upcase(&exp_lab_result_cm)=YES %then %do;
                %chart_iii_noncat(chart=F,tbl=lab_result_cm,xlbl=RESULT_DATE);
            %end;
        %end;
    %mend etsprnt;
    %etsprnt;

*- Section IV -*;
%print_iva;
%print_ivb;
%print_ivc;
%print_ivd;
%print_ive;
%print_ivf;
%print_ivg;
%print_ivh;
%print_ivi;
%print_ivi_ref;
%print_ivj;

*- Section IV -*;
%print_va;
%print_vb;
%print_vc;

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;
