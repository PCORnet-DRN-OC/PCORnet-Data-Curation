/******************************************************************************
*  $Source: edc_report $;
*    $Date: 2018/11/09
*    Study: PCORnet
*
*  Purpose: Produce PCORnet EDC report v4.12
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
        formchar="|___|||___+=|_/\<>" missing=' ' /*mprint mlogic symbolgen*/ ls=max;
goptions reset=all dev=png300 rotate=landscape gsfmode=replace htext=0.9 
         ftext='Albany AMT' hsize=9 vsize=5.5;
ods html close;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname normdata "&qpath.dmlocal" access=readonly;;
libname prevdata "&ppath.dmlocal" access=readonly;;
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
        'Missing, NI, UN or OT'='09'
        'TOTAL'='10'
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

********************************************************************************;
* Create row variable for ease of merging with exceptions
********************************************************************************;
proc sort data=dc_summary(rename=(data_check_num=row));
     by edc_table_s_ row;
run;

data missingness;
     length col1 col2 $50;
     set missingness;
    
     col1=strip(table);
     col2=strip(field);

     keep col: q2;
run;

proc sort data=missingness;
     by col1 col2;
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
          save dc_normal toc headers footers chart_months dc_summary missingness
               required_structure q2_stat_dlg_loinc &savedsn / memtype=data;
          save formats / memtype=catalog;
     quit;
%mend clean;

********************************************************************************;
* Bring in NORMALIZED data and compress it
********************************************************************************;
data dc_normal(compress=yes);
     set normdata.&dmid._&r_daten._dc_norm;
run;
    options ls=150;/*
    proc print width=min data=dc_normal;
     where datamartid=%upcase("&dmid") and dc_name IN ("PRO_L3_PXTYPE_ENCTYPE" "DIA_L3_DXTYPE_ENCTYPE");
    endsas; and 
                   category in ("09" "10" "CH") and statistic in ("RECORD_N");
endsas;*/
********************************************************************************;
* TABLE OF CONTENTS report
********************************************************************************;
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
        variable in ("LOW_CELL_CNT" "QUERY_PACKAGE" "REFRESH_MAX" "RESPONSE_DATE" "LOOKBACK_DATE");
     
     call symput("r_date",put(today(),yymmdd10.));
     if variable="LOW_CELL_CNT" then call symput("low_cell",strip(resultc));
     else if variable="QUERY_PACKAGE" then call symput("qpackage",strip(resultc));
     else if variable="REFRESH_MAX" then call symput("rmax",strip(resultc));
     else if variable="RESPONSE_DATE" then call symput("q_date",strip(resultc));
     else if variable="LOOKBACK_DATE" then call symput("lb_date",strip(resultc));
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
title6 justify=left h=2.5 "Low Cell Count Threshold:  &low_cell";
title7 justify=left h=2.5 "Query Package:  &qpackage";
title8 justify=left h=2.5 "Lookback Date:  &lb_date";
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
     length category $50;
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
     %cat(dcn=DEM_L3_GENDERDIST,cat='GQ',ord=7,ord2=1,col1=%str(GQ (Genderqueer)))
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
         define col1     /display flow "" style(header)=[just=left cellwidth=40%] /*style(column)=[just=left]*/;
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
        1='Number of unique patients with at least 1 ED, EI, IP, or AV encounter within the past 5 years'
        2='Number of unique patients with at least 1 ED, EI, IP, or AV encounter within the past 1 year'
        3='Number of unique patients with at least 1 ED, EI, IP, or AV encounter and DIAGNOSIS and VITAL records within the past 5 years'
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
         define col2     /display flow "Metric Description" style(header)=[just=left cellwidth=45%] style(column)=[just=left];;
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
         define col2     /display flow "Result" style(header)=[just=center cellwidth=15%] style(column)=[just=center];;
         define col3     /display flow "%" style(header)=[just=left cellwidth=10%] style(column)=[just=left];;
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
run;

*- If non-zero results exists, produce chart -*;
%if &_c1data^= and &_c1data^=0 %then %do;

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
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_1a_anno;
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
        ;
run;

*- Subset and Re-categorize encounter type -*;
data data;
     set dc_normal(where=(datamartid=%upcase("&dmid") and scan(dc_name,3,'_')="N" and variable in 
                  ("PATID" "ENCOUNTERID" "ENROLLID" "DIAGNOSISID" "PROCEDURESID" "VITALID" "LAB_RESULT_CM_ID" "PRESCRIBINGID" "DISPENSINGID" 
                   "CONDITIONID" "DEATHCID" "PRO_CM_ID" "PROVIDERID" "MEDADMNID" "OBSCLINID" "OBSGENID")));
     if (table="DISPENSING" and variable="PRESCRIBINGID") or (table in ("DIAGNOSIS" "PROCEDURES") and variable="PROVIDERID") then delete;

     if variable not in ("PATID" "ENCOUNTERID") then variable="ID";

     if statistic in ("ALL_N" "NULL_N") then stat=1;
     else stat=2;

     if resultn=. then resultn=0;
     if table="TRIAL" then table="PCORNET_TRIAL";

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
        variable not in ("DISCHARGE_DATE" "ENR_END_DATE" "PX_DATE" "RESOLVE_DATE"
           "SPECIMEN_DATE" "LAB_ORDER_DATE" "RX_END_DATE" "RX_START_DATE" "ONSET_DATE"
           "MEDADMIN_STOP_DATE");
run;

*- Add number of records -*;
proc sort data=normdata.datamart_all(keep=memname nobs) 
     out=datamart_all(rename=(memname=table)) nodupkey;
     by memname;
run;

*- Bring everything together -*;
data print_id;
     length col1-col8 $50;
     merge stats(in=t where=(variable="ID" and stat=1 and table not in ("DEMOGRAPHIC" "DEATH" "ENCOUNTER")) rename=(sum=records))
           stats(in=t where=(variable="PATID" and stat=1 and table in ("DEMOGRAPHIC" "DEATH" "PCORNET_TRIAL")) rename=(sum=records))
           stats(in=t where=(variable="ENCOUNTERID" and stat=1 and table in ("ENCOUNTER")) rename=(sum=records))
           stats(in=p where=(variable="PATID" and stat=2) rename=(sum=patients))
           stats(in=e where=(variable="ENCOUNTERID" and stat=2) rename=(sum=encounters))
           cross(in=c where=(statistic="P5") rename=(resultc=p5))
           cross(in=c where=(statistic="P95") rename=(resultc=p95))
           datamart_all
     ;
     by table;
     if t or p or e or c;

     if variable in ("DISCHARGE_DATE" "ENR_END_DATE" "PX_DATE" "SPECIMEN_DATE" 
                     "LAB_ORDER_DATE" "RX_END_DATE" "RX_START_DATE" "ONSET_DATE"
                     "RESOLVE_DATE" "MEDADMIN_STOP_DATE") then delete;

     ord=input(table,rowfmt.);
     col1=table;
     if nobs^="BT" then nobsn=input(nobs,16.);
     else if nobs="BT" then nobsn=.t;
     if nobsn^=.t then col2=strip(put(nobsn,comma16.));
     else col2=strip(put(nobsn,threshold.));
     if records^=.t then col3=strip(put(records,comma16.));
     else col3=strip(put(records,threshold.));
     if p and patients^=.t then col4=strip(put(patients,comma16.));
     else if p then col4=strip(put(patients,threshold.));
     else col4="---";
     if encounters^=. then col5=strip(put(encounters,comma16.));
     else if e then col5=strip(put(.t,threshold.));
     else col5="---";
     if table^ in ('DEATH_CAUSE' 'PROVIDER' 'PCORNET_TRIAL') then do;
        col6=variable;
        if p5^=" " then col7=scan(p5,1,'_') || "_" || put(input(scan(p5,2,'_'),2.),mnth.);
        if p95^=" " then col8=scan(p95,1,'_') || "_" || put(input(scan(p95,2,'_'),2.),mnth.);
        col9=strip(trim(dc_name) || "; XTBL_L3_DATES");
     end;
     else if table in ('DEATH_CAUSE' 'PROVIDER' 'PCORNET_TRIAL') then do;
        col6="---";
        col7="---";
        col8="---";
        col9=strip(trim(dc_name));
     end;

     keep ord col:;
run;

proc sort data=print_id;
     by ord;
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
         column col1 ('All Data|___________' col2)
                ('Characterized Data|_____________________________________' col3 col4 col5)
                ('Characterized Data Range|______________________________________' col6 col7 col8) 
                col9;

         define col1          /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col2          /display flow "Records" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col3          /display flow "Records" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col4          /display flow "Patients" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col5          /display flow "Encounters" style(header)=[just=center cellwidth=8.8%] style(column)=[just=center];
         define col6          /display flow "Field name" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col7          /display flow "5th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col8          /display flow "95th Percentile" style(header)=[just=center cellwidth=7.5%] style(column)=[just=center];
         define col9          /display flow "Source Tables" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
    run;

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
%macro chart_ib_cat(cat1,cat2,cat3,cat4,cat5,c1,c2,c3,c4,c5,gcont);

%let _hdr=Chart IB;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Create formats -*;
proc format;
     value $cat
        'AV'='AV (Ambulatory Visit)'
        'ED'='ED (Emergency Dept)'
        'EI'='EI (ED to IP Stay)'
        'IC'='IC (Institutional Professional Consult)'
        'IP'='IP (Inpatient Hospital Stay)'
        'IS'='IS (Non-acute Institutional Stay)'
        'OA'='OA (Other Ambulatory Visit)'
        'OS'='OS (Observation Stay)'
        ;
run;

*- Subset and create a month/year variable as a SAS date value -*;
data data;
     format xdate date9.;
     set dc_normal(where=(datamartid=%upcase("&dmid") and statistic="RECORD_N" and
         dc_name="ENC_L3_ENCTYPE_ADATE_YM" and category in ("&cat1" "&cat2" "&cat3" "&cat4" "&cat5")
         and cross_category^="NULL or missing"));

     if resultn=.t then resultn=0;

     xdate=mdy(input(scan(cross_category,2,'_'),2.),1,input(scan(cross_category,1,'_'),4.));
    
     keep category xdate resultn;
run;

proc sort data=data;
     by category xdate;
run;

data chart_months_enc;
     length category $50;
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
         call symput('_c2data',compress(put(totalsum,16.)));
     end;
run;

*- If non-zero results exists, produce chart -*;
%macro c2data;
%if &_c2data^= and &_c2data^=0 %then %do;

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
    data chart_ib_anno;
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
    symbol4  c=&c4 line=1 v=NONE interpol=join;
    symbol5  c=&c5 line=1 v=NONE interpol=join;

    proc gplot data=chart_ib;
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid annotate=chart_ib_anno;
         format category $cat. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c2data;
%c2data;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

%mend chart_ib_cat;

********************************************************************************;
* CHART IC TREND IN INSTITUTIONAL ENCOUNTERS BY DISCHARGE DATE AND ENCOUNTER TYPE
********************************************************************************;
%macro chart_ic_cat(cat1,cat2,cat3,c1,c2,c3);

%let _hdr=Chart IC;
%let _ftn=Chart;

*- Get titles and footnotes -;
%ttl_ftn;

*- Create formats -*;
proc format;
     value $cat
        'EI'='EI (ED to IP Stay)'
        'IP'='IP (Inpatient Hospital Stay)'
        'IS'='IS (Non-acute Institutional Stay)'
        ;
run;

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
     length category $50;
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
     end;
run;

*- If non-zero results exists, produce chart -*;
%macro c3data;
%if &_c3data^= and &_c3data^=0 %then %do;

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
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid annotate=chart_ic_anno;
         format category $cat. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c3data;
%c3data;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

%mend chart_ic_cat;

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
     length col1 col2 col3 col4 $50;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="XTBL_L3_METADATA"
         and variable in ("BIRTH_DATE_MGMT" "ENR_START_DATE_MGMT" "ENR_END_DATE_MGMT"
         "ADMIT_DATE_MGMT" "DISCHARGE_DATE_MGMT" "PX_DATE_MGMT" "MEASURE_DATE_MGMT"
         "RX_ORDER_DATE_MGMT" "RX_START_DATE_MGMT" "RX_END_DATE_MGMT" "DISPENSE_DATE_MGMT"
         "LAB_ORDER_DATE_MGMT" "SPECIMEN_DATE_MGMT" "RESULT_DATE_MGMT"
         "ONSET_DATE_MGMT" "REPORT_DATE_MGMT" "RESOLVE_DATE_MGMT" "PRO_DATE_MGMT"
         "DEATH_DATE_MGMT" "MEDADMIN_START_DATE_MGMT" "MEDADMIN_STOP_DATE_MGMT"
         "OBSCLIN_DATE_MGMT" "OBSGEN_DATE_MGMT")));

     col1="HARVEST";
     col2=variable;
     col3=put(resultc,$dmgmt.);
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
    footnote " ";

    proc report data=print_if split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 col3 col4;

         define col1     /display flow "Table" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=28%] style(column)=[just=left];
         define col3     /display flow "DATE_MGMT" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=25%] style(column)=[just=left];
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
                and statistic in ("RECORD_N" "RECORD_PCT" "DISTINCT_PATID_N" "INCLUDE_EDC")
                and category^='Unassigned'));

     ord=upcase(category);

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

*- Bring everything together and restrict to those parameters to be included in the edc -*;
data print_ig;
     length col1-col6 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     merge stats(where=(statistic in ("RECORD_N")) rename=(sum=record))
           stats(where=(statistic in ("RECORD_PCT")) rename=(sum=pct))
           stats(where=(statistic in ("DISTINCT_PATID_N")) rename=(sum=dist_patid))
           include_edc(in=i)
     ;
     by ord;
     if i;

     * track BT values but set missing to zero *;
     if record=.t then bt_record="*";
     if record=. then record=0;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;
    
     * retain and count for total row *;
     retain tot_record tot_dist_patid 0;
     tot_record=tot_record+record;
     tot_dist_patid=tot_dist_patid+dist_patid;
    
     * create column variables and output *;
     col1=strip(category);
     if bt_record="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(record,comma16.));
     col3=strip(put(pct,5.1));
     if bt_dist_patid="*" then col4=strip(put(.t,threshold.));
     else col4=strip(put(dist_patid,comma16.));
     if dist_patid>0 and denom>0 then col5=strip(put((dist_patid/denom)*100,5.1));
     col6="LAB_L3_DCGROUP;ENC_L3_N";
    
     keep ord col:;
run;

data print_ig;
     set print_ig;
     by ord;
    
     if _n_<=23 then pg=1;
     else if 24<=_n_<=46 then pg=2;
     else pg=3;
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
         column pg col1 col2 col3 col4 col5 col6;
         where pg=1;

         define pg       /order noprint;
         define col1     /display flow "DC_LAB_GROUP" style(header)=[just=left cellwidth=23%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col3     /display flow "Percentage of|records in the|LAB_RESULT_CM table|with a LAB_LOINC code" style(header)=[just=center cellwidth=17%] style(column)=[just=center];
         define col4     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] style(column)=[just=center];
         define col6     /display flow "Source tables" style(header)=[just=left cellwidth=20%] style(column)=[just=left];
    run;

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ig split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column pg col1 col2 col3 col4 col5 col6;
         where pg>1;

         define pg       /order noprint;
         define col1     /display flow "DC_LAB_GROUP" style(header)=[just=left cellwidth=23%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col3     /display flow "Percentage of|records in the|LAB_RESULT_CM table|with a LAB_LOINC code" style(header)=[just=center cellwidth=17%] style(column)=[just=center];
         define col4     /display flow "Patients" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /display flow "Percentage of|patients in the ENCOUNTER table" style(header)=[just=center cellwidth=17%] style(column)=[just=center];
         define col6     /display flow "Source tables" style(header)=[just=left cellwidth=20%] style(column)=[just=left];

         break after pg / page;
    run;

    ods listing;
%mend print_ig;

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
         end;
    run;

    *- If non-zero results exists, produce chart -*;
    %macro c4data;
    %if &_c4data^= and &_c4data^=0 %then %do;

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
             plot deviations*count / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_id_anno;
             format count chart.;
             where count^=. and deviations^=.;
        run;
        quit;
    %end;
    %mend c4data;
    %c4data;
%end;    

*- Clear working directory -*;
%clean(savedsn=print: chart:);

%mend chart_id;

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
run;

*- If non-zero results exists, produce chart -*;
%if &_c5data^= and &_c5data^=0 %then %do;

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
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_ie_anno;
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
run;

*- If non-zero results exists, produce chart -*;
%if &_c6data^= and &_c6data^=0 %then %do;

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
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_if_anno;
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
run;

*- If non-zero results exists, produce chart -*;
%if &_c7data^= and &_c7data^=0 %then %do;

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
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_ig_anno;
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
run;

*- If non-zero results exists, produce chart -*;
%if &_c8data^= and &_c8data^=0 %then %do;

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
         plot deviations*start / haxis=axis1 vaxis=axis2 nolegend vref=0 grid annotate=chart_ih_anno;
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
     value $cat
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
     length category $50;
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

         if mean>0 then text=strip(put(category,$cat.)) || " COV = " || compress(put((std/mean),16.3)) || "; Mean = " || 
                 compress(put(mean,16.2)) || "; SD = " || compress(put(std,16.2));
         else text=strip(put(category,$cat.)) || " COV = not calculable";

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
         plot deviations*count=category / haxis=axis1 vaxis=axis2 legend=legend1 vref=0 grid annotate=chart_ii_anno;
         format category $cat. count chart.;
         where count^=. and deviations^=.;
    run;
    quit;
%end;
%mend c9data;
%c9data;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

%mend chart_ii_cat;

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
    footnote " ";

    proc report data=print_iia split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column col1 col2 col3 col4;

         define col1     /display flow "Table" style(header)=[just=left cellwidth=17%] style(column)=[just=left];
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
     %stat(dcn=ENR_L3_BASEDIST,ord=2,ord2=1,var=ENR_BASIS)
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
     %stat(dcn=LAB_L3_NAME,ord=10,ord2=1,var=LAB_NAME)
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
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=10,var=RX_ORDER_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=11,var=RX_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=12,var=RX_END_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=13,var=DISPENSE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=14,var=LAB_ORDER_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=15,var=SPECIMEN_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=16,var=RESULT_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=17,var=MEASURE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=18,var=ONSET_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=19,var=REPORT_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=20,var=RESOLVE_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=21,var=PRO_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=22,var=DEATH_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=23,var=MEDADMIN_START_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=24,var=MEDADMIN_STOP_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=25,var=OBSCLIN_DATE_MGMT)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=26,var=OBSGEN_DATE_MGMT)
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
     %stat(dcn=OBSGEN_L3_MOD,ord=19,ord2=1,var=OBSGEN_RESULT_MODIFIER)
     %stat(dcn=OBSGEN_L3_TMOD,ord=19,ord2=2,var=OBSGEN_TABLE_MODIFIED)
     %stat(dcn=OBSGEN_L3_QUAL,ord=19,ord2=3,var=OBSGEN_RESULT_QUAL)
     %stat(dcn=OBSGEN_L3_RUNIT,ord=19,ord2=4,var=OBSGEN_RESULT_UNIT)
     %stat(dcn=OBSGEN_L3_TYPE,ord=19,ord2=5,var=OBSGEN_TYPE)
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
     else if col1 in ("HARVEST") then pg=3;
     else pg=4;
run;

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
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col3     /computed flow "Number of records with values|outside of specifications" style(header)=[just=center cellwidth=27%] 
                                        style(column)=[just=center];
         define col4     /display flow "Source table" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
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
    run;;

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
             define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
             define col3     /computed flow "Number of records with values|outside of specifications" style(header)=[just=center cellwidth=27%]
                                            style(column)=[just=center];
             define col4     /display flow "Source table" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
    
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
        if resultc^=" " then col3="0";
        else col3="1";
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
             and statistic="RECORD_N" and category in ("NULL or missing" "NI" "UN" "OT");
    run;
    
    data mult&ord(keep=ord: col:);
         length col1-col4 $50;
         set mult&ord;
             ord=&ord;
             ord2=&ord2;
             col1=strip(put(ord,tbl_row.));
             col2=strip("&var");
             col3=strip(put(sum,comma16.));
             col4=strip(dc_name);
    run;

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
    
    data cross&ord(keep=ord: col:);
         length col1-col4 $50;
         set cross&ord;
             ord=&ord;
             ord2=&ord2;
             col1=strip(put(ord,tbl_row.));
             col2=strip("&var");
             col3=strip(put(sum,comma16.));
             col4=strip(dc_name);
    run;

    proc append base=cross data=cross&ord;
    run;

%mend cross;

*- Non-cross table queries -*;
data tbl(keep=ord: col:);
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
     %stat(dcn=XTBL_L3_DATES,ord=18,ord2=3,var=OBSCLIN_DATE,stat=NMISS)
     %stat(dcn=OBSGEN_L3_N,ord=19,ord2=1,var=PATID,stat=NULL_N)
     %stat(dcn=OBSGEN_L3_N,ord=19,ord2=2,var=OBSGENID,stat=NULL_N)
     %stat(dcn=XTBL_L3_DATES,ord=19,ord2=3,var=OBSGEN_DATE,stat=NMISS)
run;

proc sort data=tbl;
     by ord ord2;
run;

*- Cross table queries -*;
%cross(dcn=DIA_L3_DXTYPE_ENCTYPE,ord=5,ord2=4,var=DX_TYPE)
%cross(dcn=PRO_L3_PXTYPE_ENCTYPE,ord=6,ord2=4,var=PX_TYPE)

proc sort data=cross;
     by ord ord2;
run;

*- Bring everything together -*;
data print_iic;
     set tbl cross;
     by ord ord2;
     if col1 in ("DEMOGRAPHIC" "ENROLLMENT" "DEATH" "ENCOUNTER" "DIAGNOSIS"
                  "PROCEDURES" "VITAL" "PRESCRIBING") then pg=1;
     else if col1 in ("DISPENSING" "LAB_RESULT_CM" "HARVEST" "CONDITION" 
                  "DEATH_CAUSE" "PRO_CM" "PCORNET_TRIAL") then pg=2;
     else pg=3;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iic;
     length edc_table_s_ $10;
     set print_iic(keep=col3);
     if col3^="0";
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
    run;;

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
        other=" "
        ;

     value dcheck
        1 = "1.01"
        2 = "1.02"
        3,4 = "1.03"
        5,6,7 = "1.04"
        ;

     value $dcdesc
        '1.01'='Required tables are not present'
        '1.02'='Expected tables are not populated'
        '1.03'='Required fields are not present'
        '1.04'='Required fields do not conform to data model specifications for data type, length, or name'
        ;

    value except
        1='Required table is not present.  All tables must be present in an instantiation of the CDM'
        2='Table expected to be populated (DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, and HARVEST) is not populated'
        3='Required numeric field is not present'
        4='Required character field is not present'
        5='Required character field is numeric'
        6='Required numeric field is character'
        7='Required field is present but of unexpected length'
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

*- Remove records from field comparison if no comparative table/obs present -*;
data compare_var;
     merge compare_var(in=dm)
           compare_tbl_var(in=mdsn where=(miss_tablen=1))
     ;
     by ord memname;
     if mdsn then delete;
     drop miss_tablen;
run;

proc sort data=compare_var;
     by row subrow;
run;

data table;
     do row = 1 to 7;
        output;
     end;
run;

data final;
     length col1-col6 $200;
     if _n_=1 then set compare_tbl;
     merge table compare_var;
     by row;
    
     col1=put(row,dcheck.);
     col2=put(col1,$dcdesc.);
     col3=put(row,except.);

     if row not in (1,2) then do;
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
     end;
     else if row in (1,2) then col5="n/a";

     col6="DATAMART_ALL";
run;

data print_iid;
     set final;
     by row;

     if col4 not in ("None" "n/a") then c4=1;
     if col5 not in ("None" "n/a") then c5=1;

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
        other=" "
        ;

     value dcheck
        8.01 = "1.08"
        9.01 = "1.09"
       10.01 = "1.10"
       11.01 = "1.11"
       12.01 = "1.12"
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
        other = ' '
        ;

    value except
         8.01='Orphan PATID(S) in the'
         8.02='CONDITION, DIAGNOSIS, DEATH,'
         8.03='DEATH_CAUSE, DISPENSING,'
         8.04='ENCOUNTER, ENROLLMENT,'
         8.05='LAB_RESULT_CM, MED_ADMIN,'
         8.06='OBS_CLIN, OBS_GEN,'
         8.07='PCORNET_TRIAL, PRESCRIBING,'
         8.08='PROCEDURES, PRO_CM, or'
         8.09='VITAL table'
         9.01='Orphan ENCOUNTERID(S) in the'
         9.02='CONDITION, DIAGNOSIS,'
         9.03='LAB_RESULT_CM, MED_ADMIN,'
         9.04='OBS_CLIN, OBS_GEN,'
         9.05='PRESCRIBING, PROCEDURES,'
         9.06='PRO_CM, or VITAL table'
        10.01='Replication error(s) in ENC_TYPE'
        10.02='or ADMIT_DATE in the DIAGNOSIS'
        10.03='or PROCEDURES table'
        11.01='An ENCOUNTERID in the'
        11.02='CONDITION, DIAGNOSIS,'
        11.03='ENCOUNTER, LAB_RESULT_CM,'
        11.04='MED_ADMIN, OBS_CLIN,'
        11.05='OBS_GEN, PRESCRIBING,'
        11.06='PROCEDURES, PRO_CM, or VITAL'
        11.07='table is associated with more'
        11.08='than 1 PATID in the same table'
        12.01='Orphan PROVIDERID(S) in the'
        12.02='ENCOUNTER, DIAGNOSIS,'
        12.03='MED_ADMIN, OBS_CLIN,'
        12.04='OBS_GEN, PRESCRIBING, or'
        12.05='PROCEDURES table.'
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
         8.13='MEDADM_L3_N'
         9.01='XTBL_L3_MISMATCH;'
         9.02='DIA_L3_N; PRO_L3_N;'
         9.03='VIT_L3_N; PRES_L3_N;'
         9.04='LAB_L3_N;'
         9.05='PROCM_L3_N;'
         9.06='COND_L3_N;'
         9.07='OBSCLIN_L3_N;'
         9.08='OBSGEN_L3_N;'
         9.09='MEDADM_L3_N'
        10.01='XTBL_L3_MISMATCH'
        11.01='XTBL_L3_NON_UNIQUE;'
        11.02='COND_L3_N; DIA_L3_N;'
        11.03='ENC_L3_N: LAB_L3_N;'
        11.04='PRES_L3_N; PRO_L3_N;'
        11.05='VIT_L3_N;'
        11.06='PROCM_L3_N;'
        11.07='OBSCLIN_L3_N;'
        11.08='OBSGEN_L3_N;'
        11.09='MEDADM_L3_N'
        12.01='XTBL_L3_MISMATCH;'
        12.02='DIA_L3_N; ENC_L3_N;'
        12.03='MEDADM_L3_N;'
        12.04='OBSCLIN_L3_N;'
        12.05='OBSGEN_L3_N;'
        12.06='PRES_L3_N; PRO_L3_N'
        other = ' '
        ;
run;

*- Create denominators -*;
data denom;
     merge dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="VIT_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_vit))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="LAB_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_lab))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PROCM_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_procm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="COND_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_cond))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="ENCOUNTERID" and statistic="DISTINCT_N") rename=(resultn=enc_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_enc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENR_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_enr))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="VIT_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_vit))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="LAB_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_lab))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DISP_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_disp))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DEATH_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_death))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DEATHC_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_deathc))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="COND_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_cond))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PROCM_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_procm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="TRIAL_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_trial))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="PATID" and statistic="DISTINCT_N") rename=(resultn=pat_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="MEDADM_L3_N" and variable="MEDADMIN_PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_medadm))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSCLIN_L3_N" and variable="OBSCLIN_PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_obsclin))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="OBSGEN_L3_N" and variable="OBSGEN_PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_obsgen))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRES_L3_N" and variable="RX_PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_pres))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_dia))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="PRO_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_pro))
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and variable="PROVIDERID" and statistic="DISTINCT_N") rename=(resultn=prov_enc))
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
     else if scan(variable,1,' ')="PROVIDERID" then row=12;
     else if scan(variable,1,' ')="ENCOUNTERID" then row=9;
     else if scan(variable,1,' ')="PATID" then row=8;
     else if scan(variable,2,' ')="Mismatch" then row=10;
    
     if resultn=0 or nonunique=0 or 
        (row=9 and scan(table,3,' ')^ in ("DIAGNOSIS" "PROCEDURES" "VITAL" "PRESCRIBING" "LAB_RESULT_CM" "CONDITION" "PRO_CM" "OBS_CLIN" "OBS_GEN" "MED_ADMIN")) then delete;

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

data table;
     do row = 8;
        do subrow = 1 to 13;
            output;
        end;
     end;
     do row = 9;
        do subrow = 1 to 8;
            output;
        end;
     end;
     do row = 10;
        do subrow = 1 to 3;
            output;
        end;
     end;
     do row = 11;
        do subrow = 1 to 8;
            output;
        end;
     end;
     do row = 12;
        do subrow = 1 to 6;
            output;
        end;
     end;
run;

data final;
     length col1-col8 $200;
     merge table(in=t) orphan(in=o);
     by row subrow;

     row_dec=row+subrow/100;

     col1=put(row_dec,dcheck.);
     col2=put(row_dec,dcdesc.);
     col3=put(row_dec,except.);

     if table^=" " and row in (8,9,10,12) then col4=scan(table,3,' ');
     else if table^=" " and row=11 then col4=strip(table);
     else if o then col4="None";

     col5=scan(variable,1,' ');

     if row in (8,9,10,12) then do;
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
     end;
     else if row=10 then do;
        col7="n/a";
     end;
     else if row=11 and nonunique>=0  then do;
        if col4="CONDITION" and pat_cond>0 then col7=strip(put((nonunique/pat_cond)*100,16.1));
        else if col4="DIAGNOSIS" and pat_dia>0 then col7=strip(put((nonunique/pat_dia)*100,16.1));
        else if col4="ENCOUNTER" and pat_enc>0 then col7=strip(put((nonunique/pat_enc)*100,16.1));
        else if col4="LAB_RESULT_CM" and pat_lab>0 then col7=strip(put((nonunique/pat_lab)*100,16.1));
        else if col4="PRESCRIBING" and pat_pres>0 then col7=strip(put((nonunique/pat_pres)*100,16.1));
        else if col4="PROCEDURES" and pat_pro>0 then col7=strip(put((nonunique/pat_pro)*100,16.1));
        else if col4="VITAL" and pat_vit>0 then col7=strip(put((nonunique/pat_vit)*100,16.1));
        else if col4="PRO_CM" and pat_procm>0 then col7=strip(put((nonunique/pat_procm)*100,16.1));
        else if col4="MED_ADMIN" and pat_medadm>0 then col7=strip(put((nonunique/pat_medadm)*100,16.1));
        else if col4="OBS_CLIN" and pat_obsclin>0 then col7=strip(put((nonunique/pat_obsclin)*100,16.1));
        else if col4="OBS_GEN" and pat_obsgen>0 then col7=strip(put((nonunique/pat_obsgen)*100,16.1));
     end;
     else if row=12 and resultn>=0 then do;
        if col4="DIAGNOSIS" and prov_dia>0 then col7=strip(put((resultn/prov_dia)*100,16.1));
        else if col4="ENCOUNTER" and prov_enc>0 then col7=strip(put((resultn/prov_enc)*100,16.1));
        else if col4="PROCEDURES" and prov_pro>0 then col7=strip(put((resultn/prov_pro)*100,16.1));
        else if col4="PRESCRIBING" and prov_pres>0 then col7=strip(put((resultn/prov_pres)*100,16.1));
        else if col4="MED_ADMIN" and prov_medadm>0 then col7=strip(put((resultn/prov_medadm)*100,16.1));
        else if col4="OBS_CLIN" and prov_obsclin>0 then col7=strip(put((resultn/prov_obsclin)*100,16.1));
        else if col4="OBS_GEN" and prov_obsgen>0 then col7=strip(put((resultn/prov_obsgen)*100,16.1));
     end;

     col8=put(row_dec,source.);
run;

data print_iie;
     set final;
     by row subrow;

     if row in (8,9,10,12) then do;
         if col6n>=.t then c6=1;
         else c6=.;
     end;
     else if row in (11) then do;
         if input(col7,16.1)>5.0 then c7=2;
         else c7=.;
     end;

     * count records for page break *;
     retain pg 1 cnt 0;
     cnt=cnt+1;
     if last.row then cnt=cnt+1;
     if (first.row and cnt>19) or cnt>22 then do;
        pg=pg+1;
        cnt=2;
     end;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iie;     
     length edc_table_s_ $10;
     set print_iie(keep=c6 c7 row_dec);
     if c6^=. or c7^=.;
     row=1+(int(row_dec)/100);
     exception=1;
     edc_table_s_="Table IIE";
     keep edc_table_s_ row exception;
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
            if c6=1 then call define(_col_, "style", "style=[color=red]");
         endcomp;
         compute col7;
            if c7=2 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    *- Determine how many page in report -*;
    proc means data=print_iie noprint;
         var pg;
         output out=_iie_pg max=max_pg;
    run;
        
    data _null_;
         set _iie_pg;
         call symputx("_nopg",compress(put(max_pg,8.)));
    run;;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_iie split='|' style(header)=[backgroundcolor=CXCCCCCC] missing;
             column pg row row_dec col1 col2 col3 c6 c7 col4 col5 col6 col7 col8;
             where pg=&_pg;

             define pg       /order order=data noprint;
             define row      /order order=data noprint;
             define row_dec  /order order=data noprint;
             define col1     /display flow "Data|Check" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col2     /display flow "Data Check Description" style(header)=[just=left cellwidth=16%] style(column)=[just=left];
             define col3     /display flow "Exception" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
             define c6       /display noprint;
             define c7       /display noprint;
             define col4     /display flow "Table(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
             define col5     /display flow "Field(s)" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
             define col6     /display flow "Count" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col7     /display flow "%" style(header)=[just=left cellwidth=6%] style(column)=[just=left];
             define col8     /display flow "Source table(s)" style(header)=[just=left cellwidth=16.25%] style(column)=[just=left];

             compute col6;
                if c6=1 then call define(_col_, "style", "style=[color=red]");
             endcomp;
             compute col7;
                if c7=2 then call define(_col_, "style", "style=[color=blue]");
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

data print_iif;
     set normdata.code_summary;
run;

*- Keep records resulting in an exception -*;
data dc_summary_iif;
     length edc_table_s_ $10;
     set print_iif(keep=pct);
     if pct>5.00;
     row=1.13;
     exception=1;
     edc_table_s_="Table IIF";
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
    footnote " ";

    proc report data=print_iif split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column table code_type bad_records records pct;
    
         define table        /display flow "Table" style(header)=[just=left cellwidth=24%] style(column)=[just=left];
         define code_type    /display flow "Code Type" style(header)=[just=center cellwidth=18%] style(column)=[just=center];
         define bad_records  /display flow "Bad Records" style(header)=[just=center cellwidth=18%] style(column)=[just=center];
         define records      /display flow "Records" style(header)=[just=center cellwidth=18%] style(column)=[just=center];
         define pct          /display flow "Percent" style(header)=[just=center cellwidth=18%] style(column)=[just=center];
         compute pct;
             if pct>5.0 then call define(_col_, "style", "style=[color=red]");
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
     %stat(dcn=XTBL_L3_DATES,ord=6,ord2=8,tbl=PROCEDURES,var=ADMIT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=6,ord2=9,tbl=PROCEDURES,var=PX_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=7,ord2=10,tbl=VITAL,var=MEASURE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=11,tbl=PRESCRIBING,var=RX_ORDER_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=12,tbl=PRESCRIBING,var=RX_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=8,ord2=13,tbl=PRESCRIBING,var=RX_END_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=9,ord2=14,tbl=DISPENSING,var=DISPENSE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=15,tbl=LAB_RESULT_CM,var=LAB_ORDER_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=16,tbl=LAB_RESULT_CM,var=SPECIMEN_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=10,ord2=17,tbl=LAB_RESULT_CM,var=RESULT_DATE,stat=FUTURE_DT_N)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=18,var=REFRESH_DEMOGRAPHIC_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=19,var=REFRESH_ENROLLMENT_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=20,var=REFRESH_ENCOUNTER_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=21,var=REFRESH_DIAGNOSIS_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=22,var=REFRESH_PROCEDURES_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=23,var=REFRESH_VITAL_DATE)
     %harvest(dcn=XTBL_L3_METADATA,ord=11,ord2=24,var=REFRESH_MAX)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=25,tbl=CONDITION,var=REPORT_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=26,tbl=CONDITION,var=RESOLVE_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=12,ord2=27,tbl=CONDITION,var=ONSET_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=14,ord2=28,tbl=PRO_CM,var=PRO_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=17,ord2=29,tbl=MED_ADMIN,var=MEDADMIN_START_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=17,ord2=30,tbl=MED_ADMIN,var=MEDADMIN_STOP_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=18,ord2=31,tbl=OBS_CLIN,var=OBSCLIN_DATE,stat=FUTURE_DT_N)
     %stat(dcn=XTBL_L3_DATES,ord=19,ord2=32,tbl=OBS_GEN,var=OBSGEN_DATE,stat=FUTURE_DT_N)
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
     %denom(dcn=XTBL_L3_DATES,ord=6,ord2=8,tbl=PROCEDURES,var=ADMIT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=6,ord2=9,tbl=PROCEDURES,var=PX_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=7,ord2=10,tbl=VITAL,var=MEASURE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=11,tbl=PRESCRIBING,var=RX_ORDER_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=12,tbl=PRESCRIBING,var=RX_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=8,ord2=13,tbl=PRESCRIBING,var=RX_END_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=9,ord2=14,tbl=DISPENSING,var=DISPENSE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=15,tbl=LAB_RESULT_CM,var=LAB_ORDER_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=16,tbl=LAB_RESULT_CM,var=SPECIMEN_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=10,ord2=17,tbl=LAB_RESULT_CM,var=RESULT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=25,tbl=CONDITION,var=REPORT_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=26,tbl=CONDITION,var=RESOLVE_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=12,ord2=27,tbl=CONDITION,var=ONSET_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=14,ord2=28,tbl=PRO_CM,var=PRO_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=17,ord2=29,tbl=MED_ADMIN,var=MEDADMIN_START_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=17,ord2=30,tbl=MED_ADMIN,var=MEDADMIN_STOP_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=18,ord2=31,tbl=OBS_CLIN,var=OBSCLIN_DATE)
     %denom(dcn=XTBL_L3_DATES,ord=19,ord2=32,tbl=OBS_GEN,var=OBSGEN_DATE)
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

     if col1^ in ("CONDITION" "PRO_CM" "MED_ADMIN" "OBS_CLIN" "OBS_GEN")
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
         define col1     /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col3     /computed format=thresholdc. "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "%" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=32%] style(column)=[just=left];
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
         define col1     /display flow "Table" style(header)=[just=left cellwidth=12%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col3     /computed format=thresholdc. "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col5     /display flow "%" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table(s)" style(header)=[just=left cellwidth=32%] style(column)=[just=left];
        
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
        'MEASURE_DATE < BIRTH_DATE'=4
        'DISPENSE_DATE < BIRTH_DATE'=5
        'RX_START_DATE < BIRTH_DATE'=6
        'RESULT_DATE < BIRTH_DATE'=7
        'DEATH_DATE < BIRTH_DATE'=8
        'MEDADMIN_START_DATE < BIRTH_DATE'=9
        'OBSCLIN_DATE < BIRTH_DATE'=10
        'OBSGEN_DATE < BIRTH_DATE'=11

        'ADMIT_DATE > DEATH_DATE'=12
        'DISCHARGE_DATE > DEATH_DATE'=13
        'PX_DATE > DEATH_DATE'=14
        'MEASURE_DATE > DEATH_DATE'=15
        'DISPENSE_DATE > DEATH_DATE'=16
        'RX_START_DATE > DEATH_DATE'=17
        'RESULT_DATE > DEATH_DATE'=18
        'MEDADMIN_START_DATE > DEATH_DATE'=19
        'OBSCLIN_DATE > DEATH_DATE'=20
        'OBSGEN_DATE > DEATH_DATE'=21
        'ADMIT_DATE > DISCHARGE_DATE'=22
        'PX_DATE < ADMIT_DATE'=23
;

*- Bring everything together -*;
data print_iiic;
     length col1 col2 col4 $50;
     if _n_=1 then set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="ENC_L3_N" and 
                       variable="PATID" and statistic="DISTINCT_N") rename=(resultn=denom));
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("XTBL_L3_DATE_LOGIC")
           and statistic in ("DISTINCT_PATID_N")) rename=(resultn=dist_patid));

     ord=input(category,rowfmt.);

     * track BT values but set missing to zero *;
     if dist_patid=.t then bt_dist_patid="*";
     if dist_patid=. then dist_patid=0;

     * create column variables and output *;
     col1=strip(category);
     if bt_dist_patid="*" then col2=strip(put(.t,threshold.));
     else col2=strip(put(dist_patid,comma16.));
     if dist_patid>.t and denom>0 then col3p=(dist_patid/denom)*100;
     col4="XTBL_L3_DATE_LOGIC;ENC_L3_N";

     keep ord col: ;
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
         column col3p col1 col2 col3 col4;

         define col3p    /display noprint;
         define col1     /display flow "DATE_COMPARISON" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         define col2     /display flow "Patients" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /computed flow format=5.1 "Percentage of|total patients in|the ENCOUNTER table" style(header)=[just=center cellwidth=27%] style(column)=[just=center];
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
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("LAB_L3_LOINC_SOURCE")
         and statistic in ("RECORD_N") and category not in ("NI" "UN" "OT" "NULL or missing")));

     if cross_category in ('BLD' 'BLD.DOT' 'BLD_TISS' 'BLD_TISS^DONOR' 'BLDA' 'BLDC' 
                           'BLDCO' 'BLDCOA' 'BLDCOV' 'BLDMV' 'BLDP' 'BLDV' 'BLD^DONOR'
                           'BLD^FETUS' 'RBC' 'RBC^DONOR' 'WBC' 'SER_PLAS' 'SER_PLAS_BLD'
                           'SER_PLAS^DONOR' 'SER_PLAS.ULTRACENTRIFUGATE' 'SER' 'SER^DONOR'
                           'PLAS' 'PPP' 'PPP_BLD' 'PPP^FETUS') then do;
        source_cat=1;
        if cross_category2 in ('BLD' 'BLD.DOT' 'BLD_TISS' 'BLD_TISS^DONOR' 'BLDA' 'BLDC' 
                           'BLDCO' 'BLDCOA' 'BLDCOV' 'BLDMV' 'BLDP' 'BLDV' 'BLD^DONOR'
                           'BLD^FETUS' 'RBC' 'RBC^DONOR' 'WBC' 'SER_PLAS' 'SER_PLAS_BLD'
                           'SER_PLAS^DONOR' 'SER_PLAS.ULTRACENTRIFUGATE' 'SER' 'SER^DONOR'
                           'PLAS' 'PPP' 'PPP_BLD' 'PPP^FETUS') then exp_cat=1;
        else exp_cat=2;
     end;
     else if substr(cross_category,1,3) in ("CSF") then do;
        source_cat=2;
        if substr(cross_category2,1,3) in ("CSF") then exp_cat=1;
        else exp_cat=2;
     end;
     else if cross_category in ("URINE" "URINE_SED") then do;
        source_cat=3;
        if cross_category2 in ("URINE" "URINE_SED") then exp_cat=1;
        else exp_cat=2;
     end;
     else if cross_category in ('AMNIO_FLD' 'BODY_FLD' 'GAST_FLD' 'PERICARD_FLD' 'PERITON_FLD'
                                'PLR_FLD' 'SALIVA' 'SEMEN' 'SPUTUM' 'SWEAT' 'SYNV_FLD') then do;
        source_cat=4;
        if cross_category2 in ('AMNIO_FLD' 'BODY_FLD' 'GAST_FLD' 'PERICARD_FLD' 'PERITON_FLD'
                                'PLR_FLD' 'SALIVA' 'SEMEN' 'SPUTUM' 'SWEAT' 'SYNV_FLD') then exp_cat=1;
        else exp_cat=2;
     end;
     else if cross_category in ('STOOL') then do;
        source_cat=5;
        if cross_category2 in ('STOOL') then exp_cat=1;
        else exp_cat=2;
     end;
     else if cross_category in ('CVX' 'CVX_VAG') then do;
        source_cat=6;
        if cross_category2 in ('CVX' 'CVX_VAG') then exp_cat=1;
        else exp_cat=2;
     end;
     else if cross_category in ('TISS' 'TISS.FNA') then do;
        source_cat=7;
        if cross_category2 in ('TISS' 'TISS.FNA') then exp_cat=1;
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
%macro _quant;

*- Get appropriate quantitative results -*;
proc sort data=q2_stat_dlg_loinc out=_quant_loinc;
     by lab_loinc;
     %if &dmid=C1BCH or &dmid=C4CMH or &dmid=C7CCHMC or &dmid=C7CPED or &dmid=C9LC %then %do;
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
data print_iiif(keep=col: statthresholdflag thresholdflag) anyout(keep=col1);
     length col1-col13 $50;
     merge data(in=d)
           _quant_loinc(in=q)           
     ;
     by lab_loinc;
     if d and q;

     col1=strip(dc_lab_group);
     col2=strip(lab_loinc);
     if norm_min^=. then col3=strip(put(norm_min,16.1));
     if norm_max^=. then col4=strip(put(norm_max,16.1));
     col5=strip(put(loinc_median,16.1));
     col6=strip(put(outlier_low,16.1));
     col7=strip(put(outlier_high,16.1));
     if loinc_median>norm_max or loinc_median<norm_min then col8="Yes";
     else col8="No";
     col9=strip(put(count,16.));
     col10=strip(put(median,16.1));
     array fl statthresholdflag thresholdflag;
     if median>outlier_high or median<outlier_low then do;
        col11="Yes";
        statthresholdflag=1;
     end;
     else col11="No";
     if median>norm_max or median<norm_min then col12="Yes";
     else col12="No";
     if col8="No" and col12="Yes" then do;
        col13="Yes";
        thresholdflag=1;
     end;
     else do;
        col13="No";
        thresholdflag=0;
     end;

     output print_iiif;
     if statthresholdflag=1 or thresholdflag=1 then output anyout;
run;

proc sort data=print_iiif;
     by col1;
run;

*- Restrict report to lab groups with at least one outlier -*;
proc sort data=anyout nodupkey;
     by col1;
run;

data print_iiif;
     merge print_iiif anyout(in=a);
     by col1;
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
         by col1;

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
         set print_iiif(keep=statthresholdflag thresholdflag);
         if statthresholdflag=1 or thresholdflag=1;
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
        array _char_ $50 col1-col13;
        pg=1;
        statthresholdflag=0;
        thresholdflag=0;
        col1="No Lab Group contains a Data Check Exception";
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
         column pg statthresholdflag thresholdflag col1 col2 
                ("Data Curation|Normal Range|_______________________" col3 col4)
                ("PCORnet Results|___________________________________" col5 col6 col7 col8)
                ("DataMart Results|_________________________________________________" col9 col10 col11 col12 col13);

         define pg       /order noprint;
         define statthresholdflag /display noprint;
         define thresholdflag /display noprint;
         define col1     /display flow "Data Curation|Lab Group" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col2     /display flow "LAB|LOINC" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col3     /display flow "Clinical|Lower|Bound" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col4     /display flow "Clinical|Upper|Bound" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /display flow "Median" style(header)=[just=center cellwidth=6.5%] style(column)=[just=center];
         define col6     /display flow "Outlier|Lower|Bound" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col7     /display flow "Outlier|Upper|Bound" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col8     /display flow "Outside|Normal|Range" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col9     /display flow "N" style(header)=[just=center cellwidth=6.5%] style(column)=[just=center];
         define col10    /display flow "Median" style(header)=[just=center cellwidth=6.5%] style(column)=[just=center];
         define col11    /display flow "Statistical|Outlier" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col12    /display flow "Outside|Normal|Range" style(header)=[just=center cellwidth=6%] style(column)=[just=center];
         define col13    /display flow "Clinical|Outlier" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         compute col11;
            if statthresholdflag=1 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
         compute col13;
            if thresholdflag=1 then call define(_col_, "style", "style=[color=blue]");
         endcomp;
        
         break after pg / page;
    run;    

    ods listing;
%mend print_iiif;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

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

     if cat in ('AV' 'ED' 'EI' 'IP') and .<perc<1.00 then thresholdflag=1;
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
%clean(savedsn=print: chart:);

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
     length category $50;
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

*- Clear working directory -*;
%clean(savedsn=print: chart:);

%mend chart_iva;

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
     merge stats(rename=(bt_flag=flag) where=(flag=" "))
           stats(in=bt drop=sum where=(bt_flag="*"))
     ;
     by dc_name cat;
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
%clean(savedsn=print: chart:);

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
     length category $50;
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
%clean(savedsn=print: chart:);

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
        "DX_TYPE"="24"
        "DX_SOURCE"="25"
        "DX_ORIGIN"="26"
        "PDX"="27"
        "DIA_ENCOUNTERID"="28"
        "DX_POA"="29"
        "DIA_PROVIDERID"="30"
        "PX_DATE"="31"
        "PX_TYPE"="32"
        "PX_SOURCE"="33"
        "PRO_ENCOUNTERID"="34"
        "PPX"="35"
        "PRO_PROVIDERID"="36"
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
        "24"="DIA_L3_DXTYPE_DXSOURCE"
        "25"="DIA_L3_DXSOURCE"
        "26"="DIA_L3_ORIGIN"
        "27"="DIA_L3_PDX_ENCTYPE"
        "28"="DIA_L3_N"
        "29"="DIA_L3_DXPOA"
        "30"="DIA_L3_N"
        "31"="XTBL_L3_DATES"
        "32"="PRO_L3_PXTYPE_ENCTYPE"
        "33"="PRO_L3_PXSOURCE"
        "34"="PRO_L3_N"
        "35"="PRO_L3_PPX"
        "36"="PRO_L3_N"
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
     else if 11<=ordn<=23 then table="ENCOUNTER";
     else if 24<=ordn<=30 then table="DIAGNOSIS";
     else if 31<=ordn<=36 then table="PROCEDURES";
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
                     "ENC_L3_PAYERTYPE1" "ENC_L3_PAYERTYPE2" "ENC_L3_FACILITYTYPE"
                     "DIA_L3_DXSOURCE" "DIA_L3_ORIGIN" "DIA_L3_DXPOA" "PRO_L3_PXSOURCE" "PRO_L3_PPX") and
                     category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
                     "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE")  and category in ("IP" "EI") and 
                     cross_category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("DIA_L3_DXTYPE_DXSOURCE") and category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("PRO_L3_PXTYPE_ENCTYPE") and cross_category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("ENC_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "FACILITYID")) or
        (dc_name in ("DIA_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "ENCOUNTERID")) or
        (dc_name in ("PRO_L3_N") and statistic="NULL_N" and variable in ("PROVIDERID" "ENCOUNTERID")) or
        (dc_name in ("XTBL_L3_DATES") and statistic="NMISS" and variable in ("BIRTH_DATE" "ENR_END_DATE" "PX_DATE")) or
        (dc_name in ("XTBL_L3_TIMES") and statistic="NMISS" and variable in ("BIRTH_TIME" "ADMIT_TIME" "DISCHARGE_TIME"))
        ;

     if dc_name in ("ENC_L3_ENCTYPE_DDATE_YM" "ENC_L3_ENCTYPE_DISDISP" "ENC_L3_ENCTYPE_DISSTAT" 
                    "ENC_L3_ENCTYPE_DRG" "ENC_L3_ENCTYPE_ADMSRC" "DIA_L3_PDX_ENCTYPE" "PRO_L3_PXTYPE_ENCTYPE"
                    "ENC_L3_PAYERTYPE1" "ENC_L3_PAYERTYPE2" "ENC_L3_FACILITYTYPE") then var=cross_variable;
     else if dc_name="DIA_L3_N" and variable="ENCOUNTERID" then var="DIA_ENCOUNTERID";
     else if dc_name="PRO_L3_N" and variable="ENCOUNTERID" then var="PRO_ENCOUNTERID";
     else if dc_name="DIA_L3_N" and variable="PROVIDERID" then var="DIA_PROVIDERID";
     else if dc_name="PRO_L3_N" and variable="PROVIDERID" then var="PRO_PROVIDERID";
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
                "DX_ORIGIN" "DIA_ENCOUNTERID" "PX_DATE" "PX_SOURCE" "PRO_ENCOUNTERID") 
                and perc>=10.00 then thresholdflag=1;
     col8=dc_name;
run;

*- Add quartile data -*;
proc sort data=print_ivc;
     by col1 col2;
run;

data print_ivc;
     length col7 $50;
     merge print_ivc(in=c) missingness;
     by col1 col2;
     if c;

     if q2^=. then col7=put(q2,8.1);
     else col7="---";
     drop q:;
run;

proc sort data=print_ivc;
     by ord;
run;

data print_ivc;
     set print_ivc;
     by ord;
     if _n_<=18 then pg=1;
     else pg=2;
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
                ("Records with missing, NI, UN, or OT values|_______________________________________________" col4 col5 col6 col7)
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
         define col7     /display flow "50th Percentile" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
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

    ods proclabel "&_ttl1 (continued)";
    title1 justify=left "&_ttl1 (continued)";
    title2 justify=left h=2.5 "&_ttl2";

    proc report data=print_ivc split='|' style(header)=[backgroundcolor=CXCCCCCC];
         column thresholdflag perc pg col1 col2 col3 
                ("Records with missing, NI, UN, or OT values|__________________________________________________" col4 col5 col6 col7)
                (" |  " col8);
         where pg>1;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col3     /display flow "Encounter Type|Constraint" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col7     /display flow "50th Percentile" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
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
%mend print_ivc;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

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
        "RX_ENCOUNTERID"="25"
        "RX_PROVIDERID"="26"
        "RX_ORDER_DATE"="27"
        "RX_ORDER_TIME"="28"
        "RX_START_DATE"="29"
        "RX_END_DATE"="30"
        "RX_DAYS_SUPPLY_GROUP"="31"
        "RX_FREQUENCY"="32"
        "RX_BASIS"="33"
        "RXNORM_CUI"="34"        
        "RX_QUANTITY"="35"
        "RX_REFILLS"="36"
        "RX_DOSE_ORDERED"="37"
        "RX_DOSE_ORDERED_UNIT"="38"
        "RX_DOSE_FORM"="39"
        "RX_PRN_FLAG"="40"
        "RX_ROUTE"="41"
        "RX_SOURCE"="42"
        "RX_DISPENSE_AS_WRITTEN"="43"
        "PRESCRIBINGID"="44"
        "DISPENSE_SUP_GROUP"="45"
        "DISPENSE_DOSE_DISP"="46"
        "DISPENSE_DOSE_DISP_UNIT"="47"
        "DISPENSE_ROUTE"="48"
        "COND_ENCOUNTERID"="49"
        "REPORT_DATE"="50"
        "RESOLVE_DATE"="51"
        "ONSET_DATE"="52"
        "CONDITION_STATUS"="53"
        "CONDITION_SOURCE"="54"        
        "DEATH_CAUSE_CONFIDENCE"="55"
        "PROCM_ENCOUNTERID"="56"
        "PRO_ITEM_LOINC"="57"
        "PRO_DATE"="58"
        "PRO_TIME"="59"
        "PRO_METHOD"="60"
        "PRO_MODE"="61"
        "PRO_CAT"="62"
        "PRO_TYPE"="63"
        "PRO_ITEM_NAME"="64"
        "PRO_ITEM_FULLNAME"="65"
        "PRO_MEASURE_NAME"="66"
        "PRO_MEASURE_FULLNAME"="67"
        "PROVIDER_NPI"="68"
        "PROVIDER_NPI_FLAG"="69"
        "PROVIDER_SPECIALTY_PRIMARY"="70"
        "PROVIDER_SEX"="71"
        "MEDADMIN_ENCOUNTERID"="72"
        "MEDADMIN_PRESCRIBINGID"="73"
        "MEDADMIN_PROVIDERID"="74"
        "MEDADMIN_DOSE_ADMIN"="75"
        "MEDADMIN_DOSE_ADMIN_UNIT"="76"
        "MEDADMIN_ROUTE"="77"
        "MEDADMIN_SOURCE"="78"
        "MEDADMIN_CODE"="79"
        "MEDADMIN_TYPE"="80"
        "OBSCLIN_ENCOUNTERID"="81"
        "OBSCLIN_PROVIDERID"="82"
        "OBSCLIN_DATE"="83"
        "OBSCLIN_TIME"="84"
        "OBSCLIN_CODE"="85"
        "OBSCLIN_TYPE"="86"
        "OBSCLIN_RESULT_QUAL"="87"
        "OBSCLIN_RESULT_MODIFIER"="88"
        "OBSCLIN_RESULT_UNIT"="89"
        "OBSGEN_ENCOUNTERID"="90"
        "OBSGEN_PROVIDERID"="91"
        "OBSGEN_DATE"="92"
        "OBSGEN_TIME"="93"
        "OBSGEN_CODE"="94"
        "OBSGEN_TYPE"="95"
        "OBSGEN_RESULT_QUAL"="96"
        "OBSGEN_RESULT_MODIFIER"="97"
        "OBSGEN_RESULT_UNIT"="98"
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
        "25"="PRES_L3_N"
        "26"="PRES_L3_N"
        "27"="XTBL_L3_DATES"
        "28"="XTBL_L3_TIMES"
        "29"="XTBL_L3_DATES"
        "30"="XTBL_L3_DATES"
        "31"="PRES_L3_SUPDIST2"
        "32"="PRES_L3_FREQ"
        "33"="PRES_L3_BASIS"
        "34"="PRES_L3_RXCUI"
        "35"="PRES_L3_RXQTY_DIST"
        "36"="PRES_L3_RXREFILL_DIST"
        "37"="PRES_L3_RXDOSEODR_DIST"
        "38"="PRES_L3_RXDOSEODRUNIT"
        "39"="PRES_L3_RXDOSEFORM"
        "40"="PRES_L3_PRNFLAG"
        "41"="PRES_L3_ROUTE"
        "42"="PRES_L3_SOURCE"
        "43"="PRES_L3_DISPASWRTN"
        "44"="DISP_L3_N"
        "45"="DISP_L3_SUPDIST2"
        "46"="DISP_L3_DOSE_DIST"
        "47"="DISP_L3_DOSE_UNIT"
        "48"="DISP_L3_ROUTE"
        "49"="CONDITION_L3_N"
        "50"="XTBL_L3_DATES"
        "51"="XTBL_L3_DATES"
        "52"="XTBL_L3_DATES"
        "53"="COND_L3_STATUS"
        "54"="COND_L3_SOURCE"        
        "55"="DEATHC_L3_CONF"
        "56"="PROCM_L3_N"
        "57"="PROCM_L3_LOINC"
        "58"="PROCM_L3_DATE"
        "59"="XTBL_L3_TIMES"
        "60"="PROCM_L3_METHOD"
        "61"="PROCM_L3_MODE"
        "62"="PROCM_L3_CAT"
        "63"="PROCM_L3_TYPE"
        "64"="PROCM_L3_ITEMNM"
        "65"="PROCM_L3_ITEMFULLNAME"
        "66"="PROCM_L3_MEASURENM"
        "67"="PROCM_L3_MEASUREFULLNAME"
        "68"="PROV_L3_N"
        "69"="PROV_L3_NPIFLAG"
        "70"="PROV_L3_SPECIALTY"
        "71"="PROV_L3_SEX"
        "72"="MEDADM_L3_N"
        "73"="MEDADM_L3_N"
        "74"="MEDADM_L3_N"
        "75"="MEDADM_L3_DOSEADM"
        "76"="MEDADM_L3_DOSEADMUNIT"
        "77"="MEDADM_L3_ROUTE"
        "78"="MEDADM_L3_SOURCE"
        "79"="MEDADM_L3_CODE_TYPE"
        "80"="MEDADM_L3_TYPE"
        "81"="OBSCLIN_L3_N"
        "82"="OBSCLIN_L3_N"
        "83"="XTBL_L3_DATES"
        "84"="XTBL_L3_TIMES"
        "85"="OBSCLIN_L3_CODE_TYPE"
        "86"="OBSCLIN_L3_TYPE"
        "87"="OBSCLIN_L3_QUAL"
        "88"="OBSCLIN_L3_MOD"
        "89"="OBSCLIN_L3_UNIT"
        "90"="OBSGEN_L3_N"
        "91"="OBSGEN_L3_N"
        "92"="XTBL_L3_DATES"
        "93"="XTBL_L3_TIMES"
        "94"="OBSGEN_L3_CODE_TYPE"
        "95"="OBSGEN_L3_TYPE"
        "96"="OBSGEN_L3_QUAL"
        "97"="OBSGEN_L3_MOD"
        "98"="OBSGEN_L3_UNIT"
        ;
run;

*- Create a dummy dataset to ensure every variable appears in the table -*;
data dum_field;
     length ord $2 var table $50 dc_name $100;
     set o_field;
     if fmtname="O_FIELD";
     var=strip(start);
     ord=strip(label);
     ordn=input(ord,2.);
     if 0<=ordn<=2 then table="VITAL";
     else if 3<=ordn<=6 then table="DEATH";
     else if 7<=ordn<=24 then table="LAB_RESULT_CM";
     else if 25<=ordn<=43 then table="PRESCRIBING";
     else if 44<=ordn<=48 then table="DISPENSING";
     else if 49<=ordn<=54 then table="CONDITION";
     else if 55<=ordn<=55 then table="DEATH_CAUSE";
     else if 56<=ordn<=67 then table="PRO_CM";
     else if 68<=ordn<=71 then table="PROVIDER";
     else if 72<=ordn<=80 then table="MED_ADMIN";
     else if 81<=ordn<=89 then table="OBS_CLIN";
     else if 90<=ordn<=98 then table="OBS_GEN";
     dc_name=put(ord,$o_source.);
    
     keep table var ord dc_name;
run;

proc sort data=dum_field;
     by table var;
run;

*- Denominators -*;
data denominators;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
                ((dc_name in ("VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "DISP_L3_N" 
                             "DEATH_L3_N" "COND_L3_N" "PROCM_L3_N" "DEATHC_L3_N" 
                             "MEDADM_L3_N" "OBSCLIN_L3_N" "OBSGEN_L3_N") and
                    statistic in ("ALL_N" "NULL_N") and variable in ("PATID")) or
                (dc_name in ("PROV_L3_N" "OBCLIN_L3_N" "OBSGEN_L3_N") and statistic in ("ALL_N" "NULL_N") and variable in ("PROVIDERID")))));

     if table="LAB" then table="LAB_RESULT_CM";
     else if table="PROV" then table="PROVIDER";
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

*- Numerators -*;
data numerators;
     length var $50;
     set dc_normal(where=(datamartid=%upcase("&dmid")));
     if (dc_name in ("VIT_L3_VITAL_SOURCE" "DEATH_L3_IMPUTE" "DEATH_L3_MATCH" "DEATH_L3_SOURCE"
                     "LAB_L3_NAME" "LAB_L3_SOURCE" "LAB_L3_LOINC" "LAB_L3_PRIORITY" 
                     "LAB_L3_LOC" "LAB_L3_PX_TYPE" "LAB_L3_QUAL" "LAB_L3_MOD" "LAB_L3_UNIT"
                     "LAB_L3_LOW" "LAB_L3_HIGH" "LAB_L3_ABN" "PRES_L3_SUPDIST2" "PRES_L3_FREQ" 
                     "PRES_L3_BASIS" "PRES_L3_RXCUI" "PRES_L3_RXQTY_DIST" "PRES_L3_RXREFILL_DIST" 
                     "PRES_L3_RXDOSEFORM" "PRES_L3_PRNFLAG" "PRES_L3_ROUTE" "PRES_L3_SOURCE" 
                     "PRES_L3_DISPASWRTN" "PRES_L3_RXDOSEODRUNIT"
                     "DISP_L3_SUPDIST2" "DISP_L3_DOSEUNIT" "DISP_L3_ROUTE"
                     "COND_L3_STATUS" "COND_L3_SOURCE" "DEATHC_L3_CONF" 
                     "PROCM_L3_METHOD" "PROCM_L3_MODE" "PROCM_L3_CAT" "PROCM_L3_LOINC"
                     "PROCM_L3_TYPE" "PROCM_L3_ITEMNM" "PROCM_L3_ITEMFULLNAME" "PROCM_L3_MEASURENM"
                     "PROCM_L3_MEASURE_FULLNAME" "PROV_L3_NPIFLAG" "PROV_L3_SPECIALTY" "PROV_L3_SEX"
                     "MEDADM_L3_DOSEADMUNIT" "MEDADM_L3_ROUTE" "MEDADM_L3_SOURCE" "MEDADM_L3_CODE_TYPE" "MEDADM_L3_TYPE"
                     "OBSCLIN_L3_CODE_TYPE" "OBSCLIN_L3_TYPE" "OBSCLIN_L3_QUAL" "OBSCLIN_L3_MOD" "OBSCLIN_L3_RUNIT"
                     "OBSGEN_L3_CODE_TYPE" "OBSGEN_L3_TYPE" "OBSGEN_L3_QUAL" "OBSGEN_L3_MOD" "OBSGEN_L3_RUNIT") and 
                     category in ("NULL or missing" "NI" "UN" "OT") and statistic="RECORD_N") or
        (dc_name in ("LAB_L3_PX_PXTYPE") and cross_category in ("NULL or missing") and statistic="RECORD_N") or
        (dc_name in ("VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "COND_L3_N" "PROCM_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID")) or
        (dc_name in ("PRES_L3_N") and statistic="NULL_N" and variable in ("RX_PROVIDERID")) or
        (dc_name in ("DISP_L3_N") and statistic="NULL_N" and variable in ("PRESCRIBINGID")) or
        (dc_name in ("PROV_L3_N") and statistic="NULL_N" and variable in ("PROVIDER_NPI")) or
        (dc_name in ("MEDADM_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "PRESCRIBINGID" "MEDADMIN_PROVIDERID")) or
        (dc_name in ("OBSCLIN_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "OBSCLIN_PROVIDERID")) or
        (dc_name in ("OBSGEN_L3_N") and statistic="NULL_N" and variable in ("ENCOUNTERID" "OBSGEN_PROVIDERID")) or
        (dc_name in ("LAB_L3_LOINC_RESULT_NUM") and statistic="NMISS" and variable in ("LAB_LOINC")) or
        (dc_name in ("XTBL_L3_DATES") and statistic="NMISS" and variable in ("LAB_ORDER_DATE" "SPECIMEN_DATE" "DEATH_DATE"
                     "RX_ORDER_DATE" "RX_START_DATE" "RX_END_DATE" "REPORT_DATE" "RESOLVE_DATE" "ONSET_DATE" "PRO_DATE" "OBSCLIN_DATE" "OBSGEN_DATE")) or
        (dc_name in ("XTBL_L3_TIMES") and statistic="NMISS" and variable in ("MEASURE_TIME" "SPECIMEN_TIME" "RESULT_TIME" 
                     "RX_ORDER_TIME" "PRO_DATE" "PRO_TIME" "OBSCLIN_TIME" "OBSGEN_TIME")) or
        (dc_name in ("PRES_L3_RXQTY_DIST" "PRES_L3_RXREFILL_DIST" "PRES_L3_RXDOSEODR_DIST" "MEDADM_L3_DOSEADM" "DISP_L3_DOSE_DIST") and statistic="NULL or missing")
        ;

     if table="LAB" then table="LAB_RESULT_CM";
     else if table="PROV" then table="PROVIDER";
     else table=table;

     if dc_name in ("LAB_L3_PX_PXTYPE") then var=cross_variable;
     else if dc_name="PRES_L3_N" and variable="ENCOUNTERID" then var="RX_ENCOUNTERID";
     else if dc_name="VIT_L3_N" and variable="ENCOUNTERID" then var="VITAL_ENCOUNTERID";
     else if dc_name="COND_L3_N" and variable="ENCOUNTERID" then var="COND_ENCOUNTERID";
     else if dc_name="PROCM_L3_N" and variable="ENCOUNTERID" then var="PROCM_ENCOUNTERID";
     else if dc_name="MEDADM_L3_N" and variable="ENCOUNTERID" then var="MEDADMIN_ENCOUNTERID";
     else if dc_name="OBSCLIN_L3_N" and variable="ENCOUNTERID" then var="OBSCLIN_ENCOUNTERID";
     else if dc_name="OBSGEN_L3_N" and variable="ENCOUNTERID" then var="OBSGEN_ENCOUNTERID";
     else if dc_name="OBSCLIN_L3_N" and variable="PROVIDERID" then var="OBSCLIN_PROVIDERID";
     else if dc_name="MEDADM_L3_N" and variable="PRESCRIBINGID" then var="MEDADMIN_PRESCRIBINGID";
     else if dc_name="LAB_L3_LOINC_RESULT_NUM" and variable="LAB_LOINC" then var="RESULT_NUM";
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
data print_ivd(keep=ord col: perc thresholdflag);
     length col1-col5 col8 $50;
     merge num(in=n) denom(in=d keep=table denom bt_flag rename=(bt_flag=bt_denom));
     by table;
     if n and d;

     ord=put(var,$o_field.);
     col1=table;
     if var in ("VITAL_ENCOUNTERID" "RX_ENCOUNTERID" "COND_ENCOUNTERID" "PROCM_ENCOUNTERID"
                "MEDADMIN_ENCOUNTERID" "OBSCLIN_ENCOUNTERID" "OBSGEN_ENCOUNTERID" ) then col2="ENCOUNTERID";
     else if var in ("OBSCLIN_PROVIDERID" "OBSGEN_PROVIDERID") then col2="PROVIDERID";
     else if var in ("MEDADMIN_PRESCRIBINGID") then col2="PRESCRIBINGID";
     else if var="RX_DAYS_SUPPLY_GROUP" then col2="RX_DAYS_SUPPLY";
     else if var="DISPENSE_SUP_GROUP" then col2="DISPENSE_SUP";
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
                "ENCOUNTERID") 
                and perc>=10.00 then thresholdflag=1;

     col8=dc_name;
run;

*- Add quartile data -*;
proc sort data=print_ivd;
     by col1 col2;
run;

data print_ivd;
     length col7 $50;
     merge print_ivd(in=d) missingness;
     by col1 col2;
     if d;

     if q2^=. then col7=put(q2,8.1);
     else col7="---";
     drop q2;
run;

proc sort data=print_ivd;
     by ord;
run;

data print_ivd;
     set print_ivd;
     by ord;
     if _n_<=19 then pg=1;
     else if _n_<=38 then pg=2;
     else if _n_<=57 then pg=3;
     else if _n_<=76 then pg=4;
     else if _n_<=95 then pg=5;
     else pg=6;
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
                ("Records with missing, NI, UN, or OT values|__________________________________________________" col4 col5 col6 col7)
                (" |  " col8);
         where pg=1;

         define thresholdflag /display noprint;
         define perc     /display noprint;
         define pg       /order noprint;
         define col1     /display flow "Table" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
         define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
         define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
         define col7     /display flow "50th Percentile" style(header)=[just=center cellwidth=7%] style(column)=[just=left];
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
    run;;

    %do _pg = 2 %to &_nopg;
        ods listing close;
        ods proclabel "&_ttl1 (continued - page &_pg of &_nopg)";
        title1 justify=left "&_ttl1 (continued - page &_pg of &_nopg)";
        title2 justify=left h=2.5 "&_ttl2";
        footnote1 justify=left "&_fnote";

        proc report data=print_ivd split='|' style(header)=[backgroundcolor=CXCCCCCC];
             column thresholdflag perc pg col1 col2 
                    ("Records with missing, NI, UN, or OT values|__________________________________________________" col4 col5 col6 col7)
                    (" |  " col8);
             where pg=&_pg;

             define thresholdflag /display noprint;
             define perc     /display noprint;
             define pg       /order noprint;
             define col1     /display flow "Table" style(header)=[just=left cellwidth=13%] style(column)=[just=left];
             define col2     /display flow "Field" style(header)=[just=left cellwidth=22%] style(column)=[just=left];
             define col4     /display flow "Numerator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col5     /display flow "Denominator" style(header)=[just=center cellwidth=11%] style(column)=[just=center];
             define col6     /computed format=5.1 "%" style(header)=[just=center cellwidth=7%] style(column)=[just=center];
             define col7     /display flow "50th Percentile" style(header)=[just=center cellwidth=7%] style(column)=[just=left];
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
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE IVE PRINCIPAL DIAGNOSES FOR INSTITUTIONAL ENCOUNTERS
********************************************************************************;

data data;
     length diag $2;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_PDX_ENCTYPE"
                          and category in ("EI" "IP" "IS" "OS")
                          and statistic in ("RECORD_N" "DISTINCT_ENCID_N")));
     if cross_category^="P" then diag="NP";
     else diag="P";

     * flag BT records *;
     if resultn=.t then do;
        bt_flag="*";
        resultn=.;
     end;
     if resultn=. then resultn=0;

     keep dc_name category statistic diag resultn bt_flag;
run;

*- Summary diagnosis -*;
proc means data=data nway noprint missing;
     class dc_name category statistic diag bt_flag;
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
     by category;
     rename dc_name=dc_name1;
run;

*- Bring everything together by adding encounters without principal diagnosis -*;
data print_ive(keep=col: perc: thresholdflag: exception:);
     length col1-col3 col5-col7 $50;
     merge print_ive
           dc_normal(where=(datamartid=%upcase("&dmid") and dc_name="DIA_L3_PDXGRP_ENCTYPE"
                     and cross_category="U" and category in ("EI" "IP" "IS" "OS") and statistic in ("DISTINCT_ENCID_N")));
           
     ;
     by category;

     col1=put(category,$etype.);
     if btflag_pat_p^="*" then col2=strip(put(pat_p,comma16.));
     else col2="BT";
     if resultn>.t then col3=strip(put(resultn,comma16.));
     else col3=strip(put(resultn,threshold.));
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
        if rec_p/pat_p > 2.0 then exceptionrate=1;
     end;
     col7=strip(dc_name1) || "; " || strip(dc_name);
run;

*- Keep records resulting in an exception -*;
data dc_summary_ive;
     length edc_table_s_ $10;
     set print_ive(keep=thresholdflag exception:);
     if thresholdflag=1 or exceptionflag=1 or exceptionrate=1;
     exception=1;
     edc_table_s_="Table IVE";
     if thresholdflag=1 or exceptionflag=1 then row=3.06;
     else if exceptionrate=1 then row=2.07;
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
         column thresholdflag exceptionflag exceptionrate perc col1 col2 col3 col4 col5 col6 col7;

         define thresholdflag /display noprint;
         define exceptionflag /display noprint;
         define exceptionrate /display noprint;
         define perc     /display noprint;
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Distinct encounter|IDs with a|principal diagnosis" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col3     /display flow "Distinct encounter|IDs without a|principal diagnosis" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col4     /computed format=8.1 "% of encounters|without a principal|diagnosis" style(header)=[just=center cellwidth=13%] style(column)=[just=center];
         define col5     /display flow "Principal|diagnoses" style(header)=[just=center cellwidth=9%] style(column)=[just=center];
         define col6     /display flow "Principal diagnoses|per encounter with any|principal diagnosis" style(header)=[just=center cellwidth=10.5%] style(column)=[just=center];
         define col7     /display flow "Source table" style(header)=[just=left cellwidth=17.5%] style(column)=[just=left];

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
%clean(savedsn=print: chart:);

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

*- Subset data -*;
data data;
     set dc_normal(keep=dc_name statistic category resultn cross_category 
                 where=(category in ("AV" "ED" "EI" "IP") and resultn>=0 and 
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
     length col1 col2 col4 col6 $35;
     merge stats(where=(dc_name="ENC_L3_ENCTYPE_ADATE_YM") rename=(sum=enc denom=enc_den bt_flag=bt_enc))
           stats(where=(dc_name="DIA_L3_ENCTYPE_ADATE_YM") rename=(sum=dia denom=dia_den bt_flag=bt_dia))
           stats(where=(dc_name="PRO_L3_ENCTYPE_ADATE_YM") rename=(sum=pro denom=pro_den bt_flag=bt_pro))
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
    
         output;
     end;
     else if ord=24 then do;
         col1="Benchmark average";
         if enc=. and bt_enc="*" then col2=strip(put(.t,threshold.));
         else col2=strip(put(enc/12,comma16.))||strip(bt_enc);

         if dia=. and bt_dia="*" then col4=strip(put(.t,threshold.));
         else col4=strip(put(dia/12,comma16.))||strip(bt_dia);

         if pro=. and bt_pro="*" then col6=strip(put(.t,threshold.));
         else col6=strip(put(pro/12,comma16.))||strip(bt_pro);

         output;
         ord=11.5;
         col1="Benchmark Period";
         col2=" ";
         col3=" ";
         col4=" ";
         col5=" ";
         col6=" ";
         col7=" ";
         output;
         ord=25;
         col1="Source table";
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
         column pg ord col3p col5p col7p col1 
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient encounters|___________________________________" col2 col3)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient diagnoses|___________________________________" col4 col5)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient procedures|___________________________________" col6 col7);
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=center cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
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
         column pg ord col3p col5p col7p col1 
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient encounters|___________________________________" col2 col3)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient diagnoses|___________________________________" col4 col5)
                ("Ambulatory, ED, Inpatient or ED-to-Inpatient procedures|___________________________________" col6 col7);
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
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
%clean(savedsn=print: chart:);


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
     length col1 col2 col4 col6 $35;
     merge stats(where=(dc_name="VIT_L3_MDATE_YM") rename=(sum=vit denom=vit_den bt_flag=bt_vit))
           stats(where=(dc_name="PRES_L3_ODATE_YM") rename=(sum=pres denom=pres_den bt_flag=bt_pres))
           stats(where=(dc_name="LAB_L3_RDATE_YM") rename=(sum=lab denom=lab_den bt_flag=bt_lab))
     ;
     by ord;

     if ord<24 then do;
         if vit=. and bt_vit="*" then col2=strip(put(.t,threshold.));
         else col2=strip(put(vit,comma16.))||strip(bt_vit);
         if vit>0 and vit_den>0 then col3p=(vit/round((vit_den/12),1))*100;

         if pres=. and bt_pres="*" then col4=strip(put(.t,threshold.));
         else col4=strip(put(pres,comma16.))||strip(bt_pres);
         if pres>0 and pres_den>0 then col5p=(pres/round((pres_den/12),1))*100;

         if lab=. and bt_lab="*" then col6=strip(put(.t,threshold.));
         else col6=strip(put(lab,comma16.))||strip(bt_lab);
         if lab>0 and lab_den>0 then col7p=(lab/round((lab_den/12),1))*100;

         col1="Month -"||compress(put(ord,2.));
    
         output;
     end;
     else if ord=24 then do;
         col1="Benchmark average";
         if vit=. and bt_vit="*" then col2=strip(put(.t,threshold.));
         else col2=strip(put(vit/12,comma16.))||strip(bt_vit);

         if pres=. and bt_pres="*" then col4=strip(put(.t,threshold.));
         else col4=strip(put(pres/12,comma16.))||strip(bt_pres);

         if lab=. and bt_lab="*" then col6=strip(put(.t,threshold.));
         else col6=strip(put(lab/12,comma16.))||strip(bt_lab);

         output;
         ord=11.5;
         col1="Benchmark Period";
         col2=" ";
         col3=" ";
         col4=" ";
         col5=" ";
         col6=" ";
         col7=" ";
         output;
         ord=25;
         col1="Source table";
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
         column pg ord col3p col5p col7p col1 
                ("Vitals|___________________________________" col2 col3)
                ("Prescriptions|___________________________________" col4 col5)
                ("Labs|___________________________________" col6 col7);
         where pg=1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=center cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
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
         column pg ord col3p col5p col7p col1 
                ("Vitals|___________________________________" col2 col3)
                ("Prescriptions|___________________________________" col4 col5)
                ("Labs|___________________________________" col6 col7);
         where pg>1;

         define pg       /order noprint;
         define ord      /display noprint;
         define col3p    /display noprint;
         define col5p    /display noprint;
         define col7p    /display noprint;
         define col1     /display flow "Month" style(header)=[just=left cellwidth=18%] style(column)=[just=left];
         define col2     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col3     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col5     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col6     /display flow "Records" style(header)=[just=center cellwidth=14%] style(column)=[just=center];
         define col7     /computed flow format=8.1 "Percent of benchmark average" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
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
%clean(savedsn=print: chart:);


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
       ;

     value tier_type
       1 = 'SCD, SBD, BPCK, and GPCK'
     1.5 = 'SBD, BPCK'
       2 = 'SBDF, SCDF, SBDG, SCDG, SBDC, BN,and MIN'
       3 = 'SCDC, PIN, and IN'
       4 = 'DF and DFG'
       5 = 'n/a'
       ;
run;

*- Subset and re-categorize -*;
data data_cui;
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("PRES_L3_RXCUI") and 
                          cross_category^="NULL or missing" and statistic="RECORD_N")) end=eof;
     retain num denom 0;
     if resultn>.t then denom=denom+resultn;
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
     set dc_normal(where=(datamartid=%upcase("&dmid") and dc_name in ("PRES_L3_RXCUI_TIER")));

     if category="NULL or missing" then do;
        category="Unknown";
        ord=5;
     end;
     else ord=input(scan(category,2),1.);

     keep ord category statistic resultn;
run;

proc sort data=data;
     by ord;
run;

*- Bring everything together -*;
data print_ivh;
     length col1 col3 col4 col6 $45 col2 $200;
     merge data(where=(statistic="RECORD_N"))
           data(where=(statistic="RECORD_PCT") rename=(resultn=col5p))
           data_cui(in=c rename=(num=resultn))
     ;
     by ord;

     col1=strip(category);
     col2=strip(put(ord,tier_desc.));
     col3=strip(put(ord,tier_type.));

     if resultn=.t then col4=strip(put(resultn,threshold.));
     else if resultn=. then col4=strip(put(0,threshold.));
     else col4=strip(put(resultn,comma16.));

     if not c then col6="PRES_L3_RXCUI_TIER";
     else if c then col6="PRES_L3_RXCUI";
    
     keep ord col:;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivh;
     length edc_table_s_ $10;
     set print_ivh(keep=ord col4 col5p);
     if ord=1 and (col5p<80.0 or compress(col4) in ("0" "BT"));
     row=3.08;
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
         column col1 col2 col3 col4 ord col5p col5 col6;

         define ord      /display noprint;
         define col1     /display flow "Term Type Tier" style(header)=[just=left cellwidth=10%] style(column)=[just=left];
         define col2     /display flow "Term Type Tier Description" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
         define col3     /display flow "Term Types" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col4     /display flow "Numerator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /computed flow format=8.2 "Percentage" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table" style(header)=[just=left cellwidth=15%] style(column)=[just=left];
         define col5p    /display noprint;
         compute ord;
            if ord=1.5 then CALL DEFINE('_c1_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col5;
            if ord=1 and col5p<80.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col4;
            if ord=1 and compress(col4) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods listing;
%mend print_ivh;

*- Clear working directory -*;
%clean(savedsn=print: chart:);


********************************************************************************;
* TABLE IVI LABORATORY RESULT DATA COMPLETENESS
********************************************************************************;

proc format;
     value known_desc
       1 = 'Number of distinct LAB_LOINCs'
       2 = 'Results mapped to a known LAB_LOINC'        
       3 = 'Results mapped to a known LAB_LOINC with a known result'
       4 = 'Quantitative results'
       5 = '~mQuantitative results which ~-2nspecify the specimen ~-2nsource'
       6 = '~mQuantitative results which ~-2nspecify the result unit'
       7 = '~mQuantitative results which ~-2nspecify the specimen ~-2nsource and result unit'
       8 = '~mQuantitative results which ~-2nfully specify the normal ~-2nrange'
       ;

     value known_crit
       1 = ' '
       2 = 'LAB_LOINC is not null'
       3 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is in ("EQ","GE","GT","LE","LT") or RESULT_QUAL is in ("BORDERLINE", "POSITIVE", "NEGATIVE" or "UNDETERMINED")'
       4 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is in ("EQ","GE","GT","LE","LT")'
       5 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is in ("EQ","GE","GT","LE","LT") and SPECIMEN_SOURCE is not a flavor of null'
       6 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is in ("EQ","GE","GT","LE","LT") and RESULT_UNIT is not a flavor of null'
       7 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is in ("EQ","GE","GT","LE","LT") and SPECIMEN_SOURCE is not a flavor of null and RESULT_UNIT is not a flavor of null'
       8 = 'LAB_LOINC is not null and RESULT_NUM is not null and RESULT_MODIFIER is not a flavor of null and NORM_MODIFIER_LOW, NORM_RANGE_LOW, NORM_MODIFIER_HIGH, and NORM_RANGE_HIGH are all populated per CDM specifications.** '
       ;

     value _data_check
       1 = 'n/a'
       2 = '3.09'
       3 = 'n/a'
       4 = 'n/a'
       5 = 'n/a'
       6 = 'n/a'
       7 = '3.12'
       8 = '3.10'
       ;
run;

*- Subset and re-categorize -*;
data data;
     set dc_normal(where=(datamartid=%upcase("&dmid") and 
         (dc_name in ("LAB_L3_RECORDC") and statistic="ALL_N") or
         (dc_name in ("LAB_L3_N") and variable="LAB_RESULT_CM_ID" and 
          statistic in ("ALL_N" "NULL_N"))));

     keep variable resultn;
run;

proc means data=data nway noprint;
     class variable;
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
     length col0 $10 col1 $130 col3 col4 col6 $30 col2 $600;
     if _n_=1 then set stats(where=(variable="LAB_RESULT_CM_ID") rename=(sum=denom_1));
     if _n_=1 then set data(where=(variable="KNOWN_TEST") rename=(resultn=denom_2));
     if _n_=1 then set data(where=(variable="KNOWN_TEST_RESULT_NUM") rename=(resultn=denom_8));
     set loinc(in=l)
         data(where=(variable^="LAB_RESULT_CM_ID"))
     ;

     if l then do;
        ord=1;
        col4=" ";
        col5p=.;
        col6="LAB_L3_LOINC";
     end;
     else if variable="KNOWN_TEST" then do;
        ord=2;
        col4=strip(put(denom_1,comma16.));
        if resultn>=0 and denom_1>0 then col5p=(resultn/denom_1)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC; LAB_L3_N";
     end;
     else if variable="KNOWN_TEST_RESULT" then do;
        ord=3;
        col4=strip(put(denom_2,comma16.));
        if resultn>=0 and denom_2>0 then col5p=(resultn/denom_2)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM" then do;
        ord=4;
        col4=" ";
        col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_SOURCE" then do;
        ord=5;
        col4=strip(put(denom_8,comma16.));
        if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_UNIT" then do;
        ord=6;
        col4=strip(put(denom_8,comma16.));
        if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_SRCE_UNIT" then do;
        ord=7;
        col4=strip(put(denom_8,comma16.));
        if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;
     else if variable="KNOWN_TEST_RESULT_NUM_RANGE" then do;
        ord=8;
        col4=strip(put(denom_8,comma16.));
        if resultn>=0 and denom_8>0 then col5p=(resultn/denom_8)*100;
        else col5p=.;
        col6="LAB_L3_RECORDC";
     end;

     col0=strip(put(ord,_data_check.));
     col1=strip(put(ord,known_desc.));
     col2=strip(put(ord,known_crit.));
     if resultn=.t then col3=strip(put(resultn,threshold.));
     else col3=strip(put(resultn,comma16.));
    
     keep ord col:;
run;

*- Keep records resulting in an exception -*;
data dc_summary_ivi;
     length edc_table_s_ $10;
     set print_ivi(keep=ord col5p col3);
     if ord in (2,7,8) and (col5p<80.0 or compress(col3) in ("0" "BT"));
     if ord=2 then row=3.09;
     else if ord=7 then row=3.12;
     else if ord=8 then row=3.10;
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
         column col0 col1 col2 col3 col4 ord col5p col5 col6;

         define ord      /display noprint;
         define col0     /display flow "Data Check" style(header)=[just=center cellwidth=4.5%] style(column)=[just=center];
         define col1     /display flow "Description" style(header)=[just=left cellwidth=17%] style(column)=[just=left];
         define col2     /display flow "Criteria" style(header)=[just=left cellwidth=35%] style(column)=[just=left];
         define col3     /display flow "Numerator" style(header)=[just=center cellwidth=12%] style(column)=[just=center];
         define col4     /display flow "Denominator" style(header)=[just=center cellwidth=10%] style(column)=[just=center];
         define col5     /computed flow format=8.2 "Percentage" style(header)=[just=center cellwidth=8%] style(column)=[just=center];
         define col6     /display flow "Source table" style(header)=[just=left cellwidth=11.5%] style(column)=[just=left];
         define col5p    /display noprint;
         compute ord;
            if ord>4 then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col5;
            if ord in (2,7,8) and col5p<80.0 then do;
                col5=col5p;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col5=col5p;
            end;
         endcomp;
         compute col3;
            if ord in (2,7,8) and compress(col3) in ("0" "BT") then call define(_col_, "style", "style=[color=blue]");
         endcomp;
    run;

    ods listing;
%mend print_ivi;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

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
        ;
run;

*- Bring current table -*;
data current(keep=ord table dc_name statistic resultn);
     set dc_normal(where=(datamartid=%upcase("&dmid") and scan(dc_name,3,"_")="N" and 
                          (variable="PATID" and statistic in ("ALL_N" "DISTINCT_N")) or
                          (table="PROVIDER" and variable="PROVIDERID")));
     if dc_name="TRIAL_L3_N" or (dc_name="PROV_L3_N" and statistic="DISTINCT_N") then delete;
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
             "OBSGEN_L3_N");
     select count(unique memname) into :prevdata_count from _prevdata
         where memname in ("DEM_L3_N" "ENR_L3_N" "ENC_L3_N" "DIA_L3_N" "PRO_L3_N" 
             "DEATH_L3_N" "VIT_L3_N" "LAB_L3_N" "PRES_L3_N" "DISP_L3_N" "COND_L3_N"
             "DEATHC_L3_N" "PROCM_L3_N" "PROV_L3_N" "MEDADM_L3_N" "OBSCLIN_L3_N" 
             "OBSGEN_L3_N");
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
     col2n=input(all_n,comma16.);
     if tag^="PROVIDERID" then col5n=input(distinct_n,comma16.);
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
     if col2n^=. then col2=strip(put(col2n,comma16.));
     else col2="---";
     if col3n^=. then col3=strip(put(col3n,comma16.));
     else col3="---";
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
     if col5n^=. then col5=strip(put(col5n,comma16.));
     else col5="---";
     if col6n^=. then col6=strip(put(col6n,comma16.));
     else col6="---";
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
            if (col3n=0 and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
         compute col7;
            if (col6n=0 and col5n>0) or (.<col7n<-5.00) then do;
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
%clean(savedsn=print: chart:);

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
                     category in ("AV" "ED" "IP") and statistic in ("RECORD_N" "DISTINCT_PATID_N")));
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
     if enc_type in ("AV" "ED" "IP");

     col2n=input(record_n,comma16.);
     if distinct_patid_n^=" " then col5n=input(distinct_patid_n,comma16.);
run;

proc sort data=previous;
     by category ord;
run;

*- Create label rows -*;
data label;
     do category = "AV", "ED", "IP";
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
     col3=strip(put(col3n,comma16.));
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
     if col5n^=. then col5=strip(put(col5n,comma16.));
     else if ord>0 then col5="---";
     col6=strip(put(col6n,comma16.));
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
         define col1     /display flow "Encounter Type" style(header)=[just=left cellwidth=16.5%] style(column)=[just=left];
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
            if (col3n=0 and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
         compute col7;
            if (col6n=0 and col5n>0) or (.<col7n<-5.00) then do;
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
%clean(savedsn=print: chart:);

********************************************************************************;
* TABLE VC CHANGES IN SELECTED CODE TYPES
********************************************************************************;

*- Create table formats -*;
proc format;
     invalue type_vc
        "DIA_L3_DXTYPE_ENCTYPE"=1
        "PRO_L3_PXTYPE_ENCTYPE"=2
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

*- Bring current table -*;
proc means data=dc_normal nway noprint;
     class dc_name cross_category;
     var resultn;
     output out=current sum=col3n;
     where datamartid=%upcase("&dmid") and dc_name IN ("PRO_L3_PXTYPE_ENCTYPE" "DIA_L3_DXTYPE_ENCTYPE") and 
                   cross_category in ("09" "10" "CH") and statistic in ("RECORD_N");
run;

data current;
     set current;
     by dc_name cross_category;
     ord=input(dc_name,type_vc.);
     keep dc_name cross_category ord col3n;
run;

*- Determine if previous data set exists -*;
proc contents data=prevdata._all_ out=_prevdata noprint;
run;
    
proc sql noprint;
     select unique(memname) into :_prevdata separated by '|'  from _prevdata
         where memname in ("PRO_L3_PXTYPE_ENCTYPE" "DIA_L3_DXTYPE_ENCTYPE");
     select count(unique memname) into :prevdata_count from _prevdata
         where memname in ("PRO_L3_PXTYPE_ENCTYPE" "DIA_L3_DXTYPE_ENCTYPE");
quit;

*- Summarize previous data -*;
%macro prevdataloop;
    %do p = 1 %to &prevdata_count;
        %let pdata=%scan(&_prevdata,&p,'|');
        data &pdata;
             set prevdata.&pdata;
             dc_name="&pdata";

             * create character and numeric result variables *;
             resultc=strip(record_n);
             dateflag=0;
             if length(strip(resultc))=10 then do;
                if char(strip(resultc),5)='-' and char(strip(resultc),8)='-' then dateflag=1;
             end;
             if resultc not in ("BT" " ") then do;
                if indexc(upcase(resultc),"ABCDEFGHIJKLMNOPQRSTUVWXYZ_():")=0 and dateflag=0
                   then resultn=input(resultc,best.);
                else resultn=.;
             end;
             else if resultc in ("BT") then resultn=.t;
             else if resultc in (" ") then resultn=.;

             %if &pdata=DIA_L3_DXTYPE_ENCTYPE %then %do;
                cross_category=dx_type;
             %end;
             %else %if &pdata=PRO_L3_PXTYPE_ENCTYPE %then %do;
                cross_category=px_type;
             %end;

             keep dc_name cross_category resultn;
        run;
        
        proc means data=&pdata nway noprint;
             class dc_name cross_category;
             var resultn;
             output out=prev_&pdata sum=col2n;
             where cross_category in ("09" "10" "CH");
        run;
    %end;
%mend prevdataloop;
%prevdataloop;

*- Bring previous table data -*;
data previous(keep=dc_name ord cross_category col:);
     set prev_pro_l3_pxtype_enctype
         prev_dia_l3_dxtype_enctype
     ;

     ord=input(dc_name,type_vc.);
run;

proc sort data=previous;
     by ord cross_category;
run;

*- Create label rows -*;
data label;
     do ord = 1;
        do cross_category = "00", "09", "10";
           output;
        end;
     end;
     do ord = 2;
        do cross_category = "00", "09", "10", "CH";
           output;
        end;
     end;
run;

*- Bring everything together -*;
data print_vc;
     length col1-col3 col5 $50;
     merge current
           previous
           label
     ;
     by ord cross_category;

     if cross_category="00" then col1=strip(put(ord,row_vc.));
     else col1=cross_category;
     if col2n^=. then col2=strip(put(col2n,comma16.));
     else if cross_category^="00" then col2="---";
     col3=strip(put(col3n,comma16.));
     if col2n>0 and col3n>=0 then col4n=((col3n-col2n)/col2n)*100;
     col5=strip(dc_name);

     pg=1;
run;

*- Keep records resulting in an exception -*;
data dc_summary_vc;
     length edc_table_s_ $10;
     set print_vc(keep=ord col2n col3n col4n);
     if (col3n=0 and col2n>0) or (.<col4n<-5.00);
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
         column pg col1 col2n col3n col4n cross_category 
                ("Records|_____________________________________________________" col2 col3 col4) 
                col5;

         define pg       /order noprint;
         define cross_category /display noprint;
         define col1     /display flow " " style(header)=[just=left cellwidth=20%] style(column)=[just=left];
         define col2     /display flow "Previous|Refresh" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col3     /display flow "Current|Refresh" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col4     /computed format=threshold1c. "Percent|Change" style(header)=[just=center cellwidth=15%] style(column)=[just=center];
         define col5     /display flow "Source table" style(header)=[just=left cellwidth=30%] style(column)=[just=left];
         define col2n    /display noprint;
         define col3n    /display noprint;
         define col4n    /display noprint;
         compute cross_category;
            if cross_category^="00" then CALL DEFINE('_c2_',"STYLE","STYLE={PRETEXT='    ' ASIS=ON}");
         endcomp;
         compute col4;
            if (col3n=0 and col2n>0) or (.<col4n<-5.00) then do;
                col4=col4n;
                call define(_col_, "style", "style=[color=blue]");
            end;
            else do;
                col4=col4n;
            end;
         endcomp;
    run;    

    ods listing;
%mend print_vc;

*- Clear working directory -*;
%clean(savedsn=print: chart:);

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
        if row in (1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.10, 1.11, 1.12, 1.13, 3.04, 3.05) then color = 1;
        else color = 2;
     end;
     else exceptions="No";
        
     if _n_<=18 then pg=1;
     else if _n_<=25 then pg=2;
     else pg=3;
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
    run;;

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
%chart_ia;
%chart_ib_cat(cat1=AV,cat2=OA,cat3=IS,cat4=IC,cat5=OS,c1=red,c2=blue,c3=green,c4=purple,c5=olive);
%chart_ib_cat(cat1=ED,cat2=EI,cat3=IP,c1=brown,c2=bipb,c3=black,gcont=(continued));
%chart_ic_cat(cat1=EI,cat2=IP, cat3=IS,c1=bipb,c2=black,c3=green);
%chart_id;
%chart_ie;
%chart_if;
%chart_ig;
%chart_ih;
%chart_ii_cat(cat1=L,cat2=N,cat3=D,cat4=S,cat5=T,c1=red,c2=blue,c3=green,c4=purple,c5=olive);

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
%print_iiif;

*- Section IV -*;
%print_iva;
%chart_iva(cat1=AV,cat2=ED,cat3=EI,cat4=IC,cat5=IP,cat6=IS,cat7=OA,cat8=OS);
%print_ivb;
%chart_ivb(cat1=AV,cat2=ED,cat3=EI,cat4=IC,cat5=IP,cat6=IS,cat7=OA,cat8=OS);
%print_ivc;
%print_ivd;
%print_ive;
%print_ivf;
%print_ivg;
%print_ivh;
%print_ivi;

*- Section IV -*;
%print_va;
%print_vb;
%print_vc;

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;
