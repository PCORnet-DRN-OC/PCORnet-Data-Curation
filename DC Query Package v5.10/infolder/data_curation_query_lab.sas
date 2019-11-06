/*******************************************************************************
*  $Source: data_curation_query_lab $;
*    $Date: 2019/09/20
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V5.10 - Lab queries only
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. lab_l3_n.sas7bdat)
*           2) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_lab.cpt)
*           3) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_lab.log)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc_lab;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_%lowcase(&_grp).log" lrecl=200;
proc printto log=qlog %if %upcase(&_grp)=LAB %then %do; new %end; ;
run ;

********************************************************************************;
* Bring in LAB_RESULT_CM and compress it
********************************************************************************;
%if &_ylab_result_cm=1 %then %do;
%let qname=lab_result_cm;
%elapsed(begin);

data lab_result_cm(compress=yes);
     set pcordata.lab_result_cm;

     * restrict data to within lookback period *;
     if result_date>=&lookback_dt or result_date=.;

     * for 5 year lookback queries *;
     if result_date>=&lookback5_dt or result_date=. then lookback5_flag=1;

     lab_loinc=strip(upcase(lab_loinc));

     * known test result *;
     if lab_loinc^=" " then do;
        known_test=1;

        if result_num^=. and result_modifier not in (' ' 'NI' 'UN' 'OT')  then known_test_result_num=1;
        else known_test_result_num=.;
        
        if ((result_num^=.  and result_modifier not in (' ' 'NI' 'UN' 'OT')) or 
           result_qual in ("BORDERLINE" "POSITIVE" "NEGATIVE" "UNDETERMINED")) then known_test_result=1;
        else known_test_result=.;

        if result_num^=. and result_modifier not in (' ' 'NI' 'UN' 'OT') and 
           specimen_source not in (' ' 'NI' 'UN' 'OT' 'UNK_SUB' 'SMPLS' 'SPECIMEN') then known_test_result_num_source=1;
        else known_test_result_num_source=.;
        
        if result_num^=. and result_modifier not in (' ' 'NI' 'UN' 'OT') and result_unit not in (' ' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
        else known_test_result_num_unit=.;
        
        if result_num^=. and result_modifier not in (' ' 'NI' 'UN' 'OT') and 
            specimen_source not in (' ' 'NI' 'UN' 'OT' 'UNK_SUB' 'SMPLS' 'SPECIMEN') and 
           result_unit not in (' ' 'NI' 'UN' 'OT') then known_test_result_num_srce_unit=1;
        else known_test_result_num_srce_unit=.;
        
     end;
     else do;
        known_test=.;
        known_test_result=.;
        known_test_result_num=.;
        known_test_result_num_source=.;
        known_test_result_num_unit=.;
        known_test_result_num_srce_unit=.;
     end;
    
     * known test result range *;
     if known_test_result_num=1 and
        ((norm_modifier_low='EQ' and norm_modifier_high='EQ' and norm_range_low^=' ' and norm_range_high^=' ') or
        (norm_modifier_low in ('GT' 'GE')and norm_modifier_high='NO' and norm_range_low^=' ' and norm_range_high=' ') or
        (norm_modifier_high in ('LE' 'LT')and norm_modifier_low='NO' and norm_range_low=' ' and norm_range_high^=' ')) then
        known_test_result_num_range=1;
     else known_test_result_num_range=.;
run;

*- test for presence of RAW_LAB_NAME variable -*;
proc contents data=lab_result_cm noprint out=raw_lab_name;
run;

data _null_;
     set raw_lab_name(keep=name) end=eof;
     retain raw_lab_name 0;
     if name="RAW_LAB_NAME" then raw_lab_name=1;
     if eof then call symput("RAW_LAB_NAME",put(raw_lab_name,1.));
run;

%let savelab=lab_result_cm;

%elapsed(end);

********************************************************************************;
* LAB_L3_N
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=lab_l3_n&sufx;
    %elapsed(begin);

    data lab_n;
         set lab_result_cm(keep=patid lab_result_cm_id encounterid lookback5_flag);
         &subset;
    run;
    
    *- Macro for each variable -*;
    %enc_oneway(encdsn=lab_n,encvar=patid,ord=1)
    %enc_oneway(encdsn=lab_n,encvar=lab_result_cm_id,ord=2)
    %enc_oneway(encdsn=lab_n,encvar=encounterid,ord=3)

    data query;
         set query;
         dataset="LAB_RESULT_CM";
    run;

    *- Order variables -*;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, dataset, tag, all_n, 
                distinct_n, null_n
         from query;
    quit;

    %elapsed(end);

    *- Clear working directory -*;
    %clean(savedsn=&savelab);

%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=if lookback5_flag=1);

********************************************************************************;
* LAB_L3_RECORDC
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=lab_l3_recordc&sufx;
    %elapsed(begin);

    data lab_recordc;
         set lab_result_cm(keep=known_test: lookback5_flag);
         &subset;
    run;
    
        *- Macro for each variable -*;
    %enc_oneway(encdsn=lab_recordc,encvar=known_test,_nc=1,ord=1)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result,_nc=1,ord=2)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result_num,_nc=1,ord=3)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result_num_source,_nc=1,ord=4)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result_num_unit,_nc=1,ord=5)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result_num_srce_unit,_nc=1,ord=6)
    %enc_oneway(encdsn=lab_recordc,encvar=known_test_result_num_range,_nc=1,ord=7)

    *- Order variables -*;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, dataset, tag, all_n
         from query;
    quit;

    %elapsed(end);

    *- Clear working directory -*;
    %clean(savedsn=&savelab);

%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=if lookback5_flag=1);

********************************************************************************;
* LAB_L3_RAW_NAME 
********************************************************************************;
%if &raw_lab_name=1 %then %do;
    
%let qname=lab_l3_raw_name;
%elapsed(begin);

proc format;
     value $rawlab
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $100;
     set lab_result_cm(keep=raw_lab_name patid);

     if raw_lab_name in ("NI") then col1="ZZZA";
     else if raw_lab_name in ("UN") then col1="ZZZB";
     else if raw_lab_name in ("OT") then col1="ZZZC";
     else if raw_lab_name=" " then col1="ZZZD";
     else col1=raw_lab_name;

     keep col1 patid ;
run;

*- Derive statistics - encounter type -*;
proc means data=data noprint missing;
     class col1;
     output out=stats;
run;

*- Derive statistics - unique patient -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1;
     output out=pid_unique;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD") then raw_lab_name=col1;
     else raw_lab_name=put(col1,$rawlab.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package raw_lab_name record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, raw_lab_name, record_n,
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

%end;

********************************************************************************;
* LAB_L3_NAME - retired with V3.12
********************************************************************************;
/*
%let qname=lab_l3_name;
%elapsed(begin);

proc format;
     value $lab_name
       "A1C"="A1C"
       "CK"="CK"
       "CK_MB"="CK_MB"
       "CK_MBI"="CK_MBI"
       "CREATININE"="CREATININE"
       "HGB"="HGB"
       "LDL"="LDL"
       "INR"="INR"
       "TROP_I"="TROP_I"
       "TROP_T_QL"="TROP_T_QL"
       "TROP_T_QN"="TROP_T_QN"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set lab_result_cm(keep=lab_name patid) end=eof;

     if lab_name in ("A1C" "CK" "CK_MB" "CK_MBI" "CREATININE" "HGB" "LDL" "INR" 
                     "TROP_I" "TROP_T_QL" "TROP_T_QN") then col1=lab_name;
     else if lab_name in ("NI") then col1="ZZZA";
     else if lab_name in ("UN") then col1="ZZZB";
     else if lab_name in ("OT") then col1="ZZZC";
     else if lab_name=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid ;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_name.;
run;

*- Derive statistics - unique patient -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $lab_name.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_name=put(col1,$lab_name.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package lab_name record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_name, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);
*/
********************************************************************************;
* LAB_L3_NAME_LOINC - retired with V3.12
********************************************************************************;
/*%let qname=lab_l3_name_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length lab_name col1 $10;
     set lab_result_cm(keep=lab_name lab_loinc);

     if lab_name in ("A1C" "CK" "CK_MB" "CK_MBI" "CREATININE" "HGB" "LDL" "INR" 
                     "TROP_I" "TROP_T_QL" "TROP_T_QN") then labname=lab_name;
     else if lab_name in ("NI") then labname="ZZZA";
     else if lab_name in ("UN") then labname="ZZZB";
     else if lab_name in ("OT") then labname="ZZZC";
     else if lab_name=" " then labname="ZZZD";
     else labname="ZZZE";
    
     if lab_loinc^=" " then col1=lab_loinc;
     else if lab_loinc=" " then col1="ZZZA";
    
     keep labname col1;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class labname col1;
     output out=stats;
     format labname $lab_name. col1 $nullout.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     loinc=put(col1,$nullout.);
     lab_name=put(labname,$lab_name.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and labname^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package lab_name loinc record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_name, loinc, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);
*/
********************************************************************************;
* LAB_L3_LOINC_UNIT
********************************************************************************;
%let qname=lab_l3_loinc_unit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length labloinc col1 $10;
     set lab_result_cm(keep=lab_loinc result_unit);

     if lab_loinc^=" " then labloinc=lab_loinc;
     else if lab_loinc=" " then labloinc="ZZZA";
    
     if result_unit^=" " then col1=result_unit;
     else if result_unit=" " then col1="ZZZA";
    
     keep labloinc col1;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class labloinc col1;
     output out=stats;
     format labloinc col1 $nullout.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     result_unit=put(col1,$nullout.);
     lab_loinc=put(labloinc,$nullout.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and labloinc^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package lab_loinc result_unit record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_loinc, result_unit, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_SNOMED
********************************************************************************;
%let qname=lab_l3_snomed;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set lab_result_cm(keep=result_snomed patid);

     if result_snomed^=" " then col1=result_snomed;
     else if result_snomed=" " then col1="ZZZA";

     keep col1 patid ;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - unique patient -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then result_snomed=col1;
     else result_snomed=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package result_snomed record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_snomed, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LOINC
********************************************************************************;
%let qname=lab_l3_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set lab_result_cm(keep=lab_loinc patid);

     if lab_loinc^=" " then col1=lab_loinc;
     else if lab_loinc=" " then col1="ZZZA";

     keep col1 patid ;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - unique patient -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_loinc=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package lab_loinc record_n record_pct
          distinct_patid_n;
run;

*- DM not required to populate, need to know if all missing or not for later query  -*;
proc sql noprint;
     select count(*) into :_loincvalues from query
            where lab_loinc^="NULL or missing";
quit;    

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_loinc, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LOINC_SOURCE
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=lab_l3_loinc_source&sufx;
    %elapsed(begin);

    *- Sort lab data and expected specimen source data for merging -*;
    proc sort data=lab_loinc_ref(keep=lab_loinc exp_specimen_source) out=lab_loinc_ref;
         by lab_loinc;
    run;

    data lab_loinc;
         set lab_result_cm(keep=lab_loinc specimen_source lookback5_flag);
         &subset;

         lab_loinc=strip(upcase(lab_loinc));
         drop lookback5_flag;
    run;

    proc sort data=lab_loinc;
         by lab_loinc;
    run;

    *- Derive categorical variable -*;
    data data;
         length col1 $10;
         merge lab_loinc(in=l) lab_loinc_ref(in=r);
         by lab_loinc;
         if l and r;

         if lab_loinc^=" " then col1=lab_loinc;
         else if lab_loinc=" " then col1="ZZZA";

         keep specimen_source exp_specimen_source col1;
    run;

    *- Derive statistics -*;
    proc means data=data nway noprint missing;
         class specimen_source exp_specimen_source col1;
         output out=stats;
         format col1 $nullout.;
    run;

    *- Derive appropriate counts and variables -*;
    data query;
         length record_n $20;
         set stats;

         * call standard variables *;
         %stdvar

         * table values *;
         lab_loinc=put(col1,$nullout.);
         if specimen_source=" " then specimen_source="NULL or missing";
    
         * apply threshold *;
         %threshold(nullval=col1^="ZZZA")

         * counts *;
         record_n=strip(put(_freq_,threshold.));

         keep datamartid response_date query_package lab_loinc specimen_source 
              exp_specimen_source record_n;
    run;

    *- Order variables -*;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, lab_loinc, specimen_source, 
             exp_specimen_source, record_n
         from query;
    quit;

    %elapsed(end);

    *- Clear working directory -*;
    %clean(savedsn=&savelab);
%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=if lookback5_flag=1);

********************************************************************************;
* LAB_L3_DCGROUP
********************************************************************************;
%let qname=lab_l3_dcgroup;
%elapsed(begin);

*- Sort lab data and expected specimen source data for merging -*;
proc sort data=lab_dcgroup_ref(keep=lab_loinc dc_lab_group include_edc) 
     out=lab_dcgroup_ref;
     by lab_loinc;
run;

data lab_loinc;
     set lab_result_cm(keep=lab_loinc patid);
     if lab_loinc^=" ";

     lab_loinc=strip(upcase(lab_loinc));
run;

proc sort data=lab_loinc;
     by lab_loinc;
run;

*- Derive categorical variable -*;
data data;
     length col1 $100;
     merge lab_loinc(in=l) lab_dcgroup_ref(in=r);
     by lab_loinc;
     if l;

     if r then col1=strip(dc_lab_group)|| "~" || strip(include_edc);
     else col1="Unassigned~0";
    
     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $labdcgrp.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $labdcgrp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length include_edc $1 record_n distinct_patid_n $20 dc_lab_group $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     dc_lab_group=scan(put(col1,$labdcgrp.),1,'~');
*     if scan(col1,1,'~') not in ("ZZZA" "ZZZB") then dc_lab_group=scan(put(col1,$labdcgrp.),1,'~');
*     else if scan(col1,1,'~')="ZZZB" then dc_lab_group="Values outside of CDM specifications";
     include_edc=strip(scan(put(col1,$labdcgrp.),2,'~'));

     * apply threshold *;
     %threshold(nullval=scan(col1,1,'~')^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=scan(col1,1,'~')^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dc_lab_group record_n
          record_pct distinct_patid_n include_edc;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dc_lab_group, include_edc,
         record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_NAME_RUNIT - retired with V3.12
********************************************************************************;
/*%let qname=lab_l3_name_runit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length lab_name col1 $10;
     set lab_result_cm(keep=lab_name result_unit);

     if lab_name in ("A1C" "CK" "CK_MB" "CK_MBI" "CREATININE" "HGB" "LDL" "INR" 
                     "TROP_I" "TROP_T_QL" "TROP_T_QN") then labname=lab_name;
     else if lab_name in ("NI") then labname="ZZZA";
     else if lab_name in ("UN") then labname="ZZZB";
     else if lab_name in ("OT") then labname="ZZZC";
     else if lab_name=" " then labname="ZZZD";
     else labname="ZZZE";
    
     if result_unit^=" " then col1=result_unit;
     else if result_unit=" " then col1="ZZZA";
    
     keep labname col1;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class labname col1;
     output out=stats;
     format labname $lab_name. col1 $nullout.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     result_unit=put(col1,$nullout.);
     lab_name=put(labname,$lab_name.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and labname^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package lab_name result_unit record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_name, result_unit, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);
*/
********************************************************************************;
* LAB_L3_RDATE_Y - modified to remove LAB_NAME and LAB_LOINC with V3.12
********************************************************************************;
%let qname=lab_l3_rdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set lab_result_cm(keep=result_date patid);

     if result_date^=. then year=year(result_date);

     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     result_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package result_date record_n
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_date, 
            record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_RDATE_YM - modified to remove LAB_NAME with V3.12
********************************************************************************;
%let qname=lab_l3_rdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year_month 5.;
     set lab_result_cm(keep=result_date patid);

     * create a year and a year/month numeric variable *;
     if result_date^=. then year_month=(year(result_date)*100)+month(result_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;

     keep year_month patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class year_month;
     output out=stats;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class year_month;
     output out=pid_unique;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by year_month;

     * call standard variables *;
     %stdvar

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then result_date=put(year_month,null.);
     else result_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);

     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package result_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_date, record_n, 
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_SOURCE
********************************************************************************;
%let qname=lab_l3_source;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set lab_result_cm(keep=specimen_source);

     col1=put(specimen_source,$_source.);
     if specimen_source^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if specimen_source^=col1 then col1="ZZZE";
     end;

     keep col1 specimen_source;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 specimen_source $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then specimen_source=col1;
     else specimen_source=put(col1,$other.);
     short_yn="NA";

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package specimen_source short_yn
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, specimen_source, short_yn,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_UNIT
********************************************************************************;
%let qname=lab_l3_unit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set lab_result_cm(keep=result_unit);

     col1=put(result_unit,_unit.);
     if result_unit^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if result_unit^=col1 then col1="ZZZE";
     end;

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 result_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then result_unit=col1;
     else result_unit=put(col1,$other.);
     short_yn="NA";

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package result_unit short_yn 
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_unit, short_yn,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_PRIORITY
********************************************************************************;
%let qname=lab_l3_priority;
%elapsed(begin);

proc format;
     value $lab_pri
       "E"="E"
       "R"="R"
       "S"="S"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set lab_result_cm(keep=priority);

     if priority in ("E" "R" "S") then col1=priority;
     else if priority in ("NI") then col1="ZZZA";
     else if priority in ("UN") then col1="ZZZB";
     else if priority in ("OT") then col1="ZZZC";
     else if priority=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_pri.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     priority=put(col1,$lab_pri.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package priority record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, priority, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LOC
********************************************************************************;
%let qname=lab_l3_loc;
%elapsed(begin);

proc format;
     value $lab_loc
       "L"="L"
       "P"="P"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set lab_result_cm(keep=result_loc);

     if result_loc in ("L" "P") then col1=result_loc;
     else if result_loc in ("NI") then col1="ZZZA";
     else if result_loc in ("UN") then col1="ZZZB";
     else if result_loc in ("OT") then col1="ZZZC";
     else if result_loc=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_loc.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     result_loc=put(col1,$lab_loc.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package result_loc record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_loc, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_PX_TYPE
********************************************************************************;
%let qname=lab_l3_px_type;
%elapsed(begin);

proc format;
     value $lab_px
       "09"="09"
       "10"="10"
       "11"="11"
       "CH"="CH"
       "LC"="LC"
       "ND"="ND"
       "RE"="RE"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set lab_result_cm(keep=lab_px_type);

     if lab_px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") 
          then col1=lab_px_type;
     else if lab_px_type in ("NI") then col1="ZZZA";
     else if lab_px_type in ("UN") then col1="ZZZB";
     else if lab_px_type in ("OT") then col1="ZZZC";
     else if lab_px_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_px.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_px_type=put(col1,$lab_px.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package lab_px_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_px_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_PX_PXTYPE
********************************************************************************;
%let qname=lab_l3_px_pxtype;
%elapsed(begin);

proc format;
     value $pxtype
         "09"="09"
         "10"="10"
         "11"="11"
         "CH"="CH"
         "LC"="LC"
         "ND"="ND"
         "RE"="RE"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length pxtype col1 $15;
     set lab_result_cm(keep=patid lab_px lab_px_type);

     if lab_px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") 
            then pxtype=lab_px_type;
     else if lab_px_type in ("NI") then pxtype="ZZZA";
     else if lab_px_type in ("UN") then pxtype="ZZZB";
     else if lab_px_type in ("OT") then pxtype="ZZZC";
     else if lab_px_type=" " then pxtype="ZZZD";
     else pxtype="ZZZE";
    
     if lab_px^=" " then col1=lab_px;
     else if lab_px=" " then col1="ZZZA";
    
     keep pxtype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class col1 pxtype;
     output out=stats;
     format pxtype $pxtype. col1 $null.;
run;

*- Derive distinct patient id for each PDX/type -*;
proc sort data=data out=pid nodupkey;
     by col1 pxtype patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1 pxtype;
     output out=pid_unique;
     format pxtype $pxtype. col1 $null.;
run;

*- Format -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 pxtype;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_px=put(col1,$null.);
     lab_px_type=put(pxtype,$lab_px.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and pxtype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and pxtype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     keep datamartid response_date query_package lab_px_type lab_px record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_px, lab_px_type, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_QUAL
********************************************************************************;
%let qname=lab_l3_qual;
%elapsed(begin);

proc format;
     value $labqual
       "POSITIVE"="POSITIVE"
       "NEGATIVE"="NEGATIVE"
       "BORDERLINE"="BORDERLINE"
       "ELEVATED"="ELEVATED"
       "HIGH"="HIGH"
       "LOW"="LOW"
       "NORMAL"="NORMAL"
       "ABNORMAL"="ABNORMAL"
       "UNDETERMINED"="UNDETERMINED"
       "UNDETECTABLE"="UNDETECTABLE"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $20;
     set lab_result_cm(keep=result_qual);
    
     if result_qual in ("POSITIVE" "NEGATIVE" "BORDERLINE" "ELEVATED" "HIGH" "LOW"
                        "NORMAL" "ABNORMAL" "UNDETERMINED" "UNDETECTABLE") then col1=result_qual;
     else if result_qual in ("NI") then col1="ZZZA";
     else if result_qual in ("UN") then col1="ZZZB";
     else if result_qual in ("OT") then col1="ZZZC";
     else if result_qual=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $labqual.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     result_qual=put(col1,$labqual.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package result_qual record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_qual, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_MOD
********************************************************************************;
%let qname=lab_l3_mod;
%elapsed(begin);

proc format;
     value $lab_mod
       "EQ"="EQ"
       "GE"="GE"
       "GT"="GT"
       "LE"="LE"
       "LT"="LT"
       "TX"="TX"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set lab_result_cm(keep=result_modifier);

     if result_modifier in ("EQ" "GE" "GT" "LE" "LT" "TX") then col1=result_modifier;
     else if result_modifier in ("NI") then col1="ZZZA";
     else if result_modifier in ("UN") then col1="ZZZB";
     else if result_modifier in ("OT") then col1="ZZZC";
     else if result_modifier=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_mod.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     result_modifier=put(col1,$lab_mod.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package result_modifier record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, result_modifier, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LOW
********************************************************************************;
%let qname=lab_l3_low;
%elapsed(begin);

proc format;
     value $lab_low
       "EQ"="EQ"
       "GE"="GE"
       "GT"="GT"
       "NO"="NO"
       "NI"="NI"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set lab_result_cm(keep=norm_modifier_low);

     if norm_modifier_low in ("EQ" "GE" "GT" "NO" "NI") then col1=norm_modifier_low;
     else if norm_modifier_low in ("NI") then col1="ZZZA";
     else if norm_modifier_low in ("UN") then col1="ZZZB";
     else if norm_modifier_low in ("OT") then col1="ZZZC";
     else if norm_modifier_low=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_low.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     norm_modifier_low=put(col1,$lab_low.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package norm_modifier_low record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, norm_modifier_low, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_HIGH
********************************************************************************;
%let qname=lab_l3_high;
%elapsed(begin);

proc format;
     value $lab_high
       "EQ"="EQ"
       "LE"="LE"
       "LT"="LT"
       "NO"="NO"
       "NI"="NI"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set lab_result_cm(keep=norm_modifier_high) ;

     if norm_modifier_high in ("EQ" "LE" "LT" "NO" "NI") then col1=norm_modifier_high;
     else if norm_modifier_high in ("NI") then col1="ZZZA";
     else if norm_modifier_high in ("UN") then col1="ZZZB";
     else if norm_modifier_high in ("OT") then col1="ZZZC";
     else if norm_modifier_high=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_high.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     norm_modifier_high=put(col1,$lab_high.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package norm_modifier_high record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, norm_modifier_high, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LOINC_RESULT_NUM
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=lab_l3_loinc_result_num&sufx;
    %elapsed(begin);

    %if &_loincvalues>0 %then %do;

    data lab_loinc_result_num;
         set lab_result_cm(keep=lab_loinc result_num lookback5_flag);
         &subset;
    run;
    
    *- Derive statistics -*;
    proc means data=lab_loinc_result_num nway noprint;
         class lab_loinc;
         var result_num;
         output out=stats min=S01 p1=S02 p5=S03 p25=S04 mean=S05 median=S06 p75=S07
                          p95=S08 p99=S09 max=S10 n=S11 nmiss=S12;
    run;

    *- Check to see if any obs  -*;
    proc sql noprint;
         select count(*) into :nobs from stats;
    quit;

    *- If obs, continue through the rest of the query -*;
    %macro poss_nobs;
        %if &nobs>0 %then %do;
            data query;
                 set stats;

                 * call standard variables *;
                 %stdvar

                 * counts *;
                 min=compress(put(s01,16.1));
                 p1=compress(put(s02,16.1));
                 p5=compress(put(s03,16.1));
                 p25=compress(put(s04,16.1));
                 median=compress(put(s06,16.1));
                 p75=compress(put(s07,16.1));
                 p95=compress(put(s08,16.1));
                 p99=compress(put(s09,16.1));
                 max=compress(put(s10,16.1));
                 n=compress(put(s11,16.));
                 nmiss=compress(put(s12,16.));

                 keep datamartid response_date query_package lab_loinc min p: median
                      max n nmiss;
            run;
        %end;
        %else %if &nobs=0 %then %do;
             *- Create dummy dataset -*;
             data query;
                  array _char_ lab_loinc min p1 p5 p25 median p75 p95 p99 max n nmiss;

                 * call standard variables *;
                 %stdvar

                  if _n_>0 then delete;
             run;
        %end;
    %mend poss_nobs;
    %poss_nobs;

    %end;
    %else %do;
             *- Create dummy dataset -*;
             data query;
                  array _char_ lab_loinc min p1 p5 p25 median p75 p95 p99 max n nmiss;

                 * call standard variables *;
                 %stdvar

                  if _n_>0 then delete;
             run;
    %end;

    *- Order variables -*;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, lab_loinc, min, p1, p5, p25, 
                    median, p75, p95, p99, max, n, nmiss
         from query;
    quit;

    %elapsed(end);

    *- Clear working directory -*;
    %clean(savedsn=&savelab);
%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=if lookback5_flag=1);

********************************************************************************;
* LAB_L3_ABN
********************************************************************************;
%let qname=lab_l3_abn;
%elapsed(begin);

proc format;
     value $lab_abn
       "AB"="AB"
       "AH"="AH"
       "AL"="AL"
       "CH"="CH"
       "CL"="CL"
       "CR"="CR"
       "IN"="IN"
       "NL"="NL"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set lab_result_cm(keep=abn_ind) ;

     if abn_ind in ("AB" "AH" "AL" "CH" "CL" "CR" "IN" "NL") then col1=abn_ind;
     else if abn_ind in ("NI") then col1="ZZZA";
     else if abn_ind in ("UN") then col1="ZZZB";
     else if abn_ind in ("OT") then col1="ZZZC";
     else if abn_ind=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_abn.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     abn_ind=put(col1,$lab_abn.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package abn_ind record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, abn_ind, record_n, 
            record_pct
     from query;
quit;

%elapsed(b_or_e=end,last=Y);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_RSOURCE
********************************************************************************;
%let qname=lab_l3_rsource;
%elapsed(begin);

proc format;
     value $lrsource
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
         "OD"="OD"
         "PM"="PM"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $40;
     set lab_result_cm(keep=patid lab_result_source);

     if lab_result_source in ("BI" "CL" "DR" "OD" "PM") then col1=put(lab_result_source,$lrsource.);
     else if lab_result_source="NI" then col1="ZZZA";
     else if lab_result_source="UN" then col1="ZZZB";
     else if lab_result_source="OT" then col1="ZZZC";
     else if lab_result_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then lab_result_source=col1;
     else lab_result_source=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package lab_result_source
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_result_source, record_n, 
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

********************************************************************************;
* LAB_L3_LSOURCE
********************************************************************************;
%let qname=lab_l3_lsource;
%elapsed(begin);

proc format;
     value $llsource
         "IN"="IN"
         "LM"="LM"
         "HL"="HL"
         "DW"="DW"
         "PC"="PC"
         "DM"="DM"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $40;
     set lab_result_cm(keep=patid lab_loinc_source);

     if lab_loinc_source in ("IN" "LM" "HL" "DW" "PC" "DM") then col1=put(lab_loinc_source,$llsource.);
     else if lab_loinc_source="NI" then col1="ZZZA";
     else if lab_loinc_source="UN" then col1="ZZZB";
     else if lab_loinc_source="OT" then col1="ZZZC";
     else if lab_loinc_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then lab_loinc_source=col1;
     else lab_loinc_source=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package lab_loinc_source
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_loinc_source, record_n,
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savelab);

%end;

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
* Macro end
********************************************************************************;
%mend dc_lab;
%dc_lab;
