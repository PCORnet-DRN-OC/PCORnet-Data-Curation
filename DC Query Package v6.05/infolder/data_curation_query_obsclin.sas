*******************************************************************************
*  $Source: data_curation_query_obsclin $;
*    $Date: 2022/05/26
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V6.05 - OBS CLIN queries,
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_main.cpt)
*           3) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_main.log)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc_obsclin;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_%lowcase(&_grp).log" 
              lrecl=200;
proc printto log=qlog %if %upcase(&_grp)=LAB %then %do; new %end; ;
run ;

********************************************************************************;
* Prep vital codes
********************************************************************************;
data obsclin_codes;
     set obsclin_codes;
    
     obsclin_codec="'"||strip(obsclin_code)||"'";
run;

********************************************************************************;
* Bring in OBS_CLIN and compress it
********************************************************************************;
%if &_yobs_clin=1 %then %do;
%let qname=obs_clin;
%elapsed(begin);

*- Determine length of variables to avoid truncation -*;
proc contents data=pcordata.&qname out=_obsclin_length noprint;
run;

data _null_;
     set _obsclin_length;
    
     if name="OBSCLIN_CODE" then call symputx("_obsclin_code_length",max(length,15));
run;

data &qname(compress=yes);
     set pcordata.&qname;
     * remove records with loinc values not permissable *;
     if obsclin_type="LC" and obsclin_code in (&_excl_loinc) then delete;

     * restrict data to within lookback period *;
     if obsclin_start_date>=&lookback_dt or obsclin_start_date=.;

     * for mismatch query *;
     providerid=obsclin_providerid;

     * known test result *;
     if obsclin_code^=" " then do;
        known_test=1;

        if obsclin_result_num^=. and obsclin_result_modifier not in (' ' 'NI' 'UN' 'OT')  
           then known_test_result_num=1;
        else known_test_result_num=.;
        
        if ((obsclin_result_num^=.  and obsclin_result_modifier not in (' ' 'NI' 'UN' 'OT')) or 
           obsclin_result_qual not in (" " "NI" "UN" "OT")) 
           then known_test_result=1;
        else known_test_result=.;

        if .<obsclin_start_date<=&mxrefreshn and 
           ((obsclin_result_modifier not in (' ' 'NI' 'UN' 'OT') and obsclin_result_num^=.) or 
            obsclin_result_qual not in (" " "NI" "UN" "OT")) 
           then known_test_result_plausible=1;
        else known_test_result_plausible=.;

        if obsclin_result_num^=. and obsclin_result_modifier not in (' ' 'NI' 'UN' 'OT') and 
           obsclin_result_unit not in (' ' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
        else known_test_result_num_unit=.;
        
     end;
     else do;
        known_test=.;
        known_test_result=.;
        known_test_result_plausible=.;
        known_test_result_num=.;
        known_test_result_num_unit=.;
     end;

run;

%let saveobsclin=obs_clin;

%elapsed(end);

********************************************************************************;
* OBSCLIN_L3_N
********************************************************************************;
%let qname=obsclin_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=obs_clin,encvar=patid,ord=1)
%enc_oneway(encdsn=obs_clin,encvar=obsclinid,ord=2)
%enc_oneway(encdsn=obs_clin,encvar=encounterid,ord=3)
%enc_oneway(encdsn=obs_clin,encvar=obsclin_providerid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_RECORDC
********************************************************************************;
%let qname=obsclin_l3_recordc;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=obs_clin,encvar=known_test,_nc=1,ord=1)
%enc_oneway(encdsn=obs_clin,encvar=known_test_result,_nc=1,ord=2)
%enc_oneway(encdsn=obs_clin,encvar=known_test_result_num,_nc=1,ord=3)
%enc_oneway(encdsn=obs_clin,encvar=known_test_result_num_unit,_nc=1,ord=4)
%enc_oneway(encdsn=obs_clin,encvar=known_test_result_plausible,_nc=1,ord=5)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_MOD
********************************************************************************;
%let qname=obsclin_l3_mod;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set obs_clin(keep=obsclin_result_modifier);

     if obsclin_result_modifier in ("EQ" "GE" "GT" "LE" "LT" "TX") then col1=obsclin_result_modifier;
     else if obsclin_result_modifier in ("NI") then col1="ZZZA";
     else if obsclin_result_modifier in ("UN") then col1="ZZZB";
     else if obsclin_result_modifier in ("OT") then col1="ZZZC";
     else if obsclin_result_modifier=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $obs_mod.;
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
     obsclin_result_modifier=put(col1,$obs_mod.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsclin_result_modifier record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_result_modifier, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_ABN
********************************************************************************;
%let qname=obsclin_l3_abn;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set obs_clin(keep=obsclin_abn_ind) ;

     if obsclin_abn_ind in ("AB" "AH" "AL" "CH" "CL" "CR" "IN" "NL") then col1=obsclin_abn_ind;
     else if obsclin_abn_ind in ("NI") then col1="ZZZA";
     else if obsclin_abn_ind in ("UN") then col1="ZZZB";
     else if obsclin_abn_ind in ("OT") then col1="ZZZC";
     else if obsclin_abn_ind=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_abn.;
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
     obsclin_abn_ind=put(col1,$_abn.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsclin_abn_ind record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_abn_ind, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_QUAL
********************************************************************************;
%let qname=obsclin_l3_qual;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     set obs_clin(keep=obsclin_result_qual);
    
     if upcase(obsclin_result_qual) in ("LOW" "HIGH") then 
        col1="_"||strip(obsclin_result_qual);
     else if obsclin_result_qual=" " then col1="ZZZA";
     else col1=put(obsclin_result_qual,$_qual.);

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_qual.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 obsclin_result_qual $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     obsclin_result_qual=compress(put(col1,$nullout.),'_');

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsclin_result_qual record_n record_pct;
run;

proc sort data=query;
     by obsclin_result_qual;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_result_qual, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_RUNIT
********************************************************************************;
%let qname=obsclin_l3_runit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set obs_clin(keep=obsclin_result_unit);

     if obsclin_result_unit=" " then col1="ZZZA";
     else col1=put(obsclin_result_unit,$_unit.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_unit.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 obsclin_result_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     obsclin_result_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsclin_result_unit
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_result_unit,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_TYPE
********************************************************************************;
%let qname=obsclin_l3_type;
%elapsed(begin);

proc format;
     value $obsclintype
         "LC"="LC"
         "SM"="SM"
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
     set obs_clin(keep=obsclin_type);

     if obsclin_type in ("LC" "SM") then col1=obsclin_type;
     else if obsclin_type in ("NI") then col1="ZZZA";
     else if obsclin_type in ("UN") then col1="ZZZB";
     else if obsclin_type in ("OT") then col1="ZZZC";
     else if obsclin_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $obsclintype.;
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
     obsclin_type=put(col1,$obsclintype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsclin_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_CODE_TYPE
********************************************************************************;
%let qname=obsclin_l3_code_type;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length obsclintype $4;
     set obs_clin(keep=obsclin_type obsclin_code patid);

     if obsclin_type in ("LC" "SM") then obsclintype=obsclin_type;
     else if obsclin_type in ("NI") then obsclintype="ZZZA";
     else if obsclin_type in ("UN") then obsclintype="ZZZB";
     else if obsclin_type in ("OT") then obsclintype="ZZZC";
     else if obsclin_type=" " then obsclintype="ZZZD";
     else obsclintype="ZZZE";
    
     if obsclin_code^=" " then col1=obsclin_code;
     else if obsclin_code=" " then col1="ZZZA";
    
     keep obsclintype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 obsclintype/preloadfmt;
     output out=stats;
     format obsclintype $obsclintype. col1 $null.;
run;

*- Derive distinct patient id for each med type -*;
proc sort data=data out=pid nodupkey;
     by col1 obsclintype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 obsclintype/preloadfmt;
     output out=pid_unique;
     format obsclintype $obsclintype. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 obsclin_code $&_obsclin_code_length;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 obsclintype;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then obsclin_code=col1;
     else obsclin_code=put(col1,$null.);
     obsclin_type=put(obsclintype,$obsclintype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and obsclintype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and obsclintype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package obsclin_type obsclin_code
          record_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_code, obsclin_type, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_SOURCE
********************************************************************************;
%let qname=obsclin_l3_source;
%elapsed(begin);

proc format;
     value $obsource
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
         "HC"="HC"
         "HD"="HD"
         "PD"="PD"
         "PR"="PR"
         "RG"="RG"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set obs_clin(keep=obsclin_source);

     if upcase(obsclin_source) in ("BI" "CL" "DR" "HC" "HD" "PD" "PR" "RG") 
        then col1=obsclin_source;
     else if obsclin_source in ("NI") then col1="ZZZA";
     else if obsclin_source in ("UN") then col1="ZZZB";
     else if obsclin_source in ("OT") then col1="ZZZC";
     else if obsclin_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $obsource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     obsclin_source=put(col1,$obsource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsclin_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_SDATE_Y
********************************************************************************;
%let qname=obsclin_l3_sdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set obs_clin(keep=patid obsclin_start_date);

     if obsclin_start_date^=. then year=year(obsclin_start_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     keep col1 patid;
run;

*- Derive statistics - year -*;
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
     obsclin_start_date=put(col1,$null.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsclin_start_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_start_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_SDATE_YM
********************************************************************************;
%let qname=obsclin_l3_sdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set obs_clin(keep=patid obsclin_start_date);

     * create a year and a year/month numeric variable *;
     if obsclin_start_date^=. then do;
        year=year(obsclin_start_date);
        year_month=(year(obsclin_start_date)*100)+month(obsclin_start_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month patid;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
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
     output out=pid_unique n=n;
run;

*- Determine the first and last year/month of the actual data -*;
data minact(keep=year_month rename=(year_month=minact))
     maxact(keep=year_month rename=(year_month=maxact));
     set stats(where=(year_month<99999999)) end=eof;
     if _n_=1 then output minact;
     else if eof then output maxact;
run;

*- Derive the first and last year values from the actual data -*;    
proc means data=data(drop=year_month) nway completetypes noprint missing;
     var year;
     output out=dummy min=min max=max;
run;

*- Create a dummy dataset beginning with Jan of the first year, 
   extending to the Dec of the either the last year of the run-date year -*;
data dummy(keep=year_month _type_ _freq_);
     set dummy;
     * in case date is not populated *;
     if min=. then do;
        min=99999999;
        max=99999999;
     end;
     do y = min to max(max,year(today()));
        do m = 1 to 12;
            _type_=1;
            _freq_=0;
            year_month=(y*100)+m;
            output;
        end;
     end;
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats(in=s) pid_unique(rename=(_freq_=distinct_patid));
     by year_month;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n $20;
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or
            after the run-date or last actual year/month *;
     if year_month<minact or maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then obsclin_start_date=put(year_month,null.);
     else obsclin_start_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package obsclin_start_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_start_date, record_n,
             distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_HT
********************************************************************************;
%let qname=obsclin_l3_ht;
%elapsed(begin);

proc format;
     value ht_dist
     low - -.000000001="<0" 
        0-10="0-10"
       11-20="11-20"
       21-45="21-45"
       46-52="46-52"
       53-58="53-58"
       59-64="59-64"
       65-70="65-70"
       71-76="71-76"
       77-82="77-82"
       83-88="83-88"
       89-94="89-94"
       95-high=">=95"
             .="NULL or missing"
        ;
run;

proc sql noprint;
      select unique obsclin_codec into :_height_code separated by ' '  from obsclin_codes
      where concept="HEIGHT";
quit;

*- Derive categorical variable -*;
data data;
     length col1 3.;
     set obs_clin(keep=patid obsclin_code obsclin_result_num);
     if obsclin_code in (&_height_code);

     if obsclin_result_num^=. then col1=int(obsclin_result_num);
     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 ht_dist.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 ht_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;
     by col1;
run;

data query;
     length record_n distinct_patid_n $20;
     merge query
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;
    
     * call standard variables *;
     %stdvar

     * table values *;
     ht_group=put(col1,ht_dist.);
    
     * apply threshold *;
     %threshold(nullval=col1^=.)
     %threshold(_tvar=distinct_patid,nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     * create special sort variable *;
     if col1^=. then sortvar=col1;
     else if col1=. then sortvar=99999999;
    
     keep datamartid response_date query_package ht_group record_n record_pct
          distinct_patid_n sortvar;
run;

proc sort data=query;
     by sortvar;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, ht_group, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_HT_DIST
********************************************************************************;
%let qname=obsclin_l3_ht_dist;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     set obs_clin(keep=obsclin_code obsclin_result_num);
     if obsclin_code in (&_height_code);

     keep obsclin_result_num;
run;

*- Derive statistics -*;
proc means data=data nway noprint;
     var obsclin_result_num;
     output out=stats min=S01 mean=S05 median=S06 max=S10 n=S11 nmiss=S12;
run;

*- Check to see if any obs  -*;
proc sql noprint;
     select count(*) into :nobs from stats;
quit;

*- If obs, continue through the rest of the query -*;
%macro poss_nobs;
    %if &nobs>0 %then %do;
        *- Transpose to one variable/multiple rows -*;
        proc transpose data=stats out=query prefix=col;
             var S01 S05 S06 S10 S11 S12;
        run;
    %end;
    %else %if &nobs=0 %then %do;
         *- Create dummy dataset -*;
         data query;
              length _name_ $50;
              array _char_ _name_ ;
              array _num_ col1;
              if _n_>0 then delete;
         run;
    %end;
%mend poss_nobs;
%poss_nobs;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);

     * counts *;
     if stat in ("N" "NULL or missing") then record_n=strip(put(col1,threshold.));
     else record_n=compress(put(col1,16.1));

     keep datamartid response_date query_package stat record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
        datamartid, response_date, query_package, stat, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_WT
********************************************************************************;
%let qname=obsclin_l3_wt;
%elapsed(begin);

proc format;
     value wt_dist
     low - -.000000001="<0" 
            0-1="0-1"
            2-6="2-6"
           7-12="7-12"
          13-20="13-20"
          21-35="21-35"
          36-50="36-50"
          51-75="51-75"
         76-100="76-100"
        101-125="101-125"
        126-150="126-150"
        151-175="151-175"
        176-200="176-200"
        201-225="201-225"
        226-250="226-250"
        251-275="251-275"
        276-300="276-300"
        301-350="301-350"
       351-high=">350"
              .="NULL or missing"
        ;
run;

proc sql noprint;
      select unique obsclin_codec into :_weight_code separated by ' '  from obsclin_codes
      where concept="WEIGHT";
quit;

*- Derive categorical variable -*;
data data;
     length col1 3.;
     set obs_clin(keep=patid obsclin_code obsclin_result_num);
     if obsclin_code in (&_weight_code);

     if obsclin_result_num^=. then col1=ceil(obsclin_result_num);
     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 wt_dist.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 wt_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;
     by col1;
run;

data query;
     length record_n distinct_patid_n $20;
     merge query
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;
    
     * call standard variables *;
     %stdvar

     * table values *;
     wt_group=put(col1,wt_dist.);
    
     * apply threshold *;
     %threshold(nullval=col1^=.)
     %threshold(_tvar=distinct_patid,nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     * create special sort variable *;
     if col1^=. then sortvar=col1;
     else if col1=. then sortvar=99999999;
    
     keep datamartid response_date query_package wt_group record_n record_pct
          distinct_patid_n sortvar;
run;

proc sort data=query;
     by sortvar;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, wt_group, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_WT_DIST
********************************************************************************;
%let qname=obsclin_l3_wt_dist;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     set obs_clin(keep=obsclin_code obsclin_result_num);
     if obsclin_code in (&_weight_code);
     keep obsclin_result_num;
run;

*- Derive statistics -*;
proc means data=data nway noprint;
     var obsclin_result_num;
     output out=stats min=S01 mean=S05 median=S06 max=S10 n=S11 nmiss=S12;
run;

*- Check to see if any obs  -*;
proc sql noprint;
     select count(*) into :nobs from stats;
quit;

*- If obs, continue through the rest of the query -*;
%macro poss_nobs;
    %if &nobs>0 %then %do;
        *- Transpose to one variable/multiple rows -*;
        proc transpose data=stats out=query prefix=col;
             var S01 S05 S06 S10 S11 S12;
        run;
    %end;
    %else %if &nobs=0 %then %do;
         *- Create dummy dataset -*;
         data query;
              length _name_ $50;
              array _char_ _name_ ;
              array _num_ col1;
              if _n_>0 then delete;
         run;
    %end;
%mend poss_nobs;
%poss_nobs;

*- Format -*;
data query;
     length record_n $20;
     set query;
    
     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);
    
     * counts *;
     if stat in ("N" "NULL or missing") then record_n=strip(put(col1,threshold.));
     else record_n=compress(put(col1,16.1));

     keep datamartid response_date query_package stat record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, stat, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_DIASTOLIC
********************************************************************************;
%let qname=obsclin_l3_diastolic;
%elapsed(begin);

proc format;
     value dia_dist
       low - <40="<40"
          40-60="40-60"
          61-75="61-75"
          76-80="76-80"
          81-90="81-90"
         91-100="91-100"
        101-110="101-110"
        111-120="111-120"
       121-high=">120"
              .="NULL or missing"
        ;
run;

proc sql noprint;
      select unique obsclin_codec into :_dbp_code separated by ' '  from obsclin_codes
      where concept="DIASTOLIC_BLOOD_PRESSURE";
quit;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set obs_clin(keep=patid obsclin_code obsclin_result_num);
     if obsclin_code in (&_dbp_code);

     if obsclin_result_num^=. then do;
        if obsclin_result_num<40 then col1=39;
        else col1=ceil(obsclin_result_num);
     end;
     keep patid col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 dia_dist.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 dia_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;
     by col1;
run;

data query;
     length record_n distinct_patid_n $20;
     merge query
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     diastolic_group=put(col1,dia_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package diastolic_group record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, diastolic_group, record_n,
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_SYSTOLIC
********************************************************************************;
%let qname=obsclin_l3_systolic;
%elapsed(begin);

proc format;
     value sys_dist
       low - <40="<40"
          40-50="40-50"
          51-60="51-60"
          61-70="61-70"
          71-80="71-80"
          81-90="81-90"
         91-100="91-100"
        101-110="101-110"
        111-120="111-120"
        121-130="121-130"
        131-140="131-140"
        141-150="141-150"
        151-160="151-160"
        161-170="161-170"
        171-180="171-180"
        181-190="181-190"
        191-200="191-200"
        201-210="201-210"
       211-high=">210"
              .="NULL or missing"
        ;
run;

proc sql noprint;
      select unique obsclin_codec into :_sbp_code separated by ' '  from obsclin_codes
      where concept="SYSTOLIC_BLOOD_PRESSURE";
quit;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set obs_clin(keep=patid obsclin_code obsclin_result_num);
     if obsclin_code in (&_sbp_code);

     if obsclin_result_num^=. then do;
        if obsclin_result_num<40 then col1=39;
        else col1=ceil(obsclin_result_num);
     end;
     keep patid col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 sys_dist.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 sys_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;
     by col1;
run;

data query;
     length record_n distinct_patid_n $20;
     merge query
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     systolic_group=put(col1,sys_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package systolic_group record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, systolic_group, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_BMI
********************************************************************************;
%let qname=obsclin_l3_bmi;
%elapsed(begin);

proc format;
     value bmi_dist
     low - -.000000001="<0" 
            0-1="0-1"
            2-5="2-5"
           6-10="6-10"
          11-15="11-15"
          16-20="16-20"
          21-25="21-25"
          26-30="26-30"
          31-35="31-35"
          36-40="36-40"
          41-45="41-45"
          46-50="46-50"
        51-high=">50"
             .="NULL or missing"
        ;
run;

proc sql noprint;
      select unique obsclin_codec into :_bmi_code separated by ' '  from obsclin_codes
      where concept="BMI";
quit;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set obs_clin(keep=patid obsclin_code obsclin_result_num);
     if obsclin_code in (&_bmi_code);

     if obsclin_result_num^=. then col1=ceil(obsclin_result_num);
     keep patid col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 bmi_dist.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 bmi_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;
     by col1;
run;

data query;
     length record_n distinct_patid_n $20;
     merge query
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     bmi_group=put(col1,bmi_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package bmi_group record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, bmi_group, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsclin);

********************************************************************************;
* OBSCLIN_L3_CODE_UNIT
********************************************************************************;
%let qname=obsclin_l3_code_unit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length obsclinunit col1 $50;
     set obs_clin(keep=obsclin_type obsclin_code obsclin_result_unit
                  where=(obsclin_type="LC"));

     if obsclin_result_unit^=" " then obsclinunit=obsclin_result_unit;
     else if obsclin_result_unit=" " then obsclinunit="ZZZA";
    
     if obsclin_code^=" " then col1=obsclin_code;
     else if obsclin_code=" " then col1="ZZZA";
    
     keep obsclinunit col1;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class col1 obsclinunit;
     output out=stats;
     format obsclinunit col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 obsclin_result_unit $50 
            obsclin_code $&_obsclin_code_length;
     set stats;
     by col1 obsclinunit;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then obsclin_code=col1;
     else obsclin_code=put(col1,$null.);
     obsclin_result_unit=put(obsclinunit,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and obsclinunit^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package obsclin_result_unit obsclin_code
          record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsclin_code, 
            obsclin_result_unit, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

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
%mend dc_obsclin;
%dc_obsclin;
