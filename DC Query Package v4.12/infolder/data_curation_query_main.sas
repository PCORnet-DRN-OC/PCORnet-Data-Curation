/*******************************************************************************
*  $Source: data_curation_query_main $;
*    $Date: 2018/09/10
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V4.12 - all queries,
*                           excluding lab and cross table (XTBL)
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_main.pdf)
*           3) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_main.cpt)
*           4) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_main.log)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc_main;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_&_grp..log" lrecl=200;
proc printto log=qlog  new ;
run ;


********************************************************************************;
* Bring in DEATH and compress it
********************************************************************************;
%if &_ydeath=1 %then %do;
%let qname=death;
%elapsed(begin);

*- Determine concatonated length of variables used to determine DEATHID -*;
proc contents data=pcordata.death out=cont_death noprint;
run;

data _null_;
     set cont_death end=eof;
     retain ulength 0;
     if name in ("PATID" "DEATH_SOURCE") then ulength=ulength+length;
    
     * add 11 to ULENGTH (9 for date and 2 for delimiter *;
     if eof then call symput("_dulength",strip(put(ulength+11,8.)));
run;

data death(compress=yes drop=dthdate);
     length deathid $&_dulength;
     set pcordata.death;
    
     dthdate=put(death_date,date9.);
     deathid=cats(patid,'_',death_source);
run;

proc sort data=death;
     by patid death_date;
run;

%let savedeath=death;

%elapsed(end);

********************************************************************************;
* DEATH_L3_N
********************************************************************************;
%let qname=death_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=death,encvar=patid,ord=1)
%enc_oneway(encdsn=death,encvar=deathid,ord=2)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

********************************************************************************;
* DEATH_L3_DATE_Y
********************************************************************************;
%let qname=death_l3_date_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set death(keep=death_date patid);

     if death_date^=. then year=year(death_date);

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
     death_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

********************************************************************************;
* DEATH_L3_DATE_YM
********************************************************************************;
%let qname=death_l3_date_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set death(keep=death_date patid);

     * create a year and a year/month numeric variable *;
     if death_date^=. then do;
         year=year(death_date);
         year_month=(year(death_date)*100)+month(death_date);
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
     set stats end=eof;
     if _n_=1 then do;
        if year_month=99999999 then year_month=.;
        output minact;
     end;        
     if eof then do;
        if year_month=99999999 then year_month=.;
        output maxact;
     end;
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
     if min>0 then do;
         do y = min to max(max,year(today()));
            do m = 1 to 12;
                _type_=1;
                _freq_=0;
                year_month=(y*100)+m;
                output;
            end;
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
     if year_month<minact or .<maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then death_date=put(year_month,null.);
     else death_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package death_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

********************************************************************************;
* DEATH_L3_IMPUTE
********************************************************************************;
%let qname=death_l3_impute;
%elapsed(begin);

proc format;
     value $dimpute
          "B"="B"
          "D"="D"
          "M"="M"
          "N"="N"
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
     set death(keep=death_date_impute);

     if upcase(death_date_impute) in ("B" "D" "M" "N") then col1=death_date_impute;
     else if death_date_impute in ("NI") then col1="ZZZA";
     else if death_date_impute in ("UN") then col1="ZZZB";
     else if death_date_impute in ("OT") then col1="ZZZC";
     else if death_date_impute=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dimpute.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_date_impute=put(col1,$dimpute.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_date_impute record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_date_impute, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

********************************************************************************;
* DEATH_L3_SOURCE
********************************************************************************;
%let qname=death_l3_source;
%elapsed(begin);

proc format;
     value $dsource
          "D"="D"
          "L"="L"
          "N"="N"
          "S"="S"
          "T"="T"
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
     set death(keep=death_source);

     if upcase(death_source) in ("D" "L" "N" "S" "T") then col1=death_source;
     else if death_source in ("NI") then col1="ZZZA";
     else if death_source in ("UN") then col1="ZZZB";
     else if death_source in ("OT") then col1="ZZZC";
     else if death_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dsource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_source=put(col1,$dsource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

********************************************************************************;
* DEATH_L3_MATCH 
********************************************************************************;
%let qname=death_l3_match;
%elapsed(begin);

proc format;
     value $dmatch
          "E"="E"
          "F"="F"
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
     length col1 $4;
     set death(keep=death_match_confidence);

     if upcase(death_match_confidence) in ("E" "F" "P") then col1=death_match_confidence;
     else if death_match_confidence in ("NI") then col1="ZZZA";
     else if death_match_confidence in ("UN") then col1="ZZZB";
     else if death_match_confidence in ("OT") then col1="ZZZC";
     else if death_match_confidence=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dmatch.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_match_confidence=put(col1,$dmatch.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_match_confidence record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_match_confidence, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath);

%end;

********************************************************************************;
* Bring in DEATH_CAUSE and compress it
********************************************************************************;
%if &_ydeathc=1 %then %do;
%let qname=death_cause;
%elapsed(begin);

*- Determine concatonated length of variables used to determine DEATHID -*;
proc contents data=pcordata.death_cause out=cont_deathc noprint;
run;

data _null_;
     set cont_deathc end=eof;
     retain ulength 0;
     if name in ("PATID" "DEATH_CAUSE" "DEATH_CAUSE_CODE" "DEATH_CAUSE_TYPE" 
                 "DEATH_CAUSE_SOURCE") then ulength=ulength+length;
    
     * add ULENGTH (5 delimiter) *;
     if eof then call symput("_dculength",strip(put(ulength+5,8.)));
run;

data death_cause(compress=yes);
     length deathcid $&_dculength;
     set pcordata.death_cause;

     deathcid=cats(patid,'_',death_cause,'_',death_cause_code,'_',death_cause_type,'_',death_cause_source);
run;

proc sort data=death_cause;
     by patid;
run;

%let savedeathc=death_cause;

%elapsed(end);

********************************************************************************;
* DEATHC_L3_N
********************************************************************************;
%let qname=deathc_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=death_cause,encvar=patid,ord=1)
%enc_oneway(encdsn=death_cause,encvar=death_cause,ord=2)
%enc_oneway(encdsn=death_cause,encvar=deathcid,ord=3)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedeathc);

********************************************************************************;
* DEATHC_L3_CODE
********************************************************************************;
%let qname=deathc_l3_code;
%elapsed(begin);

proc format;
     value $dccode
         "09"="09"
         "10"="10"
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
     set death_cause(keep=death_cause_code);

     if upcase(death_cause_code) in ("09" "10") then col1=death_cause_code;
     else if death_cause_code in ("NI") then col1="ZZZA";
     else if death_cause_code in ("UN") then col1="ZZZB";
     else if death_cause_code in ("OT") then col1="ZZZC";
     else if death_cause_code=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dccode.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_cause_code=put(col1,$dccode.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_cause_code record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_cause_code, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedeathc);

********************************************************************************;
* DEATHC_L3_TYPE
********************************************************************************;
%let qname=deathc_l3_type;
%elapsed(begin);

proc format;
     value $dctype
          "C"="C"
          "I"="I"
          "O"="O"
          "U"="U"
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
     set death_cause(keep=death_cause_type);

     if upcase(death_cause_type) in ("C" "I" "O" "U") then col1=death_cause_type;
     else if death_cause_type in ("NI") then col1="ZZZA";
     else if death_cause_type in ("UN") then col1="ZZZB";
     else if death_cause_type in ("OT") then col1="ZZZC";
     else if death_cause_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dctype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_cause_type=put(col1,$dctype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_cause_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_cause_type, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedeathc);

********************************************************************************;
* DEATHC_L3_SOURCE
********************************************************************************;
%let qname=deathc_l3_source;
%elapsed(begin);

proc format;
     value $dcsource
          "D"="D"
          "L"="L"
          "N"="N"
          "S"="S"
          "T"="T"
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
     set death_cause(keep=death_cause_source);

     if upcase(death_cause_source) in ("D" "L" "N" "S" "T") then col1=death_cause_source;
     else if death_cause_source in ("NI") then col1="ZZZA";
     else if death_cause_source in ("UN") then col1="ZZZB";
     else if death_cause_source in ("OT") then col1="ZZZC";
     else if death_cause_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dcsource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_cause_source=put(col1,$dcsource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_cause_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_cause_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedeathc);

********************************************************************************;
* DEATHC_L3_CONF
********************************************************************************;
%let qname=deathc_l3_conf;
%elapsed(begin);

proc format;
     value $dcconf
          "E"="E"
          "F"="F"
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
     length col1 $4;
     set death_cause(keep=death_cause_confidence);

     if upcase(death_cause_confidence) in ("E" "F" "P") then col1=death_cause_confidence;
     else if death_cause_confidence in ("NI") then col1="ZZZA";
     else if death_cause_confidence in ("UN") then col1="ZZZB";
     else if death_cause_confidence in ("OT") then col1="ZZZC";
     else if death_cause_confidence=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dcconf.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     death_cause_confidence=put(col1,$dcconf.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package death_cause_confidence record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_cause_confidence, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedeathc);


%end;

********************************************************************************;
* Bring in DEMOGRAPHIC and compress it
********************************************************************************;
%if &_ydemographic=1 %then %do;
%let qname=demographic;
%elapsed(begin);

data demographic(compress=yes);
     set pcordata.demographic;
run;

proc sort data=demographic;
     by patid;
run;

%let savedemog=demographic;

%elapsed(end);

********************************************************************************;
* DEM_L3_N
********************************************************************************;
%let qname=dem_l3_n;
%elapsed(begin);

*- Derive appropriate counts and variables -*;
data query;
     length all_n distinct_n null_n $20 dataset $25;
     set demographic(keep=patid) end=eof;
     by patid;

     * call standard variables *;
     %stdvar

     * table values *;
     dataset="DEMOGRAPHIC";
     tag="PATID";

     * counts *;
     retain _all_n _distinct_n _null_n 0;
     if patid=" " then _null_n=_null_n+1;
     else do;
        _all_n=_all_n+1;
        if first.patid then _distinct_n=_distinct_n+1;
     end;

     * output *;
     if eof then do;
        all_n=strip(put(_all_n,threshold.));
        distinct_n=strip(put(_distinct_n,threshold.));
        null_n=strip(put(_null_n,threshold.));
        output;
     end;

     keep datamartid response_date query_package dataset tag all_n distinct_n 
          null_n;
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
%clean(savedsn=&savedeath &savedemog);

********************************************************************************;
* DEM_L3_AGEYRSDIST1
********************************************************************************;
%if &_ydeath=1 %then %do;

%let qname=dem_l3_ageyrsdist1;
%elapsed(begin);

*- Get earliest death date per subject -*;
data death_age(keep=patid death_date);
     set death(where=(death_date^=.));
     by patid death_date;
     if first.patid;
run;

*- Derive statistical variable -*;
data data;
     length age 3.;
     merge demographic(in=d keep=patid birth_date) death_age;
     by patid;
     if d;

     if birth_date^=. then age=int(yrdif(birth_date,min(death_date,today())));
     keep age;
run;

*- Derive statistics -*;
proc means data=data nway noprint;
     var age;
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
     else record_n=compress(put(col1,16.));

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
%clean(savedsn=&savedeath &savedemog);

********************************************************************************;
* DEM_L3_AGEYRSDIST2
********************************************************************************;
%let qname=dem_l3_ageyrsdist2;
%elapsed(begin);

proc format;
     value ageyrs2dist
        low - <0="<0 yrs"
        0-1="0-1 yrs"
        2-4="2-4 yrs"
        5-9="5-9 yrs"
       10-14="10-14 yrs"
       15-18="15-18 yrs"
       19-21="19-21 yrs"
       22-44="22-44 yrs"
       45-64="45-64 yrs"
       65-74="65-74 yrs"
       75-89="75-89 yrs"
       90-9998=">89 yrs"
      9999="NULL or missing"
        ;
run;

*- Get earliest death date per subject -*;
data death_age(keep=patid death_date);
     set death(where=(death_date^=.));
     by patid death_date;
     if first.patid;
run;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     merge demographic(in=d keep=patid birth_date) death_age;
     by patid;
     if d;

     * set missing to impossible high value for categorization *;
     if birth_date^=. then col1=int(yrdif(birth_date,min(death_date,today())));
     else if birth_date=. then col1=9999;
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 ageyrs2dist.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     age_group=put(col1,ageyrs2dist.);

     * apply threshold *;
     %threshold(nullval=col1^=9999)
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package age_group record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, age_group, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

%end;
********************************************************************************;
* DEM_L3_HISPDIST
********************************************************************************;
%let qname=dem_l3_hispdist;
%elapsed(begin);

proc format;
     value $hisp
          "N"="N"
          "R"="R"
          "Y"="Y"
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
     set demographic(keep=hispanic);

     if upcase(hispanic) in ("N" "R" "Y") then col1=hispanic;
     else if hispanic in ("NI") then col1="ZZZA";
     else if hispanic in ("UN") then col1="ZZZB";
     else if hispanic in ("OT") then col1="ZZZC";
     else if hispanic=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $hisp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     hispanic=put(col1,$hisp.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package hispanic record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, hispanic, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

********************************************************************************;
* DEM_L3_RACEDIST
********************************************************************************;
%let qname=dem_l3_racedist;
%elapsed(begin);

proc format;
     value $race
         "01"="01"
         "02"="02"
         "03"="03"
         "04"="04"
         "05"="05"
         "06"="06"
         "07"="07"
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
     set demographic(keep=race);

     if upcase(race) in ("01" "02" "03" "04" "05" "06" "07") then col1=race;
     else if race in ("NI") then col1="ZZZA";
     else if race in ("UN") then col1="ZZZB";
     else if race in ("OT") then col1="ZZZC";
     else if race=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $race.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     race=put(col1,$race.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package race record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, race, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

********************************************************************************;
* DEM_L3_SEXDIST
********************************************************************************;
%let qname=dem_l3_sexdist;
%elapsed(begin);

proc format;
     value $sex
          "A"="A"
          "F"="F"
          "M"="M"
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
     set demographic(keep=sex);

     if upcase(sex) in ("A" "F" "M") then col1=sex;
     else if sex in ("NI") then col1="ZZZA";
     else if sex in ("UN") then col1="ZZZB";
     else if sex in ("OT") then col1="ZZZC";
     else if sex=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $sex.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     sex=put(col1,$sex.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package sex record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, sex, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

********************************************************************************;
* DEM_L3_GENDERDIST
********************************************************************************;
%let qname=dem_l3_genderdist;
%elapsed(begin);

proc format;
     value $gender
          "DC"="DC"
          "F"="F"
          "GQ"="GQ"
          "M"="M"
          "MU"="MU"
          "SE"="SE"
          "TF"="TF"
          "TM"="TM"
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
     set demographic(keep=gender_identity);

     if upcase(gender_identity) in ("DC" "F" "GQ" "M" "MU" "SE" "TF" "TM") 
        then col1=gender_identity;
     else if gender_identity in ("NI") then col1="ZZZA";
     else if gender_identity in ("UN") then col1="ZZZB";
     else if gender_identity in ("OT") then col1="ZZZC";
     else if gender_identity=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $gender.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     gender_identity=put(col1,$gender.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package gender_identity record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, gender_identity, record_n,
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

********************************************************************************;
* DEM_L3_PATPREFLANG
********************************************************************************;
%let qname=dem_l3_patpreflang;
%elapsed(begin);

data data;
     length col1 $4;
     set demographic(keep=pat_pref_language_spoken);

     col1=put(pat_pref_language_spoken,$preflang.);
     if pat_pref_language_spoken^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if pat_pref_language_spoken^=col1 then col1="ZZZE";
     end;

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 pat_pref_language_spoken $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then pat_pref_language_spoken=col1;
     else pat_pref_language_spoken=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pat_pref_language_spoken
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pat_pref_language_spoken, 
         record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedemog);

********************************************************************************;
* DEM_L3_ORIENTDIST
********************************************************************************;
%let qname=dem_l3_orientdist;
%elapsed(begin);

proc format;
     value $sexual
          "AS"="AS"
          "BI"="BI"
          "DC"="DC"
          "GA"="GA"
          "LE"="LE"
          "MU"="MU"
          "QS"="QS"
          "QU"="QU"
          "SE"="SE"
          "ST"="ST"
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
     set demographic(keep=sexual_orientation);

     if upcase(sexual_orientation) in ("AS" "BI" "DC" "GA" "LE" "MU" "QS" "QU" "SE" "ST") 
        then col1=sexual_orientation;
     else if sexual_orientation in ("NI") then col1="ZZZA";
     else if sexual_orientation in ("UN") then col1="ZZZB";
     else if sexual_orientation in ("OT") then col1="ZZZC";
     else if sexual_orientation=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $sexual.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     sexual_orientation=put(col1,$sexual.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package sexual_orientation record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, sexual_orientation, record_n,
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in ENCOUNTER and compress it
********************************************************************************;
%if &_yencounter=1 %then %do;
%let qname=encounter;
%elapsed(begin);

%global _providlength;

data encounter(compress=yes);
     set pcordata.encounter;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;
run;

%let saveenc=encounter;
    
%elapsed(end);

********************************************************************************;
* ENC_L3_N
********************************************************************************;
%let qname=enc_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=encounter,encvar=encounterid,ord=1)
%enc_oneway(encdsn=encounter,encvar=patid,ord=2)
%enc_oneway(encdsn=encounter,encvar=providerid,ord=3)
%enc_oneway(encdsn=encounter,encvar=facilityid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_N_VISIT - retired with V3.02
********************************************************************************;
/*%let qname=enc_l3_n_visit;

*- Derive appropriate counts - visits -*;
data all_visit(keep=all_n) distinct(keep=distinct_visit);
     length all_n $20 distinct_visit $200;
     set encounter(keep=patid encounterid admit_date providerid enc_type) end=eof;

     * retain count of all records *;
     retain _all_n 0;

     * count and output *;
     if cmiss(patid,encounterid,admit_date,providerid,enc_type)=0 then do;
        _all_n=_all_n+1;
         * create a variable that represents a unique visit *;
         distinct_visit=strip(patid)||strip(encounterid)||put(admit_date,yymmdd8.)||strip(providerid)||strip(enc_type);
        output distinct;
     end;
     if eof then do;
        all_n=strip(put(_all_n,16.));
        output all_visit;
     end;

run;
    
*- Derive count of distinct visit -*;
proc sort data=distinct out=all_distinct nodupkey;
     by distinct_visit;
run;

* Use number of observations for count -*;
proc contents data=all_distinct out=all_distinct_n noprint;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_n $20 dataset $25;
     if _n_=1 then set all_distinct_n(keep=nobs);
     set all_visit;

     * call standard variables *;
     %stdvar

     * table values *;
     dataset="ENCOUNTER";
     tag="VISIT";
    
     * counts *;
     distinct_n=strip(put(nobs,16.));
    
     keep datamartid response_date query_package dataset tag all_n distinct_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n
     from query;
quit;

*- Clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);
*/

********************************************************************************;
* ENC_L3_ADMSRC
********************************************************************************;
%let qname=enc_l3_admsrc;
%elapsed(begin);

proc format;
     value $adm_src
         "AF"="AF"
         "AL"="AL"
         "AV"="AV"
         "ED"="ED"
         "HH"="HH"
         "HO"="HO"
         "HS"="HS"
         "IH"="IH"
         "IP"="IP"
         "NH"="NH"
         "RH"="RH"
         "RS"="RS"
         "SN"="SN"
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
     set encounter(keep=admitting_source);

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'HH' 'HO' 'HS' 'IH' 'IP' 'NH' 
         'RH' 'RS' 'SN') then col1=admitting_source;
     else if admitting_source in ("NI") then col1="ZZZA";
     else if admitting_source in ("UN") then col1="ZZZB";
     else if admitting_source in ("OT") then col1="ZZZC";
     else if admitting_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $adm_src.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     admitting_source=put(col1,$adm_src.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package admitting_source 
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admitting_source, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_ADMSRC
********************************************************************************;
%let qname=enc_l3_enctype_admsrc;
%elapsed(begin);

proc format;
     value $adm_src
         "AF"="AF"
         "AL"="AL"
         "AV"="AV"
         "ED"="ED"
         "HH"="HH"
         "HO"="HO"
         "HS"="HS"
         "IH"="IH"
         "IP"="IP"
         "NH"="NH"
         "RH"="RH"
         "RS"="RS"
         "SN"="SN"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type admitting_source);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'HH' 'HO' 'HS' 'IH' 'IP' 'NH' 'RH' 
         'RS' 'SN') then col1=admitting_source;
     else if admitting_source in ("NI") then col1="ZZZA";
     else if admitting_source in ("UN") then col1="ZZZB";
     else if admitting_source in ("OT") then col1="ZZZC";
     else if admitting_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1 enctype;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $adm_src.;
run;
ods listing;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=3));

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
     admitting_source=put(col1,$adm_src.);

     * apply threshold *;
     %threshold(type=3,nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package enc_type admitting_source
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admitting_source, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ADATE_Y
********************************************************************************;
%let qname=enc_l3_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set encounter(keep=patid admit_date);

     if admit_date^=. then year=year(admit_date);
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
     admit_date=put(col1,$null.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package admit_date record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ADATE_YM
********************************************************************************;
%let qname=enc_l3_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set encounter(keep=admit_date);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then do;
        year=year(admit_date);
        year_month=(year(admit_date)*100)+month(admit_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     merge dummy 
           stats(in=s where=(_type_=1));
     by year_month;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n $20;
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats;

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_ADATE_Y
********************************************************************************;
%let qname=enc_l3_enctype_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=admit_date enc_type patid);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then year=year(admit_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep col1 enctype patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class col1;
     output out=stats;
     format col1 $null. enctype $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by enctype col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class enctype/preloadfmt;
     class col1;
     output out=pid_unique;
     format col1 $null. enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by enctype col1;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
     admit_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type admit_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=enc_l3_enctype_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set encounter(keep=admit_date enc_type patid);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by enctype year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=pid_unique;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by enctype year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);

     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type admit_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DDATE_Y
********************************************************************************;
%let qname=enc_l3_ddate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set encounter(keep=discharge_date patid);

     if discharge_date^=. then year=year(discharge_date);

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
     discharge_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package discharge_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, discharge_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DDATE_YM
********************************************************************************;
%let qname=enc_l3_ddate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set encounter(keep=discharge_date);

     * create a year and a year/month numeric variable *;
     if discharge_date^=. then do;
         year=year(discharge_date);
         year_month=(year(discharge_date)*100)+month(discharge_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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

     * loop if discharge date is populated *;
     if nmiss(min,max)=0 then do;
         do y = min to max(max,year(today()));
            do m = 1 to 12;
                _type_=1;
                _freq_=0;
                year_month=(y*100)+m;
                output;
            end;
         end;
     end;
     * loop if discharge date is not populated *;
     else do;
        _type_=1;
        _freq_=0;
        year_month=.;
     end;
     * macro variable to test presence of discharge date *;
     call symput("minact_present",strip(put(nmiss(min,max),8.)));
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats(in=s);
     by year_month;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n $20;
     %if &minact_present=0 %then %do;
         if _n_=1 then set minact;
         if _n_=1 then set maxact;
     %end;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or
            after the run-date or last actual year/month *;
     %if &minact_present=0 %then %do;
         if year_month<minact or maxact<year_month<99999999 then delete;
     %end;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then discharge_date=put(year_month,null.);
     else discharge_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package discharge_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, discharge_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_DDATE_YM
********************************************************************************;
%let qname=enc_l3_enctype_ddate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set encounter(keep=discharge_date enc_type patid);

     * create a year and a year/month numeric variable *;
     if discharge_date^=. then 
            year_month=(year(discharge_date)*100)+month(discharge_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by enctype year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=pid_unique;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by enctype year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then discharge_date=put(year_month,null.);
     else discharge_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type discharge_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, discharge_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DISDISP
********************************************************************************;
%let qname=enc_l3_disdisp;
%elapsed(begin);

proc format;
     value $disdisp
          "A"="A"
          "E"="E"
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
     set encounter(keep=discharge_disposition);

     if discharge_disposition in ('A' 'E') then col1=discharge_disposition;
     else if discharge_disposition in ("NI") then col1="ZZZA";
     else if discharge_disposition in ("UN") then col1="ZZZB";
     else if discharge_disposition in ("OT") then col1="ZZZC";
     else if discharge_disposition=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $disdisp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     discharge_disposition=put(col1,$disdisp.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package discharge_disposition
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, discharge_disposition, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_DISDISP
********************************************************************************;
%let qname=enc_l3_enctype_disdisp;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_disposition);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     if discharge_disposition in ('A' 'E') then col1=discharge_disposition;
     else if discharge_disposition in ("NI") then col1="ZZZA";
     else if discharge_disposition in ("UN") then col1="ZZZB";
     else if discharge_disposition in ("OT") then col1="ZZZC";
     else if discharge_disposition=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1 enctype;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $disdisp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=3));

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
     discharge_disposition=put(col1,$disdisp.);

     * apply threshold *;
     %threshold(type=3,nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);

     keep datamartid response_date query_package enc_type discharge_disposition
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, 
            discharge_disposition, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DISSTAT
********************************************************************************;
%let qname=enc_l3_disstat;
%elapsed(begin);

proc format;
     value $disstat
         "AF"="AF"
         "AL"="AL"
         "AM"="AM"
         "AW"="AW"
         "EX"="EX"
         "HH"="HH"
         "HO"="HO"
         "HS"="HS"
         "IP"="IP"
         "NH"="NH"
         "RH"="RH"
         "RS"="RS"
         "SH"="SH"
         "SN"="SN"
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
     set encounter(keep=discharge_status);

     if discharge_status in ('AF' 'AL' 'AM' 'AW' 'EX' 'HH' 'HO' 'HS' 'IP' 'NH' 
         'RH' 'RS' 'SH' 'SN') then col1=discharge_status;
     else if discharge_status in ("NI") then col1="ZZZA";
     else if discharge_status in ("UN") then col1="ZZZB";
     else if discharge_status in ("OT") then col1="ZZZC";
     else if discharge_status=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $disstat.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     discharge_status=put(col1,$disstat.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package discharge_status record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, discharge_status, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_DISSTAT
********************************************************************************;
%let qname=enc_l3_enctype_disstat;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_status);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     if discharge_status in ('AF' 'AL' 'AM' 'AW' 'EX' 'HH' 'HO' 'HS' 'IP' 'NH' 
         'RH' 'RS' 'SH' 'SN') then col1=discharge_status;
     else if discharge_status in ("NI") then col1="ZZZA";
     else if discharge_status in ("UN") then col1="ZZZB";
     else if discharge_status in ("OT") then col1="ZZZC";
     else if discharge_status=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1 enctype;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $disstat.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=3));

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
     discharge_status=put(col1,$disstat.);

     * apply threshold *;
     %threshold(type=3,nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package enc_type discharge_status
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, discharge_status, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DRG
********************************************************************************;
%let qname=enc_l3_drg;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set encounter(keep=drg patid);

     if length(drg)=3 then col1=drg;
     else if drg=" " then col1="ZZZA";
     else delete;

     keep col1 patid;
run;

*- Derive statistics -*;
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
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1))
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     drg=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package drg record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, drg, record_n, record_pct, 
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DRG_TYPE
********************************************************************************;
%let qname=enc_l3_drg_type;
%elapsed(begin);

proc format;
     value $drgtype
         "01"="01"
         "02"="02"
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
     set encounter(keep=drg_type);

     if drg_type in ("01" "02") then col1=drg_type;
     else if drg_type in ("NI") then col1="ZZZA";
     else if drg_type in ("UN") then col1="ZZZB";
     else if drg_type in ("OT") then col1="ZZZC";
     else if drg_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $drgtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     drg_type=put(col1,$drgtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package drg_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, drg_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE_DRG
********************************************************************************;
%let qname=enc_l3_enctype_drg;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type drg);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if length(drg)=3 then col1=drg;
     else if drg=" " then col1="ZZZA";
     else delete;
    
     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=3));

     * call standard variables *;
     %stdvar

     * table values *;
     drg=put(col1,$null.);
     enc_type=put(enctype,$enc_typ.);

     * apply threshold *;
     %threshold(type=3,nullval=col1^="ZZZA" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package enc_type drg record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, drg, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_ENCTYPE
********************************************************************************;
%let qname=enc_l3_enctype;
%elapsed(begin);

*- Determine concatonated length of variables used to determine UNIQUE_VISIT -*;
proc contents data=encounter out=cont_enc noprint;
run;

data _null_;
     set cont_enc end=eof;
     retain ulength 0;
     if name in ("PATID" "ENCOUNTERID" "PROVIDERID") then ulength=ulength+length;
    
     * add 12 to ULENGTH (9 for date and 3 for delimiter) *;
     if eof then call symput("_ulength",strip(put(ulength+12,8.)));
run;

*- Derive categorical variable -*;
data data;
     length col1 $4 _unique_visit $&_ulength;
     set encounter(keep=enc_type patid encounterid admit_date providerid:) end=eof;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     * create UNIQUE_VISIT and determine the max length needed *;
     retain maxlength_uv 0;
     if cmiss(patid,admit_date,enc_type,providerid)=0 then do;
        _unique_visit=compress(patid)|| "/" || compress(enc_type) || "/" || 
                      put(admit_date,date9.) || "/" || compress(providerid);
        maxlength_uv=max(maxlength_uv,length(_unique_visit));
     end;

     * create macro variable to re-size UNIQUE_VISIT *;
     if eof then do;
            call symput("_maxlen_uv",strip(put(max(maxlength_uv,1),8.)));
     end;

     keep col1 _unique_visit patid;
run;

*- Re-establish length of UNIQUE_VISIT to make it as small as possible -*;
data data;
     length unique_visit $&_maxlen_uv;
     set data;
     unique_visit=_unique_visit;
     drop _unique_visit;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $enc_typ.;
run;

*- Derive statistics - unique visit -*;
proc sort data=data(drop=patid) out=visit nodupkey;
     by col1 unique_visit;
     where unique_visit^=' ';
run;

proc means data=visit nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=visit_unique;
     format col1 $enc_typ.;
run;

*- Derive statistics - unique patient -*;
proc sort data=data(drop=unique_visit) out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $enc_typ.;
run;

*- Derive statistics - eligible patient -*;
proc sort data=data(drop=patid) out=elig ;
     by col1 unique_visit;
     where unique_visit^=' ';
run;

proc means data=elig nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=elig_unique;
     format col1 $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n distinct_visit_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           visit_unique(rename=(_freq_=distinct_visit))
           elig_unique(rename=(_freq_=elig_record))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(col1,$enc_typ.);

     * set missing count to zero *;
     if distinct_visit=. then distinct_visit=0;
     if elig_record=. then elig_record=0;

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_visit,nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_elig,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     distinct_visit_n=strip(put(distinct_visit,threshold.));
     elig_record_n=strip(put(elig_record,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enc_type record_n record_pct
          distinct_visit_n distinct_patid_n elig_record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, record_n, 
            record_pct, distinct_visit_n, distinct_patid_n, elig_record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DASH1
********************************************************************************;
%let qname=enc_l3_dash1;
%elapsed(begin);

*- Data with legitimate date -*;
data data;
     set encounter(keep=patid admit_date);
     if admit_date^=.;

     keep patid admit_date;
run;

*- Determine maximum date -*;
proc means data=data noprint;
     var admit_date;
     output out=max_admit max=max_admit;
run;

*- Assign a period to every record based upon maximum date -*;
data period(keep=period patid);
     length period 3.;
     if _n_=1 then set max_admit;
     set data;

     * eliminate future dates from consideration and use run date *;
     if max_admit>today() then max_admit=today();

     * reset any possible leap year to the 28th *;
     if month(max_admit)=2 and day(max_admit)=29 then 
        max_admit=mdy(month(max_admit),28,year(max_admit));

     * slot each record in appropriate window(s) *;
     period=99;
     output;
     if mdy(month(max_admit),day(max_admit),year(max_admit)-5)<=admit_date<=max_admit then do;
        period=5;
        output;
        if mdy(month(max_admit),day(max_admit),year(max_admit)-4)<=admit_date<=max_admit then do;
           period=4;
           output;
           if mdy(month(max_admit),day(max_admit),year(max_admit)-3)<=admit_date<=max_admit then do;
              period=3;
              output;
              if mdy(month(max_admit),day(max_admit),year(max_admit)-2)<=admit_date<=max_admit then do;
                 period=2;
                 output;
                 if mdy(month(max_admit),day(max_admit),year(max_admit)-1)<=admit_date<=max_admit then do;
                    period=1;
                    output;
                 end;
              end;
           end;
        end;
     end;
run;

*- Uniqueness per PATID/period -*;
proc sort data=period out=period_patid nodupkey;
     by period patid;
run;

*- Derive statistics -*;
proc means data=period_patid(keep=period) nway completetypes noprint;
     class period/preloadfmt;
     output out=stats;
     format period period.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_patid_n $20;
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar
    
     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_DASH2
********************************************************************************;
%let qname=enc_l3_dash2;
%elapsed(begin);

*- Data with legitimate date and encounter type -*;
data data;
     set encounter(keep=patid admit_date enc_type);
     if admit_date^=. and enc_type in ('AV' 'ED' 'EI' 'IP' 'OS');

     keep patid admit_date;
run;

*- Determine maximum date -*;
proc means data=data noprint;
     var admit_date;
     output out=max_admit max=max_admit;
run;

*- Assign a period to every record based upon maximum date -*;
data period(keep=period patid);
     length period 3.;
     if _n_=1 then set max_admit;
     set data;

     * eliminate future dates from consideration and use run date *;
     if max_admit>today() then max_admit=today();

     * reset any possible leap year to the 28th *;
     if month(max_admit)=2 and day(max_admit)=29 then 
        max_admit=mdy(month(max_admit),28,year(max_admit));

     * slot each record in appropriate window(s) *;
     period=99;
     output;
     if mdy(month(max_admit),day(max_admit),year(max_admit)-5)<=admit_date<=max_admit then do;
        period=5;
        output;
        if mdy(month(max_admit),day(max_admit),year(max_admit)-4)<=admit_date<=max_admit then do;
           period=4;
           output;
           if mdy(month(max_admit),day(max_admit),year(max_admit)-3)<=admit_date<=max_admit then do;
              period=3;
              output;
              if mdy(month(max_admit),day(max_admit),year(max_admit)-2)<=admit_date<=max_admit then do;
                 period=2;
                 output;
                 if mdy(month(max_admit),day(max_admit),year(max_admit)-1)<=admit_date<=max_admit then do;
                    period=1;
                    output;
                 end;
              end;
           end;
        end;
     end;
run;

*- Uniqueness per PATID/period -*;
proc sort data=period out=period_patid nodupkey;
     by period patid;
run;

*- Derive statistics -*;
proc means data=period_patid(keep=period) nway completetypes noprint;
     class period/preloadfmt;
     output out=stats;
     format period period.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_patid_n $20;
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_PAYERTYPE1
********************************************************************************;
%let qname=enc_l3_payertype1;
%elapsed(begin);

proc format;
     value $payer_grp
          '1'='MR'
          '2'='MC'
          '3'='OG'
          '4'='DC'
          '5'='PI'
          '6'='BC'
          '7'='MCU'
          '8'='NP'
          '9'='MSC'
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
         ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $8;
     set encounter(keep=patid payer_type_primary);

     col1=put(payer_type_primary,$payer_type.);
     if payer_type_primary^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if payer_type_primary^=col1 then col1="ZZZE";
     end;

     keep col1 patid payer_type_primary;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 payer_type_primary payer_type_primary_grp $40;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then payer_type_primary=col1;
     else payer_type_primary=put(col1,$other.);
     if substr(col1,1,1)^='Z' then payer_type_primary_grp=put(substr(col1,1,1),$payer_grp.);
     else payer_type_primary_grp=payer_type_primary;

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package payer_type_primary_grp 
          payer_type_primary record_n record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, payer_type_primary_grp, 
            payer_type_primary, record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_PAYERTYPE2
********************************************************************************;
%let qname=enc_l3_payertype2;
%elapsed(begin);

proc format;
     value $payer_grp
          '1'='MR'
          '2'='MC'
          '3'='OG'
          '4'='DC'
          '5'='PI'
          '6'='BC'
          '7'='MCU'
          '8'='NP'
          '9'='MSC'
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
         ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $8;
     set encounter(keep=patid payer_type_secondary);

     col1=put(payer_type_secondary,$payer_type.);
     if payer_type_secondary^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if payer_type_secondary^=col1 then col1="ZZZE";
     end;

     keep col1 patid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 payer_type_secondary payer_type_secondary_grp $40;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then payer_type_secondary=col1;
     else payer_type_secondary=put(col1,$other.);
     if substr(col1,1,1)^='Z' then payer_type_secondary_grp=put(substr(col1,1,1),$payer_grp.);
     else payer_type_secondary_grp=payer_type_secondary;

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package payer_type_secondary_grp 
          payer_type_secondary record_n record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, payer_type_secondary_grp, 
            payer_type_secondary, record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenc);

********************************************************************************;
* ENC_L3_FACILITYTYPE
********************************************************************************;
%let qname=enc_l3_facilitytype;
%elapsed(begin);

proc format;
     value $fac_grp
       'CLINIC/CENTER_AMBULATORY_OUTPATIENT_CARE-subset'='CL/CENTER_AM_OP_CARE'
       'HOSPITAL_OUTPATIENT_CLINIC-AMBULATORY_CARE-subset'='HP_OP_CL_AM_CARE'
       'INDEPENDENT_PROVIDER_OF_OUTPATIENT_AMBULATORY_CARE-subset'='INDPDNT_PROVIDER_OF_OP_AM_CARE'
       'INPATIENT_HEALTH_FACILITY_CARE-subset'='IP_HEALTH_FACILITY_CARE'
       'OTHER_CARE_SITE-subset'='OTHER_CARE_SITE'
       'OTHER_OUTPATIENT_CARE_SITE-subset'='OTHER_OP_CARE_SITE'
       ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set encounter(keep=patid facility_type);

     col1=put(facility_type,$facility_type.);
     if facility_type^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if facility_type^=col1 then col1="ZZZE";
     end;

     keep col1 patid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 facility_type facility_type_grp $60;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=s where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           facility_type(in=f rename=(start=col1));
     by col1;
     if s;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then facility_type=col1;
     else facility_type=put(col1,$other.);
     if facility_type not in ("NI" "UN" "OT" "NULL or missing" "Values outside of CDM specifications") 
        then facility_type_grp=put(descriptive_text,$fac_grp.);
     else facility_type_grp=facility_type;

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package facility_type_grp facility_type
          record_n record_pct distinct_patid_n col1;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, facility_type_grp, 
            facility_type, record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in DIAGNOSIS and compress it
********************************************************************************;
%if &_ydiagnosis=1 %then %do;
%let qname=diagnosis;
%elapsed(begin);

data diagnosis(compress=yes);
     set pcordata.diagnosis;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;
run;

%let savediag=diagnosis;

%elapsed(end);

********************************************************************************;
* DIA_L3_N
********************************************************************************;
%let qname=dia_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=diagnosis,encvar=encounterid,ord=1)
%enc_oneway(encdsn=diagnosis,encvar=patid,ord=2)
%enc_oneway(encdsn=diagnosis,encvar=diagnosisid,ord=3)
%enc_oneway(encdsn=diagnosis,encvar=providerid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DX
********************************************************************************;
%let qname=dia_l3_dx;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set diagnosis(keep=dx);

     if dx^=" " then col1=dx;
     else if dx=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then dx=col1;
     else dx=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package dx record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DX_DXTYPE
********************************************************************************;
%let qname=dia_l3_dx_dxtype;
%elapsed(begin);

proc format;
     value $dxtype
         "09"="09"
         "10"="10"
         "11"="11"
         "SM"="SM"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Group drug type -*;
data data;
     length dxtype $4 col1 $15;
     set diagnosis(keep=patid dx_type dx);

     if dx_type in ("09" "10" "11" "SM") then dxtype=dx_type;
     else if dx_type in ("NI") then dxtype="ZZZA";
     else if dx_type in ("UN") then dxtype="ZZZB";
     else if dx_type in ("OT") then dxtype="ZZZC";
     else if dx_type=" " then dxtype="ZZZD";
     else dxtype="ZZZE";
    
     if dx^=" " then col1=dx;
     else if dx=" " then col1="ZZZA";
    
     keep dxtype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class col1 dxtype;
     output out=stats;
     format dxtype $dxtype. col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 dxtype patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1 dxtype;
     output out=pid_unique;
     format dxtype $dxtype. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 dxtype;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then dx=col1;
     else dx=put(col1,$null.);    
     dx_type=put(dxtype,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and dxtype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and dxtype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     keep datamartid response_date query_package dx_type dx record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx, dx_type, record_n,
            distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXPOA
********************************************************************************;
%let qname=dia_l3_dxpoa;
%elapsed(begin);

proc format;
     value $dxpoa
         "Y"="Y"
         "N"="N"
         "U"="U"
         "W"="W"
         "1"="1"
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
     set diagnosis(keep=dx_poa patid);

     if dx_poa in ("Y" "N" "U" "W" "1") then col1=dx_poa;
     else if dx_poa in ("NI") then col1="ZZZA";
     else if dx_poa in ("UN") then col1="ZZZB";
     else if dx_poa in ("OT") then col1="ZZZC";
     else if dx_poa=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dxpoa.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $dxpoa.;
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
     dx_poa=put(col1,$dxpoa.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_poa record_n record_pct 
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_poa, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXSOURCE
********************************************************************************;
%let qname=dia_l3_dxsource;
%elapsed(begin);

proc format;
     value $dxsrc
         "AD"="AD"
         "DI"="DI"
         "FI"="FI"
         "IN"="IN"
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
     set diagnosis(keep=dx_source);

     if dx_source in ("AD" "DI" "FI" "IN") then col1=dx_source;
     else if dx_source in ("NI") then col1="ZZZA";
     else if dx_source in ("UN") then col1="ZZZB";
     else if dx_source in ("OT") then col1="ZZZC";
     else if dx_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dxsrc.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     dx_source=put(col1,$dxsrc.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_source, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXTYPE_DXSOURCE
********************************************************************************;
%let qname=dia_l3_dxtype_dxsource;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length dxtype col1 $4;
     set diagnosis(keep=dx_type dx_source);

     if dx_type in ("09" "10" "11" "SM") then dxtype=dx_type;
     else if dx_type in ("NI") then dxtype="ZZZA";
     else if dx_type in ("UN") then dxtype="ZZZB";
     else if dx_type in ("OT") then dxtype="ZZZC";
     else if dx_type=" " then dxtype="ZZZD";
     else dxtype="ZZZE";
    
     if dx_source in ("AD" "DI" "FI" "IN") then col1=dx_source;
     else if dx_source in ("NI") then col1="ZZZA";
     else if dx_source in ("UN") then col1="ZZZB";
     else if dx_source in ("OT") then col1="ZZZC";
     else if dx_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep dxtype col1;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class dxtype col1/preloadfmt;
     output out=stats;
     format dxtype $dxtype. col1 $dxsrc.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_source=put(col1,$dxsrc.);
     dx_type=put(dxtype,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and dxtype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package dx_type dx_source record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, dx_source, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_PDX
********************************************************************************;
%let qname=dia_l3_pdx;
%elapsed(begin);

proc format;
     value $pdx
         "P"="P"
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
     length col1 $4;
     set diagnosis(keep=pdx);

     if pdx in ("P" "S") then col1=pdx;
     else if pdx in ("NI") then col1="ZZZA";
     else if pdx in ("UN") then col1="ZZZB";
     else if pdx in ("OT") then col1="ZZZC";
     else if pdx=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pdx.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pdx=put(col1,$pdx.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pdx record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdx, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_PDX_ENCTYPE
********************************************************************************;
%let qname=dia_l3_pdx_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=enc_type pdx encounterid patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if pdx in ("P" "S") then col1=pdx;
     else if pdx in ("NI") then col1="ZZZA";
     else if pdx in ("UN") then col1="ZZZB";
     else if pdx in ("OT") then col1="ZZZC";
     else if pdx=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep enctype col1 encounterid patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $pdx.;
run;

*- Derive distinct encounter id for each PDX/type -*;
proc sort data=data(drop=patid) out=distinct nodupkey;
     by col1 enctype encounterid;
     where encounterid^=' ';
run;

proc means data=distinct nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats_distinct;
     format enctype $enc_typ. col1 $pdx.;
run;

*- Derive distinct patient id for each PDX/type -*;
proc sort data=data(drop=encounterid) out=pid nodupkey;
     by col1 enctype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=pid_unique;
     format enctype $enc_typ. col1 $pdx.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats 
           stats_distinct(rename=(_freq_=_freq_encid))
           pid_unique(rename=(_freq_=_freq_patid));
     by col1 enctype;

     * call standard variables *;
     %stdvar

     * table values *;
     pdx=put(col1,$pdx.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=_freq_encid,nullval=col1^="ZZZD")
     %threshold(_tvar=_freq_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_encid_n=strip(put(_freq_encid,threshold.));
     distinct_patid_n=strip(put(_freq_patid,threshold.));

     keep datamartid response_date query_package enc_type pdx record_n 
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdx, enc_type, record_n, 
            distinct_encid_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_PDXGRP_ENCTYPE
********************************************************************************;
%let qname=dia_l3_pdxgrp_enctype;
%elapsed(begin);

proc format;
     value $pdxgrp
         "P"="P"
         "U"="U"
     ;
run;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=enc_type pdx encounterid patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if pdx="P" then col1=pdx;
     else col1="U";
    
     keep enctype col1 encounterid patid pdx;
run;

*- Derive distinct encounter id for each PDX/type -*;
proc sort data=data(drop=patid) out=distinct;
     by encounterid col1;
     where encounterid^=' ';
run;

*- Take planned over non-planned -*;
data distinct;
     set distinct;
     by encounterid col1;
     if first.encounterid;
run;

proc means data=distinct nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats_distinct;
     format enctype $enc_typ. col1 $pdxgrp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_encid_n $20;
     set stats_distinct(rename=(_freq_=_freq_encid));
     by col1 enctype;

     * call standard variables *;
     %stdvar

     * table values *;
     pdx=put(col1,$pdxgrp.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(_tvar=_freq_encid,nullval=col1^="ZZZD")

     * counts *;
     distinct_encid_n=strip(put(_freq_encid,threshold.));

     keep datamartid response_date query_package enc_type pdx 
          distinct_encid_n ;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdx, enc_type,
            distinct_encid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_ADATE_Y
********************************************************************************;
%let qname=dia_l3_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set diagnosis(keep=admit_date patid encounterid);

     if admit_date^=. then year=year(admit_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     keep col1 patid encounterid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - encounter id -*;
proc sort data=data(drop=patid) out=encid nodupkey;
     by col1 encounterid;
     where encounterid^=' ';
run;

proc means data=encid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=encid_unique;
     format col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data(drop=encounterid) out=pid nodupkey;
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
     length record_n distinct_encid_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           encid_unique(rename=(_freq_=distinct_encid))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     admit_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_encid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     distinct_encid_n=strip(put(distinct_encid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package admit_date record_n record_pct
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n, 
            record_pct, distinct_encid_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_ADATE_YM
********************************************************************************;
%let qname=dia_l3_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set diagnosis(keep=admit_date);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then do;
        year=year(admit_date);
        year_month=(year(admit_date)*100)+month(admit_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     merge dummy stats(in=s);
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
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_ORIGIN
********************************************************************************;
%let qname=dia_l3_origin;
%elapsed(begin);

proc format;
     value $origin
         "OD"="OD"
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
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
     set diagnosis(keep=dx_origin patid);

     if dx_origin in ("OD" "BI" "CL" "DR") then col1=dx_origin;
     else if dx_origin in ("NI") then col1="ZZZA";
     else if dx_origin in ("UN") then col1="ZZZB";
     else if dx_origin in ("OT") then col1="ZZZC";
     else if dx_origin=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $origin.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $origin.;
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
     dx_origin=put(col1,$origin.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_origin record_n record_pct 
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_origin, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_ENCTYPE
********************************************************************************;
%let qname=dia_l3_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set diagnosis(keep=enc_type patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $enc_typ.;
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
     enc_type=put(col1,$enc_typ.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enc_type record_n record_pct 
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXTYPE_ENCTYPE
********************************************************************************;
%let qname=dia_l3_dxtype_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=enc_type dx_type);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if dx_type in ("09" "10" "11" "SM") then col1=dx_type;
     else if dx_type in ("NI") then col1="ZZZA";
     else if dx_type in ("UN") then col1="ZZZB";
     else if dx_type in ("OT") then col1="ZZZC";
     else if dx_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $dxtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_type=put(col1,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     enc_type=put(enctype,$enc_typ.);
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type dx_type record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, enc_type, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXTYPE_ADATE_Y
********************************************************************************;
%let qname=dia_l3_dxtype_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length dxtype col1 $4;
     set diagnosis(keep=admit_date dx_type patid);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then year=year(admit_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     if dx_type in ("09" "10" "11" "SM") then dxtype=dx_type;
     else if dx_type in ("NI") then dxtype="ZZZA";
     else if dx_type in ("UN") then dxtype="ZZZB";
     else if dx_type in ("OT") then dxtype="ZZZC";
     else if dx_type=" " then dxtype="ZZZD";
     else dxtype="ZZZE";

     keep col1 dxtype patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class dxtype/preloadfmt;
     class col1;
     output out=stats;
     format col1 $null. dxtype $dxtype.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by dxtype col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class dxtype/preloadfmt;
     class col1;
     output out=pid_unique;
     format col1 $null. dxtype $dxtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by dxtype col1;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_type=put(dxtype,$dxtype.);
     admit_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package dx_type admit_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, admit_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=dia_l3_enctype_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set diagnosis(keep=admit_date enc_type encounterid patid);

     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype encounterid patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive statistics - encounter id -*;
proc sort data=data out=distinct nodupkey;
     by enctype year_month encounterid;
     where encounterid^=' ';
run;
    
proc means data=distinct nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats_distinct;
     format enctype $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by enctype year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=pid_unique;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_encid_n distinct_patid_n $20;
     merge stats
           stats_distinct(rename=(_freq_=_freq_encid))
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by enctype year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)
     %threshold(_tvar=_freq_encid,nullval=enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_encid_n=strip(put(_freq_encid,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type admit_date record_n 
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            distinct_encid_n, record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DASH1
********************************************************************************;
%let qname=dia_l3_dash1;
%elapsed(begin);

*- Data with legitimate date -*;
data data;
     set diagnosis(keep=patid admit_date);
     if admit_date^=.;

     keep patid admit_date;
run;

*- Determine maximum date -*;
proc means data=data noprint;
     var admit_date;
     output out=max_admit max=max_admit;
run;

*- Assign a period to every record based upon maximum date -*;
data period(keep=period patid);
     length period 3.;
     if _n_=1 then set max_admit;
     set data;

     * eliminate future dates from consideration and use run date *;
     if max_admit>today() then max_admit=today();

     * reset any possible leap year to the 28th *;
     if month(max_admit)=2 and day(max_admit)=29 then 
        max_admit=mdy(month(max_admit),28,year(max_admit));

     * slot each record in appropriate window(s) *;
     period=99;
     output;
     if mdy(month(max_admit),day(max_admit),year(max_admit)-5)<=admit_date<=max_admit then do;
        period=5;
        output;
        if mdy(month(max_admit),day(max_admit),year(max_admit)-4)<=admit_date<=max_admit then do;
           period=4;
           output;
           if mdy(month(max_admit),day(max_admit),year(max_admit)-3)<=admit_date<=max_admit then do;
              period=3;
              output;
              if mdy(month(max_admit),day(max_admit),year(max_admit)-2)<=admit_date<=max_admit then do;
                 period=2;
                 output;
                 if mdy(month(max_admit),day(max_admit),year(max_admit)-1)<=admit_date<=max_admit then do;
                    period=1;
                    output;
                 end;
              end;
           end;
        end;
     end;
run;

*- Uniqueness per PATID/period -*;
proc sort data=period out=period_patid nodupkey;
     by period patid;
run;

*- Derive statistics -*;
proc means data=period_patid(keep=period) nway completetypes noprint;
     class period/preloadfmt;
     output out=stats;
     format period period.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_patid_n $20; 
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_PDX_DETAIL
********************************************************************************;
%let qname=dia_l3_pdx_detail;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=pdx enc_type dx_origin);
     if pdx="P";

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if dx_origin in ("OD" "BI" "CL" "DR") then col1=dx_origin;
     else if dx_origin in ("NI") then col1="ZZZA";
     else if dx_origin in ("UN") then col1="ZZZB";
     else if dx_origin in ("OT") then col1="ZZZC";
     else if dx_origin=" " then col1="ZZZD";
     else col1="ZZZE";

     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $origin.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_origin=put(col1,$origin.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type dx_origin record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, dx_origin, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in PROCEDURES and compress it
********************************************************************************;
%if &_yprocedures=1 %then %do;
%let qname=procedures;
%elapsed(begin);

data procedures(compress=yes);
     set pcordata.procedures;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;
run;

%let saveproc=procedures;

%elapsed(end);

********************************************************************************;
* PRO_L3_N
********************************************************************************;
%let qname=pro_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=procedures,encvar=encounterid,ord=1)
%enc_oneway(encdsn=procedures,encvar=patid,ord=2)
%enc_oneway(encdsn=procedures,encvar=proceduresid,ord=3)
%enc_oneway(encdsn=procedures,encvar=providerid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PX
********************************************************************************;
%let qname=pro_l3_px;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set procedures(keep=px);

     if px^=" " then col1=px;
     else if px=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     px=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package px record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_ADATE_Y
********************************************************************************;
%let qname=pro_l3_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=admit_date patid encounterid);

     if admit_date^=. then year=year(admit_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     keep col1 patid encounterid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - encounter id -*;
proc sort data=data(drop=patid) out=encid nodupkey;
     by col1 encounterid;
     where encounterid^=' ';
run;

proc means data=encid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=encid_unique;
     format col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data(drop=encounterid) out=pid nodupkey;
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
     length record_n distinct_encid_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           encid_unique(rename=(_freq_=distinct_encid))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     admit_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_encid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     distinct_encid_n=strip(put(distinct_encid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package admit_date record_n record_pct 
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n, 
            record_pct, distinct_encid_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_ADATE_YM
********************************************************************************;
%let qname=pro_l3_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set procedures(keep=admit_date);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then do;
        year=year(admit_date);
        year_month=(year(admit_date)*100)+month(admit_date);
     end;

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     do y = min to max(max,today());
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
     merge dummy stats(in=s);
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
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PXDATE_Y
********************************************************************************;
%let qname=pro_l3_pxdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=px_date patid encounterid);

     if px_date^=. then year=year(px_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     keep col1 patid encounterid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - encounter id -*;
proc sort data=data(drop=patid) out=encid nodupkey;
     by col1 encounterid;
     where encounterid^=' ';
run;

proc means data=encid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=encid_unique;
     format col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data(drop=encounterid) out=pid nodupkey;
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
     length record_n distinct_encid_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           encid_unique(rename=(_freq_=distinct_encid))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     px_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_encid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     distinct_encid_n=strip(put(distinct_encid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package px_date record_n record_pct
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_date, record_n, 
            record_pct, distinct_encid_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PX_ENCTYPE retired in v3.02 
********************************************************************************;
/*%let qname=pro_l3_px_enctype;

*- Derive categorical variable -*;
data data;
     length enctype $4 col1 $15;
     set procedures(keep=enc_type px patid);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if px^=" " then col1=px;
     else if px=" " then col1="ZZZA";
    
     keep enctype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 enctype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=pid_unique;
     format enctype $enc_typ. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 enctype;

     * call standard variables *;
     %stdvar

     * table values *;
     px=put(col1,$null.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type px record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px, enc_type, record_n,
         distinct_patid_n
     from query;
quit;

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);
*/

********************************************************************************;
* PRO_L3_ENCTYPE
********************************************************************************;
%let qname=pro_l3_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=enc_type);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(col1,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enc_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PXTYPE_ENCTYPE
********************************************************************************;
%let qname=pro_l3_pxtype_enctype;
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
     length enctype col1 $4;
     set procedures(keep=enc_type px_type);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") 
            then col1=px_type;
     else if px_type in ("NI") then col1="ZZZA";
     else if px_type in ("UN") then col1="ZZZB";
     else if px_type in ("OT") then col1="ZZZC";
     else if px_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $pxtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     px_type=put(col1,$pxtype.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type px_type record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_type, enc_type, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_DXTYPE_ADATE_Y
********************************************************************************;
%let qname=pro_l3_pxtype_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length pxtype col1 $4;
     set procedures(keep=admit_date px_type patid);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then year=year(admit_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     if px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") 
            then pxtype=px_type;
     else if px_type in ("NI") then pxtype="ZZZA";
     else if px_type in ("UN") then pxtype="ZZZB";
     else if px_type in ("OT") then pxtype="ZZZC";
     else if px_type=" " then pxtype="ZZZD";
     else pxtype="ZZZE";

     keep col1 pxtype patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class pxtype/preloadfmt;
     class col1;
     output out=stats;
     format col1 $null. pxtype $pxtype.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by pxtype col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class pxtype/preloadfmt;
     class col1;
     output out=pid_unique;
     format col1 $null. pxtype $pxtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by pxtype col1;

     * call standard variables *;
     %stdvar

     * table values *;
     px_type=put(pxtype,$pxtype.);
     admit_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package px_type admit_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_type, admit_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=pro_l3_enctype_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set procedures(keep=admit_date enc_type encounterid patid);

     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype encounterid patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive statistics - encounter id -*;
proc sort data=data out=distinct nodupkey;
     by enctype year_month encounterid;
     where encounterid^=' ';
run;
    
proc means data=distinct nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats_distinct;
     format enctype $enc_typ.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by enctype year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=pid_unique;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_encid_n distinct_patid_n $20;
     merge stats
           stats_distinct(rename=(_freq_=_freq_encid))
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by enctype year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
    
     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * Apply threshold *;
     %threshold(nullval=enctype^="ZZZD" and year_month^=99999999)
     %threshold(_tvar=_freq_encid,nullval=enctype^="ZZZD" and year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_encid_n=strip(put(_freq_encid,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package enc_type admit_date record_n 
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            distinct_encid_n, record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PX_PXTYPE
********************************************************************************;
%let qname=pro_l3_px_pxtype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length pxtype $4 col1 $15;
     set procedures(keep=px_type px patid);

     if px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") 
            then pxtype=px_type;
     else if px_type in ("NI") then pxtype="ZZZA";
     else if px_type in ("UN") then pxtype="ZZZB";
     else if px_type in ("OT") then pxtype="ZZZC";
     else if px_type=" " then pxtype="ZZZD";
     else pxtype="ZZZE";
    
     if px^=" " then col1=px;
     else if px=" " then col1="ZZZA";
    
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
     px=put(col1,$null.);
     px_type=put(pxtype,$pxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and pxtype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and pxtype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     keep datamartid response_date query_package px_type px record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px, px_type, record_n,
            distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PXSOURCE
********************************************************************************;
%let qname=pro_l3_pxsource;
%elapsed(begin);

proc format;
     value $pxsrc
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
         "OD"="OD"
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
     set procedures;

     if px_source in ("BI" "CL" "DR" "OD") then col1=px_source;
     else if px_source in ("NI") then col1="ZZZA";
     else if px_source in ("UN") then col1="ZZZB";
     else if px_source in ("OT") then col1="ZZZC";
     else if px_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pxsrc.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     px_source=put(col1,$pxsrc.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package px_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_source, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PPX
********************************************************************************;
%let qname=pro_l3_ppx;
%elapsed(begin);

proc format;
     value $ppx
         "P"="P"
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
     length col1 $4;
     set procedures;

     if ppx in ("P" "S") then col1=ppx;
     else if ppx in ("NI") then col1="ZZZA";
     else if ppx in ("UN") then col1="ZZZB";
     else if ppx in ("OT") then col1="ZZZC";
     else if ppx=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $ppx.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     ppx=put(col1,$ppx.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package ppx record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, ppx, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in ENROLLMENT and compress it
********************************************************************************;
%if &_yenrollment=1 %then %do;
%let qname=enrollment;
%elapsed(begin);

*- Determine concatonated length of variables used to determine ENROLLID -*;
proc contents data=pcordata.enrollment out=cont_enrollment noprint;
run;

data _null_;
     set cont_enrollment end=eof;
     retain ulength 0;
     if name in ("PATID" "ENR_BASIS") then ulength=ulength+length;
    
     * add 11 to ULENGTH (9 for date and 2 for delimiter *;
     if eof then call symput("_eulength",strip(put(ulength+11,8.)));
run;

data enrollment(compress=yes drop=enrstartdate);
     length enrollid $&_eulength;
     set pcordata.enrollment;

     * restrict data to within lookback period *;
     if enr_start_date>=&lookback_dt or enr_start_date=.;

     enrstartdate=put(enr_start_date,date9.);
     enrollid=cats(patid,'_',enrstartdate,'_',enr_basis);
run;

%let saveenr=enrollment;

%elapsed(end);

********************************************************************************;
* ENR_L3_N
********************************************************************************;
%let qname=enr_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=enrollment,encvar=patid,ord=1)
%enc_oneway(encdsn=enrollment,encvar=enr_start_date,_nc=1,ord=2)
%enc_oneway(encdsn=enrollment,encvar=enrollid,ord=3)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_DIST_START
********************************************************************************;
%let qname=enr_l3_dist_start;
%elapsed(begin);

*- Derive statistics -*;
proc means data=enrollment nway noprint;
     var enr_start_date;
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
        *- Transpose to one variable/multiple rows -*;
        proc transpose data=stats out=query prefix=col;
             var S01 S02 S03 S04 S06 S07 S08 S09 S10 S11 S12;
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
     length record_n $20.;
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);

     * counts *;
     if _name_ in ("S11" "S12") or col1=.n then record_n=strip(put(col1,threshold.));
     else record_n=put(col1,date9.);

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
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_DIST_END
********************************************************************************;
%let qname=enr_l3_dist_end;
%elapsed(begin);

*- Derive statistics -*;
proc means data=enrollment nway noprint;
     var enr_end_date;
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
        *- Transpose to one variable/multiple rows -*;
        proc transpose data=stats out=query prefix=col;
             var S01 S02 S03 S04 S06 S07 S08 S09 S10 S11 S12;
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
     length record_n $20.;
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);
    
     * counts *;
     if _name_ in ("S11" "S12") or col1=.n then record_n=strip(put(col1,threshold.));
     else record_n=put(col1,date9.);

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
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_DIST_ENRMONTH
********************************************************************************;
%let qname=enr_l3_dist_enrmonth;
%elapsed(begin);

proc format;
     value dist
        low - <0="<0"
        .="NULL or missing"
        ;
run;

*- Derive number of enrollment months -*;
data data;
     length col1 3.;
     set enrollment(keep=enr_start_date enr_end_date);

     if enr_start_date^=. and enr_end_date^=. then do;
        if enr_start_date>enr_end_date then col1=-1;
        else col1=int((enr_end_date-enr_start_date)/30.25);
     end;

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1;
     output out=stats;
run;

*- Derive the first and last year values from the actual data -*;    
proc means data=data nway completetypes noprint missing;
     var col1;
     output out=dummy min=min max=max;
run;

*- Create a dummy dataset beginning with earliest month, 
   extending to the last month -*;
data dummy(keep=col1 _type_ _freq_);
     set dummy;
     
     * loop if discharge date is populated *;
     if nmiss(min,max)=0 then do;
         do y = min to max;
            _type_=1;
            _freq_=0;
            col1=y;
            output;
         end;
     end;
     * loop if discharge date is not populated *;
     else do;
        _type_=1;
        _freq_=0;
        col1=.;
     end;
     * macro variable to test presence of discharge date *;
     call symput("minact_present",strip(put(nmiss(min,max),8.)));

run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats;
     by col1;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     %if &minact_present=0 %then %do;
         if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     %end;
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;

     * call standard variables *;
     %stdvar

     * table values *;
     enroll_m=put(col1,dist.);

     * apply threshold *;    
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     %if &minact_present=0 %then %do;
         if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
     %end;
     %else %do;
         if _freq_>&threshold then record_pct=put((_freq_/_freq_)*100,6.1);
     %end;

     keep datamartid response_date query_package enroll_m record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enroll_m, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_DIST_ENRYEAR
********************************************************************************;
%let qname=enr_l3_dist_enryear;
%elapsed(begin);

proc format;
     value dist
        low - <0="<0"
        .="NULL or missing"
        ;
run;

*- Derive number of enrollment months -*;
data data;
     length col1 4.;
     set enrollment(keep=enr_start_date enr_end_date);

     if enr_start_date^=. and enr_end_date^=. then do;
        if enr_start_date>enr_end_date then col1=-1;
        else col1=int(int((enr_end_date-enr_start_date)/30.25)/12);
     end;

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1;
     output out=stats;
run;

*- Derive the first and last year values from the actual data -*;    
proc means data=data nway completetypes noprint missing;
     var col1;
     output out=dummy min=min max=max;
run;

*- Create a dummy dataset beginning with earliest month, 
   extending to the last month -*;
data dummy(keep=col1 _type_ _freq_);
     set dummy;
    
     * loop if enroll end date is populated *;
     if nmiss(min,max)=0 then do;
         do y = min to max;
            _type_=1;
            _freq_=0;
            col1=y;
            output;
         end;
     end;
     * loop if enroll end date is not populated *;
     else do;
        _type_=1;
        _freq_=0;
        col1=.;
     end;
     * macro variable to test presence of enroll end date *;
     call symput("minact_present",strip(put(nmiss(min,max),8.)));
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats;
     by col1;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     %if &minact_present=0 %then %do;
         if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     %end;
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;

     * call standard variables *;
     %stdvar

     * table values *;
     enroll_y=put(col1,dist.);

     * apply threshold *;    
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     %if &minact_present=0 %then %do;
         if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
     %end;
     %else %do;
         if _freq_>&threshold then record_pct=put((_freq_/_freq_)*100,6.1);
     %end;

     keep datamartid response_date query_package enroll_y record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enroll_y, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_ENR_YM
********************************************************************************;
%let qname=enr_l3_enr_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set enrollment;

     * create a year and a year/month numeric variable *;
     if enr_start_date^=. then do;
        year=year(enr_start_date);
        year_month=(year(enr_start_date)*100)+month(enr_start_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     merge dummy stats(in=s);
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
     if year_month=99999999 then month=put(year_month,null.);
     else month=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package month record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, month, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_BASEDIST
********************************************************************************;
%let qname=enr_l3_basedist;
%elapsed(begin);

proc format;
     value $basis
     'A'='A'
     'D'='D'
     'E'='E'
     'G'='G'
     'I'='I'
     "ZZZA"="NULL or missing"
     "ZZZB"="Values outside of CDM specifications"
    ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set enrollment(keep=enr_basis);

     if enr_basis in ("A" "D" "E" "G" "I") then col1=enr_basis;
     else if enr_basis=" " then col1="ZZZA";
     else col1="ZZZB";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $basis.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     enr_basis=put(col1,$nullout.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enr_basis record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enr_basis, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_CHART
********************************************************************************;
%let qname=enr_l3_chart;
%elapsed(begin);

proc format;
     value $chart
     'Y'='Y'
     'N'='N'
     "ZZZA"="NULL or missing"
     "ZZZB"="Values outside of CDM specifications"
    ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set enrollment(keep=chart);

     if chart in ("N" "Y") then col1=chart;
     else if chart=" " then col1="ZZZA";
     else col1="ZZZB";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $chart.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     chart=put(col1,$nullout.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package chart record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, chart, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveenr);

********************************************************************************;
* ENR_L3_PER_PATID
********************************************************************************;
%let qname=enr_l3_per_patid;
%elapsed(begin);

*- Count records per PATID -*;
proc sort data=enrollment(keep=patid)
     out=data;
     by patid;
run;
    
data data;
     set data;
     by patid;
    
     retain count 0;
     if first.patid then count=0;
     count=count+1;
     if last.patid then output;
run;

*- Derive statistics -*;
proc means data=data nway noprint;
     var count;
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
        *- Transpose to one variable/multiple rows -*;
        proc transpose data=stats out=query prefix=col;
             var S01 S02 S03 S04 S06 S07 S08 S09 S10 S11 S12;
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
     if _name_ in ("S11" "S12") then record_n=put(col1,threshold.);
     else record_n=compress(put(col1,16.));
    
     * counts *;
     stat=put(_name_,$stat.);

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
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in VITAL and compress it
********************************************************************************;
%if &_yvital=1 %then %do;
%let qname=vital;
%elapsed(begin);

data vital(compress=yes);
     set pcordata.vital;

     * restrict data to within lookback period *;
     if measure_date>=&lookback_dt or measure_date=.;
run;

%let savevit=vital;

%elapsed(end);

********************************************************************************;
* VIT_L3_N
********************************************************************************;
%let qname=vit_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=vital,encvar=encounterid,ord=1)
%enc_oneway(encdsn=vital,encvar=patid,ord=2)
%enc_oneway(encdsn=vital,encvar=vitalid,ord=3)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_MDATE_Y
********************************************************************************;
%let qname=vit_l3_mdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set vital(keep=measure_date patid);

     if measure_date^=. then year=year(measure_date);
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
     measure_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package measure_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, measure_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_MDATE_YM
********************************************************************************;
%let qname=vit_l3_mdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set vital(keep=measure_date patid);

     * create a year and a year/month numeric variable *;
     if measure_date^=. then do;
        year=year(measure_date);
        year_month=(year(measure_date)*100)+month(measure_date);
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
     length record_n distinct_patid_n $20;
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then measure_date=put(year_month,null.);
     else measure_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;    
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package measure_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, measure_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_VITAL_SOURCE
********************************************************************************;
%let qname=vit_l3_vital_source;
%elapsed(begin);

proc format;
     value $vitsrc
         "HC"="HC"
         "HD"="HD"
         "PD"="PD"
         "PR"="PR"
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
     set vital(keep=vital_source);

     if vital_source in ("HC" "HD" "PD" "PR") then col1=vital_source;
     else if vital_source in ("NI") then col1="ZZZA";
     else if vital_source in ("UN") then col1="ZZZB";
     else if vital_source in ("OT") then col1="ZZZC";
     else if vital_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $vitsrc.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vital_source=put(col1,$vitsrc.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package vital_source record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vital_source, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_HT
********************************************************************************;
%let qname=vit_l3_ht;
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

*- Derive categorical variable -*;
data data;
     length col1 3.;
     set vital(keep=patid ht);
     if ht^=. then col1=int(ht);
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
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_HT_DIST
********************************************************************************;
%let qname=vit_l3_ht_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=vital nway noprint;
     var ht;
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
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_WT
********************************************************************************;
%let qname=vit_l3_wt;
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

*- Derive categorical variable -*;
data data;
     length col1 3.;
     set vital(keep=patid wt);
     if wt^=. then col1=ceil(wt);
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
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_WT_DIST
********************************************************************************;
%let qname=vit_l3_wt_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=vital nway noprint;
     var wt;
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
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_DIASTOLIC
********************************************************************************;
%let qname=vit_l3_diastolic;
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

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set vital(keep=diastolic);
     if diastolic^=. then do;
        if diastolic<40 then col1=39;
        else col1=ceil(diastolic);
     end;
     keep col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 dia_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;

     * call standard variables *;
     %stdvar

     * table values *;
     diastolic_group=put(col1,dia_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package diastolic_group record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, diastolic_group, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_SYSTOLIC
********************************************************************************;
%let qname=vit_l3_systolic;
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

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set vital(keep=systolic);
     if systolic^=. then do;
        if systolic<40 then col1=39;
        else col1=ceil(systolic);
     end;
     keep col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 sys_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;

     * call standard variables *;
     %stdvar

     * table values *;
     systolic_group=put(col1,sys_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package systolic_group record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, systolic_group, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_BMI
********************************************************************************;
%let qname=vit_l3_bmi;
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

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set vital(keep=original_bmi);
     if original_bmi^=. then col1=ceil(original_bmi);
     keep col1;
run;

*- Derive statistics -*;
proc means data=data(keep=col1) completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 bmi_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1 and col1^=.))
         stats(where=(_type_=1 and col1=.))
     ;

     * call standard variables *;
     %stdvar

     * table values *;
     bmi_group=put(col1,bmi_dist.);

     * apply threshold *;
     %threshold(nullval=col1^=.)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package bmi_group record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, bmi_group, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_BP_POSITION_TYPE
********************************************************************************;
%let qname=vit_l3_bp_position_type;
%elapsed(begin);

proc format;
     value $bppos
         "01"="01"
         "02"="02"
         "03"="03"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Group source -*;
data data;
     length col1 $4;
     set vital(keep=bp_position);

     if bp_position in ("01" "02" "03" ) then col1=bp_position;
     else if bp_position in ("NI") then col1="ZZZA";
     else if bp_position in ("UN") then col1="ZZZB";
     else if bp_position in ("OT") then col1="ZZZC";
     else if bp_position=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $bppos.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     bp_position=put(col1,$bppos.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package bp_position record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, bp_position, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_SMOKING
********************************************************************************;
%let qname=vit_l3_smoking;
%elapsed(begin);

proc format;
     value $smoke
         "01"="01"
         "02"="02"
         "03"="03"
         "04"="04"
         "05"="05"
         "06"="06"
         "07"="07"
         "08"="08"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Group source -*;
data data;
     length col1 $4;
     set vital;

     if smoking in ("01" "02" "03" "04" "05" "06" "07" "08") then col1=smoking;
     else if smoking in ("NI") then col1="ZZZA";
     else if smoking in ("UN") then col1="ZZZB";
     else if smoking in ("OT") then col1="ZZZC";
     else if smoking=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $smoke.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     smoking=put(col1,$smoke.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package smoking record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, smoking, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_TOBACCO
********************************************************************************;
%let qname=vit_l3_tobacco;
%elapsed(begin);

proc format;
     value $tobac
         "01"="01"
         "02"="02"
         "03"="03"
         "04"="04"
         "06"="06"
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
     set vital;

     if tobacco in ("01" "02" "03" "04" "06") then col1=tobacco;
     else if tobacco in ("NI") then col1="ZZZA";
     else if tobacco in ("UN") then col1="ZZZB";
     else if tobacco in ("OT") then col1="ZZZC";
     else if tobacco=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $tobac.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     tobacco=put(col1,$tobac.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package tobacco record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, tobacco, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_TOBACCO_TYPE
********************************************************************************;
%let qname=vit_l3_tobacco_type;
%elapsed(begin);

proc format;
     value $tob_type
         "01"="01"
         "02"="02"
         "03"="03"
         "04"="04"
         "05"="05"
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
     set vital;

     if tobacco_type in ("01" "02" "03" "04" "05") then col1=tobacco_type;
     else if tobacco_type in ("NI") then col1="ZZZA";
     else if tobacco_type in ("UN") then col1="ZZZB";
     else if tobacco_type in ("OT") then col1="ZZZC";
     else if tobacco_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $tob_type.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     tobacco_type=put(col1,$tob_type.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package tobacco_type record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, tobacco_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit);

********************************************************************************;
* VIT_L3_DASH1
********************************************************************************;
%let qname=vit_l3_dash1;
%elapsed(begin);

*- Data with legitimate date -*;
data data;
     set vital(keep=patid measure_date);
     if measure_date^=.;

     keep patid measure_date;
run;

*- Determine maximum date -*;
proc means data=data noprint;
     var measure_date;
     output out=max_measure max=max_measure;
run;

*- Assign a period to every record based upon maximum date -*;
data period(keep=period patid);
     length period 3.;
     if _n_=1 then set max_measure;
     set data;

     * eliminate future dates from consideration and use run date *;
     if max_measure>today() then max_measure=today();

     * reset any possible leap year to the 28th *;
     if month(max_measure)=2 and day(max_measure)=29 then 
        max_measure=mdy(month(max_measure),28,year(max_measure));

     * slot each record in appropriate window(s) *;
     period=99;
     output;
     if mdy(month(max_measure),day(max_measure),year(max_measure)-5)<=measure_date<=max_measure then do;
        period=5;
        output;
        if mdy(month(max_measure),day(max_measure),year(max_measure)-4)<=measure_date<=max_measure then do;
           period=4;
           output;
           if mdy(month(max_measure),day(max_measure),year(max_measure)-3)<=measure_date<=max_measure then do;
              period=3;
              output;
              if mdy(month(max_measure),day(max_measure),year(max_measure)-2)<=measure_date<=max_measure then do;
                 period=2;
                 output;
                 if mdy(month(max_measure),day(max_measure),year(max_measure)-1)<=measure_date<=max_measure then do;
                    period=1;
                    output;
                 end;
              end;
           end;
        end;
     end;
run;

*- Uniqueness per PATID/period -*;
proc sort data=period out=period_patid nodupkey;
     by period patid;
run;

*- Derive statistics -*;
proc means data=period_patid(keep=period) nway completetypes noprint;
     class period/preloadfmt;
     output out=stats;
     format period period.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_patid_n $20;
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=strip(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in DISPENSING and compress it
********************************************************************************;
%if &_ydispensing=1 %then %do;
%let qname=dispensing;
%elapsed(begin);

data dispensing(compress=yes);
     set pcordata.dispensing;

     * restrict data to within lookback period *;
     if dispense_date>=&lookback_dt or dispense_date=.;

     if notdigit(ndc)=0 and length(ndc)=11 then valid_ndc="Y";
     else valid_ndc=" ";
run;

%let savedisp=dispensing;

%elapsed(end);

********************************************************************************;
* DISP_L3_N
********************************************************************************;
%let qname=disp_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=dispensing,encvar=patid,ord=1)
%enc_oneway(encdsn=dispensing,encvar=dispensingid,ord=2)
%enc_oneway(encdsn=dispensing,encvar=prescribingid,ord=3)
%enc_oneway(encdsn=dispensing,encvar=ndc,ord=4)
%enc_oneway(encdsn=dispensing,encvar=valid_ndc,ord=5)

data query;
     length valid_n $20;
     if _n_=1 then set query(keep=tag all_n where=(vtag="VALID_NDC") 
                             rename=(tag=vtag all_n=valid_all));
     set query(where=(tag^="VALID_NDC"));
     if strip(tag)^="NDC" then valid_n="n/a";
     else valid_n=valid_all;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n, valid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_NDC
********************************************************************************;
%let qname=disp_l3_ndc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set dispensing(keep=ndc patid);

     if ndc^=" " then col1=ndc;
     else if ndc=" " then col1="ZZZA";
    
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
     ndc=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package ndc record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, ndc, record_n, record_pct,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_DDATE_Y
********************************************************************************;
%let qname=disp_l3_ddate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set dispensing(keep=dispense_date patid);

     if dispense_date^=. then year=year(dispense_date);
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
     dispense_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dispense_date record_n record_pct 
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_DDATE_YM
********************************************************************************;
%let qname=disp_l3_ddate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set dispensing(keep=dispense_date patid);

     * create a year and a year/month numeric variable *;
     if dispense_date^=. then do;
        year=year(dispense_date);
        year_month=(year(dispense_date)*100)+month(dispense_date);
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
     do y = min to max(max,today());
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
     length record_n distinct_patid_n $20;
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then dispense_date=put(year_month,null.);
     else dispense_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package dispense_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_SUPDIST2
********************************************************************************;
%let qname=disp_l3_supdist2;
%elapsed(begin);

proc format;
     value dispense2dist
        low - <1="<1 day"
        1-15="1-15 days"
       16-30="16-30 days"
       31-60="31-60 days"
       61-90="61-90 days"
       91-99998=">90 days"
      99999="NULL or missing"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set dispensing(keep=patid dispense_sup);

     * set non-missing for categorization *;
     if dispense_sup^=. then col1=dispense_sup;

     * set missing or impossibly high values for categorization *;
     if dispense_sup=. or dispense_sup>99998 then col1=99999;

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 dispense2dist.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     dispense_sup_group=put(col1,dispense2dist.);

     * apply threshold *;
     %threshold(nullval=col1^=99999)
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dispense_sup_group record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_sup_group, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_DISPAMT_DIST
********************************************************************************;
%let qname=disp_l3_dispamt_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=dispensing nway noprint;
     var dispense_amt;
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
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_DOSE_DIST
********************************************************************************;
%let qname=disp_l3_dose_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=dispensing nway noprint;
     var dispense_dose_disp;
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
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_DOSEUNIT
********************************************************************************;
%let qname=disp_l3_doseunit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set dispensing(keep=patid dispense_dose_disp_unit);

     col1=put(strip(dispense_dose_disp_unit),_unit.);
     if dispense_dose_disp_unit^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if dispense_dose_disp_unit^=col1 then col1="ZZZE";
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
     length record_n $20 dispense_dose_disp_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           short_x_unit(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then dispense_dose_disp_unit=col1;
     else dispense_dose_disp_unit=put(col1,$other.);
     if (d and s) or dispense_dose_disp_unit in ("NI" "UN" "OT") then short_yn="Y";
     else short_yn="N";

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dispense_dose_disp_unit short_yn
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_dose_disp_unit, short_yn, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_ROUTE
********************************************************************************;
%let qname=disp_l3_route;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set dispensing(keep=patid dispense_route);

     col1=put(dispense_route,$_route.);
     if dispense_route^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if dispense_route^=col1 then col1="ZZZE";
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
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then dispense_route=col1;
     else dispense_route=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dispense_route
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_route, record_n, 
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in PRESRIBING and compress it
********************************************************************************;
%if &_yprescribing=1 %then %do;
%let qname=prescribing;
%elapsed(begin);

data prescribing(compress=yes);
     set pcordata.prescribing;

     * restrict data to within lookback period *;
     if rx_order_date>=&lookback_dt or rx_order_date=.;

     rxnorm_cui=strip(upcase(rxnorm_cui));

     * for mismatch query *;
     providerid=rx_providerid;
run;

%let savepres=prescribing;

%elapsed(end);

********************************************************************************;
* PRES_L3_N
********************************************************************************;
%let qname=pres_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=prescribing,encvar=patid,ord=1)
%enc_oneway(encdsn=prescribing,encvar=prescribingid,ord=2)
%enc_oneway(encdsn=prescribing,encvar=encounterid,ord=3)
%enc_oneway(encdsn=prescribing,encvar=rx_providerid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXCUI
********************************************************************************;
%let qname=pres_l3_rxcui;
%elapsed(begin);

*- Sort prescribing and reference data for merging -*;
proc sort data=rxnorm_cui_ref out=rxnorm_cui_ref;
     by rxnorm_cui;
run;

proc sort data=prescribing(keep=rxnorm_cui patid) out=rxnorm_cui;
     by rxnorm_cui;
run;

*- Derive categorical variable -*;
data data;
     length rxnormcuitty col1 $15;
     merge rxnorm_cui(in=rx) rxnorm_cui_ref;
     by rxnorm_cui;
     if rx;

     if rxnorm_cui_tty^=" " then rxnormcuitty=strip(rxnorm_cui_tty);
     else if rxnorm_cui_tty=" " then rxnormcuitty="ZZZA";

     if rxnorm_cui^=" " then col1=strip(rxnorm_cui);
     else if rxnorm_cui=" " then col1="ZZZA";
    
     keep col1 rxnormcuitty patid;
run;

*- Derive statistics -*;
proc means data=data noprint missing;
     class col1 rxnormcuitty;
     output out=stats;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by rxnormcuitty col1 patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1 rxnormcuitty;
     output out=pid_unique;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=3))
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 rxnormcuitty;

     * call standard variables *;
     %stdvar

     * table values *;
     rxnorm_cui=put(col1,$null.);
     rxnorm_cui_tty=put(rxnormcuitty,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package rxnorm_cui rxnorm_cui_tty 
          record_n record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rxnorm_cui, rxnorm_cui_tty, 
             record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres rxnorm_cui rxnorm_cui_ref);

********************************************************************************;
* PRES_L3_RXCUI_TIER
********************************************************************************;
%let qname=pres_l3_rxcui_tier;
%elapsed(begin);

proc format;
     value $cuitier
       "Tier 1"="Tier 1"
       "Tier 2"="Tier 2"
       "Tier 3"="Tier 3"
       "Tier 4"="Tier 4"
       "ZZZA"="NULL or missing"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $15;
     merge rxnorm_cui(in=rx) rxnorm_cui_ref;
     by rxnorm_cui;
     if rx;

     if rxnorm_cui_tier^=" " then col1=strip(rxnorm_cui_tier);
     else if rxnorm_cui_tier=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $cuitier.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     rxnorm_cui_tier=put(col1,$cuitier.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package rxnorm_cui_tier
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rxnorm_cui_tier,
             record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXDOSEFORM
********************************************************************************;
%let qname=pres_l3_rxdoseform;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set prescribing(keep=rx_dose_form);

     col1=put(rx_dose_form,$rx_dose_form.);
     if rx_dose_form^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if rx_dose_form^=col1 then col1="ZZZE";
     end;

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1)) 
           rx_dose_form(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then rx_dose_form=col1;
     else rx_dose_form=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_dose_form record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_dose_form, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_BASIS
********************************************************************************;
%let qname=pres_l3_basis;
%elapsed(begin);

proc format;
     value $pres_bas
       "01"="01"
       "02"="02"
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
     set prescribing(keep=rx_basis);

     if rx_basis in ("01" "02") then col1=rx_basis;
     else if rx_basis in ("NI") then col1="ZZZA";
     else if rx_basis in ("UN") then col1="ZZZB";
     else if rx_basis in ("OT") then col1="ZZZC";
     else if rx_basis=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pres_bas.;
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
     rx_basis=put(col1,$pres_bas.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_basis record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_basis, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_DISPASWRTN
********************************************************************************;
%let qname=pres_l3_dispaswrtn;
%elapsed(begin);

proc format;
     value $pres_aswrtn
       "N"="N"
       "Y"="Y"
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
     set prescribing(keep=rx_dispense_as_written);

     if rx_dispense_as_written in ("N" "Y") then col1=rx_dispense_as_written;
     else if rx_dispense_as_written in ("NI") then col1="ZZZA";
     else if rx_dispense_as_written in ("UN") then col1="ZZZB";
     else if rx_dispense_as_written in ("OT") then col1="ZZZC";
     else if rx_dispense_as_written=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pres_aswrtn.;
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
     rx_dispense_as_written=put(col1,$pres_aswrtn.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_dispense_as_written record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_dispense_as_written,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_FREQ
********************************************************************************;
%let qname=pres_l3_freq;
%elapsed(begin);

proc format;
     value $pres_frq
       "01"="01"
       "02"="02"
       "03"="03"
       "04"="04"
       "05"="05"
       "06"="06"
       "07"="07"
       "08"="08"
       "10"="10"
       "11"="11"
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
     set prescribing(keep=rx_frequency);

     if rx_frequency in ("01" "02" "03" "04" "05" "06" "07" "08" "10" "11") then col1=rx_frequency;
     else if rx_frequency in ("NI") then col1="ZZZA";
     else if rx_frequency in ("UN") then col1="ZZZB";
     else if rx_frequency in ("OT") then col1="ZZZC";
     else if rx_frequency=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pres_frq.;
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
     rx_frequency=put(col1,$pres_frq.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_frequency record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_frequency, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_ODATE_Y
********************************************************************************;
%let qname=pres_l3_odate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set prescribing(keep=rx_order_date patid rxnorm_cui);

     if rx_order_date^=. then year=year(rx_order_date);
     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";
    
     keep col1 patid rxnorm_cui;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive statistics - RXNORM_CUI -*;
proc sort data=data out=rxnorm_cui nodupkey;
     by col1 rxnorm_cui;
     where rxnorm_cui^=" ";
run;

proc means data=rxnorm_cui nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=rxnormcui_unique;
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
           rxnormcui_unique(rename=(_freq_=record_rxcui))
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     rx_order_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_rxcui,nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     record_n_rxcui=strip(put(record_rxcui,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_order_date record_n record_pct 
          record_n_rxcui distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_order_date, record_n, 
            record_pct, record_n_rxcui, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_ODATE_YM
********************************************************************************;
%let qname=pres_l3_odate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set prescribing(keep=rx_order_date patid);

     * create a year and a year/month numeric variable *;
     if rx_order_date^=. then do;
        year=year(rx_order_date);
        year_month=(year(rx_order_date)*100)+month(rx_order_date);
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
     * loop if discharge date is populated *;
     if nmiss(min,max)=0 then do;
         do y = min to max(max,year(today()));
            do m = 1 to 12;
                _type_=1;
                _freq_=0;
                year_month=(y*100)+m;
                output;
            end;
         end;
     end;
     * loop if rx_order date is not populated *;
     else do;
        _type_=1;
        _freq_=0;
        year_month=.;
     end;
     * macro variable to test presence of rx_order date *;
     call symput("minact_present",strip(put(nmiss(min,max),8.)));
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats(in=s) pid_unique(rename=(_freq_=distinct_patid));
     by year_month;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n distinct_patid_n $20;
     %if &minact_present=0 %then %do;
         if _n_=1 then set minact;
         if _n_=1 then set maxact;
     %end;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     %if &minact_present=0 %then %do;
         if year_month<minact or maxact<year_month<99999999 then delete;
     %end;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then rx_order_date=put(year_month,null.);
     else rx_order_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package rx_order_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_order_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXCUI_RXSUP
********************************************************************************;
%let qname=pres_l3_rxcui_rxsup;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set prescribing(keep=patid rx:);

     if rxnorm_cui^=" " then col1=strip(rxnorm_cui);
     else if rxnorm_cui=" " then col1="ZZZA";
    
     keep col1 patid rx_days_supply rx_quantity rx_refills;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1/preloadfmt;
     var rx_days_supply;
     output out=stats n=rxn mean=rxmean max=rxmax min=rxmin nmiss=rxnmiss;
     format col1 $null.;
run;

data query;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     rxnorm_cui=put(col1,$null.);

     * stats *;
     min=strip(put(rxmean,16.));
     mean=strip(put(rxmean,16.));
     max=strip(put(rxmax,16.));
     n=strip(put(rxn,threshold.));
     nmiss=strip(put(rxnmiss,threshold.));
        
     keep datamartid response_date query_package rxnorm_cui min mean 
          max n nmiss;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rxnorm_cui, min,
         mean, max, n, nmiss
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_SUPDIST2
********************************************************************************;
%let qname=pres_l3_supdist2;
%elapsed(begin);

proc format;
     value dispense2dist
        low - <1="<1 day"
        1-15="1-15 days"
       16-30="16-30 days"
       31-60="31-60 days"
       61-90="61-90 days"
       91-99998=">90 days"
      99999="NULL or missing"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set prescribing(keep=patid rx_days_supply);

     * set missing to impossible high value for categorization *;
     if rx_days_supply^=. then col1=rx_days_supply;
     else if rx_days_supply=. then col1=99999;
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 dispense2dist.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     rx_days_supply_group=put(col1,dispense2dist.);

     * apply threshold *;
     %threshold(nullval=col1^=99999)
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package rx_days_supply_group record_n
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_days_supply_group, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXQTY_DIST
********************************************************************************;
%let qname=pres_l3_rxqty_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=prescribing nway noprint;
     var rx_quantity;
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
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXREFILL_DIST
********************************************************************************;
%let qname=pres_l3_rxrefill_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=prescribing nway noprint;
     var rx_refills;
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
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_PRNFLAG
********************************************************************************;
%let qname=pres_l3_prnflag;
%elapsed(begin);

proc format;
     value $pres_prn
       "N"="N"
       "Y"="Y"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set prescribing(keep=rx_prn_flag);

     if rx_prn_flag in ("N" "Y") then col1=rx_prn_flag;
     else if rx_prn_flag=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pres_prn.;
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
     rx_prn_flag=put(col1,$pres_prn.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_prn_flag record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_prn_flag,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXDOSEODR_DIST
********************************************************************************;
%let qname=pres_l3_rxdoseodr_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=prescribing nway noprint;
     var rx_dose_ordered;
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
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RXDOSEODRUNIT
********************************************************************************;
%let qname=pres_l3_rxdoseodrunit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set prescribing(keep=patid rx_dose_ordered_unit);

     col1=put(rx_dose_ordered_unit,_unit.);
     if rx_dose_ordered_unit^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if rx_dose_ordered_unit^=col1 then col1="ZZZE";
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
     length record_n $20 rx_dose_ordered_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           short_x_unit(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then rx_dose_ordered_unit=col1;
     else rx_dose_ordered_unit=put(col1,$other.);
     if (d and s) or rx_dose_ordered_unit in ("NI" "UN" "OT") then short_yn="Y";
     else short_yn="N";

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package rx_dose_ordered_unit short_yn
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_dose_ordered_unit, short_yn,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_ROUTE
********************************************************************************;
%let qname=pres_l3_route;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set prescribing(keep=patid rx_route);

     col1=put(rx_route,$_route.);
     if rx_route^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if rx_route^=col1 then col1="ZZZE";
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
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then rx_route=col1;
     else rx_route=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package rx_route record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_route, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_SOURCE
********************************************************************************;
%let qname=pres_l3_source;
%elapsed(begin);

proc format;
     value $pres_src
       "OD"="OD"
       "DR"="DR"
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
     set prescribing(keep=rx_source);

     if rx_source in ("OD" "DR") then col1=rx_source;
     else if rx_source in ("NI") then col1="ZZZA";
     else if rx_source in ("UN") then col1="ZZZB";
     else if rx_source in ("OT") then col1="ZZZC";
     else if rx_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pres_src.;
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
     rx_source=put(col1,$pres_src.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_source record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_source,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savepres);

********************************************************************************;
* PRES_L3_RAWRXMED
********************************************************************************;
%let qname=pres_l3_rawrxmed;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set prescribing(keep=raw_rx_med_name);

     if raw_rx_med_name^=" " then col1=raw_rx_med_name;
     else if raw_rx_med_name=" " then col1="ZZZA";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then raw_rx_med_name=col1;
     else raw_rx_med_name=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package raw_rx_med_name record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, raw_rx_med_name, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in MED_ADMIN and compress it
********************************************************************************;
%if &_ymed_admin=1 %then %do;
%let qname=med_admin;
%elapsed(begin);

data med_admin(compress=yes);
     set pcordata.med_admin;

     * restrict data to within lookback period *;
     if medadmin_start_date>=&lookback_dt or medadmin_start_date=.;

     * for mismatch query *;
     providerid=medadmin_providerid;
run;

proc sort data=med_admin;
     by patid;
run;

%let savemedadm=med_admin;

%elapsed(end);

********************************************************************************;
* MEDADM_L3_DOSEADM
********************************************************************************;
%let qname=medadm_l3_doseadm;
%elapsed(begin);

*- Derive statistics -*;
proc means data=med_admin nway noprint;
     var medadmin_dose_admin;
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
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_N
********************************************************************************;
%let qname=medadm_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=med_admin,encvar=patid,ord=1)
%enc_oneway(encdsn=med_admin,encvar=medadminid,ord=2)
%enc_oneway(encdsn=med_admin,encvar=medadmin_providerid,ord=3)
%enc_oneway(encdsn=med_admin,encvar=encounterid,ord=4)
%enc_oneway(encdsn=med_admin,encvar=prescribingid,ord=5)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_DOSEADMUNIT
********************************************************************************;
%let qname=medadm_l3_doseadmunit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set med_admin(keep=medadmin_dose_admin_unit);

     col1=put(medadmin_dose_admin_unit,_unit.);
     if medadmin_dose_admin_unit^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if medadmin_dose_admin_unit^=col1 then col1="ZZZE";
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
     length record_n $20 medadmin_dose_admin_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           short_x_unit(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then medadmin_dose_admin_unit=col1;
     else medadmin_dose_admin_unit=put(col1,$other.);
     if (d and s) or medadmin_dose_admin_unit in ("NI" "UN" "OT") then short_yn="Y";
     else short_yn="N";

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package medadmin_dose_admin_unit short_yn
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_dose_admin_unit, 
            short_yn, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_ROUTE
********************************************************************************;
%let qname=medadm_l3_route;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set med_admin(keep=patid medadmin_route);

     col1=put(medadmin_route,$_route.);
     if medadmin_route^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if medadmin_route^=col1 then col1="ZZZE";
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
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then medadmin_route=col1;
     else medadmin_route=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package medadmin_route
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_route, record_n, 
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_SOURCE
********************************************************************************;
%let qname=medadm_l3_source;
%elapsed(begin);

proc format;
     value $medsource
         "DR"="DR"
         "OD"="OD"
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
     set med_admin(keep=medadmin_source);

     if medadmin_source in ("OD" "DR") then col1=medadmin_source;
     else if medadmin_source in ("NI") then col1="ZZZA";
     else if medadmin_source in ("UN") then col1="ZZZB";
     else if medadmin_source in ("OT") then col1="ZZZC";
     else if medadmin_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $medsource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     medadmin_source=put(col1,$medsource.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package medadmin_source record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_source, record_n,
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_TYPE
********************************************************************************;
%let qname=medadm_l3_type;
%elapsed(begin);

proc format;
     value $medtype
         "ND"="ND"
         "RX"="RX"
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
     set med_admin(keep=medadmin_type);

     if medadmin_type in ("ND" "RX") then col1=medadmin_type;
     else if medadmin_type in ("NI") then col1="ZZZA";
     else if medadmin_type in ("UN") then col1="ZZZB";
     else if medadmin_type in ("OT") then col1="ZZZC";
     else if medadmin_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $medtype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     medadmin_type=put(col1,$medtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package medadmin_type record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_type, record_n,
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_CODE_TYPE
********************************************************************************;
%let qname=medadm_l3_code_type;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length medadmintype $4;
     set med_admin(keep=medadmin_type medadmin_code patid);

     if medadmin_type in ("ND" "RX") then medadmintype=medadmin_type;
     else if medadmin_type in ("NI") then medadmintype="ZZZA";
     else if medadmin_type in ("UN") then medadmintype="ZZZB";
     else if medadmin_type in ("OT") then medadmintype="ZZZC";
     else if medadmin_type=" " then medadmintype="ZZZD";
     else medadmintype="ZZZE";
    
     if medadmin_code^=" " then col1=medadmin_code;
     else if medadmin_code=" " then col1="ZZZA";
    
     keep medadmintype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 medadmintype/preloadfmt;
     output out=stats;
     format medadmintype $medtype. col1 $null.;
run;

*- Derive distinct patient id for each med type -*;
proc sort data=data out=pid nodupkey;
     by col1 medadmintype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 medadmintype/preloadfmt;
     output out=pid_unique;
     format medadmintype $medtype. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 medadmintype;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then medadmin_code=col1;
     else medadmin_code=put(col1,$null.);
     medadmin_type=put(medadmintype,$medtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and medadmintype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and medadmintype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package medadmin_type medadmin_code
          record_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_code, medadmin_type, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_SDATE_Y
********************************************************************************;
%let qname=medadm_l3_sdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set med_admin(keep=medadmin_start_date patid);

     if medadmin_start_date^=. then year=year(medadmin_start_date);

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
     medadmin_start_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package medadmin_start_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_start_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savemedadm);

********************************************************************************;
* MEDADM_L3_SDATE_YM
********************************************************************************;
%let qname=medadm_l3_sdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set med_admin(keep=medadmin_start_date patid);

     * create a year and a year/month numeric variable *;
     if medadmin_start_date^=. then do;
         year=year(medadmin_start_date);
         year_month=(year(medadmin_start_date)*100)+month(medadmin_start_date);
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
     set stats end=eof;
     if _n_=1 then do;
        if year_month=99999999 then year_month=.;
        output minact;
     end;        
     if eof then do;
        if year_month=99999999 then year_month=.;
        output maxact;
     end;
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
     if min>0 then do;
         do y = min to max(max,year(today()));
            do m = 1 to 12;
                _type_=1;
                _freq_=0;
                year_month=(y*100)+m;
                output;
            end;
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
     if year_month<minact or .<maxact<year_month<99999999 then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then medadmin_start_date=put(year_month,null.);
     else medadmin_start_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package medadmin_start_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_start_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in OBS_CLIN and compress it
********************************************************************************;
%if &_yobs_clin=1 %then %do;
%let qname=obs_clin;
%elapsed(begin);

data obs_clin(compress=yes);
     set pcordata.obs_clin;

     * restrict data to within lookback period *;
     if obsclin_date>=&lookback_dt or obsclin_date=.;

     * for mismatch query *;
     providerid=obsclin_providerid;
run;

proc sort data=obs_clin;
     by patid;
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
* OBSCLIN_L3_CODE_TYPE
********************************************************************************;
%let qname=obsclin_l3_code_type;
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
     length record_n distinct_patid_n $20;
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
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in CONDITION and compress it
********************************************************************************;
%if &_ycondition=1 %then %do;
%let qname=condition;
%elapsed(begin);

data condition(compress=yes);
     set pcordata.condition;

     * restrict data to within lookback period *;
     if report_date>=&lookback_dt or report_date=.;
run;

proc sort data=condition;
     by patid;
run;

%let savecond=condition;

%elapsed(end);

********************************************************************************;
* COND_L3_N
********************************************************************************;
%let qname=cond_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=condition,encvar=patid,ord=1)
%enc_oneway(encdsn=condition,encvar=encounterid,ord=2)
%enc_oneway(encdsn=condition,encvar=conditionid,ord=3)
%enc_oneway(encdsn=condition,encvar=condition,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_CONDITION
********************************************************************************;
%let qname=cond_l3_condition;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $20;
     set condition(keep=condition patid);

     if condition^=" " then col1=strip(condition);
     else if condition=" " then col1="ZZZA";
    
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
     if col1 not in ("ZZZA") then condition=col1;
     else condition=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package condition record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, condition, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_RDATE_Y
********************************************************************************;
%let qname=cond_l3_rdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set condition(keep=report_date patid);

     if report_date^=. then year=year(report_date);

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
     report_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package report_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, report_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_RDATE_YM
********************************************************************************;
%let qname=cond_l3_rdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set condition(keep=report_date);

     * create a year and a year/month numeric variable *;
     if report_date^=. then do;
         year=year(report_date);
         year_month=(year(report_date)*100)+month(report_date);
     end;

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;

     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     * in case REPORT_DATE is not populated *;
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
     merge dummy stats(in=s);
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
     if year_month=99999999 then report_date=put(year_month,null.);
     else report_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package report_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, report_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_STATUS
********************************************************************************;
%let qname=cond_l3_status;
%elapsed(begin);

proc format;
     value $cstatus
          "AC"="AC"
          "IN"="IN"
          "RS"="RS"
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
     set condition(keep=condition_status);

     if upcase(condition_status) in ("AC" "IN" "RS") then col1=condition_status;
     else if condition_status in ("NI") then col1="ZZZA";
     else if condition_status in ("UN") then col1="ZZZB";
     else if condition_status in ("OT") then col1="ZZZC";
     else if condition_status=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $cstatus.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     condition_status=put(col1,$cstatus.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package condition_status record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, condition_status, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_TYPE
********************************************************************************;
%let qname=cond_l3_type;
%elapsed(begin);

proc format;
     value $ctype
          "09"="09"
          "10"="10"
          "11"="11"
          "AG"="AG"
          "HP"="HP"
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
     length col1 $4;
     set condition(keep=condition_type);

     if upcase(condition_type) in ("09" "10" "11" "AG" "HP" "SM") then col1=condition_type;
     else if condition_type in ("NI") then col1="ZZZA";
     else if condition_type in ("UN") then col1="ZZZB";
     else if condition_type in ("OT") then col1="ZZZC";
     else if condition_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $ctype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     condition_type=put(col1,$dmatch.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package condition_type record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, condition_type, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savecond);

********************************************************************************;
* COND_L3_SOURCE
********************************************************************************;
%let qname=cond_l3_source;
%elapsed(begin);

proc format;
     value $csource
          "HC"="D"
          "PC"="L"
          "PR"="N"
          "RG"="S"
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
     set condition(keep=condition_source);

     if upcase(condition_source) in ("HC" "PC" "PR" "RG") then col1=condition_source;
     else if condition_source in ("NI") then col1="ZZZA";
     else if condition_source in ("UN") then col1="ZZZB";
     else if condition_source in ("OT") then col1="ZZZC";
     else if condition_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $csource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     condition_source=put(col1,$dsource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package condition_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, condition_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in PRO_CM and compress it
********************************************************************************;
%if &_ypro_cm=1 %then %do;
%let qname=pro_cm;
%elapsed(begin);

data pro_cm(compress=yes);
     set pcordata.pro_cm;

     * restrict data to within lookback period *;
     if pro_date>=&lookback_dt or pro_date=.;
run;

proc sort data=pro_cm;
     by patid;
run;

%let saveprocm=pro_cm;

%elapsed(end);

********************************************************************************;
* PROCM_L3_N
********************************************************************************;
%let qname=procm_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=pro_cm,encvar=patid,ord=1)
%enc_oneway(encdsn=pro_cm,encvar=encounterid,ord=2)
%enc_oneway(encdsn=pro_cm,encvar=pro_cm_id,ord=3)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_ITEM - retired with V4.00
********************************************************************************;
/*
%let qname=procm_l3_item;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set pro_cm(keep=pro_item patid);

     if pro_item in ("PN_0001" "PN_0002" "PN_0003" "PN_0004" "PN_0005" "PN_0006" 
                     "PN_0007" "PN_0008" "PN_0009" "PN_0010" "PN_0011" "PN_0012" 
                     "PN_0013" "PN_0014" "PN_0015" "PN_0016" "PN_0017" "PN_0018" 
                     "PN_0019" "PN_0020" "PN_0021") then col1=strip(pro_item);
     else if pro_item=" " then col1="ZZZA";
     else col1="ZZZB";
    
     keep col1 patid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $nullout.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $nullout.;
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
     pro_item=put(col1,$nullout.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_item record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_item, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savecond &saveprocm);
*/
********************************************************************************;
* PROCM_L3_PDATE_Y
********************************************************************************;
%let qname=procm_l3_pdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set pro_cm(keep=pro_date patid);

     if pro_date^=. then year=year(pro_date);

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
     pro_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_PDATE_YM
********************************************************************************;
%let qname=procm_l3_pdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set pro_cm(keep=pro_date);

     * create a year and a year/month numeric variable *;
     if pro_date^=. then do;
         year=year(pro_date);
         year_month=(year(pro_date)*100)+month(pro_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     keep year year_month;
run;

*- Derive statistics -*;
proc means data=data(drop=year) nway completetypes noprint missing;
     class year_month;
     output out=stats;
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
     merge dummy stats(in=s);
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
     if year_month=99999999 then pro_date=put(year_month,null.);
     else pro_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package pro_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_date, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_METHOD
********************************************************************************;
%let qname=procm_l3_method;
%elapsed(begin);

proc format;
     value $pmethod
          "PA"="PA"
          "EC"="EC"
          "PH"="PH"
          "IV"="IV"
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
     set pro_cm(keep=pro_method);

     if upcase(pro_method) in ("PA" "EC" "PH" "IV") then col1=pro_method;
     else if pro_method in ("NI") then col1="ZZZA";
     else if pro_method in ("UN") then col1="ZZZB";
     else if pro_method in ("OT") then col1="ZZZC";
     else if pro_method=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pmethod.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pro_method=put(col1,$pmethod.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_method record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_method, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);


********************************************************************************;
* PROCM_L3_MODE
********************************************************************************;
%let qname=procm_l3_mode;
%elapsed(begin);

proc format;
     value $pmode
          "SF"="SF"
          "SA"="SA"
          "PR"="PR"
          "PA"="PA"
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
     set pro_cm(keep=pro_mode);

     if upcase(pro_mode) in ("SF" "SA" "PR" "PA") then col1=pro_mode;
     else if pro_mode in ("NI") then col1="ZZZA";
     else if pro_mode in ("UN") then col1="ZZZB";
     else if pro_mode in ("OT") then col1="ZZZC";
     else if pro_mode=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pmode.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pro_mode=put(col1,$pmode.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_mode record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_mode, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_CAT
********************************************************************************;
%let qname=procm_l3_cat;
%elapsed(begin);

proc format;
     value $pcat
          "Y"="Y"
          "N"="N"
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
     set pro_cm(keep=pro_cat);

     if upcase(pro_cat) in ("Y" "N") then col1=pro_cat;
     else if pro_cat in ("NI") then col1="ZZZA";
     else if pro_cat in ("UN") then col1="ZZZB";
     else if pro_cat in ("OT") then col1="ZZZC";
     else if pro_cat=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pcat.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pro_cat=put(col1,$pcat.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_cat record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_cat, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_LOINC
********************************************************************************;
%let qname=procm_l3_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set pro_cm(keep=pro_item_loinc patid);

     if pro_item_loinc^=" " then col1=pro_item_loinc;
     else if pro_item_loinc=" " then col1="ZZZA";

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
     if col1 not in ("ZZZA") then pro_item_loinc=col1;
     else pro_item_loinc=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package pro_item_loinc record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_item_loinc, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_ITEMFULLNAME
********************************************************************************;
%let qname=procm_l3_itemfullname;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set pro_cm(keep=pro_item_fullname);

     if pro_item_fullname^=" " then col1=pro_item_fullname;
     else if pro_item_fullname=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;    
     if col1 not in ("ZZZA") then pro_item_fullname=col1;
     else pro_item_fullname=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_item_fullname record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_item_fullname, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_ITEMNM
********************************************************************************;
%let qname=procm_l3_itemnm;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set pro_cm(keep=pro_item_name);

     if pro_item_name^=" " then col1=pro_item_name;
     else if pro_item_name=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then pro_item_name=col1;
     else pro_item_name=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_item_name record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_item_name, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_MEASURE_FULLNAME
********************************************************************************;
%let qname=procm_l3_measure_fullname;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set pro_cm(keep=pro_measure_fullname);

     if pro_measure_fullname^=" " then col1=pro_measure_fullname;
     else if pro_measure_fullname=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then pro_measure_fullname=col1;
     else pro_measure_fullname=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_measure_fullname record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_measure_fullname, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_MEASURENM
********************************************************************************;
%let qname=procm_l3_measurenm;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set pro_cm(keep=pro_measure_name);

     if pro_measure_name^=" " then col1=pro_measure_name;
     else if pro_measure_name=" " then col1="ZZZA";
    
     keep col1;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then pro_measure_name=col1;
     else pro_measure_name=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_measure_name record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_measure_name, record_n,
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_TYPE
********************************************************************************;
%let qname=procm_l3_type;
%elapsed(begin);

proc format;
     value $ptype
          "AM"="AM"
          "HC"="HC"
          "LC"="LC"
          "NQ"="NQ"
          "NT"="NT"
          "PC"="PC"
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
     length col1 $4;
     set pro_cm(keep=pro_type);

     if pro_type in ("AM" "HC" "LC" "NQ" "NT" "PC" "PM") then col1=pro_type;
     else if pro_type in ("NI") then col1="ZZZA";
     else if pro_type in ("UN") then col1="ZZZB";
     else if pro_type in ("OT") then col1="ZZZC";
     else if pro_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $ptype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pro_type=put(col1,$ptype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_type, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in PROVIDER and compress it
********************************************************************************;
%if &_yprovider=1 %then %do;
%let qname=provider;
%elapsed(begin);

data provider(compress=yes);
     set pcordata.provider;
run;

proc sort data=provider;
     by providerid;
run;

%let saveprov=provider;

%elapsed(end);

********************************************************************************;
* PROV_L3_N
********************************************************************************;
%let qname=prov_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=provider,encvar=providerid,ord=1)
%enc_oneway(encdsn=provider,encvar=provider_npi,_nc=1,ord=2)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprov);

********************************************************************************;
* PROV_L3_NPIFLAG
********************************************************************************;
%let qname=prov_l3_npiflag;
%elapsed(begin);

proc format;
     value $npiflag
          "Y"="Y"
          "N"="N"
        "ZZZD"="NULL or missing"
        "ZZZE"="Values outside of CDM specifications"
        ;
run;
    
*- Derive categorical variable -*;
data data;
     length col1 $4;
     set provider(keep=provider_npi_flag);

     if provider_npi_flag in ("Y" "N") then col1=provider_npi_flag;
     else if provider_npi_flag=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $npiflag.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     provider_npi_flag=put(col1,$npiflag.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package provider_npi_flag record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, provider_npi_flag, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprov);

********************************************************************************;
* PROV_L3_SPECIALTY
********************************************************************************;
%let qname=prov_l3_specialty;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set provider(keep=provider_specialty_primary);

     col1=put(provider_specialty_primary,$prov_spec.);
     if provider_specialty_primary^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if provider_specialty_primary^=col1 then col1="ZZZE";
     end;

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 provider_specialty_primary $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           prov_spec(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then provider_specialty_primary=col1;
     else provider_specialty_primary=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package provider_specialty_primary
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, provider_specialty_primary, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprov);

********************************************************************************;
* PROV_L3_SPECIALTY_GROUP
********************************************************************************;
%let qname=prov_l3_specialty_group;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     set provider(keep=provider_specialty_primary);

     prime=put(provider_specialty_primary,$prov_spec.);
     col1=put(provider_specialty_primary,$prov_group.);
     if prime^=" " then do;
         if prime="NI" then col1="ZZZA";
         else if prime="UN" then col1="ZZZB";
         else if prime="OT" then col1="ZZZC";
         else if prime="ZZZD" then col1="ZZZD";
         else if provider_specialty_primary^=prime then col1="ZZZE";
     end;

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $other.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then provider_specialty_group=col1;
     else provider_specialty_group=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package provider_specialty_group
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, provider_specialty_group, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveprov);

********************************************************************************;
* PROV_L3_SEX
********************************************************************************;
%let qname=prov_l3_sex;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set provider(keep=provider_sex);

     if upcase(provider_sex) in ("A" "F" "M") then col1=provider_sex;
     else if provider_sex in ("NI") then col1="ZZZA";
     else if provider_sex in ("UN") then col1="ZZZB";
     else if provider_sex in ("OT") then col1="ZZZC";
     else if provider_sex=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $sex.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     provider_sex=put(col1,$sex.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package provider_sex record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, provider_sex, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in PCORNET_TRIAL and compress it
********************************************************************************;
%if &_yptrial=1 %then %do;
%let qname=pcornet_trial;
%elapsed(begin);

*- Determine concatonated length of variables used to determine TRIAL_KEY -*;
proc contents data=pcordata.pcornet_trial out=cont_trial noprint;
run;

data _null_;
     set cont_trial end=eof;
     retain tulength 0;
     if name in ("PATID" "TRIALID" "PARTICIPANTID") then tulength=tulength+length;
    
     * add 2 to TLENGTH (2 for delimiter) *;
     if eof then call symput("_tulength",strip(put(tulength+2,8.)));
run;

data pcornet_trial(compress=yes);
     length trial_key $&_tulength;
     set pcordata.pcornet_trial;

     * restrict data to within lookback period *;
     if trial_enroll_date>=&lookback_dt or trial_enroll_date=.;

     trial_key=strip(patid)||"_"||strip(trialid)||"_"||strip(participantid);
run;

proc sort data=pcornet_trial;
     by patid;
run;

%elapsed(end);

********************************************************************************;
* TRIAL_L3_N
********************************************************************************;
%let qname=trial_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=pcornet_trial,encvar=patid,ord=1)
%enc_oneway(encdsn=pcornet_trial,encvar=trialid,ord=2)
%enc_oneway(encdsn=pcornet_trial,encvar=participantid,ord=3)
%enc_oneway(encdsn=pcornet_trial,encvar=trial_key,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

*******************************************************************************;
* Create dataset of query execution time
*******************************************************************************;
data dmlocal.elapsed_main;
     set elapsed;
     if query^=" ";
     label _qstart="Query start time"
           _qend="Query end time"
           elapsedtime="Query run time (hh:mm:ss)"
           totalruntime="Cumulative run time (hh:mm:ss)"
     ;
     if query="DC PROGRAM" then do;
        query="DC PROGRAM - MAIN";
        _qend=datetime();
        elapsedtime=_qend-_qstart;
        totalruntime=_qend-&_pstart;
     end;

     * call standard variables *;
     %stdvar
run;
 
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
* Create a SAS transport file from all of the query datasets
********************************************************************************;
proc contents data=dmlocal._all_ out=dsn noprint;
run;

*- Place all dataset names and labels into macro variables -*;
proc sql noprint;
     select unique memname into :workdata separated by ' '  from dsn
         where (scan(upcase(memname),1,'_') in ("DEATH" "DEATHC" "DEM" "ENC" "DIA"
               "PRO" "ENR" "VIT" "DISP" "PRES" "MEDADM" "OBSCLIN" "COND" "PROCM" 
               "PROV" "TRIAL") and scan(upcase(memname),2,'_')="L3") or
               (upcase(memname)="ELAPSED_MAIN");
quit;
%put &workdata;

filename tranfile "&qpath.drnoc/&dmid._&tday._data_curation_&_grp..cpt";
proc cport library=dmlocal file=tranfile memtype=data;
     select &workdata;
run;        

********************************************************************************;

*******************************************************************************;
* Print each data set and send to a PDF file
*******************************************************************************;
ods html close;
ods listing;
ods path sashelp.tmplmst(read) library.templat(read);
ods pdf file="&qpath.drnoc/&dmid._&tday._data_curation_&_grp..pdf" style=journal;

title "Data Curation query run times - MAIN";
footnote;
proc print width=min data=dmlocal.elapsed_main label;
     var query _qstart _qend elapsedtime totalruntime;
run;
title;

%if &_ydemographic=1 %then %do;
    %prnt(pdsn=dem_l3_n);
    %if &_ydeath=1 %then %do;
        %prnt(pdsn=dem_l3_ageyrsdist1);
        %prnt(pdsn=dem_l3_ageyrsdist2);
    %end;
    %prnt(pdsn=dem_l3_hispdist);
    %prnt(pdsn=dem_l3_racedist);
    %prnt(pdsn=dem_l3_sexdist);
    %prnt(pdsn=dem_l3_orientdist);
    %prnt(pdsn=dem_l3_genderdist);
    %prnt(pdsn=dem_l3_patpreflang,_obs=100,_svar=pat_pref_language_spoken,_suppvar=record_n record_pct);
%end;
%if &_yencounter=1 %then %do;
    %prnt(pdsn=enc_l3_n);
    /*%prnt(pdsn=enc_l3_n_visit); retired in v3.02 */
    %prnt(pdsn=enc_l3_admsrc);
    %prnt(pdsn=enc_l3_enctype_admsrc,_obs=100,_svar=enc_type admitting_source,_suppvar=record_n record_pct);
    %prnt(pdsn=enc_l3_adate_y);
    %prnt(pdsn=enc_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
    %prnt(pdsn=enc_l3_enctype_adate_y);
    %prnt(pdsn=enc_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=enc_l3_ddate_y);
    %prnt(pdsn=enc_l3_ddate_ym,_obs=100,_svar=discharge_date,_suppvar=record_n );
    %prnt(pdsn=enc_l3_enctype_ddate_ym,_obs=100,_svar=enc_type discharge_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=enc_l3_disdisp);
    %prnt(pdsn=enc_l3_enctype_disdisp);
    %prnt(pdsn=enc_l3_disstat);
    %prnt(pdsn=enc_l3_enctype_disstat,_obs=100,_svar=enc_type discharge_status,_suppvar=record_n record_pct);
    %prnt(pdsn=enc_l3_drg,_obs=100,_svar=drg,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=enc_l3_drg_type);
    %prnt(pdsn=enc_l3_enctype_drg,_obs=100,_svar=enc_type drg,_suppvar=record_n record_pct);
    %prnt(pdsn=enc_l3_enctype);
    %prnt(pdsn=enc_l3_dash1);
    %prnt(pdsn=enc_l3_dash2);
    %prnt(pdsn=enc_l3_payertype1,_obs=100,_svar=payer_type_primary_grp payer_type_primary,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=enc_l3_payertype2,_obs=100,_svar=payer_type_secondary_grp payer_type_secondary,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=enc_l3_facilitytype,_obs=100,_svar=facility_type_grp facility_type,_suppvar=record_n record_pct distinct_patid_n);
%end;
%if &_ydiagnosis=1 %then %do;    
    %prnt(pdsn=dia_l3_n);
    %prnt(pdsn=dia_l3_dx,_obs=100,_svar=dx,_suppvar=record_n record_pct);
    %prnt(pdsn=dia_l3_dx_dxtype,_obs=100,_svar=dx dx_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=dia_l3_dxsource);
    %prnt(pdsn=dia_l3_dxtype_dxsource);
    %prnt(pdsn=dia_l3_pdx);
    %prnt(pdsn=dia_l3_pdx_enctype);
    %prnt(pdsn=dia_l3_pdxgrp_enctype);
    %prnt(pdsn=dia_l3_adate_y);
    %prnt(pdsn=dia_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
    %prnt(pdsn=dia_l3_origin);
    %prnt(pdsn=dia_l3_enctype);
    %prnt(pdsn=dia_l3_dxtype_enctype);
    %prnt(pdsn=dia_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_encid_n distinct_patid_n);
    %prnt(pdsn=dia_l3_dash1);
    %prnt(pdsn=dia_l3_dxtype_adate_y,_obs=100,_svar=dx_type admit_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=dia_l3_dxpoa);
    %prnt(pdsn=dia_l3_pdx_detail);
%end;
%if &_yprocedures=1 %then %do;
    %prnt(pdsn=pro_l3_n);
    %prnt(pdsn=pro_l3_px,_obs=100,_svar=px,_suppvar=record_n record_pct);
    %prnt(pdsn=pro_l3_adate_y);
    %prnt(pdsn=pro_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
    %prnt(pdsn=pro_l3_pxdate_y);
    /*%prnt(pdsn=pro_l3_px_enctype,_obs=100,_svar=px enc_type,_suppvar=record_n distinct_patid_n); retired in v3.02 */
    %prnt(pdsn=pro_l3_enctype);
    %prnt(pdsn=pro_l3_pxtype_enctype,_obs=100,_svar=px_type enc_type,_suppvar=record_n);
    %prnt(pdsn=pro_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_encid_n distinct_patid_n);
    %prnt(pdsn=pro_l3_px_pxtype,_obs=100,_svar=px px_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=pro_l3_pxsource)
    %prnt(pdsn=pro_l3_pxtype_adate_y,_obs=100,_svar=px_type admit_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=pro_l3_ppx)
%end;
%if &_yenrollment=1 %then %do;
    %prnt(pdsn=enr_l3_n);
    %prnt(pdsn=enr_l3_dist_start);
    %prnt(pdsn=enr_l3_dist_end);
    %prnt(pdsn=enr_l3_dist_enrmonth,_obs=100,_svar=enroll_m,_suppvar=record_n record_pct);
    %prnt(pdsn=enr_l3_dist_enryear);
    %prnt(pdsn=enr_l3_enr_ym,_obs=100,_svar=month,_suppvar=record_n);
    %prnt(pdsn=enr_l3_basedist)
    %prnt(pdsn=enr_l3_per_patid)
    %prnt(pdsn=enr_l3_chart)
%end;
%if &_yvital=1 %then %do;    
    %prnt(pdsn=vit_l3_n);
    %prnt(pdsn=vit_l3_mdate_y);
    %prnt(pdsn=vit_l3_mdate_ym,_obs=100,_svar=measure_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=vit_l3_vital_source);
    %prnt(pdsn=vit_l3_ht);
    %prnt(pdsn=vit_l3_ht_dist);
    %prnt(pdsn=vit_l3_wt);
    %prnt(pdsn=vit_l3_wt_dist);
    %prnt(pdsn=vit_l3_diastolic);
    %prnt(pdsn=vit_l3_systolic);
    %prnt(pdsn=vit_l3_bmi);
    %prnt(pdsn=vit_l3_bp_position_type);
    %prnt(pdsn=vit_l3_smoking);
    %prnt(pdsn=vit_l3_tobacco);
    %prnt(pdsn=vit_l3_tobacco_type);
    %prnt(pdsn=vit_l3_dash1);
%end;    
%if &_ydeath=1 %then %do;
    %prnt(pdsn=death_l3_n);
    %prnt(pdsn=death_l3_date_y);
    %prnt(pdsn=death_l3_date_ym,_obs=100,_svar=death_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=death_l3_impute);
    %prnt(pdsn=death_l3_source);
    %prnt(pdsn=death_l3_match);
%end;    
%if &_ydeathc=1 %then %do;
    %prnt(pdsn=deathc_l3_n);
    %prnt(pdsn=deathc_l3_code);
    %prnt(pdsn=deathc_l3_type);
    %prnt(pdsn=deathc_l3_source);
    %prnt(pdsn=deathc_l3_conf);
%end;
%if &_ydispensing=1 %then %do;
    %prnt(pdsn=disp_l3_n);
    %prnt(pdsn=disp_l3_ndc,_obs=100,_svar=ndc,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=disp_l3_ddate_y);
    %prnt(pdsn=disp_l3_ddate_ym,_obs=100,_svar=dispense_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=disp_l3_supdist2);
    %prnt(pdsn=disp_l3_dispamt_dist);
    %prnt(pdsn=disp_l3_dose_dist);
    %prnt(pdsn=disp_l3_doseunit,_obs=100,_svar=dispense_dose_disp_unit,_suppvar=short_yn record_n record_pct);
    %prnt(pdsn=disp_l3_route,_obs=100,_svar=dispense_route,_suppvar=record_n record_pct);
%end;    
%if &_yprescribing=1 %then %do;
    %prnt(pdsn=pres_l3_n);
    %prnt(pdsn=pres_l3_rxcui,_obs=100,_svar=rxnorm_cui,_suppvar=rxnorm_cui_tty record_n record_pct distinct_patid_n);
    %prnt(pdsn=pres_l3_rxcui_tier);
    %prnt(pdsn=pres_l3_supdist2);
    %prnt(pdsn=pres_l3_rxcui_rxsup,_obs=100,_svar=rxnorm_cui,_suppvar=min mean max n nmiss,_recordn=n);
    %prnt(pdsn=pres_l3_basis);
    %prnt(pdsn=pres_l3_freq);
/*  %prnt(pdsn=pres_l3_qtyunit); - renamed as rxdoseform with V4.00 */
    %prnt(pdsn=pres_l3_odate_y);
    %prnt(pdsn=pres_l3_odate_ym,_obs=100,_svar=rx_order_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=pres_l3_rxqty_dist);
    %prnt(pdsn=pres_l3_rxrefill_dist);
    %prnt(pdsn=pres_l3_dispaswrtn);
    %prnt(pdsn=pres_l3_prnflag);
    %prnt(pdsn=pres_l3_rxdoseform,_obs=100,_svar=rx_dose_form,_suppvar=record_n record_pct);
    %prnt(pdsn=pres_l3_rxdoseodr_dist);
    %prnt(pdsn=pres_l3_rxdoseodrunit,_obs=100,_svar=rx_dose_ordered_unit short_yn,_suppvar=record_n record_pct);
    %prnt(pdsn=pres_l3_route,_obs=100,_svar=rx_route,_suppvar=record_n record_pct);
    %prnt(pdsn=pres_l3_source);
    %prnt(pdsn=pres_l3_rawrxmed,_obs=100,_svar=raw_rx_med_name,_suppvar=record_n record_pct);
%end;    
%if &_ymed_admin=1 %then %do;
    %prnt(pdsn=medadm_l3_n);
    %prnt(pdsn=medadm_l3_doseadm);
    %prnt(pdsn=medadm_l3_doseadmunit);
    %prnt(pdsn=medadm_l3_route,_obs=100,_svar=medadmin_route,_suppvar=record_n record_pct);
    %prnt(pdsn=medadm_l3_source);
    %prnt(pdsn=medadm_l3_type);
    %prnt(pdsn=medadm_l3_code_type,_obs=100,_svar=medadmin_code medadmin_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=medadm_l3_sdate_y);
    %prnt(pdsn=medadm_l3_sdate_ym,_obs=100,_svar=medadmin_start_date,_suppvar=record_n distinct_patid_n);
%end;
%if &_ycondition=1 %then %do;
    %prnt(pdsn=cond_l3_n);
    %prnt(pdsn=cond_l3_condition,_obs=100,_svar=condition,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=cond_l3_rdate_y);
    %prnt(pdsn=cond_l3_rdate_ym,_obs=100,_svar=report_date,_suppvar=record_n);
    %prnt(pdsn=cond_l3_status);
    %prnt(pdsn=cond_l3_type);
    %prnt(pdsn=cond_l3_source);
%end;
%if &_ypro_cm=1 %then %do;
    %prnt(pdsn=procm_l3_n);
/*  %prnt(pdsn=procm_l3_item); - retired with V4.00 */
    %prnt(pdsn=procm_l3_pdate_y);
    %prnt(pdsn=procm_l3_pdate_ym,_obs=100,_svar=pro_date,_suppvar=record_n);
    %prnt(pdsn=procm_l3_method);
    %prnt(pdsn=procm_l3_mode);
    %prnt(pdsn=procm_l3_cat);
    %prnt(pdsn=procm_l3_loinc,_obs=100,_svar=pro_item_loinc,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=procm_l3_itemfullname,_obs=100,_svar=pro_item_fullname,_suppvar=record_n record_pct);
    %prnt(pdsn=procm_l3_itemnm,_obs=100,_svar=pro_item_name,_suppvar=record_n record_pct);
    %prnt(pdsn=procm_l3_measure_fullname,_obs=100,_svar=pro_measure_fullname,_suppvar=record_n record_pct);
    %prnt(pdsn=procm_l3_measurenm,_obs=100,_svar=pro_measure_name,_suppvar=record_n record_pct);
    %prnt(pdsn=procm_l3_type);
%end;
%if &_yobs_clin=1 %then %do;
    %prnt(pdsn=obsclin_l3_n);
    %prnt(pdsn=obsclin_l3_code_type,_obs=100,_svar=obsclin_code,_suppvar=obsclin_type record_n distinct_patid_n);
%end;
%if &_yprovider=1 %then %do;
    %prnt(pdsn=prov_l3_n);
    %prnt(pdsn=prov_l3_npiflag);
    %prnt(pdsn=prov_l3_specialty,_obs=100,_svar=provider_specialty_primary,_suppvar=record_n record_pct);
    %prnt(pdsn=prov_l3_specialty_group);
    %prnt(pdsn=prov_l3_sex);
%end;
%if &_yptrial=1 %then %do;
    %prnt(pdsn=trial_l3_n);
%end;

*******************************************************************************;
* Close PDF
*******************************************************************************;
%if %upcase(&_grp)=MAIN %then %do; ods pdf close; %end;

********************************************************************************;
* Macro end
********************************************************************************;
%mend dc_main;
%dc_main;
