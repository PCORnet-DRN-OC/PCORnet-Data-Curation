/*******************************************************************************
*  $Source: data_curation_query_main $;
*    $Date: 2022/06/28
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V6.05 - all queries,
*                           excluding lab and cross table (XTBL)
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
%macro dc_main;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_%lowcase(&_grp).log" 
              lrecl=200;
proc printto log=qlog  new ;
run ;

********************************************************************************;
* Bring in DEATH and compress it
********************************************************************************;
%if &_ydeath=1 %then %do;
%let qname=death;
%elapsed(begin);

*- Determine concatonated length of variables used to determine DEATHID -*;
proc contents data=pcordata.&qname out=cont_death noprint;
run;

data _null_;
     set cont_death end=eof;
     retain ulength 0;
     if name in ("PATID" "DEATH_SOURCE") then ulength=ulength+length;
    
     * add 11 to ULENGTH (9 for date and 2 for delimiter *;
     if eof then call symputx("_dulength",ulength+11);
run;

data &qname(compress=yes drop=dthdate);
     length deathid $&_dulength;
     set pcordata.&qname;
    
     dthdate=put(death_date,date9.);
     deathid=cats(patid,'_',death_source);
run;

proc sort data=&qname;
     by patid death_date;
run;

%let savedeath=&qname;

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
%clean(savedsn=&savedeath &savextbl);

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
%clean(savedsn=&savedeath &savextbl);

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
%clean(savedsn=&savedeath &savextbl);

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
%clean(savedsn=&savedeath &savextbl);

********************************************************************************;
* DEATH_L3_SOURCE
********************************************************************************;
%let qname=death_l3_source;
%elapsed(begin);

proc format;
     value $dsource
          "D"="D"
         "DR"="DR"
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

     if upcase(death_source) in ("D" "DR" "L" "N" "S" "T") then col1=death_source;
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
%clean(savedsn=&savedeath &savextbl);

********************************************************************************;
* DEATH_L3_SOURCE_YM
********************************************************************************;
%let qname=death_l3_source_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length death_source $4 year year_month 5.;
     set death(keep=death_date death_source patid);

     * create a year and a year/month numeric variable *;
     if death_date^=. then do;
         year=year(death_date);
         year_month=(year(death_date)*100)+month(death_date);
     end;
    
     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if upcase(death_source) in ("D" "L" "N" "S" "T" "DR") then col1=death_source;
     else if death_source in ("NI") then col1="ZZZA";
     else if death_source in ("UN") then col1="ZZZB";
     else if death_source in ("OT") then col1="ZZZC";
     else if death_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep year_month col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1/preloadfmt;
     class year_month;
     output out=stats;
     format col1 $dsource.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     class year_month;
     output out=pid_unique;
     format col1 $dsource.;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     death_source=put(col1,$dsource.);

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then death_date=put(year_month,null.);
     else death_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package death_source death_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, death_source, death_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savextbl);

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
%clean(savedsn=&savedeath &savextbl);

%end;

********************************************************************************;
* Bring in DEATH_CAUSE and compress it
********************************************************************************;
%if &_ydeathc=1 %then %do;
%let qname=death_cause;
%elapsed(begin);

*- Determine concatonated length of variables used to determine DEATHID -*;
proc contents data=pcordata.&qname out=cont_deathc noprint;
run;

data _null_;
     set cont_deathc end=eof;
     retain ulength 0;
     if name in ("PATID" "DEATH_CAUSE" "DEATH_CAUSE_CODE" "DEATH_CAUSE_TYPE" 
                 "DEATH_CAUSE_SOURCE") then ulength=ulength+length;
    
     * add ULENGTH (5 delimiter) *;
     if eof then call symputx("_dculength",ulength+5);
run;

data &qname(compress=yes);
     length deathcid $&_dculength;
     set pcordata.&qname;

     deathcid=cats(patid,'_',death_cause,'_',death_cause_code,'_',death_cause_type,'_',death_cause_source);
run;

proc sort data=&qname;
     by patid;
run;

%let savedeathc=&qname;

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
%clean(savedsn=&savedeath &savedeathc &savextbl);

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
%clean(savedsn=&savedeath &savedeathc &savextbl);

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
%clean(savedsn=&savedeath &savedeathc &savextbl);

********************************************************************************;
* DEATHC_L3_SOURCE
********************************************************************************;
%let qname=deathc_l3_source;
%elapsed(begin);

proc format;
     value $dcsource
          "D"="D"
         "DR"="DR"
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

     if upcase(death_cause_source) in ("D" "DR" "L" "N" "S" "T") then col1=death_cause_source;
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
%clean(savedsn=&savedeath &savedeathc &savextbl);

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
%clean(savedsn=&savedeath &savedeathc &savextbl);


%end;

********************************************************************************;
* Bring in ENCOUNTER and compress it
********************************************************************************;
%if &_yencounter=1 %then %do;
%let qname=encounter;
%elapsed(begin);

%global _providlength;

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;
    
     if notdigit(facility_location)=0 and length(facility_location)=5 then valid_floc="Y";
     else valid_floc=" ";

     * derive los group *;
     if discharge_date=. then los_group=0;
     else if discharge_date^=. then do;
        if discharge_date=admit_date then los_group=1;
        else if admit_date^=. and discharge_date-admit_date>0 then los_group=2;
     end;
run;

proc sort data=&qname(keep=providerid enc_type) out=prov_&qname;
     by providerid;
run;

%let saveenc=&qname prov_&qname;

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
%enc_oneway(encdsn=encounter,encvar=facility_location,ord=5)
%enc_oneway(encdsn=encounter,encvar=valid_floc,ord=6)

data query;
     length valid_n $20;
     if _n_=1 then set query(keep=tag all_n where=(vtag="VALID_FLOC") 
                             rename=(tag=vtag all_n=valid_all));
     set query(where=(tag^="VALID_FLOC"));
     if strip(tag)^="FACILITY_LOCATION" then valid_n="n/a";
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
         "ES"="ES"
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

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'ES' 'HH' 'HO' 'HS' 'IH' 'IP'
         'NH' 'RH' 'RS' 'SN') then col1=admitting_source;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_LOS_DIST
********************************************************************************;
%let qname=enc_l3_los_dist;
%elapsed(begin);

proc format;
     value los_dist
          0="0 days"
          1="1 day"
          2=">=2 days"
        ;
run;

*- Derive categorical variable -*;
data data;
     length enctype $4;
     set encounter(keep=enc_type los_group);
     if los_group^=.;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     col1=los_group;
    
     keep col1 enctype;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 los_dist.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=3))
           stats(drop=col1 where=(_type_=2) rename=(_freq_=denom_cat))
     ;
     by enctype;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(enctype,$enc_typ.);
     los_group=put(col1,los_dist.);

     * apply threshold *;
     %threshold(type=3,nullval=enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then do;
        record_pct=put((_freq_/denom)*100,6.2);
        record_pct_cat=put((_freq_/denom_cat)*100,6.2);
     end;

     keep datamartid response_date query_package enc_type los_group
          record_n record_pct record_pct_cat;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, los_group, 
            record_n, record_pct, record_pct_cat
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_ENCTYPE_ADMSRC
********************************************************************************;
%let qname=enc_l3_enctype_admsrc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type admitting_source);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'ES' 'HH' 'HO' 'HS' 'IH' 'IP' 'NH' 'RH' 
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
     call symputx("minact_present",nmiss(min,max));
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_ENCTYPE_DISDISP
********************************************************************************;
%let qname=enc_l3_enctype_disdisp;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_disposition);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_ENCTYPE_DISSTAT
********************************************************************************;
%let qname=enc_l3_enctype_disstat;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_status);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_ENCTYPE_DRG
********************************************************************************;
%let qname=enc_l3_enctype_drg;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type drg);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
     length record_n drg $20;
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
%clean(savedsn=&savedeath &saveenc &savextbl);

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
     if eof then call symputx("_ulength",ulength+12);
run;

*- Derive categorical variable -*;
data data;
     length col1 $4 _unique_visit $&_ulength;
     set encounter(keep=enc_type patid encounterid admit_date providerid:) end=eof;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then col1=enc_type;
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
            call symputx("_maxlen_uv",max(maxlength_uv,1));
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
%clean(savedsn=&savedeath &saveenc &savextbl);

********************************************************************************;
* ENC_L3_DASH2
********************************************************************************;
%let qname=enc_l3_dash2;
%elapsed(begin);

*- Data with legitimate date and encounter type -*;
data data;
     set encounter(keep=patid admit_date enc_type);
     if admit_date^=. and enc_type in ('AV' 'ED' 'EI' 'IP' 'OS' 'TH');

     %dash(datevar=admit_date);
     keep patid period;
run;

*- Uniqueness per PATID/period -*;
proc sort data=data out=period_patid nodupkey;
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

proc sort data=data(keep=patid) out=xtbl_obs nodupkey;
     by patid;
run;

*- Clear working directory -*;
%clean(savedsn=&savedeath &saveenc &savextbl xtbl_obs);

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

     if payer_type_primary=" " then col1="ZZZA";
     else col1=put(payer_type_primary,$payer_type.);

     keep col1 patid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $payer_type.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $payer_type.;
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
     payer_type_primary=put(col1,$nullout.);
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
%clean(savedsn=&savedeath &saveenc &savextbl xtbl_obs);

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

     if payer_type_secondary=" " then col1="ZZZA";
     else col1=put(payer_type_secondary,$payer_type.);

     keep col1 patid;
run;

*- Derive statistics - year -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $payer_type.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $payer_type.;
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
     payer_type_secondary=put(col1,$nullout.);
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
%clean(savedsn=&savedeath &saveenc &savextbl xtbl_obs);

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
       'HOSPITAL_OUTPATIENT_CLINIC-AMBULATORY_CARE_PEDIATRIC-subset'='HP_OP_PED_CL_AM_CARE'
       ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set encounter(keep=patid facility_type);

     if facility_type=" " then col1="ZZZA";
     else col1=put(facility_type,$facility_type.);

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $facility_type.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $facility_type.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 facility_type_grp $60 facility_type $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=s where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           facility_type(in=f rename=(start=col1));
     by col1;
     if s;

     * call standard variables *;
     %stdvar

     * table values *;
     facility_type=put(col1,$nullout.);
     if facility_type not in ("NI" "UN" "OT" "NULL or missing" "Values outside of CDM specifications") 
        then facility_type_grp=put(scan(descriptive_text,2,'='),$fac_grp.);
     else facility_type_grp=facility_type;

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package facility_type_grp facility_type
          record_n record_pct distinct_patid_n col1 descriptive_text;*/
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
%clean(savedsn=&savedeath &saveenc &savextbl xtbl_obs);

********************************************************************************;
* ENC_L3_FACILITYLOC
***********************************************************************s*********;
%let qname=enc_l3_facilityloc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set encounter(keep=patid facility_location);

     if facility_location^=" " then col1=facility_location;
     else if facility_location=" " then col1="ZZZA";

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
     length record_n distinct_patid_n $20 facility_location $15;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=s where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
           facility_type(in=f rename=(start=col1));
     by col1;
     if s;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then facility_location=col1;
     else facility_location=put(col1,$null.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package facility_location record_n 
          record_pct distinct_patid_n col1;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, facility_location, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &saveenc &savextbl xtbl_obs);

********************************************************************************;
* ENC_L3_FACILITYTYPE_FACILITYLOC
********************************************************************************;
%let qname=enc_l3_facilitytype_facilityloc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 facloc $200;
     set encounter(keep=patid facility_type facility_location);

     col1=put(facility_type,$facility_type.);
     if facility_type^=" " then do;
         if col1="NI" then col1="ZZZA";
         else if col1="UN" then col1="ZZZB";
         else if col1="OT" then col1="ZZZC";
         else if facility_type^=col1 then col1="ZZZE";
     end;

     if facility_location^=" " then facloc=facility_location;
     else if facility_location=" " then facloc="ZZZA";

     keep col1 facloc patid;
run;

*- Derive statistics - year -*;
proc means data=data noprint missing;
     class col1 facloc;
     output out=stats;
     format col1 $other. facloc $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 facloc patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1 facloc;
     output out=pid_unique;
     format col1 $other. facloc $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 facility_location $15 facility_type $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=3))
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 facloc;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then facility_type=col1;
     else facility_type=put(col1,$other.);
     facility_location=put(facloc,$null.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package facility_type facility_location
          record_n record_pct distinct_patid_n col1;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, facility_type, facility_location,
            record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savextbl prov_encounter xtbl_obs);

%end;

********************************************************************************;
* Bring in DEMOGRAPHIC and compress it
********************************************************************************;
%if &_ydemographic=1 %then %do;
%let qname=demographic;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;
run;

proc sort data=&qname;
     by patid;
run;

%let savedemog=&qname;

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
%clean(savedsn=&savedeath &savedemog &savextbl prov_encounter xtbl_obs);

********************************************************************************;
* DEM_L3_AGEYRSDIST1
********************************************************************************;
%if &_ydeath=1 %then %do;

*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl1(qname=dem_l3_ageyrsdist1,dsn=,subset=);

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
     merge demographic(in=d keep=patid birth_date) death_age &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedeath &savedemog &savextbl prov_encounter);

%mend xtbl1;
%xtbl1;
%xtbl1(qname=dem_obs_l3_ageyrsdist1,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_AGEYRSDIST2
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl2(qname=dem_l3_ageyrsdist2,dsn=,subset=);

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
     merge demographic(in=d keep=patid birth_date) death_age &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedeath &savedemog &savextbl prov_encounter);

%mend xtbl2;
%xtbl2;
%xtbl2(qname=dem_obs_l3_ageyrsdist2,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

%end;
********************************************************************************;
* DEM_L3_HISPDIST
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl3(qname=dem_l3_hispdist,dsn=,subset=);

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
     merge demographic(in=d keep=patid hispanic) &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl3;
%xtbl3;
%xtbl3(qname=dem_obs_l3_hispdist,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_RACEDIST
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl4(qname=dem_l3_racedist,dsn=,subset=);

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
     merge demographic(in=d keep=patid race) &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl4;
%xtbl4;
%xtbl4(qname=dem_obs_l3_racedist,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_SEXDIST
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl5(qname=dem_l3_sexdist,dsn=,subset=);

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
     merge demographic(in=d keep=patid sex) &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl5;
%xtbl5;
%xtbl5(qname=dem_obs_l3_sexdist,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_GENDERDIST
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl6(qname=dem_l3_genderdist,dsn=,subset=);

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
     merge demographic(in=d keep=patid gender_identity) &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl6;
%xtbl6;
%xtbl6(qname=dem_obs_l3_genderdist,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_PATPREFLANG
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl7(qname=dem_l3_patpreflang,dsn=,subset=);

%elapsed(begin);

data data;
     length col1 $4;
     merge demographic(in=d keep=patid pat_pref_language_spoken) &dsn;
     by patid;
     if d &subset;

     if pat_pref_language_spoken=" " then col1="ZZZA";
     else col1=put(pat_pref_language_spoken,$preflang.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $preflang.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 pat_pref_language_spoken $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pat_pref_language_spoken=put(col1,$nullout.);

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl7;
%xtbl7;
%xtbl7(qname=dem_obs_l3_patpreflang,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=&savedemog &savextbl prov_encounter);

********************************************************************************;
* DEM_L3_ORIENTDIST
********************************************************************************;
*- macro for query and, if requested, xtbl_obs query -*;
%macro xtbl8(qname=dem_l3_orientdist,dsn=,subset=);

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
     merge demographic(in=d keep=patid sexual_orientation) &dsn;
     by patid;
     if d &subset;

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
%clean(savedsn=&savedemog &savextbl prov_encounter);

%mend xtbl8;
%xtbl8;
%xtbl8(qname=dem_obs_l3_orientdist,dsn=xtbl_obs(in=x),subset=and x);

*- Clear working directory -*;
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in DIAGNOSIS and compress it
********************************************************************************;
%if &_ydiagnosis=1 %then %do;
%let qname=diagnosis;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;

     * define diagnosisid_plus *;
     if (.<admit_date<=&mxrefreshn or .<dx_date<=&mxrefreshn) and encounterid^=' ' and 
        dx_type not in ('NI' 'UN' 'OT' ' ') then diagnosisid_plus=diagnosisid;
     else diagnosisid_plus=' ';
run;

%let savediag=&qname prov_encounter;

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
%enc_oneway(encdsn=diagnosis,encvar=diagnosisid_plus,ord=5)

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
     length enctype dxorigin col1 $4;
     set diagnosis(keep=enc_type pdx dx_origin encounterid patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if dx_origin in ("BI" "CL" "DR" "OD") then dxorigin=dx_origin;
     else if dx_origin in ("NI") then dxorigin="ZZZA";
     else if dx_origin in ("UN") then dxorigin="ZZZB";
     else if dx_origin in ("OT") then dxorigin="ZZZC";
     else if dx_origin=" " then dxorigin="ZZZD";
     else dxorigin="ZZZE";
    
     if pdx in ("P" "S") then col1=pdx;
     else if pdx in ("NI") then col1="ZZZA";
     else if pdx in ("UN") then col1="ZZZB";
     else if pdx in ("OT") then col1="ZZZC";
     else if pdx=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep enctype dxorigin col1 encounterid patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype dxorigin/preloadfmt;
     output out=stats;
     format enctype $enc_typ. dxorigin $origin. col1 $pdx.;
run;

*- Derive distinct encounter id for each PDX/type -*;
proc sort data=data(drop=patid) out=distinct nodupkey;
     by col1 enctype dxorigin encounterid;
     where encounterid^=' ';
run;

proc means data=distinct nway completetypes noprint missing;
     class col1 enctype dxorigin/preloadfmt;
     output out=stats_distinct;
     format enctype $enc_typ. dxorigin $origin. col1 $pdx.;
run;

*- Derive distinct patient id for each PDX/type -*;
proc sort data=data(drop=encounterid) out=pid nodupkey;
     by col1 enctype dxorigin patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 enctype dxorigin/preloadfmt;
     output out=pid_unique;
     format enctype $enc_typ. dxorigin $origin. col1 $pdx.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats 
           stats_distinct(rename=(_freq_=_freq_encid))
           pid_unique(rename=(_freq_=_freq_patid));
     by col1 enctype dxorigin;

     * call standard variables *;
     %stdvar

     * table values *;
     pdx=put(col1,$pdx.);
     enc_type=put(enctype,$enc_typ.);
     dx_origin=put(dxorigin, $origin.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=_freq_encid,nullval=col1^="ZZZD")
     %threshold(_tvar=_freq_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_encid_n=strip(put(_freq_encid,threshold.));
     distinct_patid_n=strip(put(_freq_patid,threshold.));

     keep datamartid response_date query_package enc_type dx_origin pdx record_n
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdx, enc_type, dx_origin, 
            record_n, distinct_encid_n, distinct_patid_n
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
     length enctype dxorigin col1 $4;
     set diagnosis(keep=enc_type pdx dx_origin encounterid patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if dx_origin in ("BI" "CL" "DR" "OD") then dxorigin=dx_origin;
     else if dx_origin in ("NI") then dxorigin="ZZZA";
     else if dx_origin in ("UN") then dxorigin="ZZZB";
     else if dx_origin in ("OT") then dxorigin="ZZZC";
     else if dx_origin=" " then dxorigin="ZZZD";
     else dxorigin="ZZZE";
    
     if pdx="P" then col1=pdx;
     else col1="U";
    
     keep enctype dxorigin col1 encounterid patid pdx;
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
     class col1 enctype dxorigin/preloadfmt;
     output out=stats_distinct;
     format enctype $enc_typ. dxorigin $origin. col1 $pdxgrp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length distinct_encid_n $20;
     set stats_distinct(rename=(_freq_=_freq_encid));
     by col1 enctype dxorigin;

     * call standard variables *;
     %stdvar

     * table values *;
     pdxgrp=put(col1,$pdxgrp.);
     enc_type=put(enctype,$enc_typ.);
     dx_origin=put(dxorigin,$origin.);
            
     * apply threshold *;
     %threshold(_tvar=_freq_encid,nullval=col1^="ZZZD")

     * counts *;
     distinct_encid_n=strip(put(_freq_encid,threshold.));

     keep datamartid response_date query_package enc_type pdxgrp dx_origin
          distinct_encid_n ;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdxgrp, enc_type, dx_origin,
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
* DIA_L3_DXDATE_Y
********************************************************************************;
%let qname=dia_l3_dxdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set diagnosis(keep=dx_date patid encounterid);

     if dx_date^=. then year=year(dx_date);
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
     dx_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_encid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     distinct_encid_n=strip(put(distinct_encid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_date record_n record_pct
          distinct_encid_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_date, record_n, 
            record_pct, distinct_encid_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savediag);

********************************************************************************;
* DIA_L3_DXDATE_YM
********************************************************************************;
%let qname=dia_l3_dxdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set diagnosis(keep=dx_date);

     * create a year and a year/month numeric variable *;
     if dx_date^=. then do;
        year=year(dx_date);
        year_month=(year(dx_date)*100)+month(dx_date);
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

*- Check to see if any non-null obs  -*;
data _null_;
     set dummy;
     if min=. then call symputx('dx_nobs',0);
     else call symputx('dx_nobs',_freq_);
run;
%put &dx_nobs;

*- If obs, continue through the rest of the query -*;
%if &dx_nobs>0 %then %do;
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

    data stats;
         if _n_=1 then set minact;
         if _n_=1 then set maxact;
         set stats(where=(_type_=1));
    
         * delete dummy records that are prior to the first actual year/month or 
           after the run-date or last actual year/month *;
         if year_month<minact or maxact<year_month<99999999 then delete;
    run;
%end;

*- Add first and last actual year/month values to every record -*;
data query;
     length record_n $20;
     set stats;

     * call standard variables *;
     %stdvar

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then dx_date=put(year_month,null.);
     else dx_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package dx_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_date, record_n
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
* DIA_L3_DXTYPE
********************************************************************************;
%let qname=dia_l3_dxtype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set diagnosis(keep=dx_type patid);

     if dx_type in ("09" "10" "11" "SM") then col1=dx_type;
     else if dx_type in ("NI") then col1="ZZZA";
     else if dx_type in ("UN") then col1="ZZZB";
     else if dx_type in ("OT") then col1="ZZZC";
     else if dx_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dxtype.;
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
     dx_type=put(col1,$dxtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, record_n, 
            record_pct
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

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then col1=enc_type;
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

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
* DIA_L3_DCGROUP
********************************************************************************;
%let qname=dia_l3_dcgroup;
%elapsed(begin);

proc sort data=dx_dcgroup_ref(keep=dx dx_type dc_dx_group pedi) 
     out=dcgroup nodupkey;
     by dx dx_type;
run;

data data;
     set diagnosis;

     dx=strip(upcase(compress(dx,',.')));
     keep dx dx_type patid;
run;

proc sort data=data;
     by dx dx_type;
run;

*- Derive categorical variable -*;
data data;
     length col1 $100;
     merge data(in=d) dcgroup(in=dc);
     by dx dx_type;
     if d;

     if dc then col1=strip(dc_dx_group)|| "~" || strip(pedi);
     else col1="Unassigned";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dxdcgrp.;
run;

*- Derive distinct patient id -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $dxdcgrp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length pedi $1 distinct_patid_n $20 dc_dx_group $200;
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by col1;
     if col1="Unassigned" then delete;

     * call standard variables *;
     %stdvar

     dc_dx_group=scan(put(col1,$dxdcgrp.),1,'~');
     pedi=strip(scan(put(col1,$dxdcgrp.),2,'~'));

     * apply threshold *;
     %threshold(nullval=dc_dx_group^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=dc_dx_group^="ZZZA")

     * counts *;
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package dc_dx_group pedi distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dc_dx_group, pedi,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in PROCEDURES and compress it
********************************************************************************;
%if &_yprocedures=1 %then %do;
%let qname=procedures;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if admit_date>=&lookback_dt or admit_date=.;
    
     * define proceduresid_plus *;
     if (.<admit_date<=&mxrefreshn or .<px_date<=&mxrefreshn) and encounterid^=' ' and 
        px_type not in ('NI' 'UN' 'OT' ' ') then proceduresid_plus=proceduresid;
     else proceduresid_plus=' ';
run;

%let saveproc=&qname prov_encounter;

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
%enc_oneway(encdsn=procedures,encvar=proceduresid_plus,ord=5)

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
* PRO_L3_DCGROUP
********************************************************************************;
%let qname=pro_l3_dcgroup;
%elapsed(begin);

proc sort data=px_dcgroup_ref(keep=px px_type dc_px_group pedi) 
     out=dcgroup nodupkey;
     by px px_type;
run;

data data;
     set procedures;
     if px_type^="CH" then px=strip(upcase(compress(px,',.')));
     else px=strip(upcase(substr(compress(px,',.'),1,5)));
     keep px px_type patid;
run;

proc sort data=data;
     by px px_type;
run;
    
*- Derive categorical variable -*;
data data;
     length col1 $100;
     merge data(in=d) dcgroup(in=dc);
     by px px_type;
     if d ;

     if dc then do; flag=1; col1=strip(dc_px_group)|| "~" || strip(pedi);end;
     else col1="Unassigned";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pxdcgrp.;
run;

*- Derive distinct patient id -*;
proc sort data=data out=pid nodupkey;
     by col1 patid;
     where patid^=' ';
run;

proc means data=pid completetypes nway noprint missing;
     class col1/preloadfmt;
     output out=pid_unique;
     format col1 $pxdcgrp.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length pedi $1 distinct_patid_n $20 dc_px_group $200;
     merge stats(where=(_type_=1)) 
           pid_unique(rename=(_freq_=distinct_patid))
     ;
     by col1;
     if col1="Unassigned" then delete;

     * call standard variables *;
     %stdvar

     dc_px_group=scan(put(col1,$pxdcgrp.),1,'~');
     pedi=strip(scan(put(col1,$pxdcgrp.),2,'~'));

     * apply threshold *;
     %threshold(nullval=dc_px_group^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=dc_px_group^="ZZZA")

     * counts *;
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package dc_px_group pedi distinct_patid_n col1;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dc_px_group, pedi,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_ENCTYPE
********************************************************************************;
%let qname=pro_l3_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=enc_type patid);

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then col1=enc_type;
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

*- Derive distinct patient id -*;
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
         datamartid, response_date, query_package, enc_type, record_n, record_pct,
         distinct_patid_n
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

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
* PRO_L3_PXTYPE
********************************************************************************;
%let qname=pro_l3_pxtype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=px_type patid);

     if px_type in ("09" "10" "11" "CH" "LC" "ND" "RE") then col1=px_type;
     else if px_type in ("NI") then col1="ZZZA";
     else if px_type in ("UN") then col1="ZZZB";
     else if px_type in ("OT") then col1="ZZZC";
     else if px_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $pxtype.;
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
     px_type=put(col1,$pxtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package px_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveproc);

********************************************************************************;
* PRO_L3_PXTYPE_ADATE_Y
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
    
     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
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
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in ENROLLMENT and compress it
********************************************************************************;
%if &_yenrollment=1 %then %do;
%let qname=enrollment;
%elapsed(begin);

*- Determine concatonated length of variables used to determine ENROLLID -*;
proc contents data=pcordata.&qname out=cont_enrollment noprint;
run;

data _null_;
     set cont_enrollment end=eof;
     retain ulength 0;
     if name in ("PATID" "ENR_BASIS") then ulength=ulength+length;
    
     * add 11 to ULENGTH (9 for date and 2 for delimiter *;
     if eof then call symputx("_eulength",ulength+11);
run;

data &qname(compress=yes drop=enrstartdate);
     length enrollid $&_eulength;
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if enr_start_date>=&lookback_dt or enr_end_date>=&lookback_dt or 
        (enr_start_date=. and enr_end_date=.);

     enrstartdate=put(enr_start_date,date9.);
     enrollid=cats(patid,'_',enrstartdate,'_',enr_basis);
run;

%let saveenr=&qname prov_encounter;

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
     length record_n enr_basis $40;
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
     length record_n chart $40;
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

%end;

********************************************************************************;
* Bring in VITAL and compress it
********************************************************************************;
%if &_yvital=1 %then %do;
%let qname=vital;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if measure_date>=&lookback_dt or measure_date=.;
run;

%let savevit=&qname prov_encounter;

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
         "DR"="DR"
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

     if vital_source in ("DR" "HC" "HD" "PD" "PR") then col1=vital_source;
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

     %dash(datevar=measure_date);
     keep patid period;
run;

*- Uniqueness per PATID/period -*;
proc sort data=data out=period_patid nodupkey;
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
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in DISPENSING and compress it
********************************************************************************;
%if &_ydispensing=1 %then %do;
%let qname=dispensing;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if dispense_date>=&lookback_dt or dispense_date=.;

     if notdigit(ndc)=0 and length(ndc)=11 then valid_ndc="Y";
     else valid_ndc=" ";
run;

%let savedisp=&qname prov_encounter;

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

     if dispense_dose_disp_unit=" " then col1="ZZZA";
     else col1=put(strip(dispense_dose_disp_unit),$_unit.);

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
     length record_n $20 dispense_dose_disp_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     dispense_dose_disp_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dispense_dose_disp_unit
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_dose_disp_unit,
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

     if dispense_route=" " then col1="ZZZA";
     else col1=put(dispense_route,$_route.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_route.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 dispense_route $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     dispense_route=put(col1,$nullout.);

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
%clean(savedsn=&savedisp);

********************************************************************************;
* DISP_L3_SOURCE
********************************************************************************;
%let qname=disp_l3_source;
%elapsed(begin);

proc format;
     value $dsource
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
     length col1 $200;
     set dispensing(keep=patid dispense_source);

     if dispense_source in ("BI" "CL" "DR" "OD" "PM") then col1=dispense_source;
     else if dispense_source="NI" then col1="ZZZA";
     else if dispense_source="UN" then col1="ZZZB";
     else if dispense_source="OT" then col1="ZZZC";
     else if dispense_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $dsource.;
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
     if col1 not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then dispense_source=col1;
     else dispense_source=put(col1,$dsource.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package dispense_source
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dispense_source, record_n, 
         record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in PRESRIBING and compress it
********************************************************************************;
%if &_yprescribing=1 %then %do;
%let qname=prescribing;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if rx_order_date>=&lookback_dt or rx_order_date=.;
    
     * for 5 year lookback queries *;
     if rx_order_date>=&lookback5_dt or rx_order_date=. then lookback5_flag=1;

     rxnorm_cui=strip(upcase(rxnorm_cui));

     * for mismatch query *;
     providerid=rx_providerid;

     * define prescribingid_plus *;
     if (.<rx_start_date<=&mxrefreshn or .<rx_order_date<=&mxrefreshn) and 
        rxnorm_cui^=' ' then prescribingid_plus=prescribingid;
     else prescribingid_plus=' ';
run;

%let savepres=&qname prov_encounter;

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
%enc_oneway(encdsn=prescribing,encvar=prescribingid_plus,ord=5)

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
* PRES_L3_RXCUI - 5 yr lookback macro wraps around this and the next query
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=pres_l3_rxcui&sufx;
    %elapsed(begin);

    proc sort data=prescribing(keep=rxnorm_cui patid lookback5_flag) 
         out=rxnorm_cui;
         by rxnorm_cui;
         &subset;
    run;

    *- Merge with reference dataset and derive categorical variable -*;
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
    %let qname=pres_l3_rxcui_tier&sufx;
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

%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=where lookback5_flag=1);

********************************************************************************;
* PRES_L3_RXDOSEFORM
********************************************************************************;
%let qname=pres_l3_rxdoseform;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set prescribing(keep=rx_dose_form);

     if rx_dose_form=" " then col1="ZZZA";
     else col1=put(rx_dose_form,$_dose_form.);

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_dose_form.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 rx_dose_form $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1)) 
           _dose_form(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     rx_dose_form=put(col1,$nullout.);

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
     call symputx("minact_present",nmiss(min,max));
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

     if rx_dose_ordered_unit=" " then col1="ZZZA";
     else col1=put(rx_dose_ordered_unit,$_unit.);

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
     length record_n $20 rx_dose_ordered_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     rx_dose_ordered_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package rx_dose_ordered_unit
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_dose_ordered_unit,
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

     if rx_route=" " then col1="ZZZA";
     else col1=put(rx_route,$_route.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_route.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 rx_route $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     rx_route=put(col1,$nullout.);

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
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in MED_ADMIN and compress it
********************************************************************************;
%if &_ymed_admin=1 %then %do;
%let qname=med_admin;
%elapsed(begin);

*- Determine length of variables to avoid truncation -*;
proc contents data=pcordata.&qname out=_medadm_length noprint;
run;

data _null_;
     set _medadm_length;
    
     if name="MEDADMIN_CODE" then call symputx("_medadmin_code_length",max(length,15));
run;

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if medadmin_start_date>=&lookback_dt or medadmin_start_date=.;

     * for 5 year lookback queries *;
     if medadmin_start_date>=&lookback5_dt or medadmin_start_date=. then lookback5_flag=1;

     * for mismatch query *;
     providerid=medadmin_providerid;

     * define medadminid_plus *;
     if medadmin_code^=' ' and medadmin_type not in ('NI' 'UN' 'OT' ' ') and 
        .<medadmin_start_date<=&mxrefreshn then medadminid_plus=medadminid;
     else medadminid_plus=' ';
run;

%let savemedadm=&qname prov_encounter;

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
%enc_oneway(encdsn=med_admin,encvar=medadminid_plus,ord=6)

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

     if medadmin_dose_admin_unit=" " then col1="ZZZA";
     else col1=put(medadmin_dose_admin_unit,$_unit.);

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
     length record_n $20 medadmin_dose_admin_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     medadmin_dose_admin_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package medadmin_dose_admin_unit
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, medadmin_dose_admin_unit, 
            record_n, record_pct
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

     if medadmin_route=" " then col1="ZZZA";
     else col1=put(medadmin_route,$_route.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_route.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 medadmin_route $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     medadmin_route=put(col1,$nullout.);

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
     length record_n distinct_patid_n $20 medadmin_code $&_medadmin_code_length;
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
* MEDADM_L3_RXCUI_TIER
********************************************************************************;
%macro _5yrlook(sufx,subset);
    %let qname=medadm_l3_rxcui_tier&sufx;
    %elapsed(begin);

    proc format;
         value tier
          1 = 'Tier 1'
          2 = 'Tier 2'
          3 = 'Tier 3'
          4 = 'Tier 4'
         99 = 'NULL or missing'
          ;
    run;

    proc contents data=rxnorm_cui_ref out=_cui_length noprint;
    run;
    
    data _null_;
         set _cui_length(where=(name="RXNORM_CUI"));
         call symputx('_len_cui',length);
    run;

    *- Derive categorical variable -*;
    data data;
         length rxnorm_cui $&_len_cui..;
         set med_admin(keep=medadmin_code medadmin_type lookback5_flag);
         if medadmin_type="RX" &subset;
         rxnorm_cui=strip(medadmin_code);
         keep rxnorm_cui;
    run;

    proc sort data=data;
         by rxnorm_cui;     
    run;

    data data;
         merge data(in=m)
               rxnorm_cui_ref(keep=rxnorm_cui rxnorm_cui_tty)
         ;
         by rxnorm_cui;
         if m;

         if rxnorm_cui_tty in ('SCD','SBD','BPCK','GPCK') then col1=1;
         else if rxnorm_cui_tty in  ('SBDF','SCDF','SBDG','SCDG','SBDC','BN','MIN') 
            then col1=2;
         else if rxnorm_cui_tty in ('SCDC', 'PIN','IN') then col1=3;
         else if rxnorm_cui_tty in ('DF','DFG') then col1=4;
         else col1=99;

         keep col1;
    run;

    *- Derive statistics -*;
    proc means data=data completetypes noprint missing;
         class col1/preloadfmt;
         output out=stats;
         format col1 tier.;
    run;

    *- Derive appropriate counts and variables -*;
    data query;
         length record_n record_pct rxnorm_cui_tier $20;
         if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
         set stats(where=(_type_=1));
         by col1;

         * call standard variables *;
         %stdvar

         * table values *;
         rxnorm_cui_tier=put(col1,tier.);

         * counts *;
         record_n=strip(put(_freq_,threshold.));
         record_pct=strip(put((_freq_/denom)*100,5.1));
    
         keep datamartid response_date query_package rxnorm_cui_tier record_n 
              record_pct;
    run;

    *- Order variables -*;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, rxnorm_cui_tier, record_n,
                record_pct
         from query;
    quit;

    %elapsed(end);

    *- Clear working directory -*;
    %clean(savedsn=&savemedadm);

%mend _5yrlook;
%_5yrlook(sufx=,subset=);
%_5yrlook(sufx=_5y,subset=and lookback5_flag=1);

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
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in OBS_GEN and compress it
********************************************************************************;
%if &_yobs_gen=1 %then %do;
%let qname=obs_gen;
%elapsed(begin);

*- Determine length of variables to avoid truncation -*;
proc contents data=pcordata.&qname out=_obsgen_length noprint;
run;

data _null_;
     set _obsgen_length;
    
     if name="OBSGEN_CODE" then call symputx("_obsgen_code_length",max(length,15));
run;

data &qname(compress=yes);
     set pcordata.&qname;
     * remove records with loinc values not permissable *;
     if obsgen_type="LC" and obsgen_code in (&_excl_loinc) then delete;

     * restrict data to within lookback period *;
     if obsgen_start_date>=&lookback_dt or obsgen_start_date=.;

     * for mismatch query *;
     providerid=obsgen_providerid;

     * known test result *;
     if obsgen_code^=" " then do;
        known_test=1;

        if obsgen_result_num^=. and obsgen_result_modifier not in (' ' 'NI' 'UN' 'OT')  
           then known_test_result_num=1;
        else known_test_result_num=.;
        
        if ((obsgen_result_num^=.  and obsgen_result_modifier not in (' ' 'NI' 'UN' 'OT')) or 
           obsgen_result_qual not in (" " "NI" "UN" "OT")) 
           then known_test_result=1;
        else known_test_result=.;

        if .<obsgen_start_date<=&mxrefreshn and
           ((obsgen_result_modifier not in (' ' 'NI' 'UN' 'OT') and obsgen_result_num^=.) or 
            obsgen_result_qual not in (" " "NI" "UN" "OT")) 
           then known_test_result_plausible=1;
        else known_test_result_plausible=.;

        if obsgen_result_num^=. and obsgen_result_modifier not in (' ' 'NI' 'UN' 'OT') and 
           obsgen_result_unit not in (' ' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
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

%let saveobsgen=&qname prov_encounter;

%elapsed(end);

********************************************************************************;
* OBSGEN_L3_N
********************************************************************************;
%let qname=obsgen_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=obs_gen,encvar=patid,ord=1)
%enc_oneway(encdsn=obs_gen,encvar=obsgenid,ord=2)
%enc_oneway(encdsn=obs_gen,encvar=encounterid,ord=3)
%enc_oneway(encdsn=obs_gen,encvar=obsgen_providerid,ord=4)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_RECORDC
********************************************************************************;
%let qname=obsgen_l3_recordc;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=obs_gen,encvar=known_test,_nc=1,ord=1)
%enc_oneway(encdsn=obs_gen,encvar=known_test_result,_nc=1,ord=2)
%enc_oneway(encdsn=obs_gen,encvar=known_test_result_num,_nc=1,ord=3)
%enc_oneway(encdsn=obs_gen,encvar=known_test_result_num_unit,_nc=1,ord=4)
%enc_oneway(encdsn=obs_gen,encvar=known_test_result_plausible,_nc=1,ord=5)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_ABN
********************************************************************************;
%let qname=obsgen_l3_abn;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set obs_gen(keep=obsgen_abn_ind) ;

     if obsgen_abn_ind in ("AB" "AH" "AL" "CH" "CL" "CR" "IN" "NL") then col1=obsgen_abn_ind;
     else if obsgen_abn_ind in ("NI") then col1="ZZZA";
     else if obsgen_abn_ind in ("UN") then col1="ZZZB";
     else if obsgen_abn_ind in ("OT") then col1="ZZZC";
     else if obsgen_abn_ind=" " then col1="ZZZD";
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
     obsgen_abn_ind=put(col1,$_abn.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsgen_abn_ind record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_abn_ind, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_MOD
********************************************************************************;
%let qname=obsgen_l3_mod;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set obs_gen(keep=obsgen_result_modifier);

     if obsgen_result_modifier in ("EQ" "GE" "GT" "LE" "LT" "TX") then col1=obsgen_result_modifier;
     else if obsgen_result_modifier in ("NI") then col1="ZZZA";
     else if obsgen_result_modifier in ("UN") then col1="ZZZB";
     else if obsgen_result_modifier in ("OT") then col1="ZZZC";
     else if obsgen_result_modifier=" " then col1="ZZZD";
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
     obsgen_result_modifier=put(col1,$obs_mod.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsgen_result_modifier record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_result_modifier, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_TMOD
********************************************************************************;
%let qname=obsgen_l3_tmod;
%elapsed(begin);

proc format;
     value $tablemod
       "ENR"="ENROLLMENT"
       "ENC"="ENCOUNTER"
       "DX"="DIAGNOSIS"
       "PX"="PROCEDURES" 
       "VT"="VITAL"
       "DSP"="DISPENSING"
       "LAB"="LAB_RESULT_CM"
       "CON"="CONDITION"
       "PRO"="PRO_CM"
       "RX"="PRESCRIBING"
       "PT"="PCORNET_TRIAL"
       "DTH"="DEATH"
       "DC"="DEATH_CAUSE"
       "MA"="MED_ADMIN"
       "OC"="OBS_CLIN"
       "OB"="OBS_GEN"
       "IM"="IMMUNIZATION"
       "LDS"="LDS_ADDRESS_HISTORY"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $12;
     set obs_gen(keep=obsgen_table_modified);

     if obsgen_table_modified in ("ENR" "ENC" "DX" "PX" "VT" "DSP" "LAB" "CON" 
                                  "PRO" "RX" "PT" "DTH" "DC" "MA" "OC" "OB" "IM" 
                                  "LDS") then col1=obsgen_table_modified;
     else if obsgen_table_modified in ("OT") then col1="ZZZC";
     else if obsgen_table_modified=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $tablemod.;
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
     obsgen_table_modified=put(col1,$other.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsgen_table_modified record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_table_modified, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_QUAL
********************************************************************************;
%let qname=obsgen_l3_qual;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     set obs_gen(keep=obsgen_result_qual);
    
     if upcase(obsgen_result_qual) in ("LOW" "HIGH") 
        then col1="_"||strip(obsgen_result_qual);
     else if obsgen_result_qual=" " then col1="ZZZA";
     else col1=put(obsgen_result_qual,$_qual.);

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
     length record_n $20 obsgen_result_qual $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     obsgen_result_qual=compress(put(col1,$nullout.),'_');

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsgen_result_qual record_n record_pct;
run;

proc sort data=query;
     by obsgen_result_qual;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_result_qual, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_RUNIT
********************************************************************************;
%let qname=obsgen_l3_runit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set obs_gen(keep=obsgen_result_unit);

     if obsgen_result_unit=" " then col1="ZZZA";
     else col1=put(obsgen_result_unit,$_unit.);

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
     length record_n $20 obsgen_result_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     obsgen_result_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsgen_result_unit
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_result_unit,
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_TYPE
********************************************************************************;
%let qname=obsgen_l3_type;
%elapsed(begin);

proc format;
     value $obsgentype
        "09DX"="09DX"
        "09PX"="09PX"
        "10DX"="10DX"
        "10PX"="10PX"
        "11DX"="11DX"
        "11PX"="11PX"
        "ON"="ON"
        "SM"="SM"
        "HP"="HP"
        "HG"="HG"
        "LC"="LC"
        "RX"="RX"
        "ND"="ND"
        "GM"="GM"
        "CH"="CH"
        "UD"="UD"
        "PC"="PC"
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
     set obs_gen(keep=obsgen_type);

     if obsgen_type in ("09DX" "09PX" "10DX" "10PX" "11DX" "11PX" "ON" "SM" "HP" "HG" "LC" "RX" "ND" "CH" "GM") or 
        substr(obsgen_type,1,3) in ("UD_" "PC_") then col1=obsgen_type;
     else if obsgen_type in ("NI") then col1="ZZZA";
     else if obsgen_type in ("UN") then col1="ZZZB";
     else if obsgen_type in ("OT") then col1="ZZZC";
     else if obsgen_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $obsgentype.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 obsgen_type $40;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
     ;
     by col1;
    
     * artificial records created by completetypes *;
     if col1 in ("UD" "PC") then delete;
    
     * call standard variables *;
     %stdvar

     * table values *;
     obsgen_type=put(col1,$obsgentype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package obsgen_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_type, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_CODE_TYPE
********************************************************************************;
%let qname=obsgen_l3_code_type;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length obsgentype $40;
     set obs_gen(keep=obsgen_type obsgen_code patid);

     if obsgen_type in ("09DX" "09PX" "10DX" "10PX" "11DX" "11PX" "ON" "SM" "HP" "HG" "LC" "RX" "ND" "CH" "GM") or 
        substr(obsgen_type,1,3) in ("UD_" "PC_") then obsgentype=obsgen_type;
     else if obsgen_type in ("NI") then obsgentype="ZZZA";
     else if obsgen_type in ("UN") then obsgentype="ZZZB";
     else if obsgen_type in ("OT") then obsgentype="ZZZC";
     else if obsgen_type=" " then obsgentype="ZZZD";
     else obsgentype="ZZZE";
    
     if obsgen_code^=" " then col1=obsgen_code;
     else if obsgen_code=" " then col1="ZZZA";
    
     keep obsgentype col1 patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 obsgentype/preloadfmt;
     output out=stats;
     format obsgentype $obsgentype. col1 $null.;
run;

*- Derive distinct patient id for each med type -*;
proc sort data=data out=pid nodupkey;
     by col1 obsgentype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 obsgentype/preloadfmt;
     output out=pid_unique;
     format obsgentype $obsgentype. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 obsgen_code $&_obsgen_code_length
            obsgen_type $40 ;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 obsgentype;

     * artificial records created by completetypes *;
     if obsgentype in ("UD" "PC") then delete;
    
     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then obsgen_code=col1;
     else obsgen_code=put(col1,$null.);
     obsgen_type=put(obsgentype,$obsgentype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and obsgentype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and obsgentype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
    
     keep datamartid response_date query_package obsgen_type obsgen_code
          record_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_code, obsgen_type, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_SOURCE
********************************************************************************;
%let qname=obsgen_l3_source;
%elapsed(begin);

proc format;
     value $obgsource
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
         "HC"="HC"
         "HD"="HD"
         "PD"="PD"
         "PR"="PR"
         "RG"="RG"
         "SR"="SR"
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
     set obs_gen(keep=obsgen_source);

     if upcase(obsgen_source) in ("BI" "CL" "DR" "HC" "HD" "PD" "PR" "RG" "SR") 
        then col1=obsgen_source;
     else if obsgen_source in ("NI") then col1="ZZZA";
     else if obsgen_source in ("UN") then col1="ZZZB";
     else if obsgen_source in ("OT") then col1="ZZZC";
     else if obsgen_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $obgsource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     obsgen_source=put(col1,$obgsource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsgen_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_SDATE_Y
********************************************************************************;
%let qname=obsgen_l3_sdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set obs_gen(keep=patid obsgen_start_date);

     if obsgen_start_date^=. then year=year(obsgen_start_date);
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
     obsgen_start_date=put(col1,$null.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package obsgen_start_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_start_date, record_n,
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_SDATE_YM
********************************************************************************;
%let qname=obsgen_l3_sdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set obs_gen(keep=patid obsgen_start_date);

     * create a year and a year/month numeric variable *;
     if obsgen_start_date^=. then do;
        year=year(obsgen_start_date);
        year_month=(year(obsgen_start_date)*100)+month(obsgen_start_date);
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
     if year_month=99999999 then obsgen_start_date=put(year_month,null.);
     else obsgen_start_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package obsgen_start_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_start_date, record_n,
            distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveobsgen);

********************************************************************************;
* OBSGEN_L3_CODE_UNIT
********************************************************************************;
%let qname=obsgen_l3_code_unit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length obsgenunit col1 $50;
     set obs_gen(keep=obsgen_type obsgen_code obsgen_result_unit
                  where=(obsgen_type="LC"));

     if obsgen_result_unit^=" " then obsgenunit=obsgen_result_unit;
     else if obsgen_result_unit=" " then obsgenunit="ZZZA";
    
     if obsgen_code^=" " then col1=obsgen_code;
     else if obsgen_code=" " then col1="ZZZA";
    
     keep obsgenunit col1;
run;

*- Derive statistics -*;
proc means data=data nway noprint missing;
     class col1 obsgenunit;
     output out=stats;
     format obsgenunit col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 obsgen_result_unit $50 
            obsgen_code $&_obsgen_code_length;
     set stats;
     by col1 obsgenunit;

     * call standard variables *;
     %stdvar

     * table values *;
     if col1 not in ("ZZZA") then obsgen_code=col1;
     else obsgen_code=put(col1,$null.);
     obsgen_result_unit=put(obsgenunit,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and obsgenunit^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
    
     keep datamartid response_date query_package obsgen_result_unit obsgen_code
          record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, obsgen_code, 
            obsgen_result_unit, record_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in CONDITION and compress it
********************************************************************************;
%if &_ycondition=1 %then %do;
%let qname=condition;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if report_date>=&lookback_dt or report_date=.;
run;

%let savecond=&qname prov_encounter;

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
     condition_type=put(col1,$ctype.);
    
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
          "CC"="CC"
          "DR"="DR"
          "HC"="HC"
          "PC"="PC"
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
     set condition(keep=condition_source);

     if upcase(condition_source) in ("CC" "DR" "HC" "PC" "PR" "RG") 
        then col1=condition_source;
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
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in PRO_CM and compress it
********************************************************************************;
%if &_ypro_cm=1 %then %do;
%let qname=pro_cm;
%elapsed(begin);

*- Determine length of variables to avoid truncation -*;
proc contents data=pcordata.&qname out=_procm_length noprint;
run;

data _null_;
     set _procm_length;

     if name="PRO_ITEM_FULLNAME" then call symputx("_item_full_length",max(length,15));
     else if name="PRO_ITEM_NAME" then call symputx("_item_name_length",max(length,15));
     else if name="PRO_MEASURE_FULLNAME" then call symputx("_meas_full_length",max(length,15));
     else if name="PRO_MEASURE_NAME" then call symputx("_meas_name_length",max(length,15));
run;

data &qname(compress=yes);
     set pcordata.&qname;
     * remove records with loinc values not permissable *;
     if pro_type="LC" and (pro_item_loinc in (&_excl_loinc) or
        pro_measure_loinc in (&_excl_loinc)) then delete;

     * restrict data to within lookback period *;
     if pro_date>=&lookback_dt or pro_date=.;
run;

%let saveprocm=&qname prov_encounter;

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
     length col1 $&_item_full_length;
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
     length col1 $&_item_name_length;
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
     length record_n $20 pro_item_name $&_item_name_length;
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
     length col1 $&_meas_full_length;
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
     length col1 $&_meas_name_length;
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
     length record_n $20 pro_measure_name $&_meas_name_length;
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
* PROCM_L3_MEASURE_LOINC
********************************************************************************;
%let qname=procm_l3_measure_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set pro_cm(keep=pro_measure_loinc patid);

     if pro_measure_loinc^=" " then col1=pro_measure_loinc;
     else if pro_measure_loinc=" " then col1="ZZZA";

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
     if col1 not in ("ZZZA") then pro_measure_loinc=col1;
     else pro_measure_loinc=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package pro_measure_loinc record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_measure_loinc, record_n, 
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
%clean(savedsn=&saveprocm);

********************************************************************************;
* PROCM_L3_SOURCE
********************************************************************************;
%let qname=procm_l3_source;
%elapsed(begin);

proc format;
     value $prosource
         "BI"="BI"
         "CL"="CL"
         "DR"="DR"
         "OD"="OD"
         "SR"="SR"
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
     set pro_cm(keep=pro_source);

     if upcase(pro_source) in ("BI" "CL" "DR" "OD" "SR") then col1=pro_source;
     else if pro_source in ("NI") then col1="ZZZA";
     else if pro_source in ("UN") then col1="ZZZB";
     else if pro_source in ("OT") then col1="ZZZC";
     else if pro_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $prosource.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pro_source=put(col1,$prosource.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pro_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=prov_encounter);

%end;

********************************************************************************;
* Bring in PROVIDER and compress it
********************************************************************************;
%if &_yprovider=1 %then %do;
%let qname=provider;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;
run;

proc sort data=&qname;
     by providerid;
run;

%let saveprov=provider prov_encounter;

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

     if provider_specialty_primary=" " then col1="ZZZA";
     else col1=put(provider_specialty_primary,$prov_spec.);

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $prov_spec.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 provider_specialty_primary $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     provider_specialty_primary=put(col1,$nullout.);

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

*- Create dummy dataset of the group, from the format dataset -*;
data group;
     length col1 $100;
     set prov_group(keep=label code);

     if label^=" " then col1=label;
     else if label=" " then col1=code;
     _freq_=0;

     keep col1 _freq_;
run;

proc sort data=group nodupkey;
     by col1;
run;

*- Derive categorical variable -*;
data data;
     length col1 $100;
     set provider(keep=provider_specialty_primary);

     if provider_specialty_primary=" " then col1="ZZZA";
     else if provider_specialty_primary in ("NI" "OT" "UN") then col1=provider_specialty_primary;
     else col1=put(strip(provider_specialty_primary),$prov_group.);
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1;
     output out=stats;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20 provider_specialty_group $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge group stats(where=(_type_=1));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     provider_specialty_group=put(col1,$nullout.);

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
* PROV_L3_SPECIALTY_ENCTYPE
********************************************************************************;
%let qname=prov_l3_specialty_enctype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $100 col2 $200;
     set provider(keep=providerid provider_specialty_primary);
     if providerid^=" ";

     if provider_specialty_primary=" " then col1="ZZZA";
     else if provider_specialty_primary in ("NI" "OT" "UN") then col1=provider_specialty_primary;
     else col1=put(strip(provider_specialty_primary),$prov_group.);

     if provider_specialty_primary=" " then col2="ZZZA";
     else col2=put(provider_specialty_primary,$prov_spec.);

     keep col: providerid;
run;

proc sort data=data nodupkey;
     by providerid;
run;

*- Add encounter type -*;
data data;
     length enctype $4;
     merge data(in=d)
           prov_encounter(in=e)
     ;
     by providerid;
     if d and e;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS" "TH") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     drop enc_type;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col2 enctype/preloadfmt;
     output out=stats;
     format col2 $prov_spec. enctype $enc_typ.;
run;

proc sort data=data(keep=col1 col2) out=group nodupkey;
     by col2;
run;

*- Derive statistics - providerid -*;
proc sort data=data out=pid nodupkey;
     by col2 enctype providerid;
     where providerid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col2 enctype/preloadfmt;
     output out=pid_unique;
     format col1 $null.;
run;

data stats_group;
     merge stats(where=(_type_=3))
           group(in=g)
     ;
     by col2;
     if g;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_providerid_n $20 provider_specialty_primary $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats_group(in=g)
           pid_unique(rename=(_freq_=distinct_providerid));
     ;
     by col2 enctype;
     if g;

     * call standard variables *;
     %stdvar

     * table values *;
     provider_specialty_group=put(col1,$nullout.);
     provider_specialty_primary=put(col2,$nullout.);
     enc_type=put(enctype,$enc_typ.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(nullval=col2^="ZZZD")
     %threshold(type=7,nullval=enctype^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_providerid_n=strip(put(distinct_providerid,threshold.));
    
     keep datamartid response_date query_package provider_specialty_group
          provider_specialty_primary enc_type record_n distinct_providerid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, provider_specialty_group, 
            provider_specialty_primary, enc_type, record_n, distinct_providerid_n
     from query
     order by provider_specialty_group, provider_specialty_primary, enc_type;
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
* Bring in LDS_ADDRESS_HISTORY and compress it
********************************************************************************;
%if &_yldsadrs=1 %then %do;
%let qname=lds_address_history;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;
    
     * test for valid zip code values *;
     if notdigit(strip(address_zip5))=0 and length(address_zip5)=5 then zip5_valid="Y";
     else zip5_valid=" ";

     if notdigit(strip(address_zip9))=0 and length(address_zip9)=9 then zip9_valid="Y";
     else zip9_valid=" ";
run;

%let saveldsadrs=&qname;

%elapsed(end);

********************************************************************************;
* LDSADRS_L3_N
********************************************************************************;
%let qname=ldsadrs_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=lds_address_history,encvar=patid,ord=1)
%enc_oneway(encdsn=lds_address_history,encvar=addressid,ord=2)
%enc_oneway(encdsn=lds_address_history,encvar=address_zip5,ord=3)
%enc_oneway(encdsn=lds_address_history,encvar=address_zip9,ord=4)
%enc_oneway(encdsn=lds_address_history,encvar=zip5_valid,ord=5)
%enc_oneway(encdsn=lds_address_history,encvar=zip9_valid,ord=6)

data query;
     length valid_n $20;
     if _n_=1 then set query(keep=tag all_n where=(vtag5 in ("ZIP5_VALID"))
                             rename=(tag=vtag5 all_n=valid_all5));
     if _n_=1 then set query(keep=tag all_n where=(vtag9 in ("ZIP9_VALID"))
                             rename=(tag=vtag9 all_n=valid_all9));
     set query(where=(strip(tag) not in ("ZIP5_VALID" "ZIP9_VALID")));
     if strip(tag) not in ("ADDRESS_ZIP5" "ADDRESS_ZIP9") then valid_n="n/a";
     else if strip(tag)="ADDRESS_ZIP5" then valid_n=valid_all5;
     else if strip(tag)="ADDRESS_ZIP9" then valid_n=valid_all9;
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
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSUSE
********************************************************************************;
%let qname=ldsadrs_l3_adrsuse;
%elapsed(begin);

proc format;
     value $address
         "HO"="HO"
         "WO"="WO"
         "TP"="TP"
         "OL"="OL"
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
     set lds_address_history(keep=address_use);

     if upcase(address_use) in ("HO" "WO" "TP" "OL") then col1=address_use;
     else if address_use in ("NI") then col1="ZZZA";
     else if address_use in ("UN") then col1="ZZZB";
     else if address_use in ("OT") then col1="ZZZC";
     else if address_use=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $address.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_use=put(col1,$address.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_use record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_use, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSTYPE
********************************************************************************;
%let qname=ldsadrs_l3_adrstype;
%elapsed(begin);

proc format;
     value $adrs_type
         "PO"="PO"
         "PH"="PH"
         "BO"="BO"
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
     set lds_address_history(keep=address_type);

     if upcase(address_type) in ("PO" "PH" "BO") then col1=address_type;
     else if address_type in ("NI") then col1="ZZZA";
     else if address_type in ("UN") then col1="ZZZB";
     else if address_type in ("OT") then col1="ZZZC";
     else if address_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $adrs_type.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_type=put(col1,$adrs_type.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_type, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSPREF
********************************************************************************;
%let qname=ldsadrs_l3_adrspref;
%elapsed(begin);

proc format;
     value $adrs_pref
         "Y"="Y"
         "N"="N"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set lds_address_history(keep=address_preferred);

     if upcase(address_preferred) in ("Y" "N") then col1=address_preferred;
     else if address_preferred=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $adrs_pref.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_preferred=put(col1,$adrs_pref.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_preferred record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_preferred, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSCITY
********************************************************************************;
%let qname=ldsadrs_l3_adrscity;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set lds_address_history(keep=address_city);

     if address_city^=" " then col1=address_city;
     else if address_city=" " then col1="ZZZA";
    
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
     length record_n $20 address_city $100;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     if col1^="ZZZA" then address_city=col1;
     else address_city=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_city record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_city, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSSTATE
********************************************************************************;
%let qname=ldsadrs_l3_adrsstate;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length address_state col1 $4;
     set lds_address_history(keep=address_state);

     if address_state=" " then col1="ZZZA";
     else col1=put(address_state,$state.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $state.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n address_state $40;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_state=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_state record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_state, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSZIP5
********************************************************************************;
%let qname=ldsadrs_l3_adrszip5;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $5;
     set lds_address_history(keep=address_zip5);

     if address_zip5^=" " then col1=address_zip5;
     else if address_zip5=" " then col1="ZZZA";
    
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
     length record_n address_zip5 $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_zip5=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_zip5 record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_zip5, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveldsadrs);

********************************************************************************;
* LDSADRS_L3_ADRSZIP9
********************************************************************************;
%let qname=ldsadrs_l3_adrszip9;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $10;
     set lds_address_history(keep=address_zip9);

     if address_zip9^=" " then col1=address_zip9;
     else if address_zip9=" " then col1="ZZZA";
    
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
     length record_n address_zip9 $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     address_zip9=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package address_zip9 record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, address_zip9, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in IMMUNIZATION and compress it
********************************************************************************;
%if &_yimmunization=1 %then %do;
%let qname=immunization;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if vx_record_date>=&lookback_dt or vx_record_date=.;
run;

%let saveimmune=&qname;

%elapsed(end);

********************************************************************************;
* IMMUNE_L3_N
********************************************************************************;
%let qname=immune_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=immunization,encvar=patid,ord=1)
%enc_oneway(encdsn=immunization,encvar=immunizationid,ord=2)
%enc_oneway(encdsn=immunization,encvar=encounterid,ord=3)
%enc_oneway(encdsn=immunization,encvar=proceduresid,ord=4)
%enc_oneway(encdsn=immunization,encvar=vx_providerid,ord=5)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_RDATE_Y
********************************************************************************;
%let qname=immune_l3_rdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set immunization(keep=vx_record_date patid);

     if vx_record_date^=. then year=year(vx_record_date);

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
     vx_record_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_record_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_record_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_RDATE_YM
********************************************************************************;
%let qname=immune_l3_rdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set immunization(keep=vx_record_date patid);

     * create a year and a year/month numeric variable *;
     if vx_record_date^=. then do;
         year=year(vx_record_date);
         year_month=(year(vx_record_date)*100)+month(vx_record_date);
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
     if year_month=99999999 then vx_record_date=put(year_month,null.);
     else vx_record_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package vx_record_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_record_date, record_n,
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_ADATE_Y
********************************************************************************;
%let qname=immune_l3_adate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set immunization(keep=vx_admin_date patid);

     if vx_admin_date^=. then year=year(vx_admin_date);

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
     vx_admin_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_admin_date record_n 
          record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_admin_date, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_ADATE_YM
********************************************************************************;
%let qname=immune_l3_adate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set immunization(keep=vx_admin_date patid);

     * create a year and a year/month numeric variable *;
     if vx_admin_date^=. then do;
         year=year(vx_admin_date);
         year_month=(year(vx_admin_date)*100)+month(vx_admin_date);
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
     if year_month=99999999 then vx_admin_date=put(year_month,null.);
     else vx_admin_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";
    
     keep datamartid response_date query_package vx_admin_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_admin_date, record_n,
            distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_CODETYPE
********************************************************************************;
%let qname=immune_l3_codetype;
%elapsed(begin);

proc format;
     value $code_type
         "CX"="CX"
         "ND"="ND"
         "CH"="CH"
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
     set immunization(keep=vx_code_type);

     if upcase(vx_code_type) in ("CX" "ND" "CH" "RX") then col1=vx_code_type;
     else if vx_code_type in ("NI") then col1="ZZZA";
     else if vx_code_type in ("UN") then col1="ZZZB";
     else if vx_code_type in ("OT") then col1="ZZZC";
     else if vx_code_type=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $code_type.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vx_code_type=put(col1,$code_type.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_code_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_code_type, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_CODE_CODETYPE
********************************************************************************;
%let qname=immune_l3_code_codetype;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200 vxcodetype $4;
     set immunization(keep=patid vx_code vx_code_type);

     if upcase(vx_code_type) in ("CX" "ND" "CH" "RX") then vxcodetype=vx_code_type;
     else if vx_code_type in ("NI") then vxcodetype="ZZZA";
     else if vx_code_type in ("UN") then vxcodetype="ZZZB";
     else if vx_code_type in ("OT") then vxcodetype="ZZZC";
     else if vx_code_type=" " then vxcodetype="ZZZD";
     else vxcodetype="ZZZE";
    
     if vx_code^=" " then col1=vx_code;
     else if vx_code=" " then col1="ZZZA";

     keep col1 vxcodetype patid;
run;

*- Derive statistics - year -*;
proc means data=data noprint missing;
     class col1 vxcodetype;
     output out=stats;
     format col1 $null. vxcodetype $code_type.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 vxcodetype patid;
     where patid^=' ';
run;

proc means data=pid nway noprint missing;
     class col1 vxcodetype;
     output out=pid_unique;
     format col1 $null. vxcodetype $code_type.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20 vx_code $200 vx_code_type $40;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=3))
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 vxcodetype;

     * call standard variables *;
     %stdvar

     * table values *;
     if vxcodetype not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then vx_code_type=vxcodetype;
     else vx_code_type=put(vxcodetype,$code_type.);
     vx_code=put(col1,$null.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_code vx_code_type
          record_n record_pct distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_code, vx_code_type,
            record_n, record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_STATUS
********************************************************************************;
%let qname=immune_l3_status;
%elapsed(begin);

proc format;
     value $imm_status
         "CP"="CP"
         "ER"="ER"
         "ND"="ND"
         "IC"="IC"
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
     set immunization(keep=vx_status);

     if upcase(vx_status) in ("CP" "ND" "ER" "IC") then col1=vx_status;
     else if vx_status in ("NI") then col1="ZZZA";
     else if vx_status in ("UN") then col1="ZZZB";
     else if vx_status in ("OT") then col1="ZZZC";
     else if vx_status=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $imm_status.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vx_status=put(col1,$imm_status.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_status record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_status, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_STATUSREASON
********************************************************************************;
%let qname=immune_l3_statusreason;
%elapsed(begin);

proc format;
     value $imm_statusrsn
         "IM"="IM"
         "MP"="MP"
         "OS"="OS"
         "PO"="PO"
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
     set immunization(keep=vx_status_reason);

     if upcase(vx_status_reason) in ("IM" "MP" "OS" "PO") then col1=vx_status_reason;
     else if vx_status_reason in ("NI") then col1="ZZZA";
     else if vx_status_reason in ("UN") then col1="ZZZB";
     else if vx_status_reason in ("OT") then col1="ZZZC";
     else if vx_status_reason=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $imm_statusrsn.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vx_status_reason=put(col1,$imm_statusrsn.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_status_reason record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_status_reason, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_SOURCE
********************************************************************************;
%let qname=immune_l3_source;
%elapsed(begin);

proc format;
     value $imm_source
         "OD"="OD"
         "EF"="EF"
         "IS"="IS"
         "DR"="DR"
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
     set immunization(keep=vx_source);

     if upcase(vx_source) in ("OD" "EF" "IS" "DR" "PR") then col1=vx_source;
     else if vx_source in ("NI") then col1="ZZZA";
     else if vx_source in ("UN") then col1="ZZZB";
     else if vx_source in ("OT") then col1="ZZZC";
     else if vx_source=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $imm_source.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vx_source=put(col1,$imm_source.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_source, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_DOSE_DIST
********************************************************************************;
%let qname=immune_l3_dose_dist;
%elapsed(begin);

*- Derive statistics -*;
proc means data=immunization nway noprint;
     var vx_dose;
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
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_DOSEUNIT
********************************************************************************;
%let qname=immune_l3_doseunit;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set immunization(keep=patid vx_dose_unit);

     if vx_dose_unit=" " then col1="ZZZA";
     else col1=put(strip(vx_dose_unit),$_unit.);

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
     length record_n $20 vx_dose_unit $50;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(in=d where=(_type_=1));
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     vx_dose_unit=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_dose_unit 
          record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_dose_unit, 
            record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_ROUTE
********************************************************************************;
%let qname=immune_l3_route;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set immunization(keep=patid vx_route);

     if vx_route=" " then col1="ZZZA";
     else col1=put(vx_route,$_route.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $_route.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 vx_route $200;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           _route(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     vx_route=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_route record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_route, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_BODYSITE
********************************************************************************;
%let qname=immune_l3_bodysite;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set immunization(keep=patid vx_body_site);

     if vx_body_site=" " then col1="ZZZA";
     else col1=put(vx_body_site,$vxbodysite.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $vxbodysite.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 vx_body_site $100;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           vxbodysite(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     vx_body_site=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_body_site record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_body_site, record_n,
             record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_MANUFACTURER
********************************************************************************;
%let qname=immune_l3_manufacturer;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set immunization(keep=patid vx_manufacturer);

     if vx_manufacturer=" " then col1="ZZZA";
     else col1=put(vx_manufacturer,$vxmanu.);

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $vxmanu.;
run;

*- Re-order rows and format -*;
data query;
     length record_n $20 vx_manufacturer $100;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(in=d where=(_type_=1))
           vxmanu(in=s rename=(start=col1))
     ;
     by col1;
     if d;

     * call standard variables *;
     %stdvar

     * table values *;
     vx_manufacturer=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
    
     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_manufacturer record_n 
          record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_manufacturer, record_n,
             record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&saveimmune);

********************************************************************************;
* IMMUNE_L3_LOTNUM
********************************************************************************;
%let qname=immune_l3_lotnum;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $200;
     set immunization(keep=vx_lot_num);

     if vx_lot_num^=" " then col1=vx_lot_num;
     else if vx_lot_num=" " then col1="ZZZA";
    
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
     vx_lot_num=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package vx_lot_num record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, vx_lot_num, record_n, record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=);

%end;

********************************************************************************;
* Bring in HASH_TOKEN and compress it
********************************************************************************;
%if &_yhash_token=1 %then %do;
%let qname=hash_token;
%elapsed(begin);

data &qname(compress=yes);
     set pcordata.&qname;
run;

%let savehash=&qname;

%elapsed(end);

********************************************************************************;
* HASH_L3_N
********************************************************************************;
%let qname=hash_l3_n;
%elapsed(begin);

*- Do valid records first -*;
%macro valid(vvar,vord);
       data hash_valid_&vvar;
            set hash_token;
        
            if &vvar^=" " and substr(compress(&vvar),1,3)^="XXX";
       run;

       *- Macro for each variable -*;
       %enc_oneway(encdsn=hash_valid_&vvar,encvar=&vvar,_nc=0,ord=&vord)
        
       proc append base=query_valid data=query;
       run;
        
       proc datasets noprint;
            delete hash_valid_&vvar query;
       quit;

%mend valid;
%valid(vvar=token_01,vord=2);
%valid(vvar=token_02,vord=3);
%valid(vvar=token_03,vord=4);
%valid(vvar=token_04,vord=5);
%valid(vvar=token_05,vord=6);
%valid(vvar=token_16,vord=7);

data query_valid;
     length valid_distinct_n_pct $5;
     set query_valid;
    
     * add percentage distinct *;
     if _all_n>0 then 
        valid_distinct_n_pct=strip(put(round((_distinct_n/_all_n)*100,.1),8.1));
     else if _all_n<=0 then valid_distinct_n_pct="0";
    
     rename all_n=valid_n distinct_n=valid_distinct_n;
     keep tag all_n distinct_n valid_distinct_n_pct;
run;

proc sort data=query_valid;
     by tag;
run;

*- Macro for each variable -*;
%enc_oneway(encdsn=hash_token,encvar=patid,ord=1)
%enc_oneway(encdsn=hash_token,encvar=token_01,_nc=0,ord=2)
%enc_oneway(encdsn=hash_token,encvar=token_02,_nc=0,ord=3)
%enc_oneway(encdsn=hash_token,encvar=token_03,_nc=0,ord=4)
%enc_oneway(encdsn=hash_token,encvar=token_04,_nc=0,ord=5)
%enc_oneway(encdsn=hash_token,encvar=token_05,_nc=0,ord=6)
%enc_oneway(encdsn=hash_token,encvar=token_16,_nc=0,ord=7)

proc sort data=query;
     by tag;
run;

data query;
     length distinct_n_pct $5;
     merge query query_valid;
     by tag;
    
     * add percentage distinct *;
     if upcase(tag)="PATID" then do; 
        distinct_n_pct="N/A"; 
        valid_n="N/A"; 
        valid_distinct_n="N/A"; 
        valid_distinct_n_pct="N/A";
     end;
     else do;
        if _all_n>0 then 
           distinct_n_pct=strip(put(round((_distinct_n/_all_n)*100,.1),8.1));
        else if _all_n<=0 then distinct_n_pct="0";

        if valid_n=. then do;
           valid_n="0";
           valid_distinct_n="0";
           valid_distinct_n_pct="0";
        end;
     end;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n, valid_n, valid_distinct_n, distinct_n_pct, 
            valid_distinct_n_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savehash);


********************************************************************************;
* HASH_L3_TOKEN_AVAILABILITY
********************************************************************************;
%let qname=hash_l3_token_availability;
%elapsed(begin);

*- Create dummy dataset of all possibilities -*;
data dummy;
     array v t01 t02 t03 t04 t05 t16;
     do i = 1 to dim(v);
        v{i}=1;
        output;
     end;
run;

proc means data=dummy noprint;
     class t16 t05 t04 t03 t02 t01;
     output out=dummy_all n=n;
run;

data dummy_all;
     length token distinct_patid_n $25;
     set dummy_all;
     if _type_>0;
    
     distinct_patid_n="0";
     ord=sum(t01, t02, t03, t04, t05, t16);
     array v t01 t02 t03 t04 t05 t16;
     array f $25 f01 f02 f03 f04 f05 f16 ('T01' 'T02' 'T03' 'T04' 'T05' 'T16');
        do i = 1 to dim(v);
            if v{i}=1 then do;
               if ord=1 then token=f{i};
               else if ord>1 then token=strip(token) || " " || f{i};
            end;
            else token=token;
        end;
     keep _type_ distinct_patid_n ord t01 t02 t03 t04 t05 t16 token;
run;

*- Create binomial variables of the presence of a token value -*;
data data;
     set hash_token;
     if patid^=" ";
     
     * set error values (XXX) as empty *;
     array t token_01 token_02 token_03 token_04 token_05 token_16;
     array p t01 t02 t03 t04 t05 t16;
        do i = 1 to dim(t);
            if t{i}=" " or substr(upcase(t{i}),1,3)="XXX" then p{i}=0;
            else p{i}=1;
        end;
run;

*- Get counts of each combination -*;
proc means data=data noprint completetypes missing;
     class t16 t05 t04 t03 t02 t01;
     output out=stats n=distinct_patid;
run;

*- Output combinations of interest and create column variables -*;
data query;
     length token distinct_patid_n $25;
     set stats(where=(_type_>0));

     * counts *;
     distinct_patid_n=strip(put(distinct_patid,threshold.));

     * all missing record *;
     if nmiss(t01, t02, t03, t04, t05, t16)=0 and sum(t01, t02, t03, t04, t05, t16)=0 then do;
        _type_=0;
        ord=0;
        token="All Missing";
        output;
     end;
     * records of combinations where token(s) is populated *;
     else if min(t01, t02, t03, t04, t05, t16)=1 and max(t01, t02, t03, t04, t05, t16)=1 then do;
         ord=sum(t01, t02, t03, t04, t05, t16);
         array v t01 t02 t03 t04 t05 t16;
         array f $25 f01 f02 f03 f04 f05 f16 ('T01' 'T02' 'T03' 'T04' 'T05' 'T16');
            do i = 1 to dim(v);
               if v{i}=1 then do;
                  if ord=1 then token=f{i};
                  else if ord>1 then token=strip(token) || " " || f{i};
               end;
               else token=token;
            end;
         output;
     end;
     keep _type_ distinct_patid_n ord t01 t02 t03 t04 t05 t16 token;
run;

proc sort data=query;
     by _type_;
run;

*- Merge dummy and actual -*;    
data query;
     merge dummy_all query;
     by _type_;

     * call standard variables *;
     %stdvar
run;

*- Order records per spec -*;
proc sort data=query;
     by ord descending t01 descending t02 descending t03 descending t04 
            descending t05 descending t16;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, token, distinct_patid_n
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
proc contents data=pcordata.&qname out=cont_trial noprint;
run;

data _null_;
     set cont_trial end=eof;
     retain tulength 0;
     if name in ("PATID" "TRIALID" "PARTICIPANTID") then tulength=tulength+length;
    
     * add 2 to TLENGTH (2 for delimiter) *;
     if eof then call symputx("_tulength",tulength+2);
run;

data &qname(compress=yes);
     length trial_key $&_tulength;
     set pcordata.&qname;

     * restrict data to within lookback period *;
     if trial_enroll_date>=&lookback_dt or trial_enroll_date=.;

     trial_key=strip(patid)||"_"||strip(trialid)||"_"||strip(participantid);
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

%elapsed(b_or_e=end,last=Y);

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
%mend dc_main;
%dc_main;
