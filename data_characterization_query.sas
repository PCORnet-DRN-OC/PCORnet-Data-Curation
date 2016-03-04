/*******************************************************************************
*  $Source: data_characterization_query $;
*    $Date: 2016/02/05
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Characterization Query Package V3.00
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_characterization_query.pdf)
*           3) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>.cpt)
*           4) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_characterization_query.log)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/
options validvarname=upcase errors=0 nomprint;
ods html close;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname pcordata "&dpath" access=readonly;
libname drnoc "&qpath.drnoc";
libname dmlocal "&qpath.dmlocal";

********************************************************************************;
* Create macro variable from DataMart ID and program run date 
********************************************************************************;
data _null_;
     set pcordata.harvest;
     call symput("dmid",strip(datamartid));
     call symput("tday",left(put("&sysdate"d,yymmddn8.)));
run;

*******************************************************************************;
*- Create an external log file to capture system data information
*******************************************************************************;
filename setlog "&qpath.dmlocal/set.log" lrecl=200 ;
proc printto log=setlog  new ;
run ;
proc setinit ;
run ;
proc printto;
run ;

*- Read log file and create SAS dataset records to be in XTBL_L3_METADATA -*;
data xtbl_mdata_idsn (keep = product expire) ;
     infile "&qpath.dmlocal/set.log" truncover ;
     length  product $70 texpire $45 ;
     format expire date9. ;
     input  product $70. @ ;
     if substr(product,1,3) eq '---' then do ;
         input texpire $45. ;
         expire=input(strip(texpire),date9.) ;
     end ;
     else delete ;
run ;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_characterization_query.log" lrecl=200;
proc printto log=qlog  new ;
run ;

********************************************************************************;
* Create standard macros used in multiple queries
********************************************************************************;

*- Macro to create DATAMARTID, RESPONSE_DATE and QUERY_PACKAGE in every query -*;
%macro stdvar;
     length datamartid $20 query_package $30 response_date 4.;
     format response_date date9.;
     datamartid="&dmid";
     response_date="&sysdate"d;
     query_package="DC V3.00";
%mend stdvar;

*- Macros to apply threshold (low cell count) to applicable queries -*;
%macro threshold(tdsn=stats,type=1,qvar=col1,nullval=,_tvar=_freq_);
    if _type_>=&type and 0<&_tvar<&threshold and &nullval then &_tvar=.t;
%mend threshold;

*- Macros to determine minimum/maximum date -*;
%macro minmax(idsn,var);

     data &idsn.&var;
          length futureflag preflag 3.;
          set &idsn;

          * flag for future date records *;
          if &var^=. and &var>today() then futureflag=1;

          * flag for records pre-01JAN2010 *;
          if .<&var<'01JAN2010'd then preflag=1;
        
          keep &var futureflag preflag;
     run;

     proc means data=&idsn.&var nway noprint;
          var &var futureflag preflag;
          output out=m&idsn n=n dum1 dum2  sum=dum3 futuresum presum  min=mindt dum4 dum5  max=maxdt dum6 dum7;
     run;

     *- Derive appropriate counts -*;
     data &idsn._&var;
          length dataset tag $25;
          set m&idsn;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset=upcase("&idsn");
          tag=upcase("&var");

          * counts *;
          min=put(year(mindt),4.)||"_"||put(month(mindt),z2.);
          max=put(year(maxdt),4.)||"_"||put(month(maxdt),z2.);

          if n>=&threshold then do;
              if futuresum>. then future_dt_n=strip(put(futuresum,16.));
              else future_dt_n=strip(put(0,16.));
              if presum>. then pre2010_n=strip(put(presum,16.));
              else pre2010_n=strip(put(0,16.));
          end;
          else if n<&threshold then do;
            future_dt_n=left(put(.t,threshold.));
            pre2010_n=left(put(.t,threshold.));
          end;

          keep datamartid response_date query_package dataset tag min max
               future_dt_n pre2010_n;
     run;
%mend minmax;

*- Macro to print each query result -*;
%macro prnt(pdsn=,first=,_obs=max,dropvar=datamartid response_date query_package,_svar=,_pct=);

     * if not printing entire dataset, sort so that top frequencies are printed *;
     %if &_obs^=max %then %do;
        data &pdsn;
             set dmlocal.&pdsn(rename=(record_n=record_nc));

             if strip(record_nc) in ("0" "BT" "T") then delete;
             else record_n=input(record_nc,best.);
             drop record_nc;
        run;

        proc sort data=&pdsn;
            by descending record_n &_svar;
        run;
     %end;
    
     ods html close;
     ods listing;
     ods path sashelp.tmplmst(read) library.templat(read);
     %if &first=1 %then %do; 
        ods pdf file="&qpath.drnoc/&dmid._&tday._data_characterization.pdf" style=journal;
     %end;
     %if &_obs=max %then %do;
         title "Query name:  %upcase(&pdsn)";
     %end;
     %else %do;
         title "Query name:  %upcase(&pdsn) (Limited to most frequent &_obs above threshold observations)";
     %end;
     %if &_obs^=max %then %do;
         proc print width=min data=&pdsn(drop=&dropvar obs=&_obs);
              var &_svar record_n &_pct;
         run;
         proc datasets noprint;
            delete &pdsn;
         run;
     %end;
     %else %do;
         proc print width=min data=dmlocal.&pdsn(drop=&dropvar obs=&_obs);
         run;
     %end;
     ods listing close;
%mend prnt;

*- Macro to remove working datasets after each query result -*;
%macro clean(savedsn);
     proc datasets  noprint;
          save xtbl_mdata_idsn &savedsn / memtype=data;
          save formats / memtype=catalog;
     quit;
%mend clean;

********************************************************************************;
* Create working formats used in multiple queries
********************************************************************************;
proc format;
     value $enc_typ
         "AV"="AV"
         "ED"="ED"
         "EI"="EI"
         "IP"="IP"
         "IS"="IS"
         "OA"="OA"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
        
     value null
       99999999="NULL or missing"
        ;

     value $null
       "ZZZA"="NULL or missing"
        ;

     value $nullout
       "ZZZA"="NULL or missing"
       "ZZZB"="Values outside of CDM specifications"
        ;
    
     value period
       1="1 yr"
       2="2 yrs"
       3="3 yrs"
       4="4 yrs"
       5="5 yrs"
      99="All yrs"
        ;
    
     value $stat
       "S01"="MIN"
       "S02"="P1"
       "S03"="P5"
       "S04"="P25"
       "S05"="MEAN"
       "S06"="MEDIAN"
       "S07"="P75"
       "S08"="P95"
       "S09"="P99"
       "S10"="MAX"
       "S11"="N"
       "S12"="NULL or missing"
        ;
    
     value threshold
       .n="N/A"
       .t="BT"
        99999999="99999999"
        ;
run;

********************************************************************************;
* Bring in DEMOGRAPHIC and compress it
********************************************************************************;
data demographic(compress=yes);
     set pcordata.demographic;
run;

********************************************************************************;
* Minimum/maximum demographic for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=demographic,var=birth_date)

********************************************************************************;
* DEM_L3_N
********************************************************************************;
%let qname=dem_l3_n;

*- Create one record for every unique value -*;
proc sort data=demographic(keep=patid)
     out=data;
     by patid;
run;

*- Derive appropriate counts and variables -*;
data query;
     set data end=eof;
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
        all_n=strip(put(_all_n,16.));
        distinct_n=strip(put(_distinct_n,16.));
        null_n=strip(put(_null_n,16.));
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

*- Print query and clear working directory -*;
%clean(savedsn=demographic demographic_birth_date);

********************************************************************************;
* DEM_L3_AGEYRSDIST1
********************************************************************************;
%let qname=dem_l3_ageyrsdist1;

*- Derive statistical variable -*;
data data;
     length age 3.;
     set demographic(keep=birth_date);

     if birth_date^=. then age=int(yrdif(birth_date,today()));
     keep age;
run;

*- Derive statistics -*;
proc means data=data nway noprint;
     var age;
     output out=stats min=s01 p1=s02 p5=s03 p25=s04 mean=s05 median=s06 p75=s07
                      p95=s08 p99=s09 max=s10 n=s11 nmiss=s12;
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
             var s01 s05 s06 s10 s11 s12;
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
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);

     * counts *;
     if stat="N" then record_n=strip(put(col1,threshold.));
     else record_n=compress(put(col1,16.));

     keep datamartid response_date query_package stat record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, stat, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic demographic_birth_date);

********************************************************************************;
* DEM_L3_AGEYRSDIST2
********************************************************************************;
%let qname=dem_l3_ageyrsdist2;

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
       75-110="75-110 yrs"
      111-9998=">110 yrs"
      9999="NULL or missing"
        ;
run;

*- Derive categorical variable -*;
data data;
     length col1 4.;
     set demographic(keep=birth_date);

     * set missing to impossible high value for categorization *;
     if birth_date^=. then col1=int(yrdif(birth_date,today()));
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
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package age_group record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, age_group, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic demographic_birth_date);

********************************************************************************;
* DEM_L3_HISPDIST
********************************************************************************;
%let qname=dem_l3_hispdist;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     hispanic=put(col1,$hisp.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package hispanic record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, hispanic, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic demographic_birth_date);

********************************************************************************;
* DEM_L3_RACEDIST
********************************************************************************;
%let qname=dem_l3_racedist;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     race=put(col1,$race.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package race record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, race, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic demographic_birth_date);

********************************************************************************;
* DEM_L3_SEXDIST
********************************************************************************;
%let qname=dem_l3_sexdist;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     sex=put(col1,$sex.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package sex record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, sex, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic_birth_date);

********************************************************************************;
* Bring in ENCOUNTER and compress it
********************************************************************************;
data encounter(compress=yes);
     set pcordata.encounter;
run;

********************************************************************************;
* Minimum/maximum encounter for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=encounter,var=admit_date)
%minmax(idsn=encounter,var=discharge_date)

********************************************************************************;
* ENC_L3_N
********************************************************************************;
%let qname=enc_l3_n;

*- Macro for each variable -*;
%macro enc_oneway(encvar);
     proc sort data=encounter(keep=&encvar)
          out=&encvar;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._oneway;
          length tag $25;
          set &encvar end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset="ENCOUNTER";
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          if &encvar=" " then _null_n=_null_n+1;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;
    
     * append each variable into base dataset *;
     proc append base=query data=&encvar._oneway;
     run;

%mend enc_oneway;
%enc_oneway(encvar=encounterid)
%enc_oneway(encvar=patid)
%enc_oneway(encvar=providerid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_N_VISIT
********************************************************************************;
%let qname=enc_l3_n_visit;

*- Derive appropriate counts - visits -*;
data all_visit(keep=all_n) distinct(keep=distinct_visit);
     length distinct_visit $200;
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ADMSRC
********************************************************************************;
%let qname=enc_l3_admsrc;

proc format;
     value $adm_src
         "AF"="AF"
         "AL"="AL"
         "AV"="AV"
         "ED"="ED"
         "HH"="HH"
         "HO"="HO"
         "HS"="HS"
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

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'HH' 'HO' 'HS' 'IP' 'NH' 'RH' 
         'RS' 'SN') then col1=admitting_source;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     admitting_source=put(col1,$adm_src.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_ADMRSC
********************************************************************************;
%let qname=enc_l3_enctype_admrsc;

proc format;
     value $adm_src
         "AF"="AF"
         "AL"="AL"
         "AV"="AV"
         "ED"="ED"
         "HH"="HH"
         "HO"="HO"
         "HS"="HS"
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

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     if admitting_source in ('AF' 'AL' 'AV' 'ED' 'HH' 'HO' 'HS' 'IP' 'NH' 'RH' 
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ADATE_Y
********************************************************************************;
%let qname=enc_l3_adate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     admit_date=put(col1,$null.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ADATE_YM
********************************************************************************;
%let qname=enc_l3_adate_ym;

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
     merge dummy stats(in=s);
     by year_month;
run;

*- Add first and last actual year/month values to every record -*;
data query;
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999)
            then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=enc_l3_enctype_adate_ym;

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set encounter(keep=admit_date enc_type);

     * create a year and a year/month numeric variable *;
     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     set stats(where=(_type_^=99));

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
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DDATE_Y
********************************************************************************;
%let qname=enc_l3_ddate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     discharge_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DDATE_YM
********************************************************************************;
%let qname=enc_l3_ddate_ym;

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
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or
            after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999)
        then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then discharge_date=put(year_month,null.);
     else discharge_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));
    
     keep datamartid response_date query_package discharge_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, discharge_date, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_DDATE_YM
********************************************************************************;
%let qname=enc_l3_enctype_ddate_ym;

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set encounter(keep=discharge_date enc_type);

     * create a year and a year/month numeric variable *;
     if discharge_date^=. then 
            year_month=(year(discharge_date)*100)+month(discharge_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class enctype/preloadfmt;
     class year_month;
     output out=stats;
     format enctype $enc_typ.;
run;

*- Derive appropriate counts and variables -*;
data query;
     set stats;

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
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type discharge_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, discharge_date, 
            record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DISDISP
********************************************************************************;
%let qname=enc_l3_disdisp;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     discharge_disposition=put(col1,$disdisp.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_DISDISP
********************************************************************************;
%let qname=enc_l3_enctype_disdisp;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_disposition);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);

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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DISSTAT
********************************************************************************;
%let qname=enc_l3_disstat;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     discharge_status=put(col1,$disstat.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_DISSTAT
********************************************************************************;
%let qname=enc_l3_enctype_disstat;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type discharge_status);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DRG
********************************************************************************;
%let qname=enc_l3_drg;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set encounter(keep=drg);

     if length(drg)=3 then col1=drg;
     else if drg=" " then col1="ZZZA";
     else col1="ZZZB";

     keep col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $nullout.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     drg=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package drg record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, drg, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DRG_TYPE
********************************************************************************;
%let qname=enc_l3_drg_type;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     drg_type=put(col1,$drgtype.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package drg_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, drg_type, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE_DRG
********************************************************************************;
%let qname=enc_l3_enctype_drg;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set encounter(keep=enc_type drg);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if length(drg)=3 then col1=drg;
     else if drg=" " then col1="ZZZA";
     else col1="ZZZB";
    
     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class enctype col1/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $nullout.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=3));

     * call standard variables *;
     %stdvar

     * table values *;
     drg=put(col1,$nullout.);
     enc_type=put(enctype,$enc_typ.);

     * apply threshold *;
     %threshold(type=3,nullval=col1^="ZZZA" and enctype^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_ENCTYPE
********************************************************************************;
%let qname=enc_l3_enctype;

*- Derive categorical variable -*;
data data;
     length col1 $4 unique_visit $200;
     set encounter(keep=enc_type patid encounterid admit_date providerid);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     if cmiss(patid,encounterid,admit_date,providerid,enc_type)=0 then 
        unique_visit=compress(patid)|| "/" || compress(encounterid) || "/" || 
                     put(admit_date,date9.) || "/" || compress(providerid);

     keep col1 unique_visit patid;
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

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid))
           visit_unique(where=(_type_^=99) rename=(_freq_=distinct_visit))
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(col1,$enc_typ.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_visit,nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     distinct_visit_n=left(put(distinct_visit,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enc_type record_n record_pct
          distinct_visit_n distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, record_n, 
            record_pct, distinct_visit_n, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DASH1
********************************************************************************;
%let qname=enc_l3_dash1;

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
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar
    
     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=encounter demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* ENC_L3_DASH2
********************************************************************************;
%let qname=enc_l3_dash2;

*- Data with legitimate date and encounter type -*;
data data;
     set encounter(keep=patid admit_date enc_type);
     if admit_date^=. and enc_type in ('AV' 'ED' 'EI' 'IP');

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
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=demographic_birth_date encounter_admit_date encounter_discharge_date);

********************************************************************************;
* Bring in DIAGNOSIS and compress it
********************************************************************************;
data diagnosis(compress=yes);
     set pcordata.diagnosis;
run;

********************************************************************************;
* Minimum/maximum diagnosis for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=diagnosis,var=admit_date)

********************************************************************************;
* DIA_L3_N
********************************************************************************;
%let qname=dia_l3_n;

*- Macro for each variable -*;
%macro enc_oneway(encvar);
     proc sort data=diagnosis(keep=&encvar)
          out=&encvar;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._oneway;
          length tag $25;
          set &encvar end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset="DIAGNOSIS";
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          if &encvar=" " then _null_n=_null_n+1;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;
    
     * append each variable into base dataset *;
     proc append base=query data=&encvar._oneway;
     run;

%mend enc_oneway;
%enc_oneway(encvar=encounterid)
%enc_oneway(encvar=patid)
%enc_oneway(encvar=diagnosisid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DX
********************************************************************************;
%let qname=dia_l3_dx;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     dx=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package dx record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DX_DXTYPE
********************************************************************************;
%let qname=dia_l3_dx_dxtype;

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
proc means data=data nway completetypes noprint missing;
     class col1 dxtype/preloadfmt;
     output out=stats;
     format dxtype $dxtype. col1 $null.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by col1 dxtype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 dxtype/preloadfmt;
     output out=pid_unique;
     format dxtype $dxtype. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by col1 dxtype;

     * call standard variables *;
     %stdvar

     * table values *;
     dx=put(col1,$null.);    
     dx_type=put(dxtype,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and dxtype^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA" and dxtype^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));

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

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DXSOURCE
********************************************************************************;
%let qname=dia_l3_dxsource;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     dx_source=put(col1,$dxsrc.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package dx_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_source, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DXTYPE_DXSOURCE
********************************************************************************;
%let qname=dia_l3_dxtype_dxsource;

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
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_source=put(col1,$dxsrc.);
     dx_type=put(dxtype,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and dxtype^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package dx_type dx_source record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, dx_source, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_PDX
********************************************************************************;
%let qname=dia_l3_pdx;

proc format;
     value $pdx
         "P"="P"
         "S"="S"
         "X"="X"
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

     if pdx in ("P" "S" "X") then col1=pdx;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     pdx=put(col1,$pdx.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package pdx record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pdx, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_PDX_ENCTYPE
********************************************************************************;
%let qname=dia_l3_pdx_enctype;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=enc_type pdx encounterid patid);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if pdx in ("P" "S" "X") then col1=pdx;
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
     record_n=left(put(_freq_,threshold.));
     distinct_encid_n=left(put(_freq_encid,threshold.));
     distinct_patid_n=left(put(_freq_patid,threshold.));

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

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_ADATE_Y
********************************************************************************;
%let qname=dia_l3_adate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid))
           encid_unique(where=(_type_^=99) rename=(_freq_=distinct_encid))
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
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     distinct_encid_n=left(put(distinct_encid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_ADATE_YM
********************************************************************************;
%let qname=dia_l3_adate_ym;

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
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999)
        then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));
    
     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_ENCTYPE
********************************************************************************;
%let qname=dia_l3_enctype;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set diagnosis(keep=enc_type patid);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then col1=enc_type;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid));
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(col1,$enc_typ.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DXTYPE_ENCTYPE
********************************************************************************;
%let qname=dia_l3_dxtype_enctype;

*- Derive categorical variable -*;
data data;
     length enctype col1 $4;
     set diagnosis(keep=enc_type dx_type);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
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
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     dx_type=put(col1,$dxtype.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     enc_type=put(enctype,$enc_typ.);
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type dx_type record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dx_type, enc_type, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=dia_l3_enctype_adate_ym;

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set diagnosis(keep=admit_date enc_type encounterid);

     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype encounterid;
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

*- Derive appropriate counts and variables -*;
data query;
     merge stats
           stats_distinct(rename=(_freq_=_freq_encid))
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
     record_n=left(put(_freq_,threshold.));
     distinct_encid_n=left(put(_freq_encid,threshold.));

     keep datamartid response_date query_package enc_type admit_date record_n 
          distinct_encid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            distinct_encid_n, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* DIA_L3_DASH1
********************************************************************************;
%let qname=dia_l3_dash1;

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
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=diagnosis demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date);

********************************************************************************;
* Bring in PROCEDURES and compress it
********************************************************************************;
data procedures(compress=yes);
     set pcordata.procedures;
run;

********************************************************************************;
* Minimum/maximum procedures for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=procedures,var=admit_date)
%minmax(idsn=procedures,var=px_date)

********************************************************************************;
* PRO_L3_N
********************************************************************************;
%let qname=pro_l3_n;

*- Macro for each variable -*;
%macro enc_oneway(encvar);
     proc sort data=procedures(keep=&encvar)
          out=&encvar;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._oneway;
          length tag $25;
          set &encvar end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset="PROCEDURES";
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          if &encvar=" " then _null_n=_null_n+1;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;
    
     * append each variable into base dataset *;
     proc append base=query data=&encvar._oneway;
     run;

%mend enc_oneway;
%enc_oneway(encvar=encounterid)
%enc_oneway(encvar=patid)
%enc_oneway(encvar=proceduresid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PX
********************************************************************************;
%let qname=pro_l3_px;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     px=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package px record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_ADATE_Y
********************************************************************************;
%let qname=pro_l3_adate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid))
           encid_unique(where=(_type_^=99) rename=(_freq_=distinct_encid))
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
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     distinct_encid_n=left(put(distinct_encid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_ADATE_YM
********************************************************************************;
%let qname=pro_l3_adate_ym;

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
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999) 
        then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then admit_date=put(year_month,null.);
     else admit_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));
    
     keep datamartid response_date query_package admit_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, admit_date, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PXDATE_Y
********************************************************************************;
%let qname=pro_l3_pxdate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid))
           encid_unique(where=(_type_^=99) rename=(_freq_=distinct_encid))
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
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     distinct_encid_n=left(put(distinct_encid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PX_ENCTYPE
********************************************************************************;
%let qname=pro_l3_px_enctype;

*- Derive categorical variable -*;
data data;
     length enctype $4 col1 $15;
     set procedures(keep=enc_type px);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if px^=" " then col1=px;
     else if px=" " then col1="ZZZA";
    
     keep enctype col1;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class col1 enctype/preloadfmt;
     output out=stats;
     format enctype $enc_typ. col1 $null.;
run;

*- Derive appropriate counts and variables -*;
data query;
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     px=put(col1,$null.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA" and enctype^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type px record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px, enc_type, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_ENCTYPE
********************************************************************************;
%let qname=pro_l3_enctype;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set procedures(keep=enc_type);

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then col1=enc_type;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     enc_type=put(col1,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enc_type record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PXTYPE_ENCTYPE
********************************************************************************;
%let qname=pro_l3_pxtype_enctype;

proc format;
     value $pxtype
         "09"="09"
         "10"="10"
         "11"="11"
         "C2"="C2"
         "C3"="C3"
         "C4"="C4"
         "H3"="H3"
         "HC"="HC"
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

     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";
    
     if px_type in ("09" "10" "11" "C2" "C3" "C4" "H3" "HC" "LC" "ND" "RE") 
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
     set stats;

     * call standard variables *;
     %stdvar

     * table values *;
     px_type=put(col1,$pxtype.);
     enc_type=put(enctype,$enc_typ.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD" and enctype^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package enc_type px_type record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_type, enc_type, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_ENCTYPE_ADATE_YM
********************************************************************************;
%let qname=pro_l3_enctype_adate_ym;

*- Derive categorical variable -*;
data data;
     length enctype $4 year_month 5.;
     set procedures(keep=admit_date enc_type encounterid);

     if admit_date^=. then year_month=(year(admit_date)*100)+month(admit_date);
     if year_month=. then year_month=99999999;
    
     if enc_type in ("AV" "ED" "EI" "IP" "IS" "OA") then enctype=enc_type;
     else if enc_type in ("NI") then enctype="ZZZA";
     else if enc_type in ("UN") then enctype="ZZZB";
     else if enc_type in ("OT") then enctype="ZZZC";
     else if enc_type=" " then enctype="ZZZD";
     else enctype="ZZZE";

     keep year_month enctype encounterid;
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

*- Derive appropriate counts and variables -*;
data query;
     merge stats
           stats_distinct(rename=(_freq_=_freq_encid))
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
     record_n=left(put(_freq_,threshold.));
     distinct_encid_n=left(put(_freq_encid,threshold.));

     keep datamartid response_date query_package enc_type admit_date record_n 
          distinct_encid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enc_type, admit_date, 
            distinct_encid_n, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PX_PXTYPE
********************************************************************************;
%let qname=pro_l3_px_pxtype;

*- Derive categorical variable -*;
data data;
     length pxtype $4 col1 $15;
     set procedures(keep=px_type px patid);

     if px_type in ("09" "10" "11" "C2" "C3" "C4" "H3" "HC" "LC" "ND" "RE") 
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
proc means data=data nway completetypes noprint missing;
     class col1 pxtype/preloadfmt;
     output out=stats;
     format pxtype $pxtype. col1 $null.;
run;

*- Derive distinct patient id for each PDX/type -*;
proc sort data=data out=pid nodupkey;
     by col1 pxtype patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class col1 pxtype/preloadfmt;
     output out=pid_unique;
     format pxtype $pxtype. col1 $null.;
run;

*- Format -*;
data query;
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
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));

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

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* PRO_L3_PXSOURCE
********************************************************************************;
%let qname=pro_l3_pxsource;

proc format;
     value $pxsrc
         "BI"="BI"
         "CL"="CL"
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

     if px_source in ("BI" "CL" "OD") then col1=px_source;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     px_source=put(col1,$pxsrc.);

     * Apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package px_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, px_source, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=procedures demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date procedures_admit_date procedures_px_date);

********************************************************************************;
* Bring in ENROLLMENT and compress it
********************************************************************************;
data enrollment(compress=yes);
     set pcordata.enrollment;
run;

********************************************************************************;
* Minimum/maximum enrollment for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=enrollment,var=enr_start_date)
%minmax(idsn=enrollment,var=enr_end_date)

********************************************************************************;
* ENR_L3_N
********************************************************************************;
%let qname=enr_l3_n;

*- Derive categorical variable -*;
data enroll;
     length enr_start_date $10 enrollid $100;
     set enrollment(keep=patid enr_start_date enr_basis
            rename=(enr_start_date=enr_start_date_num));

     if enr_start_date_num^=. then enr_start_date=put(enr_start_date_num,yymmdd10.);
     if patid^=" " and enr_start_date_num^=. and enr_basis^=" " then
         enrollid=strip(patid)||"/"||put(enr_start_date_num,date9.)||"/"||
         strip(enr_basis);
    
     keep patid enr_start_date enrollid;
run;

*- Macro for each variable -*;
%macro enc_oneway(encvar);
     proc sort data=enroll(keep=&encvar)
          out=&encvar;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._oneway;
          length tag $25;
          set &encvar end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset="ENROLLMENT";
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          if &encvar=" " then _null_n=_null_n+1;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;
    
     * append each variable into base dataset *;
     proc append base=query data=&encvar._oneway;
     run;

%mend enc_oneway;
%enc_oneway(encvar=patid)
%enc_oneway(encvar=enr_start_date)
%enc_oneway(encvar=enrollid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_DIST_START
********************************************************************************;
%let qname=enr_l3_dist_start;

*- Derive statistics -*;
proc means data=enrollment nway noprint;
     var enr_start_date;
     output out=stats min=s01 p1=s02 p5=s03 p25=s04 mean=s05 median=s06 p75=s07
                      p95=s08 p99=s09 max=s10 n=s11 nmiss=s12;
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
             var s01 s02 s03 s04 s06 s07 s08 s09 s10 s11 s12;
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
     length record_n $10.;
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);

     * counts *;
     if _name_ in ("S11" "S12") or col1=.n then record_n=left(put(col1,threshold.));
     else record_n=put(col1,date9.);

     keep datamartid response_date query_package stat record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, stat, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_DIST_END
********************************************************************************;
%let qname=enr_l3_dist_end;

*- Derive statistics -*;
proc means data=enrollment nway noprint;
     var enr_end_date;
     output out=stats min=s01 p1=s02 p5=s03 p25=s04 mean=s05 median=s06 p75=s07
                      p95=s08 p99=s09 max=s10 n=s11 nmiss=s12;
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
             var s01 s02 s03 s04 s06 s07 s08 s09 s10 s11 s12;
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
     length record_n $10.;
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     stat=put(_name_,$stat.);
    
     * counts *;
     if _name_ in ("S11" "S12") or col1=.n then record_n=left(put(col1,threshold.));
     else record_n=put(col1,date9.);

     keep datamartid response_date query_package stat record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, stat, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_DIST_ENRMONTH
********************************************************************************;
%let qname=enr_l3_dist_enrmonth;

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
     do y = min to max;
        _type_=1;
        _freq_=0;
        col1=y;
        output;
     end;
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats;
     by col1;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enroll_m record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enroll_m, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_DIST_ENRYEAR
********************************************************************************;
%let qname=enr_l3_dist_enryear;

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
     do y = min to max;
        _type_=1;
        _freq_=0;
        col1=y;
        output;
     end;
run;

*- Merge actual and dummy data -*;
data stats;
     merge dummy stats;
     by col1;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enroll_y record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enroll_y, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_ENR_YM
********************************************************************************;
%let qname=enr_l3_enr_ym;

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
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999)
        then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then month=put(year_month,null.);
     else month=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));
    
     keep datamartid response_date query_package month record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, month, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_BASEDIST
********************************************************************************;
%let qname=enr_l3_basedist;

proc format;
     value $basis
     'A'='A'
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

     if enr_basis in ("A" "E" "G" "I") then col1=enr_basis;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     enr_basis=put(col1,$nullout.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package enr_basis record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, enr_basis, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=enrollment demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* ENR_L3_PER_PATID
********************************************************************************;
%let qname=enr_l3_per_patid;

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
     output out=stats min=s01 p1=s02 p5=s03 p25=s04 mean=s05 median=s06 p75=s07
                      p95=s08 p99=s09 max=s10 n=s11 nmiss=s12;
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
             var s01 s02 s03 s04 s06 s07 s08 s09 s10 s11 s12;
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
     set query;

     * call standard variables *;
     %stdvar

     * table values *;
     if _name_="S11" then record_n=put(col1,threshold.);
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

*- Print query and clear working directory -*;
%clean(savedsn=demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
               procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date);

********************************************************************************;
* Bring in VITAL and compress it
********************************************************************************;
data vital(compress=yes);
     set pcordata.vital;
run;

********************************************************************************;
* Minimum/maximum vital for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=vital,var=measure_date)

********************************************************************************;
* VIT_L3_N
********************************************************************************;
%let qname=vit_l3_n;

*- Macro for each variable -*;
%macro enc_oneway(encvar);
     proc sort data=vital(keep=&encvar)
          out=&encvar;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._oneway;
          length tag $25;
          set &encvar end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset="VITAL";
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          if &encvar=" " then _null_n=_null_n+1;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;
    
     * append each variable into base dataset *;
     proc append base=query data=&encvar._oneway;
     run;

%mend enc_oneway;
%enc_oneway(encvar=encounterid)
%enc_oneway(encvar=patid)
%enc_oneway(encvar=vitalid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_MDATE_Y
********************************************************************************;
%let qname=vit_l3_mdate_y;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     merge stats(where=(_type_=1)) 
           pid_unique(where=(_type_^=99) rename=(_freq_=distinct_patid));
     ;
     by col1;

     * call standard variables *;
     %stdvar

     * table values *;
     measure_date=put(col1,$null.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     distinct_patid_n=left(put(distinct_patid,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_MDATE_YM
********************************************************************************;
%let qname=vit_l3_mdate_ym;

*- Derive categorical variable -*;
data data;
     length year year_month 5.;
     set vital(keep=measure_date);

     * create a year and a year/month numeric variable *;
     if measure_date^=. then do;
        year=year(measure_date);
        year_month=(year(measure_date)*100)+month(measure_date);
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
     if _n_=1 then set minact;
     if _n_=1 then set maxact;
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * delete dummy records that are prior to the first actual year/month or 
       after the run-date or last actual year/month *;
     if year_month<minact or 
        (max(maxact,(year("&sysdate"d)*100)+month("&sysdate"d))<year_month<99999999) 
        then delete;

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then measure_date=put(year_month,null.);
     else measure_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);
    
     * apply threshold *;    
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=left(put(_freq_,threshold.));
    
     keep datamartid response_date query_package measure_date record_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, measure_date, record_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_VITAL_SOURCE
********************************************************************************;
%let qname=vit_l3_vital_source;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     vital_source=put(col1,$vitsrc.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_HT
********************************************************************************;
%let qname=vit_l3_ht;

proc format;
     value ht_dist
        low - <0="<0"
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
     if ht^=. then col1=ceil(ht);
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
     distinct_patid_n=left(put(distinct_patid,threshold.));

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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_HT_DIST
********************************************************************************;
%let qname=vit_l3_ht_dist;

*- Derive statistics -*;
proc means data=vital nway noprint;
     var ht;
     output out=stats min=s01 mean=s05 median=s06 max=s10 n=s11 nmiss=s12;
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
             var s01 s05 s06 s10 s11 s12;
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_WT
********************************************************************************;
%let qname=vit_l3_wt;

proc format;
     value wt_dist
       low - <0="<0"
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
     distinct_patid_n=left(put(distinct_patid,threshold.));

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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_WT_DIST
********************************************************************************;
%let qname=vit_l3_wt_dist;

*- Derive statistics -*;
proc means data=vital nway noprint;
     var wt;
     output out=stats min=s01 mean=s05 median=s06 max=s10 n=s11 nmiss=s12;
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
             var s01 s05 s06 s10 s11 s12;
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_DIASTOLIC
********************************************************************************;
%let qname=vit_l3_diastolic;

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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_SYSTOLIC
********************************************************************************;
%let qname=vit_l3_systolic;

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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_BMI
********************************************************************************;
%let qname=vit_l3_bmi;

proc format;
     value bmi_dist
       low - <0="<0"
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
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package bmi_group record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, bmi_group, record_n, 
            record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_BP_POSITION_TYPE
********************************************************************************;
%let qname=vit_l3_bp_position_type;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     bp_position=put(col1,$bppos.);
    
     * Apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.1);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_SMOKING
********************************************************************************;
%let qname=vit_l3_smoking;

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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     smoking=put(col1,$smoke.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package smoking record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, smoking, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_TOBACCO
********************************************************************************;
%let qname=vit_l3_tobacco;

proc format;
     value $tobac
         "01"="01"
         "02"="02"
         "03"="03"
         "04"="04"
         "05"="05"
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

     if tobacco in ("01" "02" "03" "04" "05" "06") then col1=tobacco;
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
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     tobacco=put(col1,$tobac.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
     keep datamartid response_date query_package tobacco record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, tobacco, record_n, record_pct
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_TOBACCO_TYPE
********************************************************************************;
%let qname=vit_l3_tobacco_type;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     set vital;

     if tobacco_type in ("01" "02" "03" "04" "05" "06") then col1=tobacco_type;
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
     format col1 $tobac.;
run;

*- Derive appropriate counts and variables -*;
data query;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     tobacco_type=put(col1,$tobac.);
    
     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=left(put(_freq_,threshold.));
     if _freq_>0 then record_pct=put((_freq_/denom)*100,6.2);
    
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

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* VIT_L3_DASH1
********************************************************************************;
%let qname=vit_l3_dash1;

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
     if max_admit>today() then max_admit=today();

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
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital demographic_birth_date encounter_admit_date encounter_discharge_date diagnosis_admit_date 
       procedures_admit_date procedures_px_date enrollment_enr_start_date enrollment_enr_end_date vital_measure_date);

********************************************************************************;
* XTBL_L3_DATES
********************************************************************************;
%let qname=xtbl_l3_dates;

%macro xminmax(idsn);
    
* append each variable into base dataset *;
proc append base=query data=&idsn;
run;

%mend xminmax;
%xminmax(idsn=demographic_birth_date)
%xminmax(idsn=encounter_admit_date)
%xminmax(idsn=encounter_discharge_date)
%xminmax(idsn=diagnosis_admit_date)
%xminmax(idsn=procedures_admit_date)
%xminmax(idsn=procedures_px_date)
%xminmax(idsn=vital_measure_date)
%xminmax(idsn=enrollment_enr_start_date)
%xminmax(idsn=enrollment_enr_end_date)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, min, max,
            future_dt_n, pre2010_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital);

******************************************************************************;
* XTBL_L3_METADATA
******************************************************************************;
%let qname=xtbl_l3_metadata;

*- Read data set created at top of program -*;
data access;
     length sas_base sas_graph sas_stat sas_ets sas_af sas_iml sas_connect
            sas_oracle sas_sql sas_mysql sas_postgres sas_teradata sas_odbc $3;
     set xtbl_mdata_idsn end=eof;

     retain sas_base sas_graph sas_stat sas_ets sas_af sas_iml sas_connect
            sas_oracle sas_sql sas_mysql sas_postgres sas_teradata sas_odbc "No";

     if product="---Base SAS Software" then sas_base="Yes";
     else if product="---SAS/GRAPH" then sas_graph="Yes";
     else if product="---SAS/STAT" then sas_stat="Yes";
     else if product="---SAS/ETS" then sas_ets="Yes";
     else if product="---SAS/AF" then sas_af="Yes";
     else if product="---SAS/IML" then sas_iml="Yes";
     else if product="---SAS/CONNECT" then sas_connect="Yes";
     else if product="---SAS/ACCESS Interface to Oracle" then sas_oracle="Yes";
     else if product="---SAS/ACCESS Interface to Microsoft SQL Server" then sas_sql="Yes";
     else if product="---SAS/ACCESS Interface to MySQL" then sas_mysql="Yes";
     else if product="---SAS/ACCESS to Postgres" then sas_postgres="Yes";
     else if product="---SAS/ACCESS Interface to Teradata" then sas_teradata="Yes";
     else if product="---SAS/ACCESS Interface to ODBC" then sas_odbc="Yes";
        
     if eof then output;
run;

*- Bring everything together -*;
data data;    
     length networkid network_name datamartid datamart_name query_package tag 
            datamart_platform cdm_version datamart_claims datamart_ehr
            birth_date_mgmt enr_start_date_mgmt enr_end_date_mgmt 
            admit_date_mgmt discharge_date_mgmt px_date_mgmt rx_order_date_mgmt 
            rx_start_date_mgmt rx_end_date_mgmt dispense_date_mgmt 
            lab_order_date_mgmt specimen_date_mgmt result_date_mgmt 
            measure_date_mgmt onset_date_mgmt report_date_mgmt resolve_date_mgmt
            pro_date_mgmt $30;
     if _n_=1 then set access;
     if _n_=1 then set sashelp.vmacro(keep=name value where=(name="SYSVER") 
                                        rename=(value=vsas));
     if _n_=1 then set sashelp.vmacro(keep=name value where=(name="SYSSCP") 
                                        rename=(value=vsops));
     set pcordata.harvest(rename=(cdm_version=cdm_version_num 
                          refresh_demographic_date=refresh_demographic_daten 
                          refresh_enrollment_date=refresh_enrollment_daten 
                          refresh_encounter_date=refresh_encounter_daten
                          refresh_diagnosis_date=refresh_diagnosis_daten
                          refresh_procedures_date=refresh_procedures_daten 
                          refresh_vital_date=refresh_vital_daten
                          refresh_dispensing_date=refresh_dispensing_daten 
                          refresh_lab_result_cm_date=refresh_lab_result_cm_daten 
                          refresh_condition_date=refresh_condition_daten
                          refresh_pro_cm_date=refresh_pro_cm_daten 
                          refresh_prescribing_date=refresh_prescribing_daten 
                          refresh_pcornet_trial_date=refresh_pcornet_trial_daten
                          refresh_death_date=refresh_death_daten 
                          refresh_death_cause_date=refresh_death_cause_daten));

     * table variables *;
     query_package="DC V3.00";
     response_date=put("&sysdate"d,yymmdd10.);
     tag=strip(query_package)||"_"||strip(datamartid)||"_"||strip(response_date);

     if datamart_platform not in ("01" "02" "03" "04" "05" "NI" "OT" "UN" " ")
        then datamart_platform="Values outside of CDM specifications";
     
     cdm_version=left(put(cdm_version_num,5.1));

     if datamart_claims not in ("01" "02" "NI" "OT" "UN" " ")
        then datamart_claims="Values outside of CDM specifications";

     if datamart_ehr not in ("01" "02" "NI" "OT" "UN" " ")
        then datamart_ehr="Values outside of CDM specifications";

     array dm birth_date_mgmt enr_start_date_mgmt enr_end_date_mgmt 
              admit_date_mgmt discharge_date_mgmt px_date_mgmt rx_order_date_mgmt
              rx_start_date_mgmt rx_end_date_mgmt dispense_date_mgmt
              lab_order_date_mgmt specimen_date_mgmt result_date_mgmt
              measure_date_mgmt onset_date_mgmt report_date_mgmt
              resolve_date_mgmt pro_date_mgmt;
        do dm1 = 1 to dim(dm);
           if dm{dm1} in ("01" "02" "03" "NI" "OT" "UN" " ")
             then dm{dm1}=dm{dm1};
           else dm{dm1}="Values outside of CDM specifications";
        end;

     array rfc $10 refresh_demographic_date refresh_enrollment_date 
                   refresh_encounter_date refresh_diagnosis_date 
                   refresh_procedures_date refresh_vital_date
                   refresh_dispensing_date refresh_lab_result_cm_date 
                   refresh_condition_date refresh_pro_cm_date 
                   refresh_prescribing_date refresh_pcornet_trial_date
                   refresh_death_date refresh_death_cause_date;
     array rfn refresh_demographic_daten refresh_enrollment_daten 
               refresh_encounter_daten refresh_diagnosis_daten 
               refresh_procedures_daten refresh_vital_daten 
               refresh_dispensing_daten refresh_lab_result_cm_daten 
               refresh_condition_daten refresh_pro_cm_daten 
               refresh_prescribing_daten refresh_pcornet_trial_daten
               refresh_death_daten refresh_death_cause_daten;
        do rf1 = 1 to dim(rfc);
           if rfn{rf1}^=. then rfc{rf1}=put(rfn{rf1},yymmdd10.);
           else if rfn{rf1}=. then rfc{rf1}=" ";
        end;

     refresh_max=put(max(refresh_demographic_daten, refresh_enrollment_daten, 
                         refresh_encounter_daten, refresh_diagnosis_daten, 
                         refresh_procedures_daten, refresh_vital_daten,
                         refresh_dispensing_daten, refresh_lab_result_cm_daten,
                         refresh_condition_daten, refresh_pro_cm_daten, 
                         refresh_prescribing_daten, refresh_pcornet_trial_daten,
                         refresh_death_daten, refresh_death_cause_daten),yymmdd10.);

     low_cell_cnt=compress(put(&threshold,16.));

     array v networkid network_name datamartid datamart_name datamart_platform
             datamart_claims datamart_ehr birth_date_mgmt enr_start_date_mgmt 
             enr_end_date_mgmt admit_date_mgmt discharge_date_mgmt px_date_mgmt
             rx_order_date_mgmt rx_start_date_mgmt rx_end_date_mgmt 
             dispense_date_mgmt lab_order_date_mgmt specimen_date_mgmt 
             result_date_mgmt measure_date_mgmt onset_date_mgmt report_date_mgmt
             resolve_date_mgmt pro_date_mgmt;
        do v1 = 1 to dim(v);
           if v{v1}^=' ' then v{v1}=v{v1};
           else if v{v1}=' ' then v{v1}="NULL or missing";
        end;

     if vsops^=" " then operating_system=strip(vsops);
     if vsas^=" " then sas_version=strip(vsas);
run;

*- Normalize data -*;
proc transpose data=data out=query(rename=(_name_=name col1=value));
     var networkid network_name datamartid datamart_name datamart_platform
         cdm_version datamart_claims datamart_ehr birth_date_mgmt 
         enr_start_date_mgmt enr_end_date_mgmt admit_date_mgmt 
         discharge_date_mgmt px_date_mgmt rx_order_date_mgmt rx_start_date_mgmt
         rx_end_date_mgmt dispense_date_mgmt lab_order_date_mgmt 
         specimen_date_mgmt result_date_mgmt measure_date_mgmt onset_date_mgmt
         report_date_mgmt resolve_date_mgmt pro_date_mgmt 
         refresh_demographic_date refresh_enrollment_date refresh_encounter_date
         refresh_diagnosis_date refresh_procedures_date refresh_vital_date
         refresh_dispensing_date refresh_lab_result_cm_date 
         refresh_condition_date refresh_pro_cm_date refresh_prescribing_date
         refresh_pcornet_trial_date refresh_death_date refresh_death_cause_date
         refresh_max low_cell_cnt operating_system query_package response_date 
         sas_version sas_base sas_graph sas_stat sas_ets sas_af sas_iml 
         sas_connect sas_mysql sas_odbc sas_oracle sas_postgres sas_sql
         sas_teradata;
run;

*- Assign attributes -*;
data query;
     set query;

     attrib name label="NAME"
            value label="VALUE"
     ;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         name, value
     from query;
quit;

*- Print query and clear working directory -*;
%clean(savedsn=vital);

********************************************************************************;
* XTBL_L3_DASH1
********************************************************************************;
%let qname=xtbl_l3_dash1;

*- Data with legitimate date -*;
data xdiagnosis(compress=yes);
     length year 4.;
     set pcordata.diagnosis(keep=admit_date patid);

     if admit_date^=. then year=year(admit_date);

     keep patid year;
run;

*- Uniqueness per PATID/year -*;
proc sort data=xdiagnosis nodupkey;
     by patid year;
     where year^=.;
run;

*- Data with legitimate date -*;
data xvital;
     length year 4.;
     set vital(keep=measure_date patid);

     if measure_date^=. then year=year(measure_date);

     keep patid year;
run;

*- Uniqueness per PATID/year -*;
proc sort data=xvital nodupkey;
     by patid year;
     where year^=.;
run;

*- Merge diagnosis and vital data -*;
data data;
     merge xdiagnosis(in=e) xvital(in=v);
     by patid year;
     if e and v;
run;

*- Determine maximum year -*;
proc means data=data noprint;
     var year;
     output out=max_year max=max_year;
run;

*- Assign a period to every record based upon maximum date -*;
data period(keep=period patid);
     length period 3.;
     if _n_=1 then set max_year;
     set data;

     * eliminate future dates from consideration and use run date *;
     if max_admit>today() then max_admit=today();

     * slot each record in appropriate window(s) *;
     period=99;
     output;
     if max_year-5<=year<=max_year then do;
        period=5;
        output;
        if max_year-4<=year<=max_year then do;
           period=4;
           output;
           if max_year-3<=year<=max_year then do;
              period=3;
              output;
              if max_year-2<=year<=max_year then do;
                 period=2;
                 output;
                 if max_year-1<=year<=max_year then do;
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
     set stats(rename=(period=periodn));
     by periodn;

     * call standard variables *;
     %stdvar

     * table values *;
     period=put(periodn,period.);

     * apply threshold *;
     %threshold(nullval=periodn^=99)

     * counts *;
     distinct_patid_n=left(put(_freq_,threshold.));

     keep datamartid response_date query_package period distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, period, distinct_patid_n
     from query;
quit;

*- Print query and clear working directory -*;
%clean;

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

********************************************************************************;
* Create a SAS transport file from all of the query datasets
********************************************************************************;
filename tranfile "&qpath.drnoc/&dmid._&tday._data_characterization.cpt";
proc cport library=dmlocal file=tranfile memtype=data;
run;        

*******************************************************************************;
* Print each data set and send to a PDF file
*******************************************************************************;
%prnt(pdsn=dem_l3_n,first=1);
%prnt(pdsn=dem_l3_ageyrsdist1);
%prnt(pdsn=dem_l3_ageyrsdist2);
%prnt(pdsn=dem_l3_hispdist);
%prnt(pdsn=dem_l3_racedist);
%prnt(pdsn=dem_l3_sexdist);
%prnt(pdsn=enc_l3_n);
%prnt(pdsn=enc_l3_n_visit);
%prnt(pdsn=enc_l3_admsrc);
%prnt(pdsn=enc_l3_enctype_admrsc,_obs=100,_svar=enc_type admitting_source);
%prnt(pdsn=enc_l3_adate_y);
%prnt(pdsn=enc_l3_adate_ym,_obs=100,_svar=admit_date);
%prnt(pdsn=enc_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date);
%prnt(pdsn=enc_l3_ddate_y);
%prnt(pdsn=enc_l3_ddate_ym,_obs=100,_svar=discharge_date);
%prnt(pdsn=enc_l3_enctype_ddate_ym,_obs=100,_svar=enc_type discharge_date);
%prnt(pdsn=enc_l3_disdisp);
%prnt(pdsn=enc_l3_enctype_disdisp);
%prnt(pdsn=enc_l3_disstat);
%prnt(pdsn=enc_l3_enctype_disstat,_obs=100,_svar=enc_type discharge_status);
%prnt(pdsn=enc_l3_drg,_obs=100,_svar=drg,_pct=record_pct);
%prnt(pdsn=enc_l3_drg_type);
%prnt(pdsn=enc_l3_enctype_drg,_obs=100,_svar=enc_type drg,_pct=record_pct);
%prnt(pdsn=enc_l3_enctype);
%prnt(pdsn=enc_l3_dash1);
%prnt(pdsn=enc_l3_dash2);
%prnt(pdsn=dia_l3_n);
%prnt(pdsn=dia_l3_dx,_obs=100,_svar=dx,_pct=record_pct);
%prnt(pdsn=dia_l3_dx_dxtype,_obs=100,_svar=dx dx_type,_pct=distinct_patid_n);
%prnt(pdsn=dia_l3_dxsource);
%prnt(pdsn=dia_l3_dxtype_dxsource);
%prnt(pdsn=dia_l3_pdx);
%prnt(pdsn=dia_l3_pdx_enctype);
%prnt(pdsn=dia_l3_adate_y);
%prnt(pdsn=dia_l3_adate_ym,_obs=100,_svar=admit_date);
%prnt(pdsn=dia_l3_enctype);
%prnt(pdsn=dia_l3_dxtype_enctype);
%prnt(pdsn=dia_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_pct=distinct_encid_n);
%prnt(pdsn=dia_l3_dash1);
%prnt(pdsn=pro_l3_n);
%prnt(pdsn=pro_l3_px,_obs=100,_svar=px,_pct=record_pct);
%prnt(pdsn=pro_l3_adate_y);
%prnt(pdsn=pro_l3_adate_ym,_obs=100,_svar=admit_date);
%prnt(pdsn=pro_l3_pxdate_y);
%prnt(pdsn=pro_l3_px_enctype,_obs=100,_svar=px enc_type);
%prnt(pdsn=pro_l3_enctype);
%prnt(pdsn=pro_l3_pxtype_enctype,_obs=100,_svar=px_type enc_type);
%prnt(pdsn=pro_l3_enctype_adate_ym,_obs=100,_svar=admit_date,_pct=distinct_encid_n);
%prnt(pdsn=pro_l3_px_pxtype,_obs=100,_svar=px px_type,_pct=distinct_patid_n);
%prnt(pdsn=pro_l3_pxsource)
%prnt(pdsn=enr_l3_n);
%prnt(pdsn=enr_l3_dist_start);
%prnt(pdsn=enr_l3_dist_end);
%prnt(pdsn=enr_l3_dist_enrmonth,_obs=100,_svar=enroll_m);
%prnt(pdsn=enr_l3_dist_enryear);
%prnt(pdsn=enr_l3_enr_ym,_obs=100,_svar=month);
%prnt(pdsn=enr_l3_basedist)
%prnt(pdsn=enr_l3_per_patid)
%prnt(pdsn=vit_l3_n);
%prnt(pdsn=vit_l3_mdate_y);
%prnt(pdsn=vit_l3_mdate_ym,_obs=100,_svar=measure_date);
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
%prnt(pdsn=xtbl_l3_dates);
%prnt(pdsn=xtbl_l3_metadata,dropvar=);
%prnt(pdsn=xtbl_l3_dash1);

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;
