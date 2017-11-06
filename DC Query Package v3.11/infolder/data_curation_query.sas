/*******************************************************************************
*  $Source: data_curation_query $;
*    $Date: 2017/09/13
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V3.11
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query.pdf)
*           3) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>.cpt)
*           4) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query.log)
*           5) SAS dataset of DataMart meta-data stored in /dmlocal
*                (datamart_all.sas7bdat)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/
options validvarname=upcase errors=0 nomprint;
ods html close;

********************************************************************************;
*- Flush anything/everything that might be in WORK
*******************************************************************************;
proc datasets  kill noprint;
quit;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname pcordata "&dpath" access=readonly;
libname drnoc "&qpath.drnoc";
libname dmlocal "&qpath.dmlocal";
filename labloinc "&qpath./infolder/lab_loinc_ref.cpt";
filename cui_ref "&qpath./infolder/rxnorm_cui_ref.cpt";
libname cptdata "&qpath.infolder";

********************************************************************************;
*- Set version number
*******************************************************************************;
%let dc = DC V3.10;

********************************************************************************;
* Create macro variable from DataMart ID and program run date 
********************************************************************************;
data _null_;
     set pcordata.harvest;
     refresh_max=put(max(refresh_demographic_date, refresh_enrollment_date,
                         refresh_encounter_date, refresh_diagnosis_date,
                         refresh_procedures_date, refresh_vital_date,
                         refresh_dispensing_date, refresh_lab_result_cm_date,
                         refresh_condition_date, refresh_pro_cm_date, 
                         refresh_prescribing_date, refresh_pcornet_trial_date,
                         refresh_death_date, refresh_death_cause_date),yymmddn8.);
     call symput("mxrefresh",strip(refresh_max));
     call symput("dmid",strip(datamartid));
     call symput("tday",left(put("&sysdate"d,yymmddn8.)));
     call symput("_pstart",datetime());
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
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query.log" lrecl=200;
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
     query_package="&dc";
%mend stdvar;

*- Macros to apply threshold (low cell count) to applicable queries -*;
%macro threshold(tdsn=stats,type=1,qvar=col1,nullval=,_tvar=_freq_);
    if _type_>=&type and 0<&_tvar<&threshold and &nullval then &_tvar=.t;
%mend threshold;

*- Macros to determine minimum/maximum date -*;
%macro minmax(idsn,var,var_tm);

     data &idsn.&var;
          length futureflag preflag 3.;
          format time time5.;
          set &idsn;

          * create a time variable, which will be missing if time is not needed *;
          time=&var_tm;

          * flag for future date records *;
          if &var^=. and &var>input("&mxrefresh",yymmdd8.) then futureflag=1;

          * flag for records pre-01JAN2010 *;
          if .<&var<'01JAN2010'd then preflag=1;
        
          keep &var time futureflag preflag;
     run;

     proc means data=&idsn.&var nway noprint;
          var &var time futureflag preflag;
          output out=m&idsn n=ndt ntm dum1 dum2  sum=dum3 dum4 futuresum presum
                 min=mindt mintm dum5 dum6  max=maxdt maxtm dum7 dum8
                 p5=p5dt p5tm dum9 dum10  p95=p95dt p95tm dum11 dum12
                 mean=meandt meantm dum13 dum14  median=mediandt mediantm dum15 dum16
                 nmiss=nmissdt nmisstm dum17 dum18;
     run;

     *- Derive appropriate counts -*;
     data &idsn._&var;
          length dataset tag tag_tm $25;
          set m&idsn;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset=upcase("&idsn");
          tag=upcase("&var");
          tag_tm=upcase("&var_tm");

          * counts *;
          min=put(year(mindt),4.)||"_"||put(month(mindt),z2.);
          max=put(year(maxdt),4.)||"_"||put(month(maxdt),z2.);
          mean=put(year(meandt),4.)||"_"||put(month(meandt),z2.);
          median=put(year(mediandt),4.)||"_"||put(month(mediandt),z2.);
          p5=put(year(p5dt),4.)||"_"||put(month(p5dt),z2.);
          p95=put(year(p95dt),4.)||"_"||put(month(p95dt),z2.);
          n=strip(put(ndt,threshold.));
          nmiss=strip(put(nmissdt,threshold.));

          if ndt>=&threshold then do;
              if futuresum>. then future_dt_n=strip(put(futuresum,threshold.));
              else future_dt_n=strip(put(0,16.));
              if presum>. then pre2010_n=strip(put(presum,threshold.));
              else pre2010_n=strip(put(0,16.));
          end;
          else if ndt<&threshold then do;
            future_dt_n=strip(put(.t,threshold.));
            pre2010_n=strip(put(.t,threshold.));
          end;

          min_tm=put(mintm,time5.);
          max_tm=put(maxtm,time5.);
          mean_tm=put(meantm,time5.);
          median_tm=put(mediantm,time5.);
          p5_tm=put(p5tm,time5.);
          p95_tm=put(p95tm,time5.);
          n_tm=strip(put(ntm,threshold.));
          nmiss_tm=strip(put(nmisstm,threshold.));

          keep datamartid response_date query_package dataset tag tag_tm 
               min max p5 p95 mean median n nmiss future_dt_n pre2010_n min_tm 
               max_tm p5_tm p95_tm mean_tm median_tm n_tm nmiss_tm;
     run;
%mend minmax;

*- Macro for each variable in _N query -*;
%macro enc_oneway(encdsn=,encvar=,_nc=0);

     proc sort data=&encdsn(keep=&encvar)
          out=&encvar._var;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data &encvar._1way;
          length all_n distinct_n null_n $20 tag dataset $35;
          set &encvar._var end=eof;
          by &encvar;

          * call standard variables *;
          %stdvar

          * table values *;
          dataset=upcase("&encdsn");
          tag=upcase("&encvar");

          * counts *;
          retain _all_n _distinct_n _null_n 0;
          %if &_nc=0 %then %do;
              if &encvar=" " then _null_n=_null_n+1;
          %end;
          %else %if &_nc=1 %then %do;
              if &encvar=. then _null_n=_null_n+1;
          %end;
          else do;
            _all_n=_all_n+1;
            if first.&encvar then _distinct_n=_distinct_n+1;
          end;

         * output *;
         if eof then do;
            * set to special missing if below threshold *;
            if 0<_all_n<&threshold then _all_n=.t;
            if 0<_distinct_n<&threshold then _distinct_n=.t;
            if 0<_null_n<&threshold then _null_n=.t;
            
            * convert to character with threshold format *;
            all_n=strip(put(_all_n,threshold.));
            distinct_n=strip(put(_distinct_n,threshold.));
            null_n=strip(put(_null_n,threshold.));

            output;
         end;

         keep datamartid response_date query_package dataset tag all_n
              distinct_n null_n;
     run;

     * append each variable into base dataset *;
     proc append base=query data=&encvar._1way;
     run;

%mend enc_oneway;

*- Macro to print each query result -*;
%macro prnt(pdsn=,_obs=max,dropvar=datamartid response_date query_package,_svar=,_suppvar=,_recordn=record_n);

     * if not printing entire dataset, sort so that top frequencies are printed *;
     %if &_obs^=max %then %do;
        data &pdsn;
             set dmlocal.&pdsn(rename=(&_recordn=record_nc));

             if strip(record_nc) in ("0" "BT" "T") then delete;
             else &_recordn=input(record_nc,best.);
             drop record_nc;
        run;

        proc sort data=&pdsn;
            by descending &_recordn &_svar;
        run;
     %end;
    
     %if &_obs=max %then %do;
         title "Query name:  %upcase(&pdsn)";
     %end;
     %else %do;
         title "Query name:  %upcase(&pdsn) (Limited to most frequent &_obs above threshold observations)";
     %end;
     %if &_obs^=max %then %do;
         proc print width=min data=&pdsn(drop=&dropvar obs=&_obs);
              var &_svar &_suppvar;
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
          save xtbl_mdata_idsn elapsed &savedsn / memtype=data;
          save formats / memtype=catalog;
     quit;
%mend clean;

*- Macro to track query processing time -*;
data elapsed;
     length query $100;
     format _qstart _qend datetime19. elapsedtime totalruntime time8.;
     array _char_ query ;
     array _num_  _qstart _qend elapsedtime totalruntime;
     query="DC PROGRAM";
     _qstart=&_pstart;
     output;
run;
    
%macro elapsed(b_or_e);

    %if &b_or_e=begin %then %do;
        data _qe;
            length query $100;
            format _qstart datetime19.;
            query="%upcase(&qname)";
            _qstart=datetime();
            output;
            put "***********************************************************************************************";
            put "Beginning query: %upcase(&qname) on " _qstart;
            put "***********************************************************************************************";
        run;
    %end;
    %else %if &b_or_e=end %then %do;
        data _qe(drop=_elapsedtime _totalruntime);
            length query $100;
            format elapsedtime totalruntime time8.;
            set _qe;
                _qend=datetime();
                elapsedtime=_qend-_qstart;
                _elapsedtime=put(elapsedtime,time8.);
                totalruntime=_qend-&_pstart;
                _totalruntime=put(totalruntime,time8.);
                 put "******************************************************************************************************";
                 put "End query:  %upcase(&qname)  on " _qstart "elapsed time (hh:mm:ss): " _elapsedtime " total run time (hh:mm:ss): " _totalruntime;
                 put "******************************************************************************************************";
        run;
    
        proc append base=elapsed data=_qe;
        run;
    %end;

%mend elapsed;

********************************************************************************;
* Create working formats used in multiple queries
********************************************************************************;
proc format;
     value $enc_typ
         "AV"="AV"
         "ED"="ED"
         "EI"="EI"
         "IC"="IC"
         "IP"="IP"
         "IS"="IS"
         "OA"="OA"
         "OS"="OS"
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
        0< - &threshold="BT"
        99999999="99999999"
        other = [16.0]
        ;
run;

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc;

%global savedeath savedemog saveenc savediag saveproc saveenr savevit savepres 
        savedisp savelab savecond saveprocm savedeathc;

********************************************************************************;
* Determine which datasets are available
********************************************************************************;
proc contents data=pcordata._all_ noprint out=pcordata;
run;

proc sort data=pcordata(keep=memname) nodupkey;
     by memname;
run;

proc sql noprint;
     select count(*) into :_ycondition from pcordata
            where memname="CONDITION";
     select count(*) into :_ydeath from pcordata
            where memname="DEATH";
     select count(*) into :_ydeathc from pcordata
            where memname="DEATH_CAUSE";
     select count(*) into :_ydemographic from pcordata
            where memname="DEMOGRAPHIC";
     select count(*) into :_ydiagnosis from pcordata
            where memname="DIAGNOSIS";
     select count(*) into :_ydispensing from pcordata
            where memname="DISPENSING";
     select count(*) into :_yencounter from pcordata
            where memname="ENCOUNTER";
     select count(*) into :_yenrollment from pcordata
            where memname="ENROLLMENT";
     select count(*) into :_yharvest from pcordata
            where memname="HARVEST";
     select count(*) into :_ylab_result_cm from pcordata
            where memname="LAB_RESULT_CM";
     select count(*) into :_yprescribing from pcordata
            where memname="PRESCRIBING";
     select count(*) into :_yprocedures from pcordata
            where memname="PROCEDURES";
     select count(*) into :_ypro_cm from pcordata
            where memname="PRO_CM";
     select count(*) into :_yvital from pcordata
            where memname="VITAL";
     select count(*) into :_yptrial from pcordata
            where memname="PCORNET_TRIAL";
quit;

********************************************************************************;
* Bring in DEATH and compress it
********************************************************************************;
%if &_ydeath=1 %then %do;

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
     deathid=cats(patid,'_',dthdate,'_',death_source);
run;

proc sort data=death;
     by patid death_date;
run;

********************************************************************************;
* Minimum/maximum death for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=death,var=death_date,var_tm=.)

%let savedeath=death death_death_date;


********************************************************************************;
* DEATH_L3_N
********************************************************************************;
%let qname=death_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=death,encvar=patid)
%enc_oneway(encdsn=death,encvar=deathid)

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
          "F"="F"
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

     if upcase(death_date_impute) in ("B" "D" "F" "M" "N") then col1=death_date_impute;
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

********************************************************************************;
* DEATHC_L3_N
********************************************************************************;
%let qname=deathc_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=death_cause,encvar=patid)
%enc_oneway(encdsn=death_cause,encvar=death_cause)
%enc_oneway(encdsn=death_cause,encvar=deathcid)

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

data demographic(compress=yes);
     set pcordata.demographic;
run;

proc sort data=demographic;
     by patid;
run;

********************************************************************************;
* Minimum/maximum demographic for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=demographic,var=birth_date,var_tm=birth_time)

%let savedemog=demographic demographic_birth_date;

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
%elapsed(begin);

%let qname=dem_l3_ageyrsdist1;

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
%clean(savedsn=&savedeath &savedemog);

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
%clean(savedsn=&savedeath &savedemog);

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
%clean(savedsn=&savedeath &savedemog);

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
%clean(savedsn=&savedeath &savedemog);

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
%clean(savedsn=&savedeath &savedemog);

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
%clean(savedsn=&savedeath &savedemog);

%end;

********************************************************************************;
* Bring in ENCOUNTER and compress it
********************************************************************************;
%if &_yencounter=1 %then %do;

%global _providlength;

data encounter(compress=yes);
     set pcordata.encounter end=eof;
run;

********************************************************************************;
* Minimum/maximum encounter for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=encounter,var=admit_date,var_tm=admit_time)
%minmax(idsn=encounter,var=discharge_date,var_tm=discharge_time)

%let saveenc=encounter encounter_admit_date encounter_discharge_date;
    
********************************************************************************;
* ENC_L3_N
********************************************************************************;
%let qname=enc_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=encounter,encvar=encounterid)
%enc_oneway(encdsn=encounter,encvar=patid)
%enc_oneway(encdsn=encounter,encvar=providerid)
%enc_oneway(encdsn=encounter,encvar=facilityid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

********************************************************************************;
* ENC_L3_DASH2
********************************************************************************;
%let qname=enc_l3_dash2;
%elapsed(begin);

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
%clean(savedsn=&savedeath &savedemog &saveenc);

%end;

********************************************************************************;
* Bring in DIAGNOSIS and compress it
********************************************************************************;
%if &_ydiagnosis=1 %then %do;

data diagnosis(compress=yes);
     set pcordata.diagnosis;
run;

********************************************************************************;
* Minimum/maximum diagnosis for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=diagnosis,var=admit_date,var_tm=.)

%let savediag=diagnosis diagnosis_admit_date;

********************************************************************************;
* DIA_L3_N
********************************************************************************;
%let qname=dia_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=diagnosis,encvar=encounterid)
%enc_oneway(encdsn=diagnosis,encvar=patid)
%enc_oneway(encdsn=diagnosis,encvar=diagnosisid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
     dx=put(col1,$null.);
    
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
     dx=put(col1,$null.);    
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

********************************************************************************;
* DIA_L3_PDX
********************************************************************************;
%let qname=dia_l3_pdx;
%elapsed(begin);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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

     if dx_origin in ("OD" "BI" "CL") then col1=dx_origin;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag);

%end;

********************************************************************************;
* Bring in PROCEDURES and compress it
********************************************************************************;
%if &_yprocedures=1 %then %do;

data procedures(compress=yes);
     set pcordata.procedures;
run;

********************************************************************************;
* Minimum/maximum procedures for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=procedures,var=admit_date,var_tm=.)
%minmax(idsn=procedures,var=px_date,var_tm=.)

%let saveproc=procedures procedures_admit_date procedures_px_date;

********************************************************************************;
* PRO_L3_N
********************************************************************************;
%let qname=pro_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=procedures,encvar=encounterid)
%enc_oneway(encdsn=procedures,encvar=patid)
%enc_oneway(encdsn=procedures,encvar=proceduresid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

********************************************************************************;
* PRO_L3_PXSOURCE
********************************************************************************;
%let qname=pro_l3_pxsource;
%elapsed(begin);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc);

%end;

********************************************************************************;
* Bring in ENROLLMENT and compress it
********************************************************************************;
%if &_yenrollment=1 %then %do;

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

     enrstartdate=put(enr_start_date,date9.);
     enrollid=cats(patid,'_',enrstartdate,'_',enr_basis);
run;

********************************************************************************;
* Minimum/maximum enrollment for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=enrollment,var=enr_start_date,var_tm=.)
%minmax(idsn=enrollment,var=enr_end_date,var_tm=.)

%let saveenr=enrollment enrollment_enr_start_date enrollment_enr_end_date;

********************************************************************************;
* ENR_L3_N
********************************************************************************;
%let qname=enr_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=enrollment,encvar=patid)
%enc_oneway(encdsn=enrollment,encvar=enr_start_date,_nc=1)
%enc_oneway(encdsn=enrollment,encvar=enrollid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr);

%end;

********************************************************************************;
* Bring in VITAL and compress it
********************************************************************************;
%if &_yvital=1 %then %do;

data vital(compress=yes);
     set pcordata.vital;
run;

********************************************************************************;
* Minimum/maximum vital for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=vital,var=measure_date,var_tm=measure_time)

%let savevit=vital vital_measure_date;

********************************************************************************;
* VIT_L3_N
********************************************************************************;
%let qname=vit_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=vital,encvar=encounterid)
%enc_oneway(encdsn=vital,encvar=patid)
%enc_oneway(encdsn=vital,encvar=vitalid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

********************************************************************************;
* VIT_L3_TOBACCO_TYPE
********************************************************************************;
%let qname=vit_l3_tobacco_type;
%elapsed(begin);

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
     length record_n $20;
     if _n_=1 then set stats(where=(_type_=0) rename=(_freq_=denom));
     set stats(where=(_type_=1));

     * call standard variables *;
     %stdvar

     * table values *;
     tobacco_type=put(col1,$tobac.);
    
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit);

%end;

********************************************************************************;
* Bring in DISPENSING and compress it
********************************************************************************;
%if &_ydispensing=1 %then %do;

data dispensing(compress=yes);
     set pcordata.dispensing;
     if notdigit(ndc)=0 and length(ndc)=11 then valid_ndc="Y";
     else valid_ndc=" ";
run;

********************************************************************************;
* Minimum/maximum dispensing for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=dispensing,var=dispense_date,var_tm=.)

%let savedisp=dispensing dispensing_dispense_date;

********************************************************************************;
* DISP_L3_N
********************************************************************************;
%let qname=disp_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=dispensing,encvar=patid)
%enc_oneway(encdsn=dispensing,encvar=dispensingid)
%enc_oneway(encdsn=dispensing,encvar=prescribingid)
%enc_oneway(encdsn=dispensing,encvar=ndc)
%enc_oneway(encdsn=dispensing,encvar=valid_ndc)

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp);

%end;

********************************************************************************;
* Bring in PRESRIBING and compress it
********************************************************************************;
%if &_yprescribing=1 %then %do;

data prescribing(compress=yes);
     set pcordata.prescribing;
     rxnorm_cui=strip(upcase(rxnorm_cui));
run;

********************************************************************************;
* Minimum/maximum prescribing for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=prescribing,var=rx_order_date,var_tm=rx_order_time)
%minmax(idsn=prescribing,var=rx_start_date,var_tm=.)
%minmax(idsn=prescribing,var=rx_end_date,var_tm=.)

%let savepres=prescribing prescribing_rx_order_date prescribing_rx_start_date 
              prescribing_rx_end_date;

********************************************************************************;
* PRES_L3_N
********************************************************************************;
%let qname=pres_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=prescribing,encvar=patid)
%enc_oneway(encdsn=prescribing,encvar=prescribingid)
%enc_oneway(encdsn=prescribing,encvar=encounterid)
%enc_oneway(encdsn=prescribing,encvar=rx_providerid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

********************************************************************************;
* PRES_L3_RXCUI
********************************************************************************;
%let qname=pres_l3_rxcui;
%elapsed(begin);

*- Import transport file of RXNORM CUI reference -*;
proc cimport library=cptdata infile=cui_ref;
run;

*- Sort prescribing and reference data for merging -*;
proc sort data=cptdata.rxnorm_cui_ref out=rxnorm_cui_ref;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres rxnorm_cui rxnorm_cui_ref);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

********************************************************************************;
* PRES_L3_QTYUNIT
********************************************************************************;
%let qname=pres_l3_qtyunit;
%elapsed(begin);

proc format;
     value $qunit
       "PI"="PI"
       "TA"="TA"
       "VI"="VI"
       "LI"="LI"
       "SO"="SO"
       "SU"="SU"
       "OI"="OI"
       "CR"="CR"
       "PO"="PO"
       "PA"="PA"
       "IN"="IN"
       "KI"="KI"
       "DE"="DE"
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
     set prescribing(keep=rx_quantity_unit) end=eof;

     if rx_quantity_unit in ("PI" "TA" "VI" "LI" "SO" "SU" "OI" "CR" "PO" "PA" "IN" "KI" "DE") then col1=rx_quantity_unit;
     else if rx_quantity_unit in ("NI") then col1="ZZZA";
     else if rx_quantity_unit in ("UN") then col1="ZZZB";
     else if rx_quantity_unit in ("OT") then col1="ZZZC";
     else if rx_quantity_unit=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $qunit.;
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
     rx_quantity_unit=put(col1,$qunit.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package rx_quantity_unit record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, rx_quantity_unit, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

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
     set prescribing(keep=rx_basis) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

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
       "09"="09"
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
     set prescribing(keep=rx_frequency) end=eof;

     if rx_frequency in ("01" "02" "03" "04" "05" "06" "07" "08" "09") then col1=rx_frequency;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savepres &savedisp);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr 
               &savevit &savedisp &savepres);

%end;

********************************************************************************;
* Bring in LAB_RESULT_CM and compress it
********************************************************************************;
%if &_ylab_result_cm=1 %then %do;

data lab_result_cm(compress=yes);
     set pcordata.lab_result_cm;

     lab_loinc=strip(upcase(lab_loinc));

     * known test result *;
     if (lab_name in ('A1C' 'CK' 'CK_MB' 'CK_MBI' 'CREATININE' 'HGB' 'LDL' 
            'INR' 'TROP_I' 'TROP_T_QL' 'TROP_T_QN') or lab_loinc^=" ") then do;
        known_test=1;

        if result_num^=. then known_test_result_num=1;
        else known_test_result_num=.;
        
        if (result_num^=. or result_qual in ("BORDERLINE" "POSITIVE" "NEGATIVE" "UNDETERMINED"))
           then known_test_result=1;
        else known_test_result=.;
     end;
     else do;
        known_test=.;
        known_test_result=.;
        known_test_result_num=.;
     end;
    
     * known test result range *;
     if known_test_result_num=1 and
        ((norm_modifier_low='EQ' and norm_modifier_high='EQ' and norm_range_low^=' ' and norm_range_high^=' ') or
        (norm_modifier_low in ('GT' 'GE')and norm_modifier_high='NO' and norm_range_low^=' ' and norm_range_high=' ') or
        (norm_modifier_high in ('LE' 'LT')and norm_modifier_low='NO' and norm_range_low=' ' and norm_range_high^=' ')) then
        known_test_result_num_range=1;
     else known_test_result_num_range=.;
run;

********************************************************************************;
* Minimum/maximum lab for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=lab_result_cm,var=lab_order_date,var_tm=.)
%minmax(idsn=lab_result_cm,var=specimen_date,var_tm=specimen_time)
%minmax(idsn=lab_result_cm,var=result_date,var_tm=result_time)

%let savelab=lab_result_cm lab_result_cm_lab_order_date lab_result_cm_specimen_date 
             lab_result_cm_result_date;

********************************************************************************;
* LAB_L3_N
********************************************************************************;
%let qname=lab_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=lab_result_cm,encvar=patid)
%enc_oneway(encdsn=lab_result_cm,encvar=lab_result_cm_id)
%enc_oneway(encdsn=lab_result_cm,encvar=encounterid)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_RECORDC
********************************************************************************;
%let qname=lab_l3_recordc;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=lab_result_cm,encvar=known_test,_nc=1)
%enc_oneway(encdsn=lab_result_cm,encvar=known_test_result,_nc=1)
%enc_oneway(encdsn=lab_result_cm,encvar=known_test_result_num,_nc=1)
%enc_oneway(encdsn=lab_result_cm,encvar=known_test_result_num_range,_nc=1)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_NAME
********************************************************************************;
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

********************************************************************************;
* LAB_L3_NAME_LOINC
********************************************************************************;
%let qname=lab_l3_name_loinc;
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

********************************************************************************;
* LAB_L3_LOINC_RUNIT
********************************************************************************;
%let qname=lab_l3_loinc_runit;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_LOINC
********************************************************************************;
%let qname=lab_l3_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set lab_result_cm(keep=lab_loinc patid) end=eof;

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

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_loinc, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_LOINC_SOURCE
********************************************************************************;
%let qname=lab_l3_loinc_source;
%elapsed(begin);

*- Import transport file of lab LOINC expected specimen source -*;
proc cimport library=cptdata infile=labloinc;
run;

*- Sort lab data and expected specimen source data for merging -*;
proc sort data=cptdata.lab_loinc_ref(keep=lab_loinc exp_specimen_source) out=lab_loinc_ref;
     by lab_loinc;
run;

data lab_loinc;
     set lab_result_cm(keep=lab_loinc specimen_source);
     if specimen_source in ("BLOOD" "CSF" "PLASMA" "PPP" "SERUM" "SR_PLS" "URINE");

     lab_loinc=strip(upcase(lab_loinc));
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_NAME_RUNIT
********************************************************************************;
%let qname=lab_l3_name_runit;
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

********************************************************************************;
* LAB_L3_NAME_RDATE_Y
********************************************************************************;
%let qname=lab_l3_name_rdate_y;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length labname $10 col1 $4;
     set lab_result_cm(keep=result_date lab_name patid lab_loinc);

     if result_date^=. then year=year(result_date);

     if year^=. then col1=put(year,4.);
     else if year=. then col1="ZZZA";

     if lab_name in ("A1C" "CK" "CK_MB" "CK_MBI" "CREATININE" "HGB" "LDL" "INR" 
                     "TROP_I" "TROP_T_QL" "TROP_T_QN") then labname=lab_name;
     else if lab_name in ("NI") then labname="ZZZA";
     else if lab_name in ("UN") then labname="ZZZB";
     else if lab_name in ("OT") then labname="ZZZC";
     else if lab_name=" " then labname="ZZZD";
     else labname="ZZZE";

     keep col1 labname patid lab_loinc;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class labname/preloadfmt;
     class col1;
     output out=stats;
     format labname $lab_name.;
run;

*- Derive statistics - LOINC -*;
proc sort data=data out=loinc ;
     by labname col1 lab_loinc;
     where lab_loinc^=' ';
run;

proc means data=loinc completetypes noprint missing;
     class labname/preloadfmt;
     class col1;
     output out=loinc_unique;
     format labname $lab_name.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by labname col1 patid;
     where patid^=' ';
run;

proc means data=pid completetypes noprint missing;
     class labname/preloadfmt;
     class col1;
     output out=pid_unique;
     format labname $lab_name.;
run;

*- Bring outputs together -*;
data query;
     merge stats(where=(_type_=3))
           loinc_unique(where=(_type_=3) rename=(_freq_=record_loinc))
           pid_unique(where=(_type_=3) rename=(_freq_=distinct_patid))
     ;
     by labname col1;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge query
           stats(where=(_type_=2) rename=(_freq_=denom))
     ;
     by labname;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_name=put(labname,$lab_name.);
     result_date=put(col1,$null.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")
     %threshold(_tvar=record_loinc,nullval=col1^="ZZZD")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     record_n_loinc=strip(put(record_loinc,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package lab_name result_date record_n
          record_pct record_n_loinc distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_name, result_date, 
            record_n, record_pct, record_n_loinc, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_NAME_RDATE_YM
********************************************************************************;
%let qname=lab_l3_name_rdate_ym;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length labname $10 year_month 5.;
     set lab_result_cm(keep=result_date lab_name patid);

     * create a year and a year/month numeric variable *;
     if result_date^=. then year_month=(year(result_date)*100)+month(result_date);

     * set missing to impossible date value so that it will sort last *;
     if year_month=. then year_month=99999999;
    
     if lab_name in ("A1C" "CK" "CK_MB" "CK_MBI" "CREATININE" "HGB" "LDL" "INR" 
                     "TROP_I" "TROP_T_QL" "TROP_T_QN") then labname=lab_name;
     else if lab_name in ("NI") then labname="ZZZA";
     else if lab_name in ("UN") then labname="ZZZB";
     else if lab_name in ("OT") then labname="ZZZC";
     else if lab_name=" " then labname="ZZZD";
     else labname="ZZZE";

     keep year_month labname patid;
run;

*- Derive statistics -*;
proc means data=data nway completetypes noprint missing;
     class labname/preloadfmt;
     class year_month;
     output out=stats;
     format labname $lab_name.;
run;

*- Derive statistics - patid -*;
proc sort data=data out=pid nodupkey;
     by labname year_month patid;
     where patid^=' ';
run;

proc means data=pid nway completetypes noprint missing;
     class labname/preloadfmt;
     class year_month;
     output out=pid_unique;
     format labname $lab_name.;
run;

*- Derive appropriate counts and variables -*;
data query;
     length record_n distinct_patid_n $20;
     merge stats
           pid_unique(rename=(_freq_=distinct_patid));
     by labname year_month;

     * call standard variables *;
     %stdvar

     * table values *;
     lab_name=put(labname,$lab_name.);

     * create formatted value (e.g. 2015_01) for date *;
     if year_month=99999999 then result_date=put(year_month,null.);
     else result_date=put(int(year_month/100),4.)||"_"||put(mod(year_month,100),z2.);

     * apply threshold *;
     %threshold(nullval=year_month^=99999999)

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if distinct_patid^=. then distinct_patid_n=strip(put(distinct_patid,threshold.));
     else distinct_patid_n="0";

     keep datamartid response_date query_package lab_name result_date record_n
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, lab_name, result_date, 
            record_n, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_SOURCE
********************************************************************************;
%let qname=lab_l3_source;
%elapsed(begin);

proc format;
     value $lab_spec
       "BLOOD"="BLOOD"
       "CSF"="CSF"
       "PLASMA"="PLASMA"
       "PPP"="PPP"
       "SERUM"="SERUM"
       "SR_PLS"="SR_PLS"
       "URINE"="URINE"
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
     set lab_result_cm(keep=specimen_source) end=eof;

     if specimen_source in ("BLOOD" "CSF" "PLASMA" "PPP" "SERUM" "SR_PLS" "URINE") 
        then col1=specimen_source;
     else if specimen_source in ("NI") then col1="ZZZA";
     else if specimen_source in ("UN") then col1="ZZZB";
     else if specimen_source in ("OT") then col1="ZZZC";
     else if specimen_source=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 ;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $lab_spec.;
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
     specimen_source=put(col1,$lab_spec.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package specimen_source record_n record_pct;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, specimen_source, record_n, 
            record_pct
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=priority) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=result_loc) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=lab_px_type) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_PX_PXTYPE
********************************************************************************;
%let qname=lab_l3_px_pxtype;
%elapsed(begin);

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
     px=put(col1,$null.);
     px_type=put(pxtype,$lab_px.);
    
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

********************************************************************************;
* LAB_L3_QUAL
********************************************************************************;
%let qname=lab_l3_qual;
%elapsed(begin);

proc format;
     value $lab_qual
       "BORDERLINE"="BORDERLINE"
       "POSITIVE"="POSITIVE"
       "NEGATIVE"="NEGATIVE"
       "UNDETERMINED"="UNDETERMINED"
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
     set lab_result_cm(keep=result_qual) end=eof;

     if result_qual in ("BORDERLINE" "POSITIVE" "NEGATIVE" "UNDETERMINED") 
          then col1=result_qual;
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
     format col1 $lab_qual.;
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
     result_qual=put(col1,$lab_qual.);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=result_modifier) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=norm_modifier_low) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=norm_modifier_high) end=eof;

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

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
     set lab_result_cm(keep=abn_ind) end=eof;

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

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab);

%end;

********************************************************************************;
* Bring in CONDITION and compress it
********************************************************************************;
%if &_ycondition=1 %then %do;

data condition(compress=yes);
     set pcordata.condition;
run;

proc sort data=condition;
     by patid;
run;

********************************************************************************;
* Minimum/maximum report date for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=condition,var=report_date,var_tm=.)
%minmax(idsn=condition,var=resolve_date,var_tm=.)
%minmax(idsn=condition,var=onset_date,var_tm=.)

%let savecond=condition condition_report_date condition_resolve_date condition_onset_date;


********************************************************************************;
* COND_L3_N
********************************************************************************;
%let qname=cond_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=condition,encvar=patid)
%enc_oneway(encdsn=condition,encvar=encounterid)
%enc_oneway(encdsn=condition,encvar=conditionid)
%enc_oneway(encdsn=condition,encvar=condition)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
     condition=put(col1,$null.);
    
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond);

%end;

********************************************************************************;
* Bring in PRO_CM and compress it
********************************************************************************;
%if &_ypro_cm=1 %then %do;

data pro_cm(compress=yes);
     set pcordata.pro_cm;
run;

proc sort data=pro_cm;
     by patid;
run;

********************************************************************************;
* Minimum/maximum PRO for XTBL_L3_DATES
********************************************************************************;
%minmax(idsn=pro_cm,var=pro_date,var_tm=pro_time)

%let saveprocm=pro_cm pro_cm_pro_date;


********************************************************************************;
* PROCM_L3_N
********************************************************************************;
%let qname=procm_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=pro_cm,encvar=patid)
%enc_oneway(encdsn=pro_cm,encvar=encounterid)
%enc_oneway(encdsn=pro_cm,encvar=pro_cm_id)
%enc_oneway(encdsn=pro_cm,encvar=pro_response,_nc=1)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* PROCM_L3_ITEM
********************************************************************************;
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
               &savepres &savedisp &savelab &savecond &saveprocm);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);


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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* PROCM_L3_LOINC
********************************************************************************;
%let qname=procm_l3_loinc;
%elapsed(begin);

*- Derive categorical variable -*;
data data;
     length col1 $15;
     set pro_cm(keep=pro_loinc patid) end=eof;

     if pro_loinc^=" " then col1=pro_loinc;
     else if pro_loinc=" " then col1="ZZZA";

     keep col1 patid ;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $nullout.;
run;

*- Derive statistics - unique patient -*;
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
     pro_loinc=put(col1,$nullout.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZA")
     %threshold(_tvar=distinct_patid,nullval=col1^="ZZZA")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);
    
     keep datamartid response_date query_package pro_loinc record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, pro_loinc, record_n, 
            record_pct, distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

%end;

********************************************************************************;
* Bring in PCORNET_TRIAL and compress it
********************************************************************************;
%if &_yptrial=1 %then %do;

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
     trial_key=strip(patid)||"_"||strip(trialid)||"_"||strip(participantid);
run;

proc sort data=pcornet_trial;
     by patid;
run;

********************************************************************************;
* TRIAL_L3_N
********************************************************************************;
%let qname=trial_l3_n;
%elapsed(begin);

*- Macro for each variable -*;
%enc_oneway(encdsn=pcornet_trial,encvar=patid)
%enc_oneway(encdsn=pcornet_trial,encvar=trialid)
%enc_oneway(encdsn=pcornet_trial,encvar=participantid)
%enc_oneway(encdsn=pcornet_trial,encvar=trial_key)

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, all_n, 
            distinct_n, null_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

%end;

********************************************************************************;
* BEGIN XTBL QUERIES
********************************************************************************;

********************************************************************************;
* XTBL_L3_NON_UNIQUE
********************************************************************************;
%let qname=xtbl_l3_non_unique;
%elapsed(begin);

%macro nonuni(udsn,uvar);

%if &&&_y&udsn=1 %then %do;
    proc sort data=&udsn(keep=patid &uvar) out=_nonu_&udsn nodupkey;
         by &uvar patid;
         where &uvar^=' ';
    run;

    *- Check to see if any obs after subset -*;
    proc sql noprint;
         select count(&uvar) into :nobs from _nonu_&udsn;
    quit;

    *- If no obs, do quick data set -*;
    %if &nobs=0 %then %do;
        data _nonu_&udsn(keep=dataset tag cnt);
             length dataset $15 tag $25;
             dataset=upcase("&udsn");
             tag=upcase("&uvar");
             cnt=0;
             output;
        run;
    %end;
    %else %do;
        data _nonu_&udsn(keep=dataset tag cnt);
             length dataset $15 tag $25;
             set _nonu_&udsn end=eof;
             by &uvar patid;

             dataset=upcase("&udsn");
             tag=upcase("&uvar");
        
             retain cnt 0;
             if first.&uvar and not last.&uvar then cnt=cnt+1;
        
             if eof;
        run;
     %end;        

    proc append base=data data=_nonu_&udsn;
    run;
%end;

%mend nonuni;
%nonuni(udsn=encounter,uvar=encounterid);
%nonuni(udsn=diagnosis,uvar=encounterid);
%nonuni(udsn=procedures,uvar=encounterid);
%nonuni(udsn=lab_result_cm,uvar=encounterid);
%nonuni(udsn=prescribing,uvar=encounterid);
%nonuni(udsn=vital,uvar=encounterid);
%nonuni(udsn=condition,uvar=encounterid);
%nonuni(udsn=pro_cm,uvar=encounterid);

*- Derive appropriate counts and variables -*;
data query;
     length distinct_n $20;
     set data;

     * call standard variables *;
     %stdvar

     * counts *;
     distinct_n=strip(put(cnt,threshold.));
    
     keep datamartid response_date query_package dataset tag distinct_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, distinct_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* XTBL_L3_LAB_ENCTYPE
********************************************************************************;
%if &_yencounter=1 and &_ylab_result_cm=1 %then %do;

%let qname=xtbl_l3_lab_enctype;
%elapsed(begin);

proc sort data=encounter(keep=encounterid patid enc_type) out=xlab_encounter nodupkey;
     by encounterid;
run;

proc sort data=lab_result_cm(keep=encounterid) out=xenc_lab_result_cm;
     by encounterid;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     merge xlab_encounter(in=e)
           xenc_lab_result_cm(in=l)
     ;
     by encounterid;
     if e and l;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $enc_typ.;
run;

*- Derive statistics - unique patient -*;
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
           pid_unique(rename=(_freq_=distinct_patid))
     ;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm xlab_encounter);

%end;

********************************************************************************;
* XTBL_L3_PRES_ENCTYPE
********************************************************************************;
%if &_yencounter=1 and &_yprescribing=1 %then %do;

%let qname=xtbl_l3_pres_enctype;
%elapsed(begin);

proc sort data=prescribing(keep=encounterid) out=xenc_prescribing;
     by encounterid;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     merge xlab_encounter(in=e)
           xenc_prescribing(in=p)
     ;
     by encounterid;
     if e and p;

     if enc_type in ("AV" "ED" "EI" "IC" "IP" "IS" "OA" "OS") then col1=enc_type;
     else if enc_type in ("NI") then col1="ZZZA";
     else if enc_type in ("UN") then col1="ZZZB";
     else if enc_type in ("OT") then col1="ZZZC";
     else if enc_type=" " then col1="ZZZD";
     else col1="ZZZE";

     keep col1 patid;
run;

*- Derive statistics - encounter type -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $enc_typ.;
run;

*- Derive statistics - unique patient -*;
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
           pid_unique(rename=(_freq_=distinct_patid))
     ;
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

%end;

********************************************************************************;
* XTBL_L3_MISMATCH
********************************************************************************;
%let qname=xtbl_l3_mismatch;
%elapsed(begin);

proc format;
     value $ord
        "DEMOGRAPHIC"="01"
        "ENROLLMENT"="02"
        "ENCOUNTER"="03"
        "DIAGNOSIS"="04"
        "PROCEDURES"="05"
        "VITAL"="06"
        "LAB_RESULT_CM"="07"
        "PRESCRIBING"="08"
        "DISPENSING"="09"
        "DEATH"="10"
        "HARVEST"="11"
        "CONDITION"="12"
        "PRO_CM"="13"
        "PCORNET_TRIAL"="14"
        "DEATH_CAUSE"="15"
        other=" "
        ;
run;

*- Get meta-data from DataMart data structures -*;
proc contents data=pcordata._all_ noprint out=datamart_all;
run;

*- Create ordering variable for output -*;
data datamart_all;
     set datamart_all;
     memname=upcase(memname);
     name=upcase(name);
     ord=put(upcase(memname),$ord.);
     query_response_date="&tday";

     * subset on required data structures *;
     if ord^=' ';
run;

*- Place all table names into a macro variable -*;
proc sql noprint;
     select unique memname into :workdata separated by '|'  from datamart_all;
     select count(unique memname) into :workdata_count from datamart_all;
     select max(length) into :maxlength from sashelp.vcolumn 
        where libname="PCORDATA" and name="ENCOUNTERID";
     select memtype into :_mtype from datamart_all;
     select engine into :_engine from datamart_all;
quit;

*- Obtain the number of observations from each table -*;
%macro nobs;
    %do d = 1 %to &workdata_count;
        %let _dsn=%scan(&workdata,&d,"|");

        *- Get the number of obs from each dataset -*;
        proc sql;
             create table nobs&d as
             select count(*) as _nobs_n from pcordata.&_dsn;
        quit;

        data nobs&d;
             length memname $32 nobs $20;
             set nobs&d;
             memname="&_dsn";
             ord=put(upcase(memname),$ord.);
             if 0<_nobs_n<&threshold then _nobs_n=.t;
             nobs=strip(put(_nobs_n,threshold.));
             drop _nobs_n;
             output;
        run;

        * compile into one dataset *;
        proc append base=nobs_all data=nobs&d;
        run;
    %end;
%mend nobs;
%nobs;

*- Add number of observations -*;
proc sort data=datamart_all;
     by ord memname;
run;

proc sort data=nobs_all;
     by ord memname;
run;

data datamart_all;
     merge datamart_all(in=dm drop=nobs) nobs_all;
     by ord memname;
     if dm;
run;

*- Create a dataset of DataMart metadata -*;
proc sort data=datamart_all out=dmlocal.datamart_all;
     by ord memname name;
run;

*- Orphan ENCOUNTERID records from each appropriate table -*;
proc sql noprint;
     create table orphan_encid as select unique encounterid, enc_type, 
        admit_date
        from pcordata.encounter
        where encounterid^=" "
        order by encounterid;
quit;

%macro orph_enc;
    %do oe = 1 %to &workdata_count;
        %let _orph=%scan(&workdata,&oe,"|");

        %if &_orph=DIAGNOSIS or &_orph=PROCEDURES or &_orph=VITAL or 
            &_orph=LAB_RESULT_CM or &_orph=PRESCRIBING or
            &_orph=CONDITION or &_orph=PRO_CM %then %do;

            * orphan rows *;
            proc sql noprint;
                 create table orph_&_orph as select unique encounterid
                    from pcordata.&_orph
                    where encounterid^=" "
                    order by encounterid;
            quit;

            data orph_&_orph(keep=memname ord recordn);
                 length encounterid $ &maxlength memname $32;
                 merge orph_&_orph(in=o) 
                       orphan_encid(in=oe rename=(enc_type=etype admit_date=adate)) end=eof;
                 by encounterid;
                 retain recordn 0;
                 if o and not oe then recordn=recordn+1;
                 if eof then do;
                     memname="&_orph";
                     ord=put(upcase(memname),$ord.);
                     output;
                end;
            run;

            * compile into one dataset *;
            proc append base=orph_encounterid data=orph_&_orph;
            run;

            * mismatch rows *;
            %if &_orph=DIAGNOSIS or &_orph=PROCEDURES %then %do;
                proc sort data=&_orph(keep=encounterid enc_type admit_date)
                     out=mismatch_&_orph;
                     by encounterid;
                     where encounterid^=" ";
                run;

                data mismatch_&_orph(keep=memname ord mis:);
                     length encounterid $ &maxlength memname $32;
                     merge mismatch_&_orph(in=o) 
                           orphan_encid(in=oe 
                                        rename=(enc_type=etype admit_date=adate)) end=eof;
                     by encounterid;
                     retain mis_etype_n mis_adate_n 0;
                     if o and oe then do;
                          if enc_type^=etype then mis_etype_n=mis_etype_n+1;
                          if admit_date^=adate then mis_adate_n=mis_adate_n+1;
                     end;

                     if eof then do;
                         memname="&_orph";
                         ord=put(upcase(memname),$ord.);
                         output;
                    end;
                run;

                * compile into one dataset *;
                proc append base=mismatch_encounterid data=mismatch_&_orph;
                run;
            %end;
        %end;
    %end;
%mend orph_enc;
%orph_enc;

proc sort data=orph_encounterid;
     by ord memname;
run;

proc sort data=mismatch_encounterid;
     by ord memname;
run;

*- Orphan PATID records from each appropriate table -*;
proc sql noprint;
     create table orphan_patid as select unique patid
        from pcordata.demographic
        order by patid;
quit;

%macro orph_pat;
    %do op = 1 %to &workdata_count;
        %let _orphp=%scan(&workdata,&op,"|");

        %if &_orphp=ENROLLMENT or &_orphp=ENCOUNTER or &_orphp=DIAGNOSIS or 
            &_orphp=PROCEDURES or &_orphp=VITAL or &_orphp=LAB_RESULT_CM or 
            &_orphp=PRESCRIBING or &_orphp=DISPENSING or &_orphp=DEATH or
            &_orphp=CONDITION or &_orphp=DEATH_CAUSE or &_orphp=PRO_CM or
            &_orphp=PCORNET_TRIAL %then %do;

            proc sql noprint;
                 create table orph_&_orphp as select unique patid
                    from pcordata.&_orphp
                    order by patid;
            quit;

            data orph_&_orphp(keep=memname ord recordn);
                 length memname $32;
                 merge orph_&_orphp(in=o) orphan_patid(in=oe) end=eof;
                 by patid;
                 retain recordn 0;
                 if o and not oe then recordn=recordn+1;

                 if eof then do;
                     memname="&_orphp";
                     ord=put(upcase(memname),$ord.);
                     output;
                end;
            run;

            * compile into one dataset *;
            proc append base=orph_patid data=orph_&_orphp;
            run;
        %end;
    %end;
%mend orph_pat;
%orph_pat;

proc sort data=orph_patid;
     by ord memname;
run;


data query;
     length dataset $100 tag $50;
     set orph_encounterid(in=oe)
         orph_patid(in=op)
         mismatch_encounterid(in=mde where=(memname="DIAGNOSIS") rename=(mis_etype_n=recordn))
         mismatch_encounterid(in=mda where=(memname="DIAGNOSIS") rename=(mis_adate_n=recordn))
         mismatch_encounterid(in=mpe where=(memname="PROCEDURES") rename=(mis_etype_n=recordn))
         mismatch_encounterid(in=mpa where=(memname="PROCEDURES") rename=(mis_adate_n=recordn))
     ;

     * call standard variables *;
     %stdvar

     distinct_n=strip(put(recordn,threshold.));
     if oe then do;
        dataset="ENCOUNTER and " || strip(upcase(memname));
        tag="ENCOUNTERID Orphan";
     end;
     else if op then do;
        dataset="DEMOGRAPHIC and " || strip(upcase(memname));
        tag="PATID Orphan";
     end;
     else if mde then do;
        dataset="ENCOUNTER and DIAGNOSIS";
        tag="ENC_TYPE Mismatch";
     end;
     else if mda then do;
        dataset="ENCOUNTER and DIAGNOSIS";
        tag="ADMIT_DATE Mismatch";
     end;
     else if mpe then do;
        dataset="ENCOUNTER and PROCEDURES";
        tag="ENC_TYPE Mismatch";
     end;
     else if mpa then do;
        dataset="ENCOUNTER and PROCEDURES";
        tag="ADMIT_DATE Mismatch";
     end;

     keep datamartid response_date query_package dataset tag distinct_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, distinct_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* XTBL_L3_DATES
********************************************************************************;
%let qname=xtbl_l3_dates;
%elapsed(begin);

%macro xminmax(idsn);
    
* append each variable into base dataset *;
proc append base=query data=&idsn;
run;

%mend xminmax;
%if &_ydemographic=1 %then %do;
    %xminmax(idsn=demographic_birth_date)
%end;
%if &_yencounter=1 %then %do;
    %xminmax(idsn=encounter_admit_date)
    %xminmax(idsn=encounter_discharge_date)
%end;
%if &_ydiagnosis=1 %then %do;
    %xminmax(idsn=diagnosis_admit_date)
%end;
%if &_yprocedures=1 %then %do;
    %xminmax(idsn=procedures_admit_date)
    %xminmax(idsn=procedures_px_date)
%end;
%if &_yvital=1 %then %do;
    %xminmax(idsn=vital_measure_date)
%end;
%if &_yenrollment=1 %then %do;
    %xminmax(idsn=enrollment_enr_start_date)
    %xminmax(idsn=enrollment_enr_end_date)
%end;
%if &_ydeath=1 %then %do;
    %xminmax(idsn=death_death_date)
%end;
%if &_ydispensing=1 %then %do;
    %xminmax(idsn=dispensing_dispense_date)
%end;
%if &_yprescribing=1 %then %do;
    %xminmax(idsn=prescribing_rx_order_date)
    %xminmax(idsn=prescribing_rx_start_date)
    %xminmax(idsn=prescribing_rx_end_date)
%end;
%if &_ylab_result_cm=1 %then %do;
    %xminmax(idsn=lab_result_cm_lab_order_date)
    %xminmax(idsn=lab_result_cm_specimen_date)
    %xminmax(idsn=lab_result_cm_result_date)
%end;
%if &_ycondition=1 %then %do;
    %xminmax(idsn=condition_report_date)
    %xminmax(idsn=condition_resolve_date)
    %xminmax(idsn=condition_onset_date)
%end;
%if &_ypro_cm=1 %then %do;
    %xminmax(idsn=pro_cm_pro_date)
%end;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag, min, p5, median,
         p95, max, n, nmiss, future_dt_n, pre2010_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* XTBL_L3_DATE_LOGIC
********************************************************************************;
%let qname=xtbl_l3_date_logic;
%elapsed(begin);

%macro xmin(idsn,var,mvar);

proc means data=&idsn nway noprint;
     class patid;
     var &var;
     output out=min&idsn min=m&var;
     where patid^=" ";
run;

data min&var(keep=compvar b_cnt d_cnt);
     length compvar $20;
     merge min&idsn(in=m)
           demographic(keep=patid birth_date where=(patid^=" ")) 
           death(keep=patid death_date where=(patid^=" "))
           end=eof;
     by patid;
     
     compvar=upcase("&var");
     retain b_cnt d_cnt 0;
     if m and birth_date^=. and m&var^=. and m&var<birth_date then b_cnt=b_cnt+1;
     if m and death_date^=. and m&var^=. and m&var>death_date then d_cnt=d_cnt+1;

     if eof then output;
run;

proc append base=data data=min&var;
run;

%mend xmin;
%if &_yencounter=1 %then %do;
    %xmin(idsn=encounter,var=admit_date)
    %xmin(idsn=encounter,var=discharge_date)
%end;
%if &_yprocedures=1 %then %do;
    %xmin(idsn=procedures,var=px_date)
%end;
%if &_yvital=1 %then %do;
    %xmin(idsn=vital,var=measure_date)
%end;
%if &_ydispensing=1 %then %do;
    %xmin(idsn=dispensing,var=dispense_date)
%end;
%if &_yprescribing=1 %then %do;
    %xmin(idsn=prescribing,var=rx_start_date)
%end;
%if &_ylab_result_cm=1 %then %do;
    %xmin(idsn=lab_result_cm,var=result_date)
%end;
%if &_ydeath=1 %then %do;
    %xmin(idsn=death,var=death_date)
%end;

data query;
     length date_comparison $50 distinct_patid_n $20;
     set data(in=b rename=(b_cnt=distinct_patid))
         data(in=d rename=(d_cnt=distinct_patid))
     ;

     * drop death-death check *;
     if d and compvar="DEATH_DATE" then delete;

     * call standard variables *;
     %stdvar

     * create comparision variable *;
     if b then date_comparison=strip(compvar) || " < BIRTH_DATE";
     else if d then date_comparison=strip(compvar) || " > DEATH_DATE";

     * apply threshold *;
     if 0<distinct_patid<&threshold then distinct_patid=.t;
     distinct_patid_n=strip(put(distinct_patid,threshold.));
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, date_comparison, 
            distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

********************************************************************************;
* XTBL_L3_TIMES
********************************************************************************;
%let qname=xtbl_l3_times;
%elapsed(begin);

%macro xminmax(idsn);
    
* append each variable into base dataset *;
proc append base=query data=&idsn(keep=datamartid response_date query_package 
                      dataset tag_tm min_tm mean_tm median_tm max_tm n_tm nmiss_tm);
run;

%mend xminmax;
%if &_ydemographic=1 %then %do;
    %xminmax(idsn=demographic_birth_date)
%end;
%if &_yencounter=1 %then %do;
    %xminmax(idsn=encounter_admit_date)
    %xminmax(idsn=encounter_discharge_date)
%end;
%if &_yvital=1 %then %do;
    %xminmax(idsn=vital_measure_date)
%end;
%if &_ylab_result_cm=1 %then %do;
    %xminmax(idsn=lab_result_cm_result_date)
    %xminmax(idsn=lab_result_cm_specimen_date)
%end;
%if &_yprescribing=1 %then %do;
    %xminmax(idsn=prescribing_rx_order_date)
%end;
%if &_ypro_cm=1 %then %do;
    %xminmax(idsn=pro_cm_pro_date)
%end;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, dataset, tag_tm as tag, 
         min_tm as min, median_tm as median, max_tm as max, 
         n_tm as n, nmiss_tm as nmiss
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit &savepres &savedisp &savelab);

******************************************************************************;
* XTBL_L3_METADATA
******************************************************************************;
%if &_yharvest=1 %then %do;

%let qname=xtbl_l3_metadata;
%elapsed(begin);

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
            pro_date_mgmt $50 datastore $5;
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
     query_package="&dc";
     response_date=put("&sysdate"d,yymmdd10.);
     tag=strip(query_package)||"_"||strip(datamartid)||"_"||strip(response_date);

     if datamart_platform not in ("01" "02" "03" "04" "05" "NI" "OT" "UN" " ")
        then datamart_platform="Values outside of CDM specifications";
     
     cdm_version=strip(put(cdm_version_num,5.1));

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
           if dm{dm1} in ("01" "02" "03" "04" "NI" "OT" "UN" " ")
             then dm{dm1}=dm{dm1};
           else dm{dm1}="Values outside of CDM specifications";
        end;

     array rfc $15 refresh_demographic_date refresh_enrollment_date 
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

     if %upcase("&_engine")="V9" and %upcase("&_mtype")="DATA" then datastore="SAS";
     else datastore="RDBMS";
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
         sas_teradata datastore;
run;

*- Assign attributes -*;
data query;
     set query;

     name=upcase(name);
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

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savevit &savepres &savedisp &savelab);

%end;

********************************************************************************;
* Create work DIAGNOSIS & VITAL datasets for multiple XTBL_L3_DASHx queries
********************************************************************************;
%if &_ydiagnosis=1 %then %do;

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
%end;

%if &_yvital=1 %then %do;
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
%end;

********************************************************************************;
* XTBL_L3_DASH1
********************************************************************************;
%if &_ydiagnosis=1 and &_yvital=1 %then %do;

%let qname=xtbl_l3_dash1;
%elapsed(begin);

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
     if max_year>year(today()) then max_year=year(today());

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
%clean(savedsn=xdiagnosis xvital &savepres &savedisp &savelab);

%end;

********************************************************************************;
* XTBL_L3_DASH2
********************************************************************************;
%if &_yprescribing=1 and &_ydispensing=1 and &_yvital=1 and &_ydiagnosis=1 %then %do;

%let qname=xtbl_l3_dash2;
%elapsed(begin);

*- Data with legitimate date -*;
data xprescribing;
     length year 4.;
     set prescribing(keep=rxnorm_cui rx_order_date patid);
     if rxnorm_cui^=" ";

     if rx_order_date^=. then year=year(rx_order_date);

     keep patid year;
run;

*- Uniqueness per PATID/year -*;
proc sort data=xprescribing nodupkey;
     by patid year;
     where year^=.;
run;

*- Data with legitimate date -*;
data xdispensing;
     length year 4.;
     set dispensing(keep=ndc dispense_date patid);
     if ndc^=" ";

     if dispense_date^=. then year=year(dispense_date);

     keep patid year;
run;

*- Uniqueness per PATID/year -*;
proc sort data=xdispensing nodupkey;
     by patid year;
     where year^=.;
run;

*- Merge diagnosis, vital, prescribing, and dispensing data -*;
data data;
     merge xdiagnosis(in=e) xvital(in=v) xprescribing(in=p) xdispensing(in=d);
     by patid year;
     if e and v and (p or d);
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
     if max_year>year(today()) then max_year=year(today());

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
%clean(savedsn=xdiagnosis xvital xprescribing xdispensing &savelab);

%end;

********************************************************************************;
* XTBL_L3_DASH3
********************************************************************************;
%if &_yprescribing=1 and &_ydispensing=1 and &_ydiagnosis=1 and &_yvital=1 and 
    &_ylab_result_cm=1 %then %do;

%let qname=xtbl_l3_dash3;
%elapsed(begin);

*- Data with legitimate date -*;
data xlab_result_cm;
     length year 4.;
     set lab_result_cm(keep=lab_name result_date patid);

     if result_date^=. then year=year(result_date);

     keep patid year;
run;

*- Uniqueness per PATID/year -*;
proc sort data=xlab_result_cm nodupkey;
     by patid year;
     where year^=.;
run;

*- Merge diagnosis, vital, lab data, prescribing, and dispensing data -*;
data data;
     merge xdiagnosis(in=e) xvital(in=v) xlab_result_cm(in=l) xprescribing(in=p)
           xdispensing(in=d);
     by patid year;
     if e and v and l and (p or d);
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
     if max_year>year(today()) then max_year=year(today());

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
%clean;

%end;

*******************************************************************************;
* Create dataset of query execution time
*******************************************************************************;
data dmlocal.elapsed;
     set elapsed;
     if query^=" ";
     label _qstart="Query start time"
           _qend="Query end time"
           elapsedtime="Query run time (hh:mm:ss)"
           totalruntime="Cumulative run time (hh:mm:ss)"
     ;
     if query="DC PROGRAM" then do;
        _qend=datetime();
        elapsedtime=_qend-_qstart;
        totalruntime=_qend-&_pstart;
     end;

     * call standard variables *;
     %stdvar
run;
 
*******************************************************************************;
* Add DC program run time to XTBL_L3_METADATA
*******************************************************************************;
data dmlocal.xtbl_l3_metadata;
     set dmlocal.xtbl_l3_metadata
         dmlocal.elapsed(in=e where=(query="DC PROGRAM"))
     ;
     if e then do;
        name=strip(query) || " (HH:MM:SS)";
        value=put(totalruntime,time8.);
     end;
     keep name value;
run;

*******************************************************************************;
* Dataset for printing number of observations in each dataset
*******************************************************************************;
data datamart_all_nobs;
     set dmlocal.datamart_all;
     by ord memname;
     if first.ord;
     keep memname nobs;
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

********************************************************************************;
* Create a SAS transport file from all of the query datasets
********************************************************************************;
filename tranfile "&qpath.drnoc/&dmid._&tday._data_curation.cpt";
proc cport library=dmlocal file=tranfile memtype=data;
run;        

*******************************************************************************;
* Print each data set and send to a PDF file
*******************************************************************************;
ods html close;
ods listing;
ods path sashelp.tmplmst(read) library.templat(read);
ods pdf file="&qpath.drnoc/&dmid._&tday._data_curation.pdf" style=journal;

title "Data Curation query run times";
footnote;
proc print width=min data=dmlocal.elapsed label;
     var query _qstart _qend elapsedtime totalruntime;
run;
title;

title "Number of observations in each CDM table";
proc print width=min data=datamart_all_nobs label;
     var memname nobs;
run;
title;

%if &_yharvest=1 %then %do;
    %prnt(pdsn=xtbl_l3_metadata,dropvar=);
%end;
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
%if &_ydemographic=1 or &_yencounter=1 or &_ydiagnosis=1 or &_yprocedures=1 or
    &_yvital=1 or &_yenrollment=1 or &_ydeath=1 or &_ydispensing=1 or
    &_yprescribing=1 or &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_dates);
%end;
%if &_ydemographic=1 or &_yencounter=1 or &_yprocedures=1 or &_yvital=1 or
    &_ydeath=1 or &_ydispensing=1 or &_yprescribing=1 or &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_date_logic);
%end;
%if &_ydemographic=1 or &_yencounter=1 or &_yvital=1 or &_yprescribing=1 or
    &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_times);
%end;
%if &_ydiagnosis=1 and &_yvital=1 %then %do;
    %prnt(pdsn=xtbl_l3_dash1);
%end;
%if &_yprescribing=1 and &_ydispensing=1 and &_yvital=1 and &_ydiagnosis=1 %then %do;
    %prnt(pdsn=xtbl_l3_dash2);
%end;
%if &_ydiagnosis=1 and &_yvital=1 and &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_dash3);
%end;
    %prnt(pdsn=xtbl_l3_mismatch);
%if &_yencounter=1 and &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_lab_enctype);
%end;
%if &_yencounter=1 and &_yprescribing=1 %then %do;
    %prnt(pdsn=xtbl_l3_pres_enctype);
%end;
%if &_yencounter=1 or &_yprocedures=1 or &_yvital=1 or &_ycondition=1 or 
    &_ydiagnosis=1 or &_ydispensing=1 or &_yprescribing=1 or 
    &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=xtbl_l3_non_unique);
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
%end;    
%if &_yprescribing=1 %then %do;
    %prnt(pdsn=pres_l3_n);
    %prnt(pdsn=pres_l3_rxcui,_obs=100,_svar=rxnorm_cui,_suppvar=rxnorm_cui_tty record_n record_pct distinct_patid_n);
    %prnt(pdsn=pres_l3_rxcui_tier);
    %prnt(pdsn=pres_l3_supdist2);
    %prnt(pdsn=pres_l3_rxcui_rxsup,_obs=100,_svar=rxnorm_cui,_suppvar=min mean max n nmiss,_recordn=n);
    %prnt(pdsn=pres_l3_basis);
    %prnt(pdsn=pres_l3_freq);
    %prnt(pdsn=pres_l3_qtyunit);
    %prnt(pdsn=pres_l3_odate_y);
    %prnt(pdsn=pres_l3_odate_ym,_obs=100,_svar=rx_order_date,_suppvar=record_n distinct_patid_n);
%end;    
%if &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=lab_l3_n);
    %prnt(pdsn=lab_l3_name);
    %prnt(pdsn=lab_l3_name_loinc,_obs=100,_svar=lab_name loinc,_suppvar=record_n);
    %prnt(pdsn=lab_l3_name_runit,_obs=100,_svar=lab_name result_unit,_suppvar=record_n);
    %prnt(pdsn=lab_l3_name_rdate_y);
    %prnt(pdsn=lab_l3_name_rdate_ym,_obs=100,_svar=lab_name result_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=lab_l3_source);
    %prnt(pdsn=lab_l3_priority);
    %prnt(pdsn=lab_l3_loc);
    %prnt(pdsn=lab_l3_loinc,_obs=100,_svar=lab_loinc,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=lab_l3_loinc_runit,_obs=100,_svar=lab_loinc result_unit,_suppvar=record_n);
    %prnt(pdsn=lab_l3_loinc_source,_obs=100,_svar=lab_loinc,_suppvar=specimen_source exp_specimen_source record_n);
    %prnt(pdsn=lab_l3_px_type);
    %prnt(pdsn=lab_l3_px_pxtype,_obs=100,_svar=px px_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=lab_l3_qual);
    %prnt(pdsn=lab_l3_mod);
    %prnt(pdsn=lab_l3_low);
    %prnt(pdsn=lab_l3_high);
    %prnt(pdsn=lab_l3_abn);
    %prnt(pdsn=lab_l3_recordc);
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
    %prnt(pdsn=procm_l3_item);
    %prnt(pdsn=procm_l3_pdate_y);
    %prnt(pdsn=procm_l3_pdate_ym,_obs=100,_svar=pro_date,_suppvar=record_n);
    %prnt(pdsn=procm_l3_method);
    %prnt(pdsn=procm_l3_mode);
    %prnt(pdsn=procm_l3_cat);
    %prnt(pdsn=procm_l3_loinc,_obs=100,_svar=pro_loinc,_suppvar=record_n record_pct distinct_patid_n);
%end;
%if &_yptrial=1 %then %do;
    %prnt(pdsn=trial_l3_n);
%end;

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;

********************************************************************************;
* Macro end
********************************************************************************;
%mend dc;
%dc;    
