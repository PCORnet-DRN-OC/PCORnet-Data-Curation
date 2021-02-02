/*******************************************************************************
*  $Source: data_curation_query_base $;
*    $Date: 2020/10/14
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V6.0 - 
*           preamble program to allow production of queries from:
*               1) data_curation_query_main.sas
*               2) data_curation_query_lab.sas
*               3) data_curation_query_xtbl.sas
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: None
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/
options validvarname=upcase errors=0 nomprint;
ods html close;

********************************************************************************;

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
filename dcref "&qpath./infolder/dc_reference.cpt";
libname cptdata "&qpath.infolder";

********************************************************************************;
*- Set version number
********************************************************************************;
%let dc = DC V6.00;

*- Create macro variable to determine lookback cutoff -*;
data _null_;
     today="&sysdate"d;
     lookback=mdy(month(today),day(today),year(today)-&lookback);
     call symputx('lookback_dt',put(lookback,8.));

     * for 5 yr lookback queries *;
     lookback5=mdy(month(today),day(today),year(today)-5);
     call symputx('lookback5_dt',put(lookback5,8.));

     * DASH queries, cut-off 3 mth prior to system run date and 12 mth intervals *;
     call symputx('cutoff_dash',intnx('month',today(),-3));
     call symputx('cutoff_dashyr1',intnx('month',today(),-15));
     call symputx('cutoff_dashyr2',intnx('month',today(),-27));
     call symputx('cutoff_dashyr3',intnx('month',today(),-39));
     call symputx('cutoff_dashyr4',intnx('month',today(),-51));
     call symputx('cutoff_dashyr5',intnx('month',today(),-63));
run;

********************************************************************************;
* Create macro variable from DataMart ID and program run date 
********************************************************************************;
data _null;
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
     call symputx("_pstart",datetime());
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
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_base.log" lrecl=200;
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
          if mindt^=. then min=put(year(mindt),4.)||"_"||put(month(mindt),z2.);
          else min=" ";
          if maxdt^=. then max=put(year(maxdt),4.)||"_"||put(month(maxdt),z2.);
          else max=" ";
          if meandt^=. then mean=put(year(meandt),4.)||"_"||put(month(meandt),z2.);
          else mean=" ";
          if mediandt^=. then median=put(year(mediandt),4.)||"_"||put(month(mediandt),z2.);
          else median=" ";
          if p5dt^=. then p5=put(year(p5dt),4.)||"_"||put(month(p5dt),z2.);
          else p5=" ";
          if p95dt^=. then p95=put(year(p95dt),4.)||"_"||put(month(p95dt),z2.);
          else p95=" ";
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

          if mintm^=. then min_tm=put(mintm,time5.);
          else min_tm=" ";
          if maxtm^=. then max_tm=put(maxtm,time5.);
          else max_tm=" ";
          if meantm^=. then mean_tm=put(meantm,time5.);
          else mean_tm=" ";
          if mediantm^=. then median_tm=put(mediantm,time5.);
          else median_tm=" ";
          if p5tm^=. then p5_tm=put(p5tm,time5.);
          else p5_tm=" ";
          if p95tm^=. then p95_tm=put(p95tm,time5.);
          else p95_tm=" ";
          n_tm=strip(put(ntm,threshold.));
          nmiss_tm=strip(put(nmisstm,threshold.));

          keep datamartid response_date query_package dataset tag tag_tm 
               min max p5 p95 mean median n nmiss future_dt_n pre2010_n min_tm 
               max_tm p5_tm p95_tm mean_tm median_tm n_tm nmiss_tm;
     run;
%mend minmax;

*- Macro for each variable in _N query -*;
%macro enc_oneway(encdsn=,encvar=,_nc=0,ord=);

     proc sort data=&encdsn(keep=&encvar)
          out=var_&ord;
          by &encvar;
     run;

     *- Derive appropriate counts and variables -*;
     data var_&ord._1way;
          length all_n distinct_n null_n $20 tag dataset $35;
          set var_&ord end=eof;
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
     proc append base=query data=var_&ord._1way;
     run;

     proc datasets  noprint;
          delete var_&ord._1way;
     quit;

%mend enc_oneway;

*- Macro to print each query result -*;
%macro prnt(pdsn=,_obs=max,dropvar=datamartid response_date query_package,_svar=,_suppvar=,_recordn=record_n,orient=portrait);

     options orientation=&orient;

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
          save xtbl_mdata_idsn elapsed 
               facility_type _route prov_spec prov_group lab_dcgroup_ref 
               lab_loinc_ref dx_dcgroup_ref px_dcgroup_ref rxnorm_cui_ref 
               _dose_form progress vxbodysite vxmanu &savedsn / memtype=data;
          save formats / memtype=catalog;
     quit;
%mend clean;

*- Macro for DASH queries -*;
%macro dash(datevar);
         if &cutoff_dashyr5<=&datevar<=&cutoff_dash then do;
            period=5;
            output;
            if &cutoff_dashyr4<=&datevar<=&cutoff_dash then do;
               period=4;
               output;
               if &cutoff_dashyr3<=&datevar<=&cutoff_dash then do;
                  period=3;
                  output;
                  if &cutoff_dashyr2<=&datevar<=&cutoff_dash then do;
                     period=2;
                     output;
                     if &cutoff_dashyr1<=&datevar<=&cutoff_dash then do;
                        period=1;
                        output;
                     end;
                  end;
               end;
            end;
         end;
%mend dash;

*- Macro to track query processing time -*;
data elapsed;
     length query $100;
     format _qstart _qend datetime19. elapsedtime totalruntime time8.;
     array _char_ query ;
     array _num_  _qstart _qend elapsedtime totalruntime dflag;
     query="DC PROGRAM";
     _qstart=&_pstart;
     output;
run;
    
%macro elapsed(b_or_e,last);

    %if &b_or_e=begin %then %do;
        data _qe;
            length query $100;
            format _qstart datetime19.;
            dflag=0;
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
            length query $100 description $70;
            format elapsedtime totalruntime time8. _qstart _qend datetime19.;
            set _qe;
                description="%upcase(&qname) has finished processing";
                dflag=0;
                _qend=datetime();
                elapsedtime=_qend-_qstart;
                _elapsedtime=put(elapsedtime,time8.);
                totalruntime=_qend-&_pstart;
                _totalruntime=put(totalruntime,time8.);
                output;
                 put "******************************************************************************************************";
                 put "End query:  %upcase(&qname)  on " _qstart "elapsed time (hh:mm:ss): " _elapsedtime " total run time (hh:mm:ss): " _totalruntime;
                 put "******************************************************************************************************";
                 %if &last=Y %then %do;
                     dflag=1;
                     description="All of %upcase(&_grp) queries have finished processing";
                     output;
                 %end;
        run;

        * append current run-time to elapsed dataset *;
        proc append base=elapsed data=_qe(where=(dflag=0) drop=description);
        run;

        * append current run-time to progress dataset *;
        proc append base=progress data=_qe(drop=dflag);
        run;

        * print data set of progress *;
        ods html close;
        ods listing;
        ods path sashelp.tmplmst(read) library.templat(read);
        ods rtf file="&qpath.drnoc/&dmid._&tday._data_curation_progress_report.rtf" style=journal;

        title "Current progress of Data Curation processing";
        footnote;
        proc print width=min data=progress label;
             var description _qstart _qend elapsedtime totalruntime;
             label _qstart="Query start time"
                   _qend="Query end time"
                   elapsedtime="Query run time (hh:mm:ss)"
                   totalruntime="Cumulative run time (hh:mm:ss)"
                   description="Description"
             ;
        run;
        title;
        ods rtf close;
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
         "TH"="TH"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
        ;
        
     value $other
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
       other = [$200.]    
        ;
    
     value period
       1="1 yr"
       2="2 yrs"
       3="3 yrs"
       4="4 yrs"
       5="5 yrs"
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
        .=" "
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
%macro dc_base;

%global savedeath savedemog saveenc savediag saveproc saveenr savevit savepres 
        savedisp savelab savecond saveprocm savedeathc savemedadm saveobsclin
        saveobsgen saveprov saveldsadrs saveimmune savehashtoken savelabhistory
        _ycondition _ydeath _ydeathc _ydemographic _ydiagnosis _ydispensing 
        _yencounter _yenrollment _yharvest _yhash_token _yimmunization 
        _ylab_result_cm _ylab_history _yldsadrs _ymed_admin _yobs_clin _yobs_gen 
        _yprescribing _yprocedures _ypro_cm _yprovider _yvital _yptrial 
        raw_lab_name
        ;

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
     select count(*) into :_yhash_token from pcordata
            where memname="HASH_TOKEN";
     select count(*) into :_yimmunization from pcordata
            where memname="IMMUNIZATION";
     select count(*) into :_ylab_history from pcordata
            where memname="LAB_HISTORY";
     select count(*) into :_ylab_result_cm from pcordata
            where memname="LAB_RESULT_CM";
     select count(*) into :_yldsadrs from pcordata
            where memname="LDS_ADDRESS_HISTORY";
     select count(*) into :_ymed_admin from pcordata
            where memname="MED_ADMIN";
     select count(*) into :_yobs_clin from pcordata
            where memname="OBS_CLIN";
     select count(*) into :_yobs_gen from pcordata
            where memname="OBS_GEN";
     select count(*) into :_yprescribing from pcordata
            where memname="PRESCRIBING";
     select count(*) into :_yprocedures from pcordata
            where memname="PROCEDURES";
     select count(*) into :_ypro_cm from pcordata
            where memname="PRO_CM";
     select count(*) into :_yprovider from pcordata
            where memname="PROVIDER";
     select count(*) into :_yvital from pcordata
            where memname="VITAL";
     select count(*) into :_yptrial from pcordata
            where memname="PCORNET_TRIAL";
quit;

********************************************************************************;
*- Import transport file for all reference data sets
********************************************************************************;
proc cimport library=work infile=dcref;
run;

%macro fmt(fmt=,sheet=,keyword=0);

    * code needs to be converted to character *;
    %if &fmt=payer_type %then %do;
        data &sheet;
             length code $8;
             set &sheet(rename=(code=coden));
             code=strip(put(coden,8.));
             drop coden;
        run;
    %end;
    %else %if &fmt=short_result_unit %then %do;
        data &sheet;
             set &sheet(rename=(ucum_code=code));
        run;
    %end;

        * create necessary variables for a format *;
        data &fmt;
             length start end label $200;
             set &sheet end=eof;
    
             * format name *;
             fmtname="$"||upcase("&fmt");

             * modify due to key words for PROC FORMAT *;
             %if &keyword=1 %then %do;
                 if upcase(code) in ("LOW" "HIGH") then code="_"||strip(code);
             %end;

             * start and label and output record *;
             start=code;
             end=start;
             label=start;

             output;
             * special values *;
             if eof then do;
                 * other to cover values outside CDM *;
                 start="other";
                 end="other";
                 label="ZZZB";
                 output;
                 * ZZZA *;
                 start="ZZZA";
                 end="ZZZA";
                 label="ZZZA";
                 output;
                 * ZZZB *;
                 start="ZZZB";
                 end="ZZZB";
                 label="ZZZB";
                 output;
             end;
        run;

    proc sort data=&fmt;
         by start;
    run;

    * create format *;
    proc format library=work cntlin=&fmt;
    run;

    * need a second format for grouping *;
    %if &fmt=prov_spec %then %do;
        data prov_group;
             length fmtname $15;
             set &fmt;
             fmtname="$PROV_GROUP";
             if start not in ("ZZZA" "ZZZB" "ZZZC" "ZZZD" "ZZZE") then label=grouping;
             else label=start;
        run;

        proc format library=work cntlin=prov_group;
        run;
    %end;
%mend fmt;
%fmt(fmt=preflang,sheet=PAT_PREF_LANGUAGE_SPOKEN)
%fmt(fmt=payer_type,sheet=PAYER_TYPE_)
%fmt(fmt=facility_type,sheet=FACILITY_TYPE)
%fmt(fmt=_unit,sheet=_UNIT)
%fmt(fmt=_qual,sheet=_QUAL,keyword=1)
%fmt(fmt=_route,sheet=_ROUTE)
%fmt(fmt=_source,sheet=SPECIMEN_SOURCE)
%fmt(fmt=_dose_form,sheet=_DOSE_FORM)
%fmt(fmt=prov_spec,sheet=PROVIDER_SPECIALTY_PRIMARY)
%fmt(fmt=vxbodysite,sheet=VX_BODY_SITE)
%fmt(fmt=vxmanu,sheet=VX_MANUFACTURER)
%fmt(fmt=state,sheet=STATE)

********************************************************************************;
*- Create a format of all of the lab group values
********************************************************************************;
data lab_dcgroup_ref_fmt;
     set lab_dcgroup_ref;

     output;
     * create record for unassigned *;
     include_edc='0';
     dc_lab_group="Unassigned";
     output;
run;

proc freq data=lab_dcgroup_ref_fmt noprint;
     table dc_lab_group*include_edc / out=prefmt;
run;

data prefmt;
     set prefmt;
     retain fmtname '$labdcgrp' type 'C';
     rename dc_lab_group = start;
     label = strip(dc_lab_group)|| "~" || strip(include_edc);
run;

proc format cntlin=prefmt;
run;

********************************************************************************;
*- Create a format of all of the diagnostic group values
********************************************************************************;
proc freq data=dx_dcgroup_ref noprint;
     table dc_dx_group*pedi / out=prefmt_dx;
run;

data prefmt_dx;
     set prefmt_dx;
     retain fmtname '$dxdcgrp' type 'C';
     rename dc_dx_group = start;
     label = strip(dc_dx_group)|| "~" || strip(pedi);
run;

proc format cntlin=prefmt_dx;
run;

********************************************************************************;
*- Create a format of all of the procedures group values
********************************************************************************;
proc freq data=px_dcgroup_ref noprint;
     table dc_px_group*pedi / out=prefmt_px;
run;

data prefmt_px;
     set prefmt_px;
     retain fmtname '$pxdcgrp' type 'C';
     rename dc_px_group = start;
     label = strip(dc_px_group)|| "~" || strip(pedi);
run;

proc format cntlin=prefmt_px;
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

********************************************************************************
* Submit sub-data curation query program(s)
********************************************************************************;
%if %upcase(&_grp)=MAIN %then %do;
    %include "&qpath.infolder/data_curation_query_main.sas";
%end;
%else %if %upcase(&_grp)=LAB %then %do;
    %include "&qpath.infolder/data_curation_query_lab.sas";
%end;
%else %if %upcase(&_grp)=XTBL %then %do;
    %include "&qpath.infolder/data_curation_query_xtbl.sas";
%end;
%else %if %upcase(&_grp)=ALL %then %do;
    %include "&qpath.infolder/data_curation_query_main.sas";
    %include "&qpath.infolder/data_curation_query_lab.sas";
    %include "&qpath.infolder/data_curation_query_xtbl.sas";
%end;
%include "&qpath.infolder/data_curation_print.sas";

********************************************************************************;
* Macro end
********************************************************************************;
%mend dc_base;
%dc_base;
