/*******************************************************************************
*  $Source: data_curation_base $;
*    $Date: 2024/10/28
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V6.15 - 
*           preamble program to allow production of queries from:
*               1) data_curation_tables.sas
*               2) data_curation_print.sas
* 
*   Inputs: SAS program:  /sasprograms/02_run_queries.sas
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
filename loinc "&qpath./infolder/loinc.cpt";
libname cptdata "&qpath.infolder";


********************************************************************************;
*- Set version number
********************************************************************************;
%let dc = DC V6.15;

*- Create macro variable to determine lookback cutoff -*;
data _null_;
     today="&sysdate"d;
     sys_year_month=(year(today)*100)+month(today);
     call symputx('sys_year_month',put(sys_year_month,8.));
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
data _null_;
     set pcordata.harvest;
     refresh_maxn=max(refresh_demographic_date, refresh_enrollment_date,
                         refresh_encounter_date, refresh_diagnosis_date,
                         refresh_procedures_date, refresh_vital_date,
                         refresh_dispensing_date, refresh_lab_result_cm_date,
                         refresh_condition_date, refresh_pro_cm_date, 
                         refresh_prescribing_date, refresh_pcornet_trial_date,
                         refresh_death_date, refresh_death_cause_date);
     call symputx("mxrefreshn",refresh_maxn);
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
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_base.log" lrecl=200;
proc printto log=qlog  new ;
run ;

********************************************************************************;
* Create standard macros used in multiple queries
********************************************************************************;

*- Macro to create DATAMARTID, RESPONSE_DATE and QUERY_PACKAGE in every query -*;
%macro stdvar;
     length datamartid $20 query_package $8 response_date 4.;
     format response_date date9.;
     datamartid="&dmid";
     response_date="&sysdate"d;
     query_package="&dc";
%mend stdvar;

*- Macros to determine minimum/maximum date -*;
%macro minmax(idsn,var,var_tm,_n);

     proc means data=&idsn._dates nway noprint;
          var &var time&_n futureflag&_n preflag&_n invdtflag&_n imdtflag&_n invtmflag&_n;
          output out=m&idsn 
                 n=ndt ntm dum1 dum2 dum3 dum4 dum5
                 sum=dum6 dum7 futuresum presum invdtsum imdtsum invtmsum
                 min=mindt mintm dum8 dum9 dum10 dum11 dum12
                 max=maxdt maxtm dum13 dum14 dum15 dum16 dum17
                 p5=p5dt p5tm dum18 dum19 dum20 dum21 dum22
                 p95=p95dt p95tm dum23 dum24 dum25 dum26 dum27
                 mean=meandt meantm dum28 dum29 dum30 dum31 dum32
                 median=mediandt mediantm dum33 dum34 dum35 dum36 dum37
                 nmiss=nmissdt nmisstm dum38 dum39 dum40 dum41 dum42;
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
          n=strip(put(ndt,16.));
          nmiss=strip(put(nmissdt,16.));

          if futuresum>. then future_dt_n=strip(put(futuresum,16.));
          else future_dt_n=strip(put(0,16.));
          if presum>. then pre2010_n=strip(put(presum,16.));
          else pre2010_n=strip(put(0,16.));
          if invdtsum>. then invalid_n=strip(put(invdtsum,16.));
          else invalid_n=strip(put(0,16.));
          if imdtsum>. then impl_n=strip(put(imdtsum,16.));
          else impl_n=strip(put(0,16.));

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
          n_tm=strip(put(ntm,16.));
          nmiss_tm=strip(put(nmisstm,16.));

          if invtmsum>. then invalid_n_tm=strip(put(invtmsum,16.));
          else invalid_n_tm=strip(put(0,16.));

          keep datamartid response_date query_package dataset tag tag_tm 
               min max p5 p95 mean median n nmiss future_dt_n pre2010_n 
               invalid_n impl_n min_tm max_tm p5_tm p95_tm mean_tm median_tm
               n_tm nmiss_tm invalid_n_tm;
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
            * convert to character *;
            all_n=strip(put(_all_n,16.));
            distinct_n=strip(put(_distinct_n,16.));
            null_n=strip(put(_null_n,16.));
            output;
          end;

          keep datamartid response_date query_package dataset tag all_n
               distinct_n null_n _all_n _distinct_n;
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

             &_recordn=input(record_nc,best.);
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
         title "Query name:  %upcase(&pdsn) (Limited to most frequent &_obs observations)";
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
          save xtbl_mdata_idsn elapsed cdm_version payer_type facility_type loinc
               _route prov_spec prov_group rxnorm_cui_ref _dose_form progress 
               vxbodysite vxmanu &savedsn dummy_y_missing dummy_ym_missing 
               address_rank / memtype=data;
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
                     description="All queries have finished processing";
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

     value $nulltwo_
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
    
     value $_abn
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

     value $obs_mod
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

     value $disdisp
       "A"="A"
       "E"="E"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
     ;

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

    value $pdx
       "P"="P"
       "S"="S"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
    ;

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

    value $medtype
       "ND"="ND"
       "RX"="RX"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
    ;

    value $obsclintype
       "LC"="LC"
       "SM"="SM"
       "ZZZA"="NI"
       "ZZZB"="UN"
       "ZZZC"="OT"
       "ZZZD"="NULL or missing"
       "ZZZE"="Values outside of CDM specifications"
    ;
    
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

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc_base;

%global savedeath savedemog saveenc savediag saveproc saveenr savevit savepres 
        savedisp savelab savecond saveprocm savedeathc savemedadm 
        saveobsgen saveprov saveldsadrs saveimmune savehashtoken savelabhistory
        _ycondition _ydeath _ydeathc _ydemographic _ydiagnosis _ydispensing 
        _yencounter _yenrollment _yharvest _yhash_token _yimmunization 
        _ylab_result_cm _ylab_history _yldsadrs _ymed_admin _yobs_clin _yobs_gen 
        _yprescribing _yprocedures _ypro_cm _yprovider _yvital _yptrial 
        raw_lab_name savepenc mxrefreshn savextbl _part1_mstring _part2_mstring
        _cnt_part1 _cnt_part2 _dcpart
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
*- DC reference data sets -*;
proc cimport library=work infile=dcref;
run;

proc cimport library=work infile=loinc;
run;

*- Sort RXNORM_CUI_REF for future merging -*;
proc sort data=rxnorm_cui_ref out=rxnorm_cui_ref;
     by rxnorm_cui;
run;

*- Add quotes around each version -*;
data cdm_version;
     length code_w_str $20;
     set cdm_version;

     code_w_str="'"||strip(code)||"'";
run;

*- Create formats from specific reference datasets -*;
%macro fmt(fmt=,sheet=,keyword=0);

    * code needs to be converted to character *;
    %if &fmt=payer_type %then %do;
        data &sheet;
             length code $100;
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

    proc sort data=&fmt nodupkey;
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
%fmt(fmt=payer_type,sheet=PAYER_TYPE)
%fmt(fmt=facility_type,sheet=FACILITY_TYPE)
%fmt(fmt=_unit,sheet=UNIT)
%fmt(fmt=_qual,sheet=QUAL,keyword=1)
%fmt(fmt=_route,sheet=ROUTE)
%fmt(fmt=_source,sheet=SPECIMEN_SOURCE)
%fmt(fmt=_dose_form,sheet=DOSE_FORM)
%fmt(fmt=prov_spec,sheet=PROVIDER_SPECIALTY_PRIMARY)
%fmt(fmt=vxbodysite,sheet=VX_BODY_SITE)
%fmt(fmt=vxmanu,sheet=VX_MANUFACTURER)
%fmt(fmt=state,sheet=STATE)

********************************************************************************;
*- Create a dummy dataset of Null or missing record for _YM queries
********************************************************************************;
data dummy_y_missing(keep=col1 _freq_ distinct:);
     col1="ZZZA";
     _freq_=0;
     distinct_patid=0;
     distinct_encid=0;
     output;
run;

data dummy_ym_missing(keep=year_month _type_ _freq_);
     _type_=1;
     _freq_=0;
     year_month=99999999;
     output;
run;

********************************************************************************;
*- Create macro variable containing all XTBL_L3_OBS dataset names
********************************************************************************;
%macro xo(dsn);
    data &dsn;
    run;
%mend xo;
%xo(xtbl_obs)
%xo(xtbl_l3_obs_ageyrsdist1)
%xo(xtbl_l3_obs_ageyrsdist2)
%xo(xtbl_l3_obs_genderdist)
%xo(xtbl_l3_obs_hispdist)
%xo(xtbl_l3_obs_orientdist)
%xo(xtbl_l3_obs_patpreflang)
%xo(xtbl_l3_obs_racedist)
%xo(xtbl_l3_obs_sexdist)

%let savextbl=xtbl_obs xtbl_l3_obs_ageyrsdist1 xtbl_l3_obs_ageyrsdist2 
              xtbl_l3_obs_genderdist xtbl_l3_obs_hispdist xtbl_l3_obs_orientdist
              xtbl_l3_obs_patpreflang xtbl_l3_obs_racedist xtbl_l3_obs_sexdist;

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
%if %upcase(&_part1)=YES %then %do;
    data _null_;
        length _part1_string $3000;
        set dc_tables(where=(upcase(dcpart_macro_value)="PART1")) end=eof;
        retain _part1_string; 
        retain _cnt_part1 0;
        
        _cnt_part1=_cnt_part1+1;
        _part1_string=strip(_part1_string) || ' ' || upcase(strip(table));
        
        if eof then do;
           call symput('_part1_mstring',strip(_part1_string));
           call symputx('_cnt_part1',_cnt_part1);
        end;
    run;
%end;
%else %if %upcase(&_part1)^=YES %then %do;
    data _null_;
           call symput('_part1_mstring',' ');
           call symputx('_cnt_part1',0);
    run;
%end;    
%put &_part1_mstring;

%if %upcase(&_part2)=YES %then %do;
    data _null_;
        length _part2_string $3000;
        set dc_tables(where=(upcase(dcpart_macro_value)="PART2")) end=eof;
        retain _part2_string;
        retain _cnt_part2 0;
        
        _cnt_part2=_cnt_part2+1;
        _part2_string=strip(_part2_string) || ' ' || upcase(strip(table));
        
        if eof then do;
           call symput('_part2_mstring',strip(_part2_string));
           call symputx('_cnt_part2',_cnt_part2);
        end;
    run;
%end;
%else %if %upcase(&_part2)^=YES %then %do;
    data _null_;
           call symput('_part2_mstring',' ');
           call symputx('_cnt_part2',0);
    run;
%end;    
%put &_part2_mstring;

%include "&qpath.infolder/data_curation_tables.sas";
%include "&qpath.infolder/data_curation_print.sas";

********************************************************************************;
* Macro end
********************************************************************************;
%mend dc_base;
%dc_base;
