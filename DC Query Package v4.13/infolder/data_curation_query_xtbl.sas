/*******************************************************************************
*  $Source: data_curation_query_xtbl $;
*    $Date: 2018/12/05
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V4.13 - 
*                                Cross table queries only
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. xtbl_l3_mismatch.sas7bdat)
*           2) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_xtbl.cpt)
*           3) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_xtbl.log)
*           4) SAS dataset of DataMart meta-data stored in /dmlocal
*                (datamart_all.sas7bdat)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro dc_xtbl;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_query_%lowcase(&_grp).log" lrecl=200;
proc printto log=qlog  %if %upcase(&_grp)=XTBL %then %do; new %end; ;
run ;

********************************************************************************;
* Create working formats used in multiple queries
********************************************************************************;
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
    
     * restrict data to within lookback period *;
     if death_date>=&lookback_dt or death_date=.;

     dthdate=put(death_date,date9.);
     deathid=cats(patid,'_',death_source);
run;

proc sort data=death;
     by patid death_date;
run;

%let savedeath=death;

%elapsed(end);
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
%end;
********************************************************************************;
* Bring in OBS_GEN and compress it
********************************************************************************;
%if &_yobs_gen=1 %then %do;
%let qname=obs_gen;
%elapsed(begin);

data obs_gen(compress=yes);
     set pcordata.obs_gen;

     * restrict data to within lookback period *;
     if obsgen_date>=&lookback_dt or obsgen_date=.;

     * for mismatch query *;
     providerid=obsgen_providerid;
run;

proc sort data=obs_gen;
     by patid;
run;

%let saveobsgen=obs_gen;

%elapsed(end);
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
%end;
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

     lab_loinc=strip(upcase(lab_loinc));

     * known test result *;
     if lab_loinc^=" " then do;
        known_test=1;

        if result_num^=. and result_modifier^=' ' then known_test_result_num=1;
        else known_test_result_num=.;
        
        if ((result_num^=.  and result_modifier^=' ') or 
           result_qual in ("BORDERLINE" "POSITIVE" "NEGATIVE" "UNDETERMINED")) then known_test_result=1;
        else known_test_result=.;

        if result_num^=. and result_modifier^=' ' and specimen_source^=' ' then known_test_result_num_source=1;
        else known_test_result_num_source=.;
        
        if result_num^=. and result_modifier^=' ' and result_unit^=' ' then known_test_result_num_unit=1;
        else known_test_result_num_unit=.;
        
        if result_num^=. and result_modifier^=' ' and specimen_source^=' ' and result_unit^=' ' then known_test_result_num_srce_unit=1;
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
%end;
********************************************************************************;
* BEGIN XTBL QUERIES
********************************************************************************;

********************************************************************************;
* XTBL_L3_RACE_ENC
********************************************************************************;
%if &_ydemographic=1 and &_yencounter=1 %then %do;

%let qname=xtbl_l3_race_enc;
%elapsed(begin);

proc sort data=encounter(keep=patid admit_date) out=enc_2010 nodupkey;
     by patid;
     where admit_date>'31DEC2011'd;
run;

proc sort data=demographic(keep=patid race) out=xtbl_demog nodupkey;
     by patid;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     merge xtbl_demog enc_2010(in=e);
     by patid;
     if e;

     if upcase(race) in ("01" "02" "03" "04" "05" "06" "07") then col1=race;
     else if race in ("NI") then col1="ZZZA";
     else if race in ("UN") then col1="ZZZB";
     else if race in ("OT") then col1="ZZZC";
     else if race=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $race.;
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
     race=put(col1,$race.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package race record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, race, record_n, record_pct, 
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen
               &savecond &saveprocm);

%end;

********************************************************************************;
* XTBL_L3_SMOKING_ENC - retired V3.12
********************************************************************************;
/*
%if &_yvital=1 and &_yencounter=1 %then %do;

%let qname=xtbl_l3_smoking_enc;
%elapsed(begin);

proc sort data=encounter(keep=patid admit_date) out=enc_2010 nodupkey;
     by patid;
     where admit_date>'31DEC2011'd;
run;

proc sort data=vital(keep=patid smoking) out=xtbl_vital nodupkey;
     by patid;
run;

*- Derive categorical variable -*;
data data;
     length col1 $4;
     merge xtbl_vital enc_2010(in=e);
     by patid;
     if e;

     if smoking in ("01" "02" "03" "04" "05" "06" "07" "08") then col1=smoking;
     else if smoking in ("NI") then col1="ZZZA";
     else if smoking in ("UN") then col1="ZZZB";
     else if smoking in ("OT") then col1="ZZZC";
     else if smoking=" " then col1="ZZZD";
     else col1="ZZZE";
    
     keep col1 patid;
run;

*- Derive statistics -*;
proc means data=data completetypes noprint missing;
     class col1/preloadfmt;
     output out=stats;
     format col1 $smoke.;
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
     smoking=put(col1,$smoke.);

     * apply threshold *;
     %threshold(nullval=col1^="ZZZD")

     * counts *;
     record_n=strip(put(_freq_,threshold.));
     distinct_patid_n=strip(put(distinct_patid,threshold.));
     if _freq_>&threshold then record_pct=put((_freq_/denom)*100,6.1);

     keep datamartid response_date query_package smoking record_n record_pct
          distinct_patid_n;
run;

*- Order variables -*;
proc sql;
     create table dmlocal.&qname as select
         datamartid, response_date, query_package, smoking, record_n, record_pct, 
         distinct_patid_n
     from query;
quit;

%elapsed(end);

*- Clear working directory -*;
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savecond &saveprocm);

%end;
*/
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
%nonuni(udsn=med_admin,uvar=encounterid);
%nonuni(udsn=obs_clin,uvar=encounterid);
%nonuni(udsn=obs_gen,uvar=encounterid);

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
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen
               &savecond &saveprocm);

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
               &savepres &savedisp &savelab &savemedadm &saveobsclin  &saveobsgen
               &savecond &saveprocm xlab_encounter);

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
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen
               &savecond &saveprocm);

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
        "MED_ADMIN"="16"
        "OBS_CLIN"="17"
        "OBS_GEN"="18"
        "PROVIDER"="19"
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
     select max(length) into :maxlength_enc from sashelp.vcolumn 
        where libname="PCORDATA" and name="ENCOUNTERID";
     select max(length) into :maxlength_prov from sashelp.vcolumn 
        where libname="PCORDATA" and name="PROVIDERID";
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
     create table orphan_provid as select unique providerid
        from pcordata.provider
        where providerid^=" "
        order by providerid;
quit;

%macro orph_enc;
    %do oe = 1 %to &workdata_count;
        %let _orph=%scan(&workdata,&oe,"|");

        %if &_orph=DIAGNOSIS or &_orph=PROCEDURES or &_orph=VITAL or 
            &_orph=LAB_RESULT_CM or &_orph=PRESCRIBING or
            &_orph=CONDITION or &_orph=PRO_CM or &_orph=MED_ADMIN or 
            &_orph=OBS_CLIN or &_orph=OBS_GEN %then %do;

            * orphan rows *;
            proc sql noprint;
                 create table orph_&_orph as select unique encounterid
                    from pcordata.&_orph
                    where encounterid^=" "
                    order by encounterid;
            quit;

            data orph_&_orph(keep=memname ord recordn);
                 length encounterid $ &maxlength_enc memname $32;
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

            * mismatch rows - encounter *;
            %if &_orph=DIAGNOSIS or &_orph=PROCEDURES %then %do;
                proc sort data=&_orph(keep=encounterid enc_type admit_date)
                     out=mismatch_&_orph;
                     by encounterid;
                     where encounterid^=" ";
                run;

                data mismatch_&_orph(keep=memname ord mis:);
                     length encounterid $ &maxlength_enc memname $32;
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
        * mismatch rows - provider *;
        %if &_yprovider=1 %then %do;
            %if &_orph=DIAGNOSIS or &_orph=ENCOUNTER or &_orph=MED_ADMIN or &_orph=OBS_CLIN or 
                &_orph=OBS_GEN or &_orph=PRESCRIBING or &_orph=PROCEDURES %then %do;                    
                proc sort data=&_orph(keep=providerid) out=mismatch_providerid_&_orph;
                     by providerid;
                     where providerid^=" ";
                run;

                data mismatch_providerid_&_orph(keep=memname ord mis:);
                     length providerid $ &maxlength_prov memname $32;
                     merge mismatch_providerid_&_orph(in=o) 
                           orphan_provid(in=op) end=eof;
                     by providerid;
                     retain mis_n 0;
                     if o and not op and first.providerid then mis_n=mis_n+1;

                     if eof then do;
                         memname="&_orph";
                         ord=put(upcase(memname),$ord.);
                         output;
                     end;
                run;

                * compile into one dataset *;
                proc append base=mismatch_providerid data=mismatch_providerid_&_orph;
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

proc sort data=mismatch_providerid;
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
            &_orphp=PCORNET_TRIAL or &_orphp=MED_ADMIN or &_orphp=OBS_CLIN or 
            &_orphp=OBS_GEN %then %do;

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
         mismatch_providerid(in=mpre where=(memname="ENCOUNTER") rename=(mis_n=recordn))
         mismatch_providerid(in=mprd where=(memname="DIAGNOSIS") rename=(mis_n=recordn))
         mismatch_providerid(in=mprp where=(memname="PROCEDURES") rename=(mis_n=recordn))
         mismatch_providerid(in=mprpr where=(memname="PRESCRIBING") rename=(mis_n=recordn))
         mismatch_providerid(in=mprma where=(memname="MED_ADMIN") rename=(mis_n=recordn))
         mismatch_providerid(in=mproc where=(memname="OBS_CLIN") rename=(mis_n=recordn))
         mismatch_providerid(in=mprog where=(memname="OBS_GEN") rename=(mis_n=recordn))
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
     else if mpre then do;
        dataset="PROVIDER and ENCOUNTER";
        tag="PROVIDERID Orphan";
     end;
     else if mprd then do;
        dataset="PROVIDER and DIAGNOSIS";
        tag="PROVIDERID Orphan";
     end;
     else if mprp then do;
        dataset="PROVIDER and PROCEDURES";
        tag="PROVIDERID Orphan";
     end;
     else if mprpr then do;
        dataset="PROVIDER and PRESCRIBING";
        tag="PROVIDERID Orphan";
     end;
     else if mprma then do;
        dataset="PROVIDER and MED_ADMIN";
        tag="PROVIDERID Orphan";
     end;
     else if mproc then do;
        dataset="PROVIDER and OBS_CLIN";
        tag="PROVIDERID Orphan";
     end;
     else if mprog then do;
        dataset="PROVIDER and OBS_GEN";
        tag="PROVIDERID Orphan";
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
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen 
               &savecond &saveprocm);

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
    %minmax(idsn=demographic,var=birth_date,var_tm=birth_time)
    %xminmax(idsn=demographic_birth_date)
%end;
%if &_yencounter=1 %then %do;
    %minmax(idsn=encounter,var=admit_date,var_tm=admit_time)
    %minmax(idsn=encounter,var=discharge_date,var_tm=discharge_time)
    %xminmax(idsn=encounter_admit_date)
    %xminmax(idsn=encounter_discharge_date)
%end;
%if &_ydiagnosis=1 %then %do;
    %minmax(idsn=diagnosis,var=admit_date,var_tm=.)
    %xminmax(idsn=diagnosis_admit_date)
%end;
%if &_yprocedures=1 %then %do;
    %minmax(idsn=procedures,var=admit_date,var_tm=.)
    %minmax(idsn=procedures,var=px_date,var_tm=.)
    %xminmax(idsn=procedures_admit_date)
    %xminmax(idsn=procedures_px_date)
%end;
%if &_yvital=1 %then %do;
    %minmax(idsn=vital,var=measure_date,var_tm=measure_time)
    %xminmax(idsn=vital_measure_date)
%end;
%if &_yenrollment=1 %then %do;
    %minmax(idsn=enrollment,var=enr_start_date,var_tm=.)
    %minmax(idsn=enrollment,var=enr_end_date,var_tm=.)
    %xminmax(idsn=enrollment_enr_start_date)
    %xminmax(idsn=enrollment_enr_end_date)
%end;
%if &_ydeath=1 %then %do;
    %minmax(idsn=death,var=death_date,var_tm=.)
    %xminmax(idsn=death_death_date)
%end;
%if &_ydispensing=1 %then %do;
    %minmax(idsn=dispensing,var=dispense_date,var_tm=.)
    %xminmax(idsn=dispensing_dispense_date)
%end;
%if &_yprescribing=1 %then %do;
    %minmax(idsn=prescribing,var=rx_order_date,var_tm=rx_order_time)
    %minmax(idsn=prescribing,var=rx_start_date,var_tm=.)
    %minmax(idsn=prescribing,var=rx_end_date,var_tm=.)
    %xminmax(idsn=prescribing_rx_order_date)
    %xminmax(idsn=prescribing_rx_start_date)
    %xminmax(idsn=prescribing_rx_end_date)
%end;
%if &_ylab_result_cm=1 %then %do;
    %minmax(idsn=lab_result_cm,var=lab_order_date,var_tm=.)
    %minmax(idsn=lab_result_cm,var=specimen_date,var_tm=specimen_time)
    %minmax(idsn=lab_result_cm,var=result_date,var_tm=result_time)
    %xminmax(idsn=lab_result_cm_lab_order_date)
    %xminmax(idsn=lab_result_cm_specimen_date)
    %xminmax(idsn=lab_result_cm_result_date)
%end;
%if &_ycondition=1 %then %do;
    %minmax(idsn=condition,var=report_date,var_tm=.)
    %minmax(idsn=condition,var=resolve_date,var_tm=.)
    %minmax(idsn=condition,var=onset_date,var_tm=.)
    %xminmax(idsn=condition_report_date)
    %xminmax(idsn=condition_resolve_date)
    %xminmax(idsn=condition_onset_date)
%end;
%if &_ypro_cm=1 %then %do;
    %minmax(idsn=pro_cm,var=pro_date,var_tm=pro_time)
    %xminmax(idsn=pro_cm_pro_date)
%end;
%if &_ymed_admin=1 %then %do;
    %minmax(idsn=med_admin,var=medadmin_start_date,var_tm=medadmin_start_time)
    %minmax(idsn=med_admin,var=medadmin_stop_date,var_tm=medadmin_stop_time)
    %xminmax(idsn=med_admin_medadmin_start_date)
    %xminmax(idsn=med_admin_medadmin_stop_date)
%end;
%if &_yobs_clin=1 %then %do;
    %minmax(idsn=obs_clin,var=obsclin_date,var_tm=obsclin_time)
    %xminmax(idsn=obs_clin_obsclin_date)
%end;
%if &_yobs_gen=1 %then %do;
    %minmax(idsn=obs_gen,var=obsgen_date,var_tm=obsgen_time)
    %xminmax(idsn=obs_gen_obsgen_date)
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
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen &savecond &saveprocm
               demographic_birth_date encounter_admit_date encounter_discharge_date
               vital_measure_date lab_result_cm_specimen_date lab_result_cm_result_date
               prescribing_rx_order_date pro_cm_pro_date med_admin_medadmin_start_date
               med_admin_medadmin_stop_date obs_clin_obsclin_date obs_gen_obsgen_date);

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
%if &_ymed_admin=1 %then %do;
    %xminmax(idsn=med_admin_medadmin_start_date)
    %xminmax(idsn=med_admin_medadmin_stop_date)
%end;
%if &_yobs_clin=1 %then %do;
    %xminmax(idsn=obs_clin_obsclin_date)
%end;
%if &_yobs_gen=1 %then %do;
    %xminmax(idsn=obs_gen_obsgen_date)
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
%clean(savedsn=&savedeath &savedemog &saveenc &savediag &saveproc &saveenr &savevit 
               &savepres &savedisp &savelab &savemedadm &saveobsclin &saveobsgen 
               &savecond &saveprocm);

********************************************************************************;
* XTBL_L3_DATE_LOGIC
********************************************************************************;
%let qname=xtbl_l3_date_logic;
%elapsed(begin);

*- Macro to compare dates against birth and death -*;
%macro xmin(idsn,var);

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
%if &_ymed_admin=1 %then %do;
    %xmin(idsn=med_admin,var=medadmin_start_date)
%end;
%if &_yobs_clin=1 %then %do;
    %xmin(idsn=obs_clin,var=obsclin_date)
%end;
%if &_yobs_gen=1 %then %do;
    %xmin(idsn=obs_gen,var=obsgen_date)
%end;

*- Macro to compare dates within a data table -*;
%macro within(idsn,var,var2);

proc sort data=&idsn(keep=patid &var &var2) out=&idsn&var;
     by patid;
run;

data &idsn&var(keep=patid cnt_within);
     set &idsn&var end=eof;
     by patid;
     
     retain cnt_within;
     if first.patid then cnt_within=0;
     if &var^=. and &var2^=. and &var<&var2 then cnt_within=1;
    
     if last.patid;
run;

proc means data=&idsn&var nway noprint;
     var cnt_within;
     output out=sum&idsn&var sum=sum_within;
run;

%mend within;
%if &_yencounter=1 %then %do;
    %within(idsn=encounter,var=discharge_date,var2=admit_date)
%end;
%if &_yprocedures=1 %then %do;
    %within(idsn=procedures,var=px_date,var2=admit_date)
%end;

data query;
     length date_comparison $50 distinct_patid_n $20;
     set data(in=b rename=(b_cnt=distinct_patid))
         data(in=d rename=(d_cnt=distinct_patid))
         sumprocedurespx_date(in=pp rename=(sum_within=distinct_patid))
         sumencounterdischarge_date(in=ed rename=(sum_within=distinct_patid))
     ;

     * drop death-death check *;
     if d and compvar="DEATH_DATE" then delete;

     * call standard variables *;
     %stdvar

     * create comparision variable *;
     if b then date_comparison=strip(compvar) || " < BIRTH_DATE";
     else if d then date_comparison=strip(compvar) || " > DEATH_DATE";
     else if ed then date_comparison="ADMIT_DATE > DISCHARGE_DATE";
     else if pp then date_comparison="PX_DATE < ADMIT_DATE";

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
            pro_date_mgmt death_date_mgmt medadmin_start_date_mgmt 
            medadmin_stop_date_mgmt obsclin_date_mgmt obsgen_date_mgmt $50 
            datastore $5 operating_system sas_version $100;
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
                          refresh_death_cause_date=refresh_death_cause_daten
                          refresh_med_admin_date=refresh_med_admin_daten
                          refresh_obs_clin_date=refresh_obs_clin_daten
                          refresh_obs_gen_date=refresh_obs_gen_daten
                          refresh_provider_date=refresh_provider_daten));

     * table variables *;
     query_package="&dc";
     lookback_months=%eval(&lookback*12);
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
              resolve_date_mgmt pro_date_mgmt death_date_mgmt 
              medadmin_start_date_mgmt medadmin_stop_date_mgmt obsclin_date_mgmt
              obsgen_date_mgmt;
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
                   refresh_death_date refresh_death_cause_date
                   refresh_med_admin_date refresh_obs_clin_date
                   refresh_obs_gen_date refresh_provider_date;
     array rfn refresh_demographic_daten refresh_enrollment_daten 
               refresh_encounter_daten refresh_diagnosis_daten 
               refresh_procedures_daten refresh_vital_daten 
               refresh_dispensing_daten refresh_lab_result_cm_daten 
               refresh_condition_daten refresh_pro_cm_daten 
               refresh_prescribing_daten refresh_pcornet_trial_daten
               refresh_death_daten refresh_death_cause_daten
               refresh_med_admin_daten refresh_obs_clin_daten
               refresh_obs_gen_daten refresh_provider_daten;
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
                         refresh_death_daten, refresh_death_cause_daten,
                         refresh_med_admin_daten, refresh_obs_clin_daten,
                         refresh_obs_gen_daten, refresh_provider_daten),yymmdd10.);

     low_cell_cnt=compress(put(&threshold,16.));

     array v networkid network_name datamartid datamart_name datamart_platform
             datamart_claims datamart_ehr birth_date_mgmt enr_start_date_mgmt 
             enr_end_date_mgmt admit_date_mgmt discharge_date_mgmt px_date_mgmt
             rx_order_date_mgmt rx_start_date_mgmt rx_end_date_mgmt 
             dispense_date_mgmt lab_order_date_mgmt specimen_date_mgmt 
             result_date_mgmt measure_date_mgmt onset_date_mgmt report_date_mgmt
             resolve_date_mgmt pro_date_mgmt death_date_mgmt 
             medadmin_start_date_mgmt medadmin_stop_date_mgmt obsclin_date_mgmt
             obsgen_date_mgmt;
        do v1 = 1 to dim(v);
           if v{v1}^=' ' then v{v1}=v{v1};
           else if v{v1}=' ' then v{v1}="NULL or missing";
        end;

     if vsops^=" " then operating_system=strip(vsops);
     if vsas^=" " then sas_version=strip(vsas);

     if %upcase("&_engine")="V9" and %upcase("&_mtype")="DATA" then datastore="SAS";
     else datastore="RDBMS";

     lookback_date=put(&lookback_dt,yymmdd10.);
run;

*- Normalize data -*;
proc transpose data=data out=query(rename=(_name_=name col1=value));
     var networkid network_name datamartid datamart_name datamart_platform
         cdm_version datamart_claims datamart_ehr birth_date_mgmt 
         enr_start_date_mgmt enr_end_date_mgmt admit_date_mgmt 
         discharge_date_mgmt px_date_mgmt rx_order_date_mgmt rx_start_date_mgmt
         rx_end_date_mgmt dispense_date_mgmt lab_order_date_mgmt 
         specimen_date_mgmt result_date_mgmt measure_date_mgmt onset_date_mgmt
         report_date_mgmt resolve_date_mgmt pro_date_mgmt death_date_mgmt 
         medadmin_start_date_mgmt medadmin_stop_date_mgmt obsclin_date_mgmt
         obsgen_date_mgmt refresh_demographic_date refresh_enrollment_date
         refresh_encounter_date refresh_diagnosis_date refresh_procedures_date 
         refresh_vital_date refresh_dispensing_date refresh_lab_result_cm_date 
         refresh_condition_date refresh_pro_cm_date refresh_prescribing_date
         refresh_pcornet_trial_date refresh_death_date refresh_death_cause_date
         refresh_med_admin_date refresh_obs_clin_date refresh_obs_gen_date 
         refresh_provider_date refresh_max low_cell_cnt operating_system 
         query_package lookback_months lookback_date response_date sas_version 
         sas_base sas_graph sas_stat sas_ets sas_af sas_iml sas_connect sas_mysql 
         sas_odbc sas_oracle sas_postgres sas_sql sas_teradata datastore;
run;

*- Assign attributes -*;
data query;
     length name $125;
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
     set lab_result_cm(keep=result_date patid);

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

%elapsed(b_or_e=end,last=Y);

*- Clear working directory -*;
%clean;

%end;

*******************************************************************************;
* Dataset for printing number of observations in each dataset
*******************************************************************************;
data datamart_all;
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
* Macro end
********************************************************************************;
%mend dc_xtbl;
%dc_xtbl;
