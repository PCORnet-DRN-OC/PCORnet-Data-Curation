/*******************************************************************************
*  $Source: normalization $;
*    $Date: 2024/11/25
*    Study: PCORnet
*
*  Purpose: To consolidate PCORnet Data Curation Query Package V6.15
*              results into one standardized dataset for reporting usage
* 
*   Inputs: DC Query Package SAS datasets
*
*  Outputs: <dmid>_dc_norm_<response date>.sas7bdat
*
*  Requirements:  
*                1) Program run in SAS 9.3 or higher
*                2) The entire DC Query Package of datasets in defined &qpath
*******************************************************************************/
options validvarname=upcase mlogic mprint symbolgen;

********************************************************************************;
*- Flush anything/everything that might be in WORK
*******************************************************************************;
proc datasets  kill noprint;
quit;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname pcordata "&dpath" access=readonly;
libname dmlocal "&qpath.dmlocal";
filename edc_ref "&qpath./infolder/edc_reference.cpt";

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro norm;

********************************************************************************;
* Create macro variables which queries to process
********************************************************************************;
%if %upcase(&_part1)=YES and %upcase(&_part2)=YES %then %do;
    %let _dcpart=_all;
%end;
%if %upcase(&_part1)=YES and %upcase(&_part2)^=YES %then %do;
    %let _dcpart=_dcpart1;
%end;
%if %upcase(&_part1)^=YES and %upcase(&_part2)=YES %then %do;
    %let _dcpart=_dcpart2;
%end;
%put &_dcpart;

********************************************************************************;
* Create macro variables for DataMart ID and response date
********************************************************************************;
data _null_;
     %if %upcase(&_dcpart)=_DCPART1 or %upcase(&_dcpart)=_ALL %then %do;
         set dmlocal.dem_l3_n(keep=datamartid tag where=(tag="PATID"));
         call symput("dmid",strip(datamartid));
     %end;
     %if %upcase(&_dcpart)=_DCPART2 or %upcase(&_dcpart)=_ALL %then %do;
         set dmlocal.xtbl_l3_metadata;
         if name="DATAMARTID" then call symput("dmid",strip(value));
     %end;
     today="&sysdate"d;
     call symput("r_date",compress(put(today,yymmdd10.),'-'));
run;

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename qlog "&qpath.drnoc/%upcase(&dmid)_&r_date._normalization.log" lrecl=200;
proc printto log=qlog  new ;
run ;

********************************************************************************;
* Create working formats 
********************************************************************************;
proc format;
     value $dsn
       'COND'='CONDITION'
       'DEATH'='DEATH'
       'DEATHC'='DEATH_CAUSE'
       'DEM'='DEMOGRAPHIC'
       'DIA'='DIAGNOSIS'
       'DISP'='DISPENSING'
       'ENC'='ENCOUNTER'
       'ENR'='ENROLLMENT'
       'HASH'='HASH_TOKEN'
       'IMMUNE'='IMMUNIZATION'
       'LAB'='LAB_RESULT_CM'
       'LDSADRS'='LDS_ADDRESS_HISTORY'
       'MEDADM'='MED_ADMIN'
       'OBSCLIN'='OBS_CLIN'
       'OBSGEN'='OBS_GEN'
       'PRES'='PRESCRIBING'
       'PRO'='PROCEDURES'
       'PROCM'='PRO_CM'
       'PROV'='PROVIDER'
       'VIT'='VITAL'
       ;
run;

********************************************************************************;
* Determine which datasets are available
********************************************************************************;
proc contents data=pcordata._all_ noprint out=pcordata_all;
run;

proc sort data=pcordata_all(keep=memname) out=pcordata nodupkey;
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
     select count(*) into :_yhash_token from pcordata
            where memname="HASH_TOKEN";
     select count(*) into :_yharvest from pcordata
            where memname="HARVEST";
     select count(*) into :_yimmunization from pcordata
            where memname="IMMUNIZATION";
     select count(*) into :_ylab_history from pcordata
            where memname="LAB_HISTORY";
     select count(*) into :_ylab_result_cm from pcordata
            where memname="LAB_RESULT_CM";
     select count(*) into :_ylab_raw_name from pcordata_all
            where memname="LAB_RESULT_CM" and name="RAW_LAB_NAME";
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
*- Macro to convert each DC query dataset to standard format -*;
********************************************************************************;

* initialize macro variables for determine maximum length of category variables *;
%let iter=0;
%let maxlen_cat=0;
%let maxlen_ccat=0;
%let maxlen_ccat2=0;

%macro _recordn(dsn=,type=,var=,scat=,scat2=,addvar=,stdvar=1);
    data &dsn;
        length dc_name $100 table resultc statistic variable cross_variable 
               cross_variable2 $50 category $255 cross_category: $100;
        set dmlocal.&dsn %if &stdvar=1 %then %do; 
                           (drop=datamartid query_package response_date) %end; ;

        * create common variables based upon DC query name *;
        dc_name="&dsn";
        table=put(scan("&dsn",1,'_'),$dsn.);

        * create common variables based upon type of query *;
        %if &type=_recordn %then %do;
            variable="&var";
            category=strip(&var);
            cross_variable=" ";
            cross_category=" ";
            cross_variable2=" ";
            cross_category2=" ";
        %end;
        %if &type=_recordn and &dsn=HASH_L3_TOKEN_AVAILABILITY %then %do;
            variable="&var";
            category=strip(&var);
            cross_variable=" ";
            cross_category="token_encryption_key";
            cross_variable2=" ";
            cross_category2=" ";
        %end;
        %else %if &type=_n %then %do;
            variable=strip(&var);
            category=" ";
            cross_variable=" ";
            cross_category=" ";
            cross_variable2=" ";
            cross_category2=" ";
        %end;
        %else %if &type=_stat %then %do;
            variable="&var";
            category=" ";
            cross_variable=" ";
            cross_category=" ";
            cross_variable2=" ";
            cross_category2=" ";
        %end;
        %else %if &type=_cross %then %do;
            variable="&var";
            category=strip(&var);
            cross_variable="&scat";
            cross_category=strip(&scat);
            cross_variable2=" ";
            cross_category2=" ";
        %end;
        %else %if &type=_cross2 %then %do;
            variable="&var";
            category=strip(&var);
            cross_variable="&scat";
            cross_category=strip(&scat);
            cross_variable2="&scat2";
            cross_category2=strip(&scat2);
        %end;
        %else %if &type=_xtbl %then %do;
            table=dataset;
            variable=strip(&var);
            category=" ";
            cross_variable=" ";
            cross_category=" ";
            cross_variable2=" ";
            cross_category2=" ";
        %end;
    
        * array result variables to transpose *;
        array abc{*} &addvar;
        do i=1 to dim(abc);
          * write name of variable to variable result *;
          call vname(abc{i},statistic);
          * over-write for this type of query *;
          %if &type=_stat %then %do;
              statistic=strip(stat);
          %end;

          * create character and numeric result variables *;
          resultc=strip(abc{i});
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

          * length of category variables *;
        
          * output as transposed records *;
          output;

        end;

        keep dc_name table variable cross_variable cross_variable2 category cross_category 
             cross_category2 statistic resultc resultn;
    run;

    *- gets skipped for first iteration -*;
    %if &iter=1 %then %do;
        * put the length from the current dsn into macro variables *;
        proc contents data=&dsn out=c&dsn(keep=name length) noprint;
        run;
        
        data _null_;
             set c&dsn(where=(name in ("CATEGORY" "CROSS_CATEGORY" "CROSS_CATEGORY2")));
            
             if name="CATEGORY" then call symputx('len_cat',length);
             else if name="CROSS_CATEGORY" then call symputx('len_ccat',length);
             else if name="CROSS_CATEGORY2" then call symputx('len_ccat2',length);
        run;
        
        * reset the lengths to the maximum of the current or previous *;
        %macro catlength(var,lvar,lmvar,mvar);
            %if &lmvar>=&lvar %then %do;
                * increase current dsn variable length *;
                data &dsn;
                     length &var $&lmvar;
                     set &dsn;
                run;
            %end;
            %else %do;
                * increase query dsn variable length *;
                data query;
                     length &var $&lvar;
                     set query;
                run;
                * reset macro variable to current dsn variable length *;
                data _null_;
                      call symput("&mvar",strip(put(&lvar,8.)));
                run;
            %end;
        %mend catlength;
        %catlength(var=category,lvar=&len_cat,lmvar=&maxlen_cat,mvar=maxlen_cat);
        %catlength(var=cross_category,lvar=&len_ccat,lmvar=&maxlen_ccat,mvar=maxlen_ccat);
        %catlength(var=cross_category2,lvar=&len_ccat2,lmvar=&maxlen_ccat2,mvar=maxlen_ccat2);
    %end;
    *- after first call, set flag to process previous loop -*;
    %let iter=1;

    * append to one like data structure *;
    proc append base=query data=&dsn;
    run;

%mend _recordn;

********************************************************************************;
*- Macro for conditional code on availability of CDM datasets -*;
********************************************************************************;
%macro cdm;

*- print if Part 1 or All -*;
%if %upcase(&_dcpart)=_DCPART1 or %upcase(&_dcpart)=_ALL %then %do;

    %if &_ycondition=1 %then %do;
        %_recordn(dsn=COND_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=COND_L3_STATUS,type=_recordn,var=CONDITION_STATUS,addvar=record_n record_pct)
        %_recordn(dsn=COND_L3_TYPE,type=_recordn,var=CONDITION_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=COND_L3_SOURCE,type=_recordn,var=CONDITION_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_ydeath=1 %then %do;
        %_recordn(dsn=DEATH_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=DEATH_L3_IMPUTE,type=_recordn,var=DEATH_DATE_IMPUTE,addvar=record_n record_pct)
        %_recordn(dsn=DEATH_L3_SOURCE,type=_recordn,var=DEATH_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=DEATH_L3_MATCH,type=_recordn,var=DEATH_MATCH_CONFIDENCE,addvar=record_n record_pct)
    %end;
    %if &_ydeathc=1 %then %do;
        %_recordn(dsn=DEATHC_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=DEATHC_L3_CODE,type=_recordn,var=DEATH_CAUSE_CODE,addvar=record_n record_pct)
        %_recordn(dsn=DEATHC_L3_TYPE,type=_recordn,var=DEATH_CAUSE_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=DEATHC_L3_SOURCE,type=_recordn,var=DEATH_CAUSE_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=DEATHC_L3_CONF,type=_recordn,var=DEATH_CAUSE_CONFIDENCE,addvar=record_n record_pct)
    %end;
    %if &_ydemographic=1 %then %do;
        %_recordn(dsn=DEM_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=DEM_L3_AGEYRSDIST1,type=_stat,var=AGE,addvar=record_n)
        %_recordn(dsn=DEM_L3_AGEYRSDIST2,type=_recordn,var=AGE_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_GENDERDIST,type=_recordn,var=GENDER_IDENTITY,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_HISPDIST,type=_recordn,var=HISPANIC,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_RACEDIST,type=_recordn,var=RACE,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_SEXDIST,type=_recordn,var=SEX,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_ORIENTDIST,type=_recordn,var=SEXUAL_ORIENTATION,addvar=record_n record_pct)
        %_recordn(dsn=DEM_L3_PATPREFLANG,type=_recordn,var=PAT_PREF_LANGUAGE_SPOKEN,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_AGEYRSDIST1,type=_stat,var=AGE,addvar=record_n)
        %_recordn(dsn=DEM_OBS_L3_AGEYRSDIST2,type=_recordn,var=AGE_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_GENDERDIST,type=_recordn,var=GENDER_IDENTITY,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_HISPDIST,type=_recordn,var=HISPANIC,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_RACEDIST,type=_recordn,var=RACE,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_SEXDIST,type=_recordn,var=SEX,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_ORIENTDIST,type=_recordn,var=SEXUAL_ORIENTATION,addvar=record_n record_pct)
        %_recordn(dsn=DEM_OBS_L3_PATPREFLANG,type=_recordn,var=PAT_PREF_LANGUAGE_SPOKEN,addvar=record_n record_pct)
    %end;
    %if &_ydiagnosis=1 %then %do;
        %_recordn(dsn=DIA_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=DIA_L3_DX,type=_recordn,var=DX,addvar=record_n record_pct)
        %_recordn(dsn=DIA_L3_DXSOURCE,type=_recordn,var=DX_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=DIA_L3_PDX,type=_recordn,var=PDX,addvar=record_n record_pct)
        %_recordn(dsn=DIA_L3_DXPOA,type=_recordn,var=DX_POA,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DIA_L3_DXTYPE_ENCTYPE,type=_cross,var=ENC_TYPE,scat=DX_TYPE,addvar=record_n)
        %_recordn(dsn=DIA_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DIA_L3_DXTYPE_DXSOURCE,type=_cross,var=DX_TYPE,scat=DX_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_ydispensing=1 %then %do;
        %_recordn(dsn=DISP_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n valid_n)
        %_recordn(dsn=DISP_L3_DOSEUNIT,type=_recordn,var=DISPENSE_DOSE_DISP_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=DISP_L3_ROUTE,type=_recordn,var=DISPENSE_ROUTE,addvar=record_n record_pct)
        %_recordn(dsn=DISP_L3_SOURCE,type=_recordn,var=DISPENSE_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_yencounter=1 %then %do;
        %_recordn(dsn=ENC_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n valid_n)
        %_recordn(dsn=ENC_L3_ADMSRC,type=_recordn,var=ADMITTING_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_DRG_TYPE,type=_recordn,var=DRG_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_DISDISP,type=_recordn,var=DISCHARGE_DISPOSITION,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_DISSTAT,type=_recordn,var=DISCHARGE_STATUS,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_visit_n distinct_patid_n elig_record_n)
        %_recordn(dsn=ENC_L3_DASH2,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
        %_recordn(dsn=ENC_L3_PAYERTYPE1,type=_cross,var=PAYER_TYPE_PRIMARY_GRP,scat=PAYER_TYPE_PRIMARY,
                    addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_PAYERTYPE2,type=_cross,var=PAYER_TYPE_SECONDARY_GRP,scat=PAYER_TYPE_SECONDARY,
                    addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_FACILITYTYPE,type=_cross,var=FACILITY_TYPE_GRP,scat=FACILITY_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_yenrollment=1 %then %do;
        %_recordn(dsn=ENR_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=ENR_L3_BASEDIST,type=_recordn,var=ENR_BASIS,addvar=record_n record_pct)
        %_recordn(dsn=ENR_L3_CHART,type=_recordn,var=CHART,addvar=record_n record_pct)
    %end;
    %if &_yimmunization=1 %then %do;
        %_recordn(dsn=IMMUNE_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=IMMUNE_L3_CODE_CODETYPE,type=_cross,var=VX_CODE,scat=VX_CODE_TYPE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=IMMUNE_L3_STATUS,type=_recordn,var=VX_STATUS,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_STATUSREASON,type=_recordn,var=VX_STATUS_REASON,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_SOURCE,type=_recordn,var=VX_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_DOSEUNIT,type=_recordn,var=VX_DOSE_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_ROUTE,type=_recordn,var=VX_ROUTE,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_BODYSITE,type=_recordn,var=VX_BODY_SITE,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_MANUFACTURER,type=_recordn,var=VX_MANUFACTURER,addvar=record_n record_pct)
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %_recordn(dsn=LAB_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=LAB_L3_SOURCE,type=_recordn,var=SPECIMEN_SOURCE, addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_PRIORITY,type=_recordn,var=PRIORITY,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_LOC,type=_recordn,var=RESULT_LOC,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_PX_TYPE,type=_recordn,var=LAB_PX_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_QUAL,type=_recordn,var=RESULT_QUAL,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_MOD,type=_recordn,var=RESULT_MODIFIER,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_LOW,type=_recordn,var=NORM_MODIFIER_LOW,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_HIGH,type=_recordn,var=NORM_MODIFIER_HIGH,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_ABN,type=_recordn,var=ABN_IND,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_UNIT,type=_recordn,var=RESULT_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_RSOURCE,type=_recordn,var=LAB_RESULT_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_LSOURCE,type=_recordn,var=LAB_LOINC_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_yldsadrs=1 %then %do;
        %_recordn(dsn=LDSADRS_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n valid_n)
        %_recordn(dsn=LDSADRS_L3_ADRSUSE,type=_recordn,var=ADDRESS_USE,addvar=record_n record_pct)
        %_recordn(dsn=LDSADRS_L3_ADRSTYPE,type=_recordn,var=ADDRESS_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=LDSADRS_L3_ADRSPREF,type=_recordn,var=ADDRESS_PREFERRED,addvar=record_n record_pct)
        %_recordn(dsn=LDSADRS_L3_ADRSSTATE,type=_recordn,var=ADDRESS_STATE,addvar=record_n record_pct)
    %end;
    %if &_ymed_admin=1 %then %do;
        %_recordn(dsn=MEDADM_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=MEDADM_L3_ROUTE,type=_recordn,var=MEDADMIN_ROUTE,addvar=record_n record_pct)
        %_recordn(dsn=MEDADM_L3_SOURCE,type=_recordn,var=MEDADMIN_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=MEDADM_L3_TYPE,type=_recordn,var=MEDADMIN_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=MEDADM_L3_DOSEADMUNIT,type=_recordn,var=MEDADMIN_DOSE_ADMIN_UNIT,addvar=record_n record_pct)
    %end;
    %if &_yobs_clin=1 %then %do;
        %_recordn(dsn=OBSCLIN_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=OBSCLIN_L3_MOD,type=_recordn,var=OBSCLIN_RESULT_MODIFIER,addvar=record_n percent_n)
        %_recordn(dsn=OBSCLIN_L3_QUAL,type=_recordn,var=OBSCLIN_RESULT_QUAL,addvar=record_n percent_n)
/*    %_recordn(dsn=OBSCLIN_L3_SNOMED,type=_recordn,var=OBSCLIN_RESULT_SNOMED,addvar=record_n percent_n distinct_patid_n)*/
        %_recordn(dsn=OBSCLIN_L3_RUNIT,type=_recordn,var=OBSCLIN_RESULT_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=OBSCLIN_L3_TYPE,type=_recordn,var=OBSCLIN_TYPE,addvar=record_n percent_n)
        %_recordn(dsn=OBSCLIN_L3_SOURCE,type=_recordn,var=OBSCLIN_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=OBSCLIN_L3_ABN,type=_recordn,var=OBSCLIN_ABN_IND,addvar=record_n percent_n)
    %end;
    %if &_yobs_gen=1 %then %do;
        %_recordn(dsn=OBSGEN_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=OBSGEN_L3_MOD,type=_recordn,var=OBSGEN_RESULT_MODIFIER,addvar=record_n percent_n)
        %_recordn(dsn=OBSGEN_L3_TMOD,type=_recordn,var=OBSGEN_TABLE_MODIFIED,addvar=record_n percent_n)
        %_recordn(dsn=OBSGEN_L3_QUAL,type=_recordn,var=OBSGEN_RESULT_QUAL,addvar=record_n percent_n)
        %_recordn(dsn=OBSGEN_L3_RUNIT,type=_recordn,var=OBSGEN_RESULT_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=OBSGEN_L3_TYPE,type=_recordn,var=OBSGEN_TYPE,addvar=record_n percent_n)
        %_recordn(dsn=OBSGEN_L3_ABN,type=_recordn,var=OBSGEN_ABN_IND,addvar=record_n percent_n)
        %_recordn(dsn=OBSGEN_L3_SOURCE,type=_recordn,var=OBSGEN_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_yprescribing=1 %then %do;
        %_recordn(dsn=PRES_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=PRES_L3_BASIS,type=_recordn,var=RX_BASIS,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_FREQ,type=_recordn,var=RX_FREQUENCY,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_DISPASWRTN,type=_recordn,var=RX_DISPENSE_AS_WRITTEN,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_PRNFLAG,type=_recordn,var=RX_PRN_FLAG,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_RXDOSEFORM,type=_recordn,var=RX_DOSE_FORM,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_RXDOSEODRUNIT,type=_recordn,var=RX_DOSE_ORDERED_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_ROUTE,type=_recordn,var=RX_ROUTE,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_SOURCE,type=_recordn,var=RX_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_yprocedures=1 %then %do;
        %_recordn(dsn=PRO_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=PRO_L3_PX,type=_recordn,var=PX,addvar=record_n record_pct)
        %_recordn(dsn=PRO_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=PRO_L3_PXSOURCE,type=_recordn,var=PX_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=PRO_L3_PXTYPE_ENCTYPE,type=_cross,var=ENC_TYPE,scat=PX_TYPE,addvar=record_n)
        %_recordn(dsn=PRO_L3_PPX,type=_recordn,var=PPX,addvar=record_n record_pct)
    %end;
    %if &_ypro_cm=1 %then %do;
        %_recordn(dsn=PROCM_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=PROCM_L3_METHOD,type=_recordn,var=PRO_METHOD,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_MODE,type=_recordn,var=PRO_MODE,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_CAT,type=_recordn,var=PRO_CAT,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_TYPE,type=_recordn,var=PRO_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_SOURCE,type=_recordn,var=PRO_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_yprovider=1 %then %do;
        %_recordn(dsn=PROV_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=PROV_L3_NPIFLAG,type=_recordn,var=PROVIDER_NPI_FLAG,addvar=record_n record_pct)
        %_recordn(dsn=PROV_L3_SPECIALTY,type=_recordn,var=PROVIDER_SPECIALTY_PRIMARY,addvar=record_n record_pct)
        %_recordn(dsn=PROV_L3_SPECIALTY_GROUP,type=_recordn,var=PROVIDER_SPECIALTY_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=PROV_L3_SEX,type=_recordn,var=PROVIDER_SEX,addvar=record_n record_pct)
        %_recordn(dsn=PROV_L3_SPECIALTY_ENCTYPE,type=_cross2,var=PROVIDER_SPECIALTY_GROUP,scat=PROVIDER_SPECIALTY_PRIMARY,scat2=ENC_TYPE,addvar=record_n distinct_providerid_n)
    %end;
    %if &_yvital=1 %then %do;
        %_recordn(dsn=VIT_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=VIT_L3_VITAL_SOURCE,type=_recordn,var=VITAL_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_BP_POSITION_TYPE,type=_recordn,var=BP_POSITION,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_SMOKING,type=_recordn,var=SMOKING,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_TOBACCO,type=_recordn,var=TOBACCO,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_TOBACCO_TYPE,type=_recordn,var=TOBACCO_TYPE,addvar=record_n record_pct)
    %end;
    %if &_ylab_history=1 %then %do;
        %_recordn(dsn=LABHIST_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
        %_recordn(dsn=LABHIST_L3_LOINC,type=_recordn,var=LAB_LOINC,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_SEXDIST,type=_recordn,var=SEX,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_RACEDIST,type=_recordn,var=RACE,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_UNIT,type=_recordn,var=RESULT_UNIT,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_HIGH,type=_recordn,var=NORM_MODIFIER_HIGH,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_LOW,type=_recordn,var=NORM_MODIFIER_LOW,addvar=record_n percent_n)
    %end;
    %if &_yencounter=1 and &_ydemographic=1 %then %do;
        %_recordn(dsn=XTBL_L3_RACE_ENC,type=_recordn,var=RACE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_ydiagnosis=1 and &_yvital=1 %then %do;
        %_recordn(dsn=XTBL_L3_DASH1,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %end;
    %if &_yprescribing=1 and &_ydispensing=1 and &_yvital=1 and &_ydiagnosis=1 %then %do;
        %_recordn(dsn=XTBL_L3_DASH2,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %end;
    %if &_ydiagnosis=1 and &_yvital=1 and &_ylab_result_cm=1 %then %do;
        %_recordn(dsn=XTBL_L3_DASH3,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %end;
    %if &_yharvest=1 %then %do;
        %_recordn(dsn=XTBL_L3_METADATA,type=_n,var=name,addvar=value,stdvar=0)
        %_recordn(dsn=XTBL_L3_DATES,type=_xtbl,var=tag,addvar=min p5 median p95 max n nmiss future_dt_n pre2010_n invalid_n impl_n)
        %_recordn(dsn=XTBL_L3_TIMES,type=_xtbl,var=tag,addvar=min median max n nmiss invalid_n)
        %_recordn(dsn=XTBL_L3_DATE_LOGIC,type=_recordn,var=DATE_COMPARISON,addvar=distinct_patid_n)
        %_recordn(dsn=XTBL_L3_MISMATCH,type=_xtbl,var=tag,addvar=distinct_n)
        %_recordn(dsn=XTBL_L3_NON_UNIQUE,type=_xtbl,var=tag,addvar=distinct_n)
        %_recordn(dsn=XTBL_L3_REFRESH_DATE,type=_xtbl,var="REFRESH_DATE",addvar=refresh_date)
        %_recordn(dsn=XTBL_L3_REFRESH_DATE,type=_xtbl,var="NOBS",addvar=nobs)
    %end;
    %if &_yhash_token=1 %then %do;
        %_recordn(dsn=HASH_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n distinct_n_pct valid_n valid_distinct_n valid_distinct_n_pct)
        %_recordn(dsn=HASH_L3_TOKEN_AVAILABILITY,type=_recordn,var=token,addvar=distinct_patid_n)
    %end;
    %if &_yptrial=1 %then %do;
        %_recordn(dsn=TRIAL_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %end;
%end;
%if %upcase(&_dcpart)=_DCPART2 or %upcase(&_dcpart)=_ALL %then %do;
    %if &_yencounter=1 %then %do;
        %_recordn(dsn=ENC_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
        %_recordn(dsn=ENC_L3_DDATE_Y,type=_recordn,var=DISCHARGE_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_DDATE_YM,type=_recordn,var=DISCHARGE_DATE,addvar=record_n )
        %_recordn(dsn=ENC_L3_ENCTYPE_ADATE_Y,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=ENC_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=ENC_L3_ENCTYPE_DDATE_YM,type=_cross,var=ENC_TYPE,scat=DISCHARGE_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=ENC_L3_LOS_DIST,type=_cross,var=ENC_TYPE,scat=LOS_GROUP,addvar=record_n record_pct record_pct_cat)
        %_recordn(dsn=ENC_L3_PAYERTYPE_Y,type=_cross,var=ADMIT_DATE,scat=PAYER_TYPE_PRIMARY,addvar=record_n distinct_patid_n)
        %_recordn(dsn=ENC_L3_FACILITYLOC,type=_recordn,var=FACILITY_LOCATION,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_FACILITYTYPE_FACILITYLOC,type=_cross,var=FACILITY_TYPE,scat=FACILITY_LOCATION,
                    addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=ENC_L3_ENCTYPE_ADMSRC,type=_cross,var=ENC_TYPE,scat=ADMITTING_SOURCE,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_ENCTYPE_DISDISP,type=_cross,var=ENC_TYPE,scat=DISCHARGE_DISPOSITION,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_ENCTYPE_DISSTAT,type=_cross,var=ENC_TYPE,scat=DISCHARGE_STATUS,addvar=record_n record_pct)
        %_recordn(dsn=ENC_L3_ENCTYPE_DRG,type=_cross,var=ENC_TYPE,scat=DRG,addvar=record_n record_pct)
    %end;
    %if &_ydiagnosis=1 %then %do;
        %_recordn(dsn=DIA_L3_ORIGIN,type=_recordn,var=DX_ORIGIN,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DIA_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
        %_recordn(dsn=DIA_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
        %_recordn(dsn=DIA_L3_DXDATE_Y,type=_recordn,var=DX_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
        %_recordn(dsn=DIA_L3_DXDATE_YM,type=_recordn,var=DX_DATE,addvar=record_n )
        %_recordn(dsn=DIA_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_patid_n distinct_encid_n)
        %_recordn(dsn=DIA_L3_DXTYPE_ADATE_Y,type=_cross,var=DX_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=DIA_L3_DXTYPE,type=_recordn,var=DX_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=DIA_L3_DX_DXTYPE,type=_cross,var=DX_TYPE,scat=DX,addvar=record_n distinct_patid_n)
        %_recordn(dsn=DIA_L3_PDX_ENCTYPE,type=_cross2,var=ENC_TYPE,scat=PDX,scat2=DX_ORIGIN,addvar=record_n distinct_encid_n distinct_patid_n)
        %_recordn(dsn=DIA_L3_PDXGRP_ENCTYPE,type=_cross2,var=ENC_TYPE,scat=PDXGRP,scat2=DX_ORIGIN,addvar=distinct_encid_n)
    %end;
    %if &_yprocedures=1 %then %do;
        %_recordn(dsn=PRO_L3_PXTYPE,type=_recordn,var=PX_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=PRO_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
        %_recordn(dsn=PRO_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
        %_recordn(dsn=PRO_L3_PXDATE_Y,type=_recordn,var=PX_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
        %_recordn(dsn=PRO_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_encid_n distinct_patid_n)
        %_recordn(dsn=PRO_L3_PX_PXTYPE,type=_cross,var=PX_TYPE,scat=PX,addvar=record_n distinct_patid_n)
        %_recordn(dsn=PRO_L3_PXTYPE_ADATE_Y,type=_cross,var=PX_TYPE,scat=ADMIT_DATE,addvar=record_n distinct_patid_n)
    %end;
    %if &_yvital=1 %then %do;
        %_recordn(dsn=VIT_L3_HT,type=_recordn,var=HT_GROUP,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=VIT_L3_HT_DIST,type=_stat,var=HEIGHT,addvar=record_n)
        %_recordn(dsn=VIT_L3_WT,type=_recordn,var=WT_GROUP,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=VIT_L3_WT_DIST,type=_stat,var=WEIGHT,addvar=record_n)
        %_recordn(dsn=VIT_L3_DIASTOLIC,type=_recordn,var=DIASTOLIC_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_SYSTOLIC,type=_recordn,var=SYSTOLIC_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_BMI,type=_recordn,var=BMI_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=VIT_L3_DASH1,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
        %_recordn(dsn=VIT_L3_MDATE_Y,type=_recordn,var=MEASURE_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=VIT_L3_MDATE_YM,type=_recordn,var=MEASURE_DATE,addvar=record_n distinct_patid_n)
    %end;
    %if &_ydeath=1 %then %do;
        %_recordn(dsn=DEATH_L3_DATE_Y,type=_recordn,var=DEATH_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DEATH_L3_DATE_YM,type=_recordn,var=DEATH_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=DEATH_L3_SOURCE_YM,type=_cross,var=DEATH_DATE,scat=DEATH_SOURCE,addvar=record_n record_pct)
    %end;
    %if &_ydispensing=1 %then %do;
        %_recordn(dsn=DISP_L3_DISPAMT_DIST,type=_stat,var=DISPENSE_AMT,addvar=record_n)
        %_recordn(dsn=DISP_L3_DOSE_DIST,type=_stat,var=DISPENSE_DOSE_DISP,addvar=record_n)
        %_recordn(dsn=DISP_L3_SUPDIST2,type=_recordn,var=DISPENSE_SUP_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=DISP_L3_NDC,type=_recordn,var=NDC,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DISP_L3_DDATE_Y,type=_recordn,var=DISPENSE_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=DISP_L3_DDATE_YM,type=_recordn,var=DISPENSE_DATE,addvar=record_n distinct_patid_n)
    %end;
    %if &_yprescribing=1 %then %do;
        %_recordn(dsn=PRES_L3_RXCUI,type=_cross,var=RXNORM_CUI,scat=RXNORM_CUI_TTY,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=PRES_L3_RXCUI_TIER,type=_recordn,var=RXNORM_CUI_TIER,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_SUPDIST2,type=_recordn,var=RX_DAYS_SUPPLY_GROUP,addvar=record_n record_pct)
        %_recordn(dsn=PRES_L3_RXCUI_RXSUP,type=_recordn,var=RXNORM_CUI,addvar=min mean max sum n nmiss)
        %_recordn(dsn=PRES_L3_RXDOSEODR_DIST,type=_stat,var=RX_DOSE_ORDERED,addvar=record_n)
        %_recordn(dsn=PRES_L3_ODATE_Y,type=_recordn,var=RX_ORDER_DATE,addvar=record_n_rxcui record_n record_pct distinct_patid_n)
        %_recordn(dsn=PRES_L3_ODATE_YM,type=_recordn,var=RX_ORDER_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=PRES_L3_RXQTY_DIST,type=_stat,var=RX_QUANTITY,addvar=record_n)
        %_recordn(dsn=PRES_L3_RXREFILL_DIST,type=_stat,var=RX_REFILLS,addvar=record_n)
    %end;
    %if &_ymed_admin=1 %then %do;
        %_recordn(dsn=MEDADM_L3_SDATE_Y,type=_recordn,var=MEDADMIN_START_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=MEDADM_L3_SDATE_YM,type=_recordn,var=MEDADMIN_START_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=MEDADM_L3_CODE_TYPE,type=_cross,var=MEDADMIN_CODE,scat=MEDADMIN_TYPE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=MEDADM_L3_DOSEADM,type=_stat,var=MEDADMIN_DOSE_ADMIN,addvar=record_n)
        %_recordn(dsn=MEDADM_L3_RXCUI_TIER,type=_recordn,var=RXNORM_CUI_TIER,addvar=record_n record_pct)
    %end;
    %if &_ycondition=1 %then %do;
        %_recordn(dsn=COND_L3_CONDITION,type=_recordn,var=CONDITION,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=COND_L3_RDATE_Y,type=_recordn,var=REPORT_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=COND_L3_RDATE_YM,type=_recordn,var=REPORT_DATE,addvar=record_n )
    %end;
    %if &_ypro_cm=1 %then %do;
        %_recordn(dsn=PROCM_L3_ITEMFULLNAME,type=_recordn,var=PRO_ITEM_FULLNAME,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_ITEMNM,type=_recordn,var=PRO_ITEM_NAME,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_MEASURE_FULLNAME,type=_recordn,var=PRO_MEASURE_FULLNAME,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_MEASURENM,type=_recordn,var=PRO_MEASURE_NAME,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_MEASURE_LOINC,type=_recordn,var=PRO_MEASURE_LOINC,addvar=record_n record_pct)
        %_recordn(dsn=PROCM_L3_LOINC,type=_cross,var=PRO_ITEM_LOINC,scat=PANEL_TYPE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=PROCM_L3_PDATE_Y,type=_recordn,var=PRO_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=PROCM_L3_PDATE_YM,type=_recordn,var=PRO_DATE,addvar=record_n )
    %end;
    %if &_yobs_gen=1 %then %do;
        %_recordn(dsn=OBSGEN_L3_CODE_TYPE,type=_cross,var=OBSGEN_CODE,scat=OBSGEN_TYPE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=OBSGEN_L3_SDATE_Y,type=_recordn,var=OBSGEN_START_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=OBSGEN_L3_SDATE_YM,type=_recordn,var=OBSGEN_START_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=OBSGEN_L3_CODE_UNIT,type=_cross,var=OBSGEN_CODE,scat=OBSGEN_RESULT_UNIT,addvar=record_n)
        %_recordn(dsn=OBSGEN_L3_RECORDC,type=_n,var=tag,addvar=all_n)
    %end;
    %if &_yldsadrs=1 %then %do;
        %_recordn(dsn=LDSADRS_L3_ADRSCITY,type=_recordn,var=ADDRESS_CITY,addvar=record_n record_pct)
        %_recordn(dsn=LDSADRS_L3_ADRSZIP5,type=_recordn,var=ADDRESS_ZIP5,addvar=record_n record_pct)
        %_recordn(dsn=LDSADRS_L3_ADRSZIP9,type=_recordn,var=ADDRESS_ZIP9,addvar=record_n record_pct)
    %end;
    %if &_yimmunization=1 %then %do;
        %_recordn(dsn=IMMUNE_L3_RDATE_Y,type=_recordn,var=VX_RECORD_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=IMMUNE_L3_RDATE_YM,type=_recordn,var=VX_RECORD_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=IMMUNE_L3_ADATE_Y,type=_recordn,var=VX_ADMIN_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=IMMUNE_L3_ADATE_YM,type=_recordn,var=VX_ADMIN_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=IMMUNE_L3_CODETYPE,type=_recordn,var=VX_CODE_TYPE,addvar=record_n record_pct)
        %_recordn(dsn=IMMUNE_L3_DOSE_DIST,type=_stat,var=VX_DOSE,addvar=record_n)
        %_recordn(dsn=IMMUNE_L3_LOTNUM,type=_recordn,var=VX_LOT_NUM,addvar=record_n record_pct)
    %end;
    %if &_yobs_clin=1 %then %do;
        %_recordn(dsn=OBSCLIN_L3_CODE_TYPE,type=_cross,var=OBSCLIN_CODE,scat=OBSCLIN_TYPE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=OBSCLIN_L3_SDATE_Y,type=_recordn,var=OBSCLIN_START_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=OBSCLIN_L3_SDATE_YM,type=_recordn,var=OBSCLIN_START_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=OBSCLIN_L3_CODE_UNIT,type=_cross,var=OBSCLIN_CODE,scat=OBSCLIN_RESULT_UNIT,addvar=record_n)
        %_recordn(dsn=OBSCLIN_L3_RECORDC,type=_n,var=tag,addvar=all_n)
        %_recordn(dsn=OBSCLIN_L3_LOINC,type=_cross,var=OBSCLIN_CODE,scat=PANEL_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %_recordn(dsn=LAB_L3_PX_PXTYPE,type=_cross,var=LAB_PX_TYPE,scat=LAB_PX,addvar=record_n distinct_patid_n)
        %_recordn(dsn=LAB_L3_LOINC_UNIT,type=_cross,var=LAB_LOINC,scat=RESULT_UNIT,addvar=record_n record_pct)
        %_recordn(dsn=LAB_L3_RDATE_Y,type=_recordn,var=RESULT_DATE,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=LAB_L3_RDATE_YM,type=_recordn,var=RESULT_DATE,addvar=record_n distinct_patid_n)
        %_recordn(dsn=LAB_L3_LOINC_RESULT_NUM,type=_recordn,var=LAB_LOINC,addvar=min p1 p5 p25 median p75 p95 p99 max n nmiss)
        %_recordn(dsn=LAB_L3_SNOMED,type=_recordn,var=RESULT_SNOMED,addvar=record_n record_pct distinct_patid_n)
        %_recordn(dsn=LAB_L3_RECORDC,type=_n,var=tag,addvar=all_n)
        %_recordn(dsn=LAB_L3_LOINC,type=_cross,var=LAB_LOINC,scat=PANEL_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_ylab_history=1 %then %do;
        %_recordn(dsn=LABHIST_L3_PDEND_Y,type=_recordn,var=PERIOD_END,addvar=record_n percent_n)
        %_recordn(dsn=LABHIST_L3_MIN_WKS,type=_stat,var=AGE_MIN_WKS,addvar=record_n)
        %_recordn(dsn=LABHIST_L3_MAX_WKS,type=_stat,var=AGE_MAX_WKS,addvar=record_n)
        %_recordn(dsn=LABHIST_L3_PDSTART_Y,type=_recordn,var=PERIOD_START,addvar=record_n percent_n)
    %end;
    %if &_yencounter=1 and &_ylab_result_cm=1 %then %do;
        %_recordn(dsn=XTBL_L3_LAB_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_yencounter=1 and &_yprescribing=1 %then %do;
        %_recordn(dsn=XTBL_L3_PRES_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_yencounter=1 and &_ymed_admin=1 %then %do;
        %_recordn(dsn=XTBL_L3_MEDADM_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
    %end;
    %if &_yldsadrs=1 and &_yencounter=1 %then %do;
        %_recordn(dsn=XTBL_L3_ZIP5_5Y,type=_n,var=ADDRESS_ZIP5,addvar=record_n distinct_patid_n)
        %_recordn(dsn=XTBL_L3_ZIP5_1Y,type=_n,var=ADDRESS_ZIP5,addvar=record_n distinct_patid_n)
    %end;
%end;
%if %upcase(&_dcpart)=_DCPART2 %then %do;
    %if &_yharvest=1 %then %do;
        %_recordn(dsn=XTBL_L3_METADATA,type=_n,var=name,addvar=value,stdvar=0)
    %end;
%end;
%mend cdm;
%cdm;

%if %upcase(&_dcpart)=_DCPART1 or %upcase(&_dcpart)=_ALL %then %do;
    ********************************************************************************;
    *- Add number of obs in each table
    ********************************************************************************;
    data datamart_all;
         length dc_name $100 table resultc statistic variable $50;
         set dmlocal.datamart_all;
         by ord memname;
         if first.ord;

         dc_name="DATAMART_ALL";
         table=memname;
         variable="NOBS";
         statistic="RECORD_N";
         resultc=nobs;
         if resultc^="BT" then resultn=input(nobs,16.);
         else if resultc="BT" then resultn=.t;
         keep dc_name table variable statistic resultc resultn;
    run;

    data query;
         set query datamart_all;
    run;

    ********************************************************************************;
    * Add DC summary dataset
    ********************************************************************************;
    proc cimport library=work infile=edc_ref;
    run;

    ********************************************************************************;
    * Add code summary dataset
    ********************************************************************************;
    data dc_summary;
         length dc_name $100 table resultc statistic variable $50;
         set dmlocal.code_summary;

         dc_name="CODE_SUMMARY";
         variable="CODE TYPE";
         category=code_type;
         array s records codes bad_records bad_codes misplaced_records misplaced_codes 
                 bad_record_pct misplaced_record_pct;
            do i = 1 to dim(s);
               statistic=upcase(vname(s{i}));
               resultn=s{i};
               if i<=6 then resultc=put(s{i},8.);
               else if i>6 then resultc=put(s{i},8.2);
               output;
            end;

         keep dc_name table variable category statistic resultc resultn;
    run;

    data query;
         set query dc_summary;
    run;
%end;
    
********************************************************************************;
*- Add standard variables -*;
********************************************************************************;
%if %upcase(&_dcpart)=_DCPART2 %then %do;
    data response_dt;
         set dmlocal.dia_l3_dxtype(keep=datamartid query_package response_date 
                              rename=(response_date=r_date));
         if _n_=1;
         length response_date $100;
         response_date=compress(put(r_date,yymmdd10.));
         drop tag r_date;
    run;
         
    data query;
         if _n_=1 then set response_dt;
%end;
%if %upcase(&_dcpart)=_DCPART1 or %upcase(&_dcpart)=_ALL  %then %do;
    data query;
         if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="DATAMARTID") 
                rename=(value=datamartid));
         if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="QUERY_PACKAGE") 
                rename=(value=query_package));
         if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="RESPONSE_DATE") 
                rename=(value=response_date));
%end;
     set query;

     attrib 
        datamartid     label='DATAMART ID'
        response_date  label='Response date of query package'
        query_package  label='Query package version'
        dc_name        label='Query name'
        table          label='Data table'
        variable       label='Query variable'
        cross_variable label='Query cross-table variable'
        category       label='Categorical value'
        cross_category label='Cross-table variable categorical value'
        statistic      label='Statistic'
        resultc        label='Result (character)'
        resultn        label='Result (numeric)'
     ;
run;

********************************************************************************;
*- Order variables and output as permanent dataset -*;
********************************************************************************;
proc sql;
     create table &dmid._&r_date._dc_norm as select
         datamartid, response_date, query_package, dc_name, table, variable, 
           cross_variable, category, cross_category,  cross_variable2, 
           cross_category2, statistic, resultc, resultn
     from query;
quit;

data dmlocal.&dmid._&r_date._dc_norm(compress=yes label="Normalized DC data");
     set &dmid._&r_date._dc_norm;
run;

********************************************************************************;
* Create a SAS transport file of the normalization dataset
********************************************************************************;
filename tranfile "&qpath.drnoc/&dmid._&r_date._dc_norm.cpt";
proc cport library=dmlocal file=tranfile memtype=data;
     select &dmid._&r_date._dc_norm;
run;        

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;
********************************************************************************;

********************************************************************************;
* Macro end
********************************************************************************;
%mend norm;
%norm;
