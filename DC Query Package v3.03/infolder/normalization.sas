/*******************************************************************************
*  $Source: normalization $;
*    $Date: 2016/08/05
*    Study: PCORnet
*
*  Purpose: To consolidate PCORnet Data Characterization Query Package v3.02
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
*- Set LIBNAMES for data and output
*******************************************************************************;
libname pcordata "&dpath" access=readonly;
libname dmlocal "&qpath.dmlocal";

********************************************************************************;
* Create macro variables for DataMart ID and response date
********************************************************************************;
data _null_;
     set dmlocal.xtbl_l3_metadata;
     if name="DATAMARTID" then call symput("dmid",strip(value));
     if name="RESPONSE_DATE" then call symput("r_date",strip(compress(value,'-')));
run;

********************************************************************************;
* Create working formats 
********************************************************************************;
proc format;
     value $dsn
       'DEATH'='DEATH'
       'DEM'='DEMOGRAPHIC'
       'DIA'='DIAGNOSIS'
       'DISP'='DISPENSING'
       'ENC'='ENCOUNTER'
       'ENR'='ENROLLMENT'
       'LAB'='LAB_RESULT_CM'
       'PRES'='PRESCRIBING'
       'PRO'='PROCEDURES'
       'VIT'='VITAL'
       ;
run;

********************************************************************************;
* Determine which datasets are available
********************************************************************************;
proc contents data=pcordata._all_ noprint out=pcordata;
run;

proc sort data=pcordata(keep=memname) nodupkey;
     by memname;
run;

proc sql noprint;
     select count(*) into :_ydeath from pcordata
            where memname="DEATH";
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
     select count(*) into :_yvital from pcordata
            where memname="VITAL";
quit;

********************************************************************************;
*- Macro to convert each DC query dataset to standard format -*;
********************************************************************************;
%macro _recordn(dsn=,type=,var=,scat=,addvar=,stdvar=1);
    data &dsn;
        length dc_name $100 table resultc statistic variable cross_variable 
               category cross_category $50;
        set dmlocal.&dsn %if &stdvar=1 %then %do; 
                           (drop=datamartid query_package response_date) %end; ;

        * create common variables based upon DC query name *;
        dc_name="&dsn";
        table=put(scan("&dsn",1,'_'),$dsn.);

        * create common variables based upon type of query *;
        %if &type=_recordn %then %do;
            variable="&var";
            %if &var=DX or &var=PX %then %do;
                category=strip(compress(&var,'.'));
            %end;
            %else %do;
                category=strip(&var);
            %end;
            cross_variable=" ";
            cross_category=" ";
        %end;
        %else %if &type=_n %then %do;
            variable=strip(&var);
            category=" ";
            cross_variable=" ";
            cross_category=" ";
        %end;
        %else %if &type=_stat %then %do;
            variable="&var";
            category=" ";
            cross_variable=" ";
            cross_category=" ";
        %end;
        %else %if &type=_cross %then %do;
            variable="&var";
            category=strip(&var);
            cross_variable="&scat";
            %if &scat=DX or &scat=PX %then %do;
                cross_category=strip(compress(&scat,'.'));
            %end;
            %else %do;
                cross_category=strip(&scat);
            %end;
        %end;
        %else %if &type=_xtbl %then %do;
            table=dataset;
            variable=strip(&var);
            category=" ";
            cross_variable=" ";
            cross_category=" ";
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
          if resultc not in ("BT" " ") then do;
             if indexc(upcase(resultc),"ABCDEFGHIJKLMNOPQRSTUVWXYZ-_():")=0 
                then resultn=input(resultc,best.);
             else resultn=.;
          end;
          else if resultc in ("BT") then resultn=.t;
          else if resultc in (" ") then resultn=.;

          * output as transposed records *;
          output;

        end;

        keep dc_name table variable cross_variable category cross_category 
             statistic resultc resultn;
    run;
    
    * append to one like data structure *;
    proc append base=query data=&dsn;
    run;

%mend _recordn;

********************************************************************************;
*- Macro for conditional code on availability of CDM datasets -*;
********************************************************************************;
%macro cdm;
%if &_ydeath=1 %then %do;
    %_recordn(dsn=DEATH_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=DEATH_L3_DATE_Y,type=_recordn,var=DEATH_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=DEATH_L3_DATE_YM,type=_recordn,var=DEATH_DATE,addvar=record_n )
    %_recordn(dsn=DEATH_L3_IMPUTE,type=_recordn,var=DEATH_DATE_IMPUTE,addvar=record_n record_pct)
    %_recordn(dsn=DEATH_L3_SOURCE,type=_recordn,var=DEATH_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=DEATH_L3_MATCH,type=_recordn,var=DEATH_MATCH_CONFIDENCE,addvar=record_n record_pct)
%end;
%if &_ydemographic=1 %then %do;
    %_recordn(dsn=DEM_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=DEM_L3_AGEYRSDIST1,type=_stat,var=AGE,addvar=record_n)
    %_recordn(dsn=DEM_L3_AGEYRSDIST2,type=_recordn,var=AGE_GROUP,addvar=record_n record_pct)
    %_recordn(dsn=DEM_L3_HISPDIST,type=_recordn,var=HISPANIC,addvar=record_n record_pct)
    %_recordn(dsn=DEM_L3_RACEDIST,type=_recordn,var=RACE,addvar=record_n record_pct)
    %_recordn(dsn=DEM_L3_SEXDIST,type=_recordn,var=SEX,addvar=record_n record_pct)
%end;
%if &_ydiagnosis=1 %then %do;
    %_recordn(dsn=DIA_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=DIA_L3_DX,type=_recordn,var=DX,addvar=record_n record_pct)
    %_recordn(dsn=DIA_L3_DXSOURCE,type=_recordn,var=DX_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=DIA_L3_PDX,type=_recordn,var=PDX,addvar=record_n record_pct)
    %_recordn(dsn=DIA_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
    %_recordn(dsn=DIA_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
    %_recordn(dsn=DIA_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=DIA_L3_DASH1,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %_recordn(dsn=DIA_L3_DX_DXTYPE,type=_cross,var=DX_TYPE,scat=DX,addvar=record_n distinct_patid_n)
    %_recordn(dsn=DIA_L3_DXTYPE_DXSOURCE,type=_cross,var=DX_TYPE,scat=DX_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=DIA_L3_PDX_ENCTYPE,type=_cross,var=ENC_TYPE,scat=PDX,addvar=record_n distinct_encid_n distinct_patid_n)
    %_recordn(dsn=DIA_L3_PDXGRP_ENCTYPE,type=_cross,var=ENC_TYPE,scat=PDX,addvar=distinct_encid_n)
    %_recordn(dsn=DIA_L3_DXTYPE_ENCTYPE,type=_cross,var=ENC_TYPE,scat=DX_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=DIA_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n record_pct distinct_patid_n distinct_encid_n)
%end;
%if &_ydispensing=1 %then %do;
    %_recordn(dsn=DISP_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n valid_n)
    %_recordn(dsn=DISP_L3_NDC,type=_recordn,var=NDC,addvar=record_n record_pct)
    %_recordn(dsn=DISP_L3_DDATE_Y,type=_recordn,var=DISPENSE_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=DISP_L3_DDATE_YM,type=_recordn,var=DISPENSE_DATE,addvar=record_n distinct_patid_n)
    %_recordn(dsn=DISP_L3_SUPDIST2,type=_recordn,var=DISPENSE_SUP_GROUP,addvar=record_n record_pct)
%end;
%if &_yencounter=1 %then %do;
    %_recordn(dsn=ENC_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=ENC_L3_ADMSRC,type=_recordn,var=ADMITTING_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_DRG,type=_recordn,var=DRG,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_DRG_TYPE,type=_recordn,var=DRG_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=ENC_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
    %_recordn(dsn=ENC_L3_DDATE_Y,type=_recordn,var=DISCHARGE_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=ENC_L3_DDATE_YM,type=_recordn,var=DISCHARGE_DATE,addvar=record_n )
    %_recordn(dsn=ENC_L3_DISDISP,type=_recordn,var=DISCHARGE_DISPOSITION,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_DISSTAT,type=_recordn,var=DISCHARGE_STATUS,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct distinct_visit_n distinct_patid_n elig_record_n)
    %_recordn(dsn=ENC_L3_DASH1,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %_recordn(dsn=ENC_L3_DASH2,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
    %_recordn(dsn=ENC_L3_ENCTYPE_ADMRSC,type=_cross,var=ENC_TYPE,scat=ADMITTING_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_ENCTYPE_DRG,type=_cross,var=ENC_TYPE,scat=DRG,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=ENC_L3_ENCTYPE_DDATE_YM,type=_cross,var=ENC_TYPE,scat=DISCHARGE_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=ENC_L3_ENCTYPE_DISDISP,type=_cross,var=ENC_TYPE,scat=DISCHARGE_DISPOSITION,addvar=record_n record_pct)
    %_recordn(dsn=ENC_L3_ENCTYPE_DISSTAT,type=_cross,var=ENC_TYPE,scat=DISCHARGE_STATUS,addvar=record_n record_pct)
%end;
%if &_yenrollment=1 %then %do;
    %_recordn(dsn=ENR_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=ENR_L3_DIST_ENRMONTH,type=_recordn,var=ENROLL_M,addvar=record_n record_pct)
    %_recordn(dsn=ENR_L3_DIST_ENRYEAR,type=_recordn,var=ENROLL_Y,addvar=record_n record_pct)
    %_recordn(dsn=ENR_L3_ENR_YM,type=_recordn,var=MONTH,addvar=record_n record_pct)
    %_recordn(dsn=ENR_L3_BASEDIST,type=_recordn,var=ENR_BASIS,addvar=record_n record_pct)
    %_recordn(dsn=ENR_L3_DIST_END,type=_stat,var=ENROLLMENT END DATE,addvar=record_n)
    %_recordn(dsn=ENR_L3_DIST_START,type=_stat,var=ENROLLMENT START DATE,addvar=record_n)
    %_recordn(dsn=ENR_L3_PER_PATID,type=_stat,var=ENROLLMENT RECORDS PER PATID,addvar=record_n)
%end;
%if &_ylab_result_cm=1 %then %do;
    %_recordn(dsn=LAB_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=LAB_L3_NAME,type=_recordn,var=LAB_NAME,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=LAB_L3_NAME_LOINC,type=_cross,var=LAB_NAME,scat=LOINC,addvar=record_n)
    %_recordn(dsn=LAB_L3_NAME_RDATE_Y,type=_cross,var=LAB_NAME,scat=RESULT_DATE,addvar=record_n record_pct record_n_loinc distinct_patid_n)
    %_recordn(dsn=LAB_L3_NAME_RDATE_YM,type=_cross,var=LAB_NAME,scat=RESULT_DATE,addvar=record_n distinct_patid_n)
    %_recordn(dsn=LAB_L3_SOURCE,type=_recordn,var=SPECIMEN_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_PRIORITY,type=_recordn,var=PRIORITY,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_LOC,type=_recordn,var=RESULT_LOC,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_PX_TYPE,type=_recordn,var=LAB_PX_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_PX_PXTYPE,type=_cross,var=PX_TYPE,scat=PX,addvar=record_n distinct_patid_n)
    %_recordn(dsn=LAB_L3_QUAL,type=_recordn,var=RESULT_QUAL,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_MOD,type=_recordn,var=RESULT_MODIFIER,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_LOW,type=_recordn,var=NORM_MODIFIER_LOW,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_HIGH,type=_recordn,var=NORM_MODIFIER_HIGH,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_ABN,type=_recordn,var=ABN_IND,addvar=record_n record_pct)
    %_recordn(dsn=LAB_L3_NAME_RUNIT,type=_cross,var=LAB_NAME,scat=RESULT_UNIT,addvar=record_n)
%end;
%if &_yprescribing=1 %then %do;
    %_recordn(dsn=PRES_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=PRES_L3_RXCUI,type=_recordn,var=RXNORM_CUI,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=PRES_L3_SUPDIST2,type=_recordn,var=RX_DAYS_SUPPLY_GROUP,addvar=record_n record_pct)
    %_recordn(dsn=PRES_L3_RXCUI_RXSUP,type=_recordn,var=RXNORM_CUI,addvar=min mean max sum n nmiss)
    %_recordn(dsn=PRES_L3_BASIS,type=_recordn,var=RX_BASIS,addvar=record_n record_pct)
    %_recordn(dsn=PRES_L3_FREQ,type=_recordn,var=RX_FREQUENCY,addvar=record_n record_pct)
    %_recordn(dsn=PRES_L3_ODATE_Y,type=_recordn,var=RX_ORDER_DATE,addvar=record_n_rxcui record_n record_pct distinct_patid_n)
    %_recordn(dsn=PRES_L3_ODATE_YM,type=_recordn,var=RX_ORDER_DATE,addvar=record_n distinct_patid_n)
%end;
%if &_yprocedures=1 %then %do;
    %_recordn(dsn=PRO_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=PRO_L3_PX,type=_recordn,var=PX,addvar=record_n record_pct)
    %_recordn(dsn=PRO_L3_ADATE_Y,type=_recordn,var=ADMIT_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
    %_recordn(dsn=PRO_L3_ADATE_YM,type=_recordn,var=ADMIT_DATE,addvar=record_n )
    %_recordn(dsn=PRO_L3_PXDATE_Y,type=_recordn,var=PX_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
    %_recordn(dsn=PRO_L3_ENCTYPE,type=_recordn,var=ENC_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=PRO_L3_PXSOURCE,type=_recordn,var=PX_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=PRO_L3_PXTYPE_ENCTYPE,type=_cross,var=ENC_TYPE,scat=PX_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=PRO_L3_ENCTYPE_ADATE_YM,type=_cross,var=ENC_TYPE,scat=ADMIT_DATE,addvar=record_n record_pct distinct_encid_n distinct_patid_n)
    %_recordn(dsn=PRO_L3_PX_PXTYPE,type=_cross,var=PX_TYPE,scat=PX,addvar=record_n record_pct distinct_patid_n)
%end;
%if &_yvital=1 %then %do;
    %_recordn(dsn=VIT_L3_N,type=_n,var=tag,addvar=all_n distinct_n null_n)
    %_recordn(dsn=VIT_L3_MDATE_Y,type=_recordn,var=MEASURE_DATE,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=VIT_L3_MDATE_YM,type=_recordn,var=MEASURE_DATE,addvar=record_n distinct_patid_n)
    %_recordn(dsn=VIT_L3_VITAL_SOURCE,type=_recordn,var=VITAL_SOURCE,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_HT,type=_recordn,var=HT_GROUP,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=VIT_L3_HT_DIST,type=_stat,var=HEIGHT,addvar=record_n)
    %_recordn(dsn=VIT_L3_WT,type=_recordn,var=WT_GROUP,addvar=record_n record_pct distinct_patid_n)
    %_recordn(dsn=VIT_L3_WT_DIST,type=_stat,var=WEIGHT,addvar=record_n)
    %_recordn(dsn=VIT_L3_DIASTOLIC,type=_recordn,var=DIASTOLIC_GROUP,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_SYSTOLIC,type=_recordn,var=SYSTOLIC_GROUP,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_BMI,type=_recordn,var=BMI_GROUP,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_BP_POSITION_TYPE,type=_recordn,var=BP_POSITION,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_SMOKING,type=_recordn,var=SMOKING,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_TOBACCO,type=_recordn,var=TOBACCO,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_TOBACCO_TYPE,type=_recordn,var=TOBACCO_TYPE,addvar=record_n record_pct)
    %_recordn(dsn=VIT_L3_DASH1,type=_recordn,var=PERIOD,addvar=distinct_patid_n)
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
    %_recordn(dsn=XTBL_L3_DATES,type=_xtbl,var=tag,addvar=min median max n nmiss future_dt_n pre2010_n)
    %_recordn(dsn=XTBL_L3_TIMES,type=_xtbl,var=tag,addvar=min median max n nmiss)
    %_recordn(dsn=XTBL_L3_DATE_LOGIC,type=_recordn,var=DATE_COMPARISON,addvar=distinct_patid_n)
    %_recordn(dsn=XTBL_L3_MISMATCH,type=_xtbl,var=tag,addvar=distinct_n)
%end;
%mend cdm;
%cdm;

********************************************************************************;
*- Add umber of obs in each table
********************************************************************************;
data datamart_all_nobs;
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
     set query datamart_all_nobs;
run;

********************************************************************************;
*- Add standard variables -*;
********************************************************************************;
data query;
     if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="DATAMARTID") rename=(value=datamartid));
     if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="QUERY_PACKAGE") rename=(value=query_package));
     if _n_=1 then set dmlocal.xtbl_l3_metadata(where=(name="RESPONSE_DATE") rename=(value=response_date));
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
           cross_variable, category, cross_category, statistic, resultc, resultn
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

