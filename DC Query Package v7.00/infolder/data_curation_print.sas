/*******************************************************************************
*  $Source: data_curation_print $;
*    $Date: 2025/04/28
*    Study: PCORnet
*
*  Purpose: Print PCORnet Data Curation Query Package v7.00
* 
*   Inputs: SAS program:  /sasprograms/02_run_queries.sas
*
*  Outputs: 
*           1) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_<dcpart>.pdf)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

*******************************************************************************;
* Create an external log file
*******************************************************************************;
filename plog "&qpath.drnoc/%upcase(&dmid)_&tday._data_curation_print%lowcase(&_dcpart).log" lrecl=200;
proc printto log=plog new;
run;

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro _print;

*******************************************************************************;
* Create dataset of query execution time
*******************************************************************************;
* Sort ELAPSED dataset to match historical display sort;
proc sort data=elapsed_order;
    by query;
run;

proc sort data=elapsed;
    by query;
run;

* Output ELAPSED dataset to DMLOCAL;
data elapsed;
     merge 
        elapsed (in=ine)
        elapsed_order
        ;
    by query;
    if ine;

    if query^=" ";
    label _qstart="Query start time"
        _qend="Query end time"
        elapsedtime="Query run time (hh:mm:ss)"
        totalruntime="Cumulative run time (hh:mm:ss)"
    ;
    if query="DC PROGRAM" then do;
        query="DC PROGRAM - %upcase(&_dcpart)";
        _qend=datetime();
        elapsedtime=_qend-_qstart;
        totalruntime=_qend-&_pstart;
    end;

    * call standard variables;
    %stdvar
run;

proc sort data=elapsed out=dmlocal.elapsed%lowcase(&_dcpart) (drop=order);
    by order;
run;    
        
*******************************************************************************;
* Close DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;

********************************************************************************;
* Create a SAS transport file from all of the query datasets
********************************************************************************;
OPTIONS NOMPRINT NOMLOGIC NOSYMBOLGEN;
filename tranfile "&qpath.drnoc/&dmid._&tday._data_curation%lowcase(&_dcpart).cpt";    

*******************************************************************************;
* Close default log
*******************************************************************************;
proc printto log=log;
run;

*******************************************************************************;
* Re-direct to DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

proc cport library=dmlocal file=tranfile memtype=data;
     select elapsed%lowcase(&_dcpart)
        %if %upcase(&_part1)=YES %then %do; &_part1_mstring %end;
        %if %upcase(&_part2)=YES %then %do; &_part2_mstring %end;
     ;
run;    
        
*******************************************************************************;
* Close DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;

********************************************************************************;
* Create CSV if requested from all of the query datasets
********************************************************************************;
%if %upcase(&_csv)=YES %then %do;

    *- Export each dataset into a CSV file -*;
    %macro csv;
        %if %upcase(&_part1)=YES %then %do;
            %do d = 1 %to &_cnt_part1;
                %let _dsn=%scan(&_part1_mstring,&d," ");

                filename csvout "&qpath.dmlocal/&_dsn..csv";
                proc export data=dmlocal.&_dsn
                    outfile=csvout
                    dbms=dlm label replace;
                    delimiter=',';
                run;
            %end;
        %end;
        %if %upcase(&_part2)=YES %then %do;
            %do d2 = 1 %to &_cnt_part2;
                %let _dsn=%scan(&_part2_mstring,&d2," ");

                filename csvout "&qpath.dmlocal/&_dsn..csv";
                proc export data=dmlocal.&_dsn
                    outfile=csvout
                    dbms=dlm label replace;
                    delimiter=',';
                run;
            %end;
        %end;
    %mend csv;
    %csv
%end;      

*******************************************************************************;
* Close default log
*******************************************************************************;
proc printto log=log;
run;

*******************************************************************************;
* Re-direct to DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

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
* Close DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

*******************************************************************************;
* Re-direct to default log
*******************************************************************************;
proc printto log=log;
run;

*******************************************************************************;
* Print each data set and send to a PDF file
*******************************************************************************;
ods html close;
ods listing;
ods path sashelp.tmplmst(read) library.templat(read);
ods pdf file="&qpath.drnoc/&dmid._&tday._data_curation%lowcase(&_dcpart).pdf" style=journal;
    
*******************************************************************************;
* Close default log
*******************************************************************************;
proc printto log=log;
run;

*******************************************************************************;
* Re-direct to DRNOC log
*******************************************************************************;
proc printto log=plog;
run;

title "Data Curation query run times - ELAPSED%upcase(&_dcpart)";
footnote;
proc print width=min data=dmlocal.elapsed%lowcase(&_dcpart) label;
     var query _qstart _qend elapsedtime;
run;
title;

title "DATAMART_ALL_NOBS";
proc print width=min data=datamart_all label;
     var memname nobs;
run;
title;

*- print if Part 1 or All -*;
%if %upcase(&_dcpart)=_DCPART1 or %upcase(&_dcpart)=_ALL %then %do;

    %if &_ydemographic=1 %then %do;
        %prnt(pdsn=dem_l3_n);
        %if &_ydeath=1 %then %do;
            %prnt(pdsn=dem_l3_ageyrsdist1);
            %prnt(pdsn=dem_l3_ageyrsdist2);
            %prnt(pdsn=dem_obs_l3_ageyrsdist1);
            %prnt(pdsn=dem_obs_l3_ageyrsdist2);
        %end;
        %prnt(pdsn=dem_l3_biobank);
        %prnt(pdsn=dem_l3_hispdist);
        %prnt(pdsn=dem_l3_racedist);
        %prnt(pdsn=dem_l3_raceethmiss);
        %prnt(pdsn=dem_l3_raceethaian);
        %prnt(pdsn=dem_l3_raceethasian);
        %prnt(pdsn=dem_l3_raceethblack);
        %prnt(pdsn=dem_l3_raceethhispanic);
        %prnt(pdsn=dem_l3_raceethmena);
        %prnt(pdsn=dem_l3_raceethnhpi);
        %prnt(pdsn=dem_l3_raceethwhite);
        %prnt(pdsn=dem_l3_sexdist);
        %prnt(pdsn=dem_l3_orientdist);
        %prnt(pdsn=dem_l3_genderdist);
        %prnt(pdsn=dem_l3_patpreflang,_obs=100,_svar=pat_pref_language_spoken,_suppvar=record_n record_pct);
        %prnt(pdsn=dem_obs_l3_hispdist);
        %prnt(pdsn=dem_obs_l3_racedist);
        %prnt(pdsn=dem_obs_l3_sexdist);
        %prnt(pdsn=dem_obs_l3_orientdist);
        %prnt(pdsn=dem_obs_l3_genderdist);
        %prnt(pdsn=dem_obs_l3_patpreflang,_obs=100,_svar=pat_pref_language_spoken,_suppvar=record_n record_pct);
    %end;
    %if &_yencounter=1 %then %do;
        %prnt(pdsn=enc_l3_n);
        %prnt(pdsn=enc_l3_admsrc);
        %prnt(pdsn=enc_l3_disdisp);
        %prnt(pdsn=enc_l3_disstat);
        %prnt(pdsn=enc_l3_drg_type);
        %prnt(pdsn=enc_l3_enctype);
        %prnt(pdsn=enc_l3_dash2);
        %prnt(pdsn=enc_l3_payertype1,_obs=100,_svar=payer_type_primary_grp payer_type_primary,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=enc_l3_payertype2,_obs=100,_svar=payer_type_secondary_grp payer_type_secondary,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=enc_l3_facilitytype,_obs=100,_svar=facility_type_grp facility_type,_suppvar=record_n record_pct distinct_patid_n,orient=landscape);
    %end;
    %if &_ydiagnosis=1 %then %do;    
        %prnt(pdsn=dia_l3_n);
        %prnt(pdsn=dia_l3_dx,_obs=100,_svar=dx,_suppvar=record_n record_pct);
        %prnt(pdsn=dia_l3_dxsource);
        %prnt(pdsn=dia_l3_dxtype_dxsource);
        %prnt(pdsn=dia_l3_pdx);
        %prnt(pdsn=dia_l3_enctype);
        %prnt(pdsn=dia_l3_dxtype_enctype,_obs=100,_svar=dx_type enc_type,_suppvar=record_n);
        %prnt(pdsn=dia_l3_dxpoa);
    %end;
    %if &_yprocedures=1 %then %do;
        %prnt(pdsn=pro_l3_n);
        %prnt(pdsn=pro_l3_px,_obs=100,_svar=px,_suppvar=record_n record_pct);
        %prnt(pdsn=pro_l3_enctype);
        %prnt(pdsn=pro_l3_pxtype_enctype,_obs=100,_svar=px_type enc_type,_suppvar=record_n);
        %prnt(pdsn=pro_l3_pxsource);
        %prnt(pdsn=pro_l3_ppx);
    %end;
    %if &_yenrollment=1 %then %do;
        %prnt(pdsn=enr_l3_n);
        %prnt(pdsn=enr_l3_basedist);
        %prnt(pdsn=enr_l3_chart);
    %end;
    %if &_yvital=1 %then %do;    
        %prnt(pdsn=vit_l3_n);
        %prnt(pdsn=vit_l3_vital_source);
        %prnt(pdsn=vit_l3_bp_position_type);
        %prnt(pdsn=vit_l3_smoking);
        %prnt(pdsn=vit_l3_tobacco);
        %prnt(pdsn=vit_l3_tobacco_type);
    %end;    
    %if &_ydeath=1 %then %do;
        %prnt(pdsn=death_l3_n);
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
        %prnt(pdsn=disp_l3_doseunit,_obs=100,_svar=dispense_dose_disp_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=disp_l3_route,_obs=100,_svar=dispense_route,_suppvar=record_n record_pct);
        %prnt(pdsn=disp_l3_source);
    %end;    
    %if &_yprescribing=1 %then %do;
        %prnt(pdsn=pres_l3_n);
        %prnt(pdsn=pres_l3_basis);
        %prnt(pdsn=pres_l3_freq);
        %prnt(pdsn=pres_l3_dispaswrtn);
        %prnt(pdsn=pres_l3_prnflag);
        %prnt(pdsn=pres_l3_rxdoseform,_obs=100,_svar=rx_dose_form,_suppvar=record_n record_pct);
        %prnt(pdsn=pres_l3_rxdoseodrunit,_obs=100,_svar=rx_dose_ordered_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=pres_l3_route,_obs=100,_svar=rx_route,_suppvar=record_n record_pct);
        %prnt(pdsn=pres_l3_source);
    %end;    
    %if &_ymed_admin=1 %then %do;
        %prnt(pdsn=medadm_l3_n);
        %prnt(pdsn=medadm_l3_doseadmunit,_obs=100,_svar=medadmin_dose_admin_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=medadm_l3_route,_obs=100,_svar=medadmin_route,_suppvar=record_n record_pct);
        %prnt(pdsn=medadm_l3_source);
        %prnt(pdsn=medadm_l3_type);
    %end;
    %if &_ycondition=1 %then %do;
        %prnt(pdsn=cond_l3_n);
        %prnt(pdsn=cond_l3_status);
        %prnt(pdsn=cond_l3_type);
        %prnt(pdsn=cond_l3_source);
    %end;
    %if &_ypro_cm=1 %then %do;
        %prnt(pdsn=procm_l3_n);
        %prnt(pdsn=procm_l3_method);
        %prnt(pdsn=procm_l3_mode);
        %prnt(pdsn=procm_l3_cat);
        %prnt(pdsn=procm_l3_type);
        %prnt(pdsn=procm_l3_source);
        %prnt(pdsn=procm_l3_code_type,_obs=100,_svar=pro_code,_suppvar=pro_type record_n distinct_patid_n);
    %end;
    %if &_yobs_gen=1 %then %do;
        %prnt(pdsn=obsgen_l3_n);
        %prnt(pdsn=obsgen_l3_mod);
        %prnt(pdsn=obsgen_l3_tmod);
        %prnt(pdsn=obsgen_l3_qual,_obs=100,_svar=obsgen_result_qual,_suppvar=record_n record_pct);
        %prnt(pdsn=obsgen_l3_runit,_obs=100,_svar=obsgen_result_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=obsgen_l3_type);
        %prnt(pdsn=obsgen_l3_abn);
        %prnt(pdsn=obsgen_l3_source);
    %end;
    %if &_yprovider=1 %then %do;
        %prnt(pdsn=prov_l3_n);
        %prnt(pdsn=prov_l3_npiflag);
        %prnt(pdsn=prov_l3_specialty,_obs=100,_svar=provider_specialty_primary,_suppvar=record_n record_pct);
        %prnt(pdsn=prov_l3_specialty_group);
        %prnt(pdsn=prov_l3_specialty_enctype,_obs=100,_svar=provider_specialty_group,_suppvar=provider_specialty_primary enc_type record_n distinct_providerid_n);
        %prnt(pdsn=prov_l3_sex);
    %end;
    %if &_yldsadrs=1 %then %do;
        %prnt(pdsn=ldsadrs_l3_n);
        %prnt(pdsn=ldsadrs_l3_current);
        %prnt(pdsn=ldsadrs_l3_statefips);
        %prnt(pdsn=ldsadrs_l3_countyfips,_obs=100,_svar=county_fips,_suppvar=record_n record_pct);
        %prnt(pdsn=ldsadrs_l3_rucazip);
        %prnt(pdsn=ldsadrs_l3_adrsuse);
        %prnt(pdsn=ldsadrs_l3_adrstype);
        %prnt(pdsn=ldsadrs_l3_adrspref);
        %prnt(pdsn=ldsadrs_l3_adrsstate);
    %end;
    %if &_yimmunization=1 %then %do;
        %prnt(pdsn=immune_l3_n);
        %prnt(pdsn=immune_l3_code_codetype,_obs=100,_svar=vx_code,_suppvar=vx_code_type record_n distinct_patid_n);
        %prnt(pdsn=immune_l3_status);
        %prnt(pdsn=immune_l3_statusreason);
        %prnt(pdsn=immune_l3_source);
        %prnt(pdsn=immune_l3_doseunit,_obs=100,_svar=vx_dose_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=immune_l3_route,_obs=100,_svar=vx_route,_suppvar=record_n record_pct);
        %prnt(pdsn=immune_l3_bodysite);
        %prnt(pdsn=immune_l3_manufacturer);
    %end;
    %if &_yhash_token=1 %then %do;
        %prnt(pdsn=hash_l3_n);
        %prnt(pdsn=hash_l3_token_availability);
    %end;
    %if &_yptrial=1 %then %do;
        %prnt(pdsn=trial_l3_n);
    %end;
    %if &_yobs_clin=1 %then %do;
        %prnt(pdsn=obsclin_l3_n);
        %prnt(pdsn=obsclin_l3_mod);
        %prnt(pdsn=obsclin_l3_qual,_obs=100,_svar=obsclin_result_qual,_suppvar=record_n record_pct);
        %prnt(pdsn=obsclin_l3_source);
        %prnt(pdsn=obsclin_l3_abn);
        %prnt(pdsn=obsclin_l3_runit,_obs=100,_svar=obsclin_result_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=obsclin_l3_type);
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %prnt(pdsn=lab_l3_n);
        %prnt(pdsn=lab_l3_priority);
        %prnt(pdsn=lab_l3_loc);
        %prnt(pdsn=lab_l3_px_type);
        %prnt(pdsn=lab_l3_mod);
        %prnt(pdsn=lab_l3_low);
        %prnt(pdsn=lab_l3_high);
        %prnt(pdsn=lab_l3_abn);
        %prnt(pdsn=lab_l3_source,_obs=100,_svar=specimen_source,_suppvar=record_n record_pct);
        %prnt(pdsn=lab_l3_unit,_obs=100,_svar=result_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=lab_l3_qual,_obs=100,_svar=result_qual,_suppvar=record_n record_pct);
        %prnt(pdsn=lab_l3_rsource);
        %prnt(pdsn=lab_l3_lsource);
    %end;
    %if &_ylab_history=1 %then %do;
        %prnt(pdsn=labhist_l3_n);
        %prnt(pdsn=labhist_l3_loinc,_obs=100,_svar=lab_loinc,_suppvar=record_n record_pct);
        %prnt(pdsn=labhist_l3_sexdist);
        %prnt(pdsn=labhist_l3_racedist);
        %prnt(pdsn=labhist_l3_unit,_obs=100,_svar=result_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=labhist_l3_low);
        %prnt(pdsn=labhist_l3_high);
    %end;
    %if &_ypat_relationship=1 %then %do;
        %prnt(pdsn=patrel_l3_n);
        %prnt(pdsn=patrel_l3_type);
    %end;
    %if &_yexternal_meds=1 %then %do;
        %prnt(pdsn=extmed_l3_n);
        %prnt(pdsn=extmed_l3_rxdoseform,_obs=100,_svar=ext_dose_form,_suppvar=record_n record_pct);
        %prnt(pdsn=extmed_l3_rxdose_unit,_obs=100,_svar=ext_dose_ordered_unit,_suppvar=record_n record_pct);
        %prnt(pdsn=extmed_l3_route,_obs=100,_svar=ext_route,_suppvar=record_n record_pct);
        %prnt(pdsn=extmed_l3_basis);
        %prnt(pdsn=extmed_l3_source);
    %end;
    %if &_ydemographic=1 or &_yencounter=1 or &_ydiagnosis=1 or &_yprocedures=1 or
        &_yvital=1 or &_yenrollment=1 or &_ydeath=1 or &_ydispensing=1 or
        &_yprescribing=1 or &_ylab_result_cm=1 %then %do;
            %prnt(pdsn=xtbl_l3_dates,orient=landscape);
    %end;
    %if &_ydemographic=1 or &_yencounter=1 or &_yprocedures=1 or &_yvital=1 or
        &_ydeath=1 or &_ydispensing=1 or &_yprescribing=1 or &_ylab_result_cm=1 %then %do;
            %prnt(pdsn=xtbl_l3_date_logic);
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
    %if &_yencounter=1 or &_yprocedures=1 or &_yvital=1 or &_ycondition=1 or 
        &_ydiagnosis=1 or &_ydispensing=1 or &_yprescribing=1 or 
        &_ylab_result_cm=1 %then %do;
            %prnt(pdsn=xtbl_l3_non_unique);
    %end;
    %if &_yencounter=1 and &_ydemographic=1 %then %do;
        %prnt(pdsn=xtbl_l3_race_enc);
    %end;
    %if &_ydemographic=1 or &_yencounter=1 or &_yvital=1 or &_yprescribing=1 or
        &_ylab_result_cm=1 %then %do;
            %prnt(pdsn=xtbl_l3_times);
    %end;
    %if &_yharvest=1 %then %do;
        %prnt(pdsn=xtbl_l3_metadata,dropvar=);
        %prnt(pdsn=xtbl_l3_refresh_date,dropvar=);
    %end;
%end;
%if %upcase(&_dcpart)=_DCPART2 or %upcase(&_dcpart)=_ALL %then %do;
    %if &_yencounter=1 %then %do;
        %prnt(pdsn=enc_l3_adate_y);
        %prnt(pdsn=enc_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
        %prnt(pdsn=enc_l3_enctype_adate_y,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=enc_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=enc_l3_ddate_y);
        %prnt(pdsn=enc_l3_ddate_ym,_obs=100,_svar=discharge_date,_suppvar=record_n );
        %prnt(pdsn=enc_l3_enctype_ddate_ym,_obs=100,_svar=enc_type discharge_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=enc_l3_enctype_admsrc,_obs=100,_svar=enc_type admitting_source,_suppvar=record_n record_pct);
        %prnt(pdsn=enc_l3_enctype_disdisp);
        %prnt(pdsn=enc_l3_enctype_disstat,_obs=100,_svar=enc_type discharge_status,_suppvar=record_n record_pct);
        %prnt(pdsn=enc_l3_enctype_drg,_obs=100,_svar=enc_type drg,_suppvar=record_n record_pct);
        %prnt(pdsn=enc_l3_facilityloc,_obs=100,_svar=facility_location,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=enc_l3_facilitytype_facilityloc,_obs=100,_svar=facility_type facility_location,_suppvar=record_n record_pct distinct_patid_n,
                    orient=landscape);
        %prnt(pdsn=enc_l3_los_dist,_svar=enc_type los_group,_suppvar=record_n record_pct);
        %prnt(pdsn=enc_l3_payertype_y,_obs=100,_svar=admit_date payer_type_primary,_suppvar=record_n distinct_patid_n);
    %end;
    %if &_ydiagnosis=1 %then %do;    
        %prnt(pdsn=dia_l3_adate_y);
        %prnt(pdsn=dia_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
        %prnt(pdsn=dia_l3_dxdate_y);
        %prnt(pdsn=dia_l3_dxdate_ym,_obs=100,_svar=dx_date,_suppvar=record_n);
        %prnt(pdsn=dia_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_encid_n distinct_patid_n);
        %prnt(pdsn=dia_l3_dxtype_adate_y);
        %prnt(pdsn=dia_l3_pdx_enctype,_obs=100,_svar=pdx,_suppvar=enc_type dx_origin record_n distinct_encid_n distinct_patid_n);
        %prnt(pdsn=dia_l3_pdxgrp_enctype,_obs=100,_svar=pdxgrp,_suppvar=enc_type dx_origin distinct_encid_n,_recordn=distinct_encid_n);
        %prnt(pdsn=dia_l3_origin);
        %prnt(pdsn=dia_l3_dxtype);
        %prnt(pdsn=dia_l3_dx_dxtype,_obs=100,_svar=dx dx_type,_suppvar=record_n distinct_patid_n);
    %end;
    %if &_yprocedures=1 %then %do;
        %prnt(pdsn=pro_l3_adate_y);
        %prnt(pdsn=pro_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
        %prnt(pdsn=pro_l3_pxdate_y);
        %prnt(pdsn=pro_l3_pxtype_adate_y,_obs=100,_svar=px_type admit_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=pro_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_encid_n distinct_patid_n);
        %prnt(pdsn=pro_l3_px_pxtype,_obs=100,_svar=px px_type,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=pro_l3_pxtype);
    %end;
    %if &_yvital=1 %then %do;    
        %prnt(pdsn=vit_l3_mdate_y);
        %prnt(pdsn=vit_l3_mdate_ym,_obs=100,_svar=measure_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=vit_l3_ht);
        %prnt(pdsn=vit_l3_ht_dist);
        %prnt(pdsn=vit_l3_wt);
        %prnt(pdsn=vit_l3_wt_dist);
        %prnt(pdsn=vit_l3_diastolic);
        %prnt(pdsn=vit_l3_systolic);
        %prnt(pdsn=vit_l3_bmi);
        %prnt(pdsn=vit_l3_dash1);
    %end;
    %if &_ydeath=1 %then %do;
        %prnt(pdsn=death_l3_date_y);
        %prnt(pdsn=death_l3_date_ym,_obs=100,_svar=death_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=death_l3_source_ym,_obs=100,_svar=death_source,_suppvar=death_date record_n distinct_patid_n);
    %end;
    %if &_ydispensing=1 %then %do;
        %prnt(pdsn=disp_l3_ddate_y);
        %prnt(pdsn=disp_l3_ddate_ym,_obs=100,_svar=dispense_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=disp_l3_ndc,_obs=100,_svar=ndc,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=disp_l3_supdist2);
        %prnt(pdsn=disp_l3_dispamt_dist);
        %prnt(pdsn=disp_l3_dose_dist);
    %end;
    %if &_yprescribing=1 %then %do;
        %prnt(pdsn=pres_l3_rxcui,_obs=100,_svar=rxnorm_cui,_suppvar=rxnorm_cui_tty record_n record_pct distinct_patid_n);
        %prnt(pdsn=pres_l3_rxcui_tier);
        %prnt(pdsn=pres_l3_odate_y);
        %prnt(pdsn=pres_l3_odate_ym,_obs=100,_svar=rx_order_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=pres_l3_supdist2);
        %prnt(pdsn=pres_l3_rxqty_dist);
        %prnt(pdsn=pres_l3_rxrefill_dist);
        %prnt(pdsn=pres_l3_rxdoseodr_dist);
        %prnt(pdsn=pres_l3_rxcui_rxsup,_obs=100,_svar=rxnorm_cui,_suppvar=min mean max n nmiss,_recordn=n);
    %end;
    %if &_ymed_admin=1 %then %do;
        %prnt(pdsn=medadm_l3_doseadm);
        %prnt(pdsn=medadm_l3_code_type,_obs=100,_svar=medadmin_code medadmin_type,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=medadm_l3_sdate_y);
        %prnt(pdsn=medadm_l3_sdate_ym,_obs=100,_svar=medadmin_start_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=medadm_l3_rxcui_tier);
    %end;
    %if &_ycondition=1 %then %do;
        %prnt(pdsn=cond_l3_condition,_obs=100,_svar=condition,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=cond_l3_rdate_y);
        %prnt(pdsn=cond_l3_rdate_ym,_obs=100,_svar=report_date,_suppvar=record_n);
    %end;
    %if &_ypro_cm=1 %then %do;
        %prnt(pdsn=procm_l3_pdate_y);
        %prnt(pdsn=procm_l3_pdate_ym,_obs=100,_svar=pro_date,_suppvar=record_n);
        %prnt(pdsn=procm_l3_loinc,_obs=100,_svar=pro_code,_suppvar=panel_type record_n record_pct distinct_patid_n);
        %prnt(pdsn=procm_l3_fullname,_obs=100,_svar=pro_fullname,_suppvar=record_n record_pct);
        %prnt(pdsn=procm_l3_name,_obs=100,_svar=pro_name,_suppvar=record_n record_pct);
    %end;
    %if &_yobs_gen=1 %then %do;
        %prnt(pdsn=obsgen_l3_sdate_y);
        %prnt(pdsn=obsgen_l3_sdate_ym,_obs=100,_svar=obsgen_start_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=obsgen_l3_code_type,_obs=100,_svar=obsgen_code,_suppvar=obsgen_type record_n distinct_patid_n);
        %prnt(pdsn=obsgen_l3_code_unit,_obs=100,_svar=obsgen_code,_suppvar=obsgen_result_unit record_n);
        %prnt(pdsn=obsgen_l3_recordc);
    %end;
    %if &_yldsadrs=1 %then %do;
        %prnt(pdsn=ldsadrs_l3_adrscity,_obs=100,_svar=address_city,_suppvar=record_n record_pct);
        %prnt(pdsn=ldsadrs_l3_adrszip5,_obs=100,_svar=address_zip5,_suppvar=record_n record_pct);
        %prnt(pdsn=ldsadrs_l3_adrszip9,_obs=100,_svar=address_zip9,_suppvar=record_n record_pct);
    %end;
    %if &_yimmunization=1 %then %do;
        %prnt(pdsn=immune_l3_rdate_y);
        %prnt(pdsn=immune_l3_rdate_ym,_obs=100,_svar=vx_record_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=immune_l3_adate_y);
        %prnt(pdsn=immune_l3_adate_ym,_obs=100,_svar=vx_admin_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=immune_l3_codetype);
        %prnt(pdsn=immune_l3_dose_dist);
        %prnt(pdsn=immune_l3_lotnum,_obs=100,_svar=vx_lot_num,_suppvar=record_n record_pct);
    %end;
    %if &_yobs_clin=1 %then %do;
        %prnt(pdsn=obsclin_l3_code_type,_obs=100,_svar=obsclin_code,_suppvar=obsclin_type record_n distinct_patid_n);
        %prnt(pdsn=obsclin_l3_sdate_y);
        %prnt(pdsn=obsclin_l3_sdate_ym,_obs=100,_svar=obsclin_start_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=obsclin_l3_code_unit,_obs=100,_svar=obsclin_code,_suppvar=obsclin_result_unit record_n);
        %prnt(pdsn=obsclin_l3_recordc);
        %prnt(pdsn=obsclin_l3_loinc,_obs=100,_svar=obsclin_code,_suppvar=panel_type record_n record_pct distinct_patid_n);
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %prnt(pdsn=lab_l3_recordc);
        %prnt(pdsn=lab_l3_snomed,_obs=100,_svar=result_snomed,_suppvar=record_n record_pct distinct_patid_n);
        %prnt(pdsn=lab_l3_px_pxtype,_obs=100,_svar=lab_px lab_px_type,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=lab_l3_rdate_y);
        %prnt(pdsn=lab_l3_rdate_ym,_obs=100,_svar=result_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=lab_l3_loinc,_obs=100,_svar=lab_loinc,_suppvar=panel_type record_n record_pct distinct_patid_n);
        %prnt(pdsn=lab_l3_loinc_unit,_obs=100,_svar=lab_loinc,_suppvar=result_unit record_n);
        %prnt(pdsn=lab_l3_loinc_result_num,_obs=100,_svar=lab_loinc,_suppvar=min p1 p5 p25 median p75 p95 p99 max n nmiss,_recordn=n);
    %end;
    %if &_ylab_history=1 %then %do;
        %prnt(pdsn=labhist_l3_min_wks);
        %prnt(pdsn=labhist_l3_max_wks);
        %prnt(pdsn=labhist_l3_pdstart_y);
        %prnt(pdsn=labhist_l3_pdend_y);
    %end;
    %if &_ypat_relationship=1 %then %do;
        %prnt(pdsn=patrel_l3_sdate_y);
        %prnt(pdsn=patrel_l3_sdate_ym,_obs=100,_svar=relationship_start,_suppvar=record_n distinct_patid_1_n distinct_patid_2_n);
    %end;
    %if &_yexternal_meds=1 %then %do;
        %prnt(pdsn=extmed_l3_rdate_y);
        %prnt(pdsn=extmed_l3_rdate_ym,_obs=100,_svar=ext_record_date,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=extmed_l3_rxdose_dist);
        %prnt(pdsn=extmed_l3_rxcui);
        %prnt(pdsn=extmed_l3_rxcui_tier);
    %end;
    %if &_yencounter=1 and &_ylab_result_cm=1 %then %do;
        %prnt(pdsn=xtbl_l3_lab_enctype);
    %end;
    %if &_yencounter=1 and &_yprescribing=1 %then %do;
        %prnt(pdsn=xtbl_l3_pres_enctype);
    %end;
    %if &_yencounter=1 and &_ymed_admin=1 %then %do;
        %prnt(pdsn=xtbl_l3_medadm_enctype);
    %end;
    %if &_yldsadrs=1 and &_yencounter=1 %then %do;
        %prnt(pdsn=xtbl_l3_zip5_5y,_obs=100,_svar=address_zip5,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=xtbl_l3_zip5_1y,_obs=100,_svar=address_zip5,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=xtbl_l3_zip5c_5y,_obs=100,_svar=address_zip5,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=xtbl_l3_zip5c_1y,_obs=100,_svar=address_zip5,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=xtbl_l3_countyfipsc_5y,_obs=100,_svar=county_fips,_suppvar=record_n distinct_patid_n);
        %prnt(pdsn=xtbl_l3_statefipsc_5y);
        %prnt(pdsn=xtbl_l3_rucac_5y);
    %end;
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

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;

********************************************************************************;
* Macro end
********************************************************************************;
%mend _print;
%_print;
