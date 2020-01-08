/*******************************************************************************
*  $Source: data_curation_print $;
*    $Date: 2019/11/21
*    Study: PCORnet
*
*  Purpose: Print PCORnet Data Curation Query Package V5.11
* 
*   Inputs: SAS program:  /sasprograms/run_queries.sas
*
*  Outputs: 
*           1) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_<group>.pdf)
*
*  Requirements: Program run in SAS 9.3 or higher
********************************************************************************/

********************************************************************************;
* Macro to prevent open code
********************************************************************************;
%macro _print;

*******************************************************************************;
* Create dataset of query execution time
*******************************************************************************;
data dmlocal.elapsed_%lowcase(&_grp);
     set elapsed;
     if query^=" ";
     label _qstart="Query start time"
           _qend="Query end time"
           elapsedtime="Query run time (hh:mm:ss)"
           totalruntime="Cumulative run time (hh:mm:ss)"
     ;
     if query="DC PROGRAM" then do;
        query="DC PROGRAM - %upcase(&_grp)";
        _qend=datetime();
        elapsedtime=_qend-_qstart;
        totalruntime=_qend-&_pstart;
     end;

     * call standard variables *;
     %stdvar
run;

********************************************************************************;
* Create a SAS transport file from all of the query datasets
********************************************************************************;
proc contents data=dmlocal._all_ out=dsn noprint;
run;

*- Place all dataset names and labels into macro variables -*;
proc contents data=dmlocal._all_ noprint 
     %if %upcase(&_grp)=MAIN %then %do;
          out=dmlocal(where=((scan(upcase(memname),1,'_') in ("DEATH" "DEATHC" 
              "DEM" "ENC" "DIA" "PRO" "ENR" "VIT" "DISP" "IMMUNE" "PRES" "MEDADM"
              "OBSCLIN" "OBSGEN" "COND" "PROCM" "PROV" "LDSADRS" "HASH" "TRIAL") 
              and scan(upcase(memname),2,'_')="L3") or (upcase(memname)="ELAPSED_MAIN")));
     %end;
     %else %if %upcase(&_grp)=LAB %then %do;
          out=dmlocal(where=(substr(upcase(memname),1,7)="LAB_L3_" or 
              (upcase(memname)="ELAPSED_LAB")));
     %end;
     %else %if %upcase(&_grp)=XTBL %then %do;
          out=dmlocal(where=(substr(upcase(memname),1,8)="XTBL_L3_" or 
                            (upcase(memname) in ("DATAMART_ALL" "ELAPSED_XTBL"))));
     %end;
     %else %if %upcase(&_grp)=ALL %then %do;
          out=dmlocal(where=(upcase(memname) not in ("ELAPSED_MAIN" "ELAPSED_LAB" "ELAPSED_XTBL" "CODE_SUMMARY") and 
                             scan(upcase(memname),1,'_')^="BAD"));
     %end;
run;

proc sql noprint;
      select unique memname into :workdata separated by ' '  from dmlocal;
      select count(unique memname) into :workdata_count from dmlocal;
quit;
%put &workdata;

filename tranfile "&qpath.drnoc/&dmid._&tday._data_curation_%lowcase(&_grp).cpt";
proc cport library=dmlocal file=tranfile memtype=data;
     select &workdata;
run;        

********************************************************************************;
* Create CSV if requested from all of the query datasets
********************************************************************************;
%if %upcase(&_csv)=YES %then %do;

    *- Export each dataset into a CSV file -*;
    %macro csv;
        %do d = 1 %to &workdata_count;
        %let _dsn=%scan(&workdata,&d," ");

        filename csvout "&qpath.dmlocal/&_dsn..csv";
        proc export data=dmlocal.&_dsn
            outfile=csvout
            dbms=dlm label replace;
            delimiter=',';
        run;
    %end;
    %mend csv;
    %csv
%end;

*******************************************************************************;
* Print each data set and send to a PDF file
*******************************************************************************;
ods html close;
ods listing;
ods path sashelp.tmplmst(read) library.templat(read);
ods pdf file="&qpath.drnoc/&dmid._&tday._data_curation_%lowcase(&_grp).pdf" style=journal;

title "Data Curation query run times - ELAPSED_%upcase(&_grp)";
footnote;
proc print width=min data=dmlocal.elapsed_%lowcase(&_grp) label;
     var query _qstart _qend elapsedtime totalruntime;
run;
title;

*- Print if ALL -*;
%if %upcase(&_grp)=ALL %then %do;
    title "DATAMART_ALL_NOBS";
    proc print width=min data=datamart_all label;
         var memname nobs;
    run;
    title;
%end;

*- print if Main or All -*;
%if %upcase(&_grp)=MAIN or %upcase(&_grp)=ALL %then %do;

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
    %prnt(pdsn=enc_l3_facilitytype,_obs=100,_svar=facility_type_grp facility_type,_suppvar=record_n record_pct distinct_patid_n,orient=landscape);
    %prnt(pdsn=enc_l3_facilityloc,_obs=100,_svar=facility_location,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=enc_l3_facilitytype_facilityloc,_obs=100,_svar=facility_type facility_location,_suppvar=record_n record_pct distinct_patid_n,
                orient=landscape);
%end;
%if &_ydiagnosis=1 %then %do;    
    %prnt(pdsn=dia_l3_n);
    %prnt(pdsn=dia_l3_dx,_obs=100,_svar=dx,_suppvar=record_n record_pct);
    %prnt(pdsn=dia_l3_dx_dxtype,_obs=100,_svar=dx dx_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=dia_l3_dxtype);
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
    %prnt(pdsn=dia_l3_dxdate_y);
    %prnt(pdsn=dia_l3_dxdate_ym,_obs=100,_svar=dx_date,_suppvar=record_n);
    %prnt(pdsn=dia_l3_dcgroup);
%end;
%if &_yprocedures=1 %then %do;
    %prnt(pdsn=pro_l3_n);
    %prnt(pdsn=pro_l3_px,_obs=100,_svar=px,_suppvar=record_n record_pct);
    %prnt(pdsn=pro_l3_pxtype);
    %prnt(pdsn=pro_l3_adate_y);
    %prnt(pdsn=pro_l3_adate_ym,_obs=100,_svar=admit_date,_suppvar=record_n);
    %prnt(pdsn=pro_l3_pxdate_y);
    %prnt(pdsn=pro_l3_enctype);
    %prnt(pdsn=pro_l3_pxtype_enctype,_obs=100,_svar=px_type enc_type,_suppvar=record_n);
    %prnt(pdsn=pro_l3_enctype_adate_ym,_obs=100,_svar=enc_type admit_date,_suppvar=record_n distinct_encid_n distinct_patid_n);
    %prnt(pdsn=pro_l3_px_pxtype,_obs=100,_svar=px px_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=pro_l3_pxsource)
    %prnt(pdsn=pro_l3_pxtype_adate_y,_obs=100,_svar=px_type admit_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=pro_l3_ppx)
    %prnt(pdsn=pro_l3_dcgroup);
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
    %prnt(pdsn=death_l3_source_ym,_obs=100,_svar=death_source,_suppvar=death_date record_n distinct_patid_n);
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
    %prnt(pdsn=disp_l3_source);
%end;    
%if &_yprescribing=1 %then %do;
    %prnt(pdsn=pres_l3_n);
    %prnt(pdsn=pres_l3_rxcui,_obs=100,_svar=rxnorm_cui,_suppvar=rxnorm_cui_tty record_n record_pct distinct_patid_n);
    %prnt(pdsn=pres_l3_rxcui_5y,_obs=100,_svar=rxnorm_cui,_suppvar=rxnorm_cui_tty record_n record_pct distinct_patid_n);
    %prnt(pdsn=pres_l3_rxcui_tier);
    %prnt(pdsn=pres_l3_rxcui_tier_5y);
    %prnt(pdsn=pres_l3_supdist2);
    %prnt(pdsn=pres_l3_rxcui_rxsup,_obs=100,_svar=rxnorm_cui,_suppvar=min mean max n nmiss,_recordn=n);
    %prnt(pdsn=pres_l3_basis);
    %prnt(pdsn=pres_l3_freq);
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
    %prnt(pdsn=procm_l3_source);
%end;
%if &_yobs_clin=1 %then %do;
    %prnt(pdsn=obsclin_l3_n);
    %prnt(pdsn=obsclin_l3_mod);
    %prnt(pdsn=obsclin_l3_qual);
    %prnt(pdsn=obsclin_l3_runit,_obs=100,_svar=obsclin_result_unit,_suppvar=short_yn record_n record_pct);
    %prnt(pdsn=obsclin_l3_type);
    %prnt(pdsn=obsclin_l3_code_type,_obs=100,_svar=obsclin_code,_suppvar=obsclin_type record_n distinct_patid_n);
    %prnt(pdsn=obsclin_l3_source);
%end;
%if &_yobs_gen=1 %then %do;
    %prnt(pdsn=obsgen_l3_n);
    %prnt(pdsn=obsgen_l3_mod);
    %prnt(pdsn=obsgen_l3_tmod);
    %prnt(pdsn=obsgen_l3_qual);
    %prnt(pdsn=obsgen_l3_runit,_obs=100,_svar=obsgen_result_unit,_suppvar=short_yn record_n record_pct);
    %prnt(pdsn=obsgen_l3_type);
    %prnt(pdsn=obsgen_l3_code_type,_obs=100,_svar=obsgen_code,_suppvar=obsgen_type record_n distinct_patid_n);
    %prnt(pdsn=obsgen_l3_source);
%end;
%if &_yprovider=1 %then %do;
    %prnt(pdsn=prov_l3_n);
    %prnt(pdsn=prov_l3_npiflag);
    %prnt(pdsn=prov_l3_specialty,_obs=100,_svar=provider_specialty_primary,_suppvar=record_n record_pct);
    %prnt(pdsn=prov_l3_specialty_group);
    %prnt(pdsn=prov_l3_sex);
%end;
%if &_yldsadrs=1 %then %do;
    %prnt(pdsn=ldsadrs_l3_n);
    %prnt(pdsn=ldsadrs_l3_adrsuse);
    %prnt(pdsn=ldsadrs_l3_adrstype);
    %prnt(pdsn=ldsadrs_l3_adrspref);
    %prnt(pdsn=ldsadrs_l3_adrscity,_obs=100,_svar=address_city,_suppvar=record_n record_pct);
    %prnt(pdsn=ldsadrs_l3_adrsstate);
    %prnt(pdsn=ldsadrs_l3_adrszip5,_obs=100,_svar=address_zip5,_suppvar=record_n record_pct);
    %prnt(pdsn=ldsadrs_l3_adrszip9,_obs=100,_svar=address_zip9,_suppvar=record_n record_pct);
%end;
%if &_yimmunization=1 %then %do;
    %prnt(pdsn=immune_l3_n);
    %prnt(pdsn=immune_l3_rdate_y);
    %prnt(pdsn=immune_l3_rdate_ym,_obs=100,_svar=vx_record_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=immune_l3_adate_y);
    %prnt(pdsn=immune_l3_adate_ym,_obs=100,_svar=vx_admin_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=immune_l3_codetype);
    %prnt(pdsn=immune_l3_code_codetype);
    %prnt(pdsn=immune_l3_status);
    %prnt(pdsn=immune_l3_statusreason);
    %prnt(pdsn=immune_l3_source);
    %prnt(pdsn=immune_l3_dose_dist);
    %prnt(pdsn=immune_l3_doseunit,_obs=100,_svar=vx_dose_unit,_suppvar=record_n record_pct);
    %prnt(pdsn=immune_l3_route,_obs=100,_svar=vx_route,_suppvar=record_n record_pct);
    %prnt(pdsn=immune_l3_bodysite,_obs=100,_svar=vx_body_site,_suppvar=record_n record_pct);
    %prnt(pdsn=immune_l3_manufacturer,_obs=100,_svar=vx_manufacturer,_suppvar=record_n record_pct);
    %prnt(pdsn=immune_l3_lotnum,_obs=100,_svar=vx_lot_num,_suppvar=record_n record_pct);
%end;
%if &_yhash_token=1 %then %do;
    %prnt(pdsn=hash_l3_n);
%end;
%if &_yptrial=1 %then %do;
    %prnt(pdsn=trial_l3_n);
%end;

*- end print of Main or All -*;
%end;

*- print if Lab or All -*;
%if %upcase(&_grp)=LAB or %upcase(&_grp)=ALL %then %do;

%if &_ylab_result_cm=1 %then %do;
    %prnt(pdsn=lab_l3_n);
    %prnt(pdsn=lab_l3_n_5y);
    %if &raw_lab_name=1 %then %do;
        %prnt(pdsn=lab_l3_raw_name,_obs=100,_svar=raw_lab_name,_suppvar=record_n record_pct distinct_patid_n);
    %end;
    %prnt(pdsn=lab_l3_rdate_y);
    %prnt(pdsn=lab_l3_rdate_ym,_obs=100,_svar=result_date,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=lab_l3_priority);
    %prnt(pdsn=lab_l3_loc);
    %prnt(pdsn=lab_l3_loinc,_obs=100,_svar=lab_loinc,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=lab_l3_loinc_unit,_obs=100,_svar=lab_loinc,_suppvar=result_unit record_n);
    %prnt(pdsn=lab_l3_loinc_source,_obs=100,_svar=lab_loinc,_suppvar=specimen_source exp_specimen_source record_n);
    %prnt(pdsn=lab_l3_loinc_source_5y,_obs=100,_svar=lab_loinc,_suppvar=specimen_source exp_specimen_source record_n);
    %prnt(pdsn=lab_l3_dcgroup,_obs=100,_svar=dc_lab_group,_suppvar=include_edc record_n record_pct distinct_patid_n);
    %prnt(pdsn=lab_l3_px_type);
    %prnt(pdsn=lab_l3_px_pxtype,_obs=100,_svar=lab_px lab_px_type,_suppvar=record_n distinct_patid_n);
    %prnt(pdsn=lab_l3_mod);
    %prnt(pdsn=lab_l3_low);
    %prnt(pdsn=lab_l3_high);
    %prnt(pdsn=lab_l3_abn);
    %prnt(pdsn=lab_l3_recordc);
    %prnt(pdsn=lab_l3_recordc_5y);
    %prnt(pdsn=lab_l3_loinc_result_num,_obs=100,_svar=lab_loinc,_suppvar=min p1 p5 p25 median p75 p95 p99 max n nmiss,_recordn=n);
    %prnt(pdsn=lab_l3_loinc_result_num_5y,_obs=100,_svar=lab_loinc,_suppvar=min p1 p5 p25 median p75 p95 p99 max n nmiss,_recordn=n);
    %prnt(pdsn=lab_l3_snomed,_obs=100,_svar=result_snomed,_suppvar=record_n record_pct distinct_patid_n);
    %prnt(pdsn=lab_l3_source,_obs=100,_svar=specimen_source,_suppvar=short_yn record_n record_pct);
    %prnt(pdsn=lab_l3_unit,_obs=100,_svar=result_unit,_suppvar=short_yn record_n record_pct);
    %prnt(pdsn=lab_l3_qual);
    %prnt(pdsn=lab_l3_rsource);
    %prnt(pdsn=lab_l3_lsource);
%end;
    
*- end print of Lab or All -*;
%end;
    
*- print if XTBL or All -*;
%if %upcase(&_grp)=XTBL or %upcase(&_grp)=ALL %then %do;

    %if %upcase(&_grp)=XTBL %then %do;
        title "DATAMART_ALL_NOBS";
        proc print width=min data=datamart_all label;
             var memname nobs;
        run;
        title;
    %end;

%if &_yharvest=1 %then %do;
    %prnt(pdsn=xtbl_l3_metadata,dropvar=);
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
%if &_yencounter=1 and &_ydemographic=1 %then %do;
    %prnt(pdsn=xtbl_l3_race_enc);
%end;

*- end print of XTBL or All -*;
%end;

*******************************************************************************;
* Close PDF
*******************************************************************************;
ods pdf close;

********************************************************************************;
* Macro end
********************************************************************************;
%mend _print;
%_print;
