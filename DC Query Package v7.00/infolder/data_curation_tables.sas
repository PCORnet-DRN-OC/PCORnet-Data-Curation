/*******************************************************************************
*  $Source: data_curation_query_tables $;
*    $Date: 2025/04/28
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package v7.00 - all queries
* 
*   Inputs: SAS program:  /sasprograms/02_run_queries.sas
*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) SAS transport file of #1 stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_<dcpart>.cpt)
*           3) SAS log file of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_<dcpart>.log)
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

filename qlog "&qpath.drnoc/&dmid._&tday._data_curation%lowcase(&_dcpart).log" lrecl=200;

proc printto log=qlog new;
run;

******************************************************************************;
* DATAMART_ALL
******************************************************************************;  
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
	"HASH_TOKEN"="20"
	"LDS_ADDRESS_HISTORY"="21"
	"IMMUNIZATION"="22"
	"LAB_HISTORY"="23"
    "PAT_RELATIONSHIP"="24"
    "EXTERNAL_MEDS"="25"
	other=" "
	;
run;
	
* Get metadata from DataMart data structures;
proc contents data=pcordata._all_ noprint out=datamart_all;
run;

* Create ordering variable for output;
data datamart_all;
	set datamart_all;
	memname=upcase(memname);
	name=upcase(name);
	ord=put(upcase(memname),$ord.);
	query_response_date="&tday";
	
	* Subset on required data structures;
	if ord^=' ';
run;

%let _mtyped = 0;
%let _mtypev = 0;

* Place all table names into a macro variable;
proc sql noprint;
	select unique memname into :workdata separated by '|'  from datamart_all;
	select count(unique memname) into :workdata_count from datamart_all;
	select max(length) into :maxlength_enc from sashelp.vcolumn 
		where upcase(libname)="PCORDATA" and upcase(name)="ENCOUNTERID";
	select max(length) into :maxlength_prov from sashelp.vcolumn 
		where upcase(libname)="PCORDATA" and upcase(name) in 
			("PROVIDERID", "VX_PROVIDERID" "MEDADMIN_PROVIDERID" 
			"OBSCLIN_PROVIDERID" "OBSGEN_PROVIDERID" "RX_PROVIDERID");
	select 1 into :_mtyped from datamart_all where memtype="DATA";
	select 1 into :_mtypev from datamart_all where memtype="VIEW";
quit;

* Obtain the number of observations from each table;
%macro nobs;
		
	%do d=1 %to &workdata_count;
			
		%let _dsn=%scan(&workdata,&d,"|");
	
		%global nobs_&_dsn;
	
		* Get the number of obs from each dataset;
		proc sql noprint;
			* Output into dataset for compliation with other source dataset info;
			create table nobs&d as
			select count(*) as _nobs_n from pcordata.&_dsn;
	
			* Output into macro variable for later conditional coding;
			select count(*) into: nobs_&_dsn from pcordata.&_dsn;
		quit;
	
		data nobs&d;
			length memname $32 nobs $20;
			set nobs&d;
			memname="&_dsn";
			ord=put(upcase(memname),$ord.);
			nobs=strip(put(_nobs_n,16.));
			drop _nobs_n;
			output;
		run;
	
		* Compile into one dataset;
		proc append base=nobs_all data=nobs&d;
		run;
	
		* Delete intermediary dataset;
		proc delete data=nobs&d;
		quit;
			
	%end;
		
%mend nobs;
	
%nobs;

* Add number of observations ;
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

%if %upcase(&_part1)=YES %then %do;    
	
	* Create a dataset of DataMart metadata ;
	proc sort data=datamart_all out=dmlocal.datamart_all;
		by ord memname name;
	run;

%end;

* Delete intermediary datasets;
proc delete data=datamart_all;
quit;

******************************************************************************;
* XTBL_L3_METADATA
******************************************************************************;
%if &_yharvest=1 and %upcase(&_part1)=YES %then %do;

	%let qname=xtbl_l3_metadata;
	%elapsed(begin);
	
	* Place all CDM version numbers into macro variable ;
		proc sql noprint;
			select code_w_str into :_cdm_ver separated by ', ' from cdm_version;
		quit;
	
	* Read data set created at top of program ;
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
	
	* Bring everything together;
	data data;    
		length networkid network_name datamartid datamart_name query_package tag 
			datamart_platform cdm_version datamart_claims datamart_ehr
			birth_date_mgmt enr_start_date_mgmt enr_end_date_mgmt admit_date_mgmt
			discharge_date_mgmt px_date_mgmt rx_order_date_mgmt rx_start_date_mgmt
			rx_end_date_mgmt dispense_date_mgmt lab_order_date_mgmt 
			specimen_date_mgmt result_date_mgmt measure_date_mgmt onset_date_mgmt
			report_date_mgmt resolve_date_mgmt pro_date_mgmt death_date_mgmt
			medadmin_start_date_mgmt medadmin_stop_date_mgmt obsclin_start_date_mgmt
			obsclin_stop_date_mgmt obsgen_start_date_mgmt obsgen_stop_date_mgmt
			dx_date_mgmt address_period_start_mgmt address_period_end_mgmt 
			vx_record_date_mgmt vx_admin_date_mgmt vx_exp_date_mgmt $50 
			datastore $5 operating_system sas_version $100 ;
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
							refresh_provider_date=refresh_provider_daten
							refresh_hash_token_date=refresh_hash_token_daten
							refresh_lds_address_hx_date=refresh_lds_address_hx_datn
							refresh_immunization_date=refresh_immunization_daten));
	
		* Table variables;
		query_package="&dc";
		lookback_months=strip(put(%eval(&lookback*12),8.));
		response_date=put("&sysdate"d,yymmdd10.);
		tag=strip(query_package)||"_"||strip(datamartid)||"_"||strip(response_date);
	
		if datamart_platform not in ("01" "02" "03" "04" "05" "NI" "OT" "UN" " ")
			then datamart_platform="Values outside of CDM specifications";
	
		if cdm_version_num not in (&_cdm_ver) 
			then cdm_version="Values outside of CDM specifications";
		else cdm_version=strip(cdm_version_num);
	
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
					medadmin_start_date_mgmt medadmin_stop_date_mgmt 
					obsclin_start_date_mgmt obsclin_stop_date_mgmt 
					obsgen_start_date_mgmt obsgen_stop_date_mgmt 
					dx_date_mgmt address_period_start_mgmt address_period_end_mgmt
					vx_record_date_mgmt vx_admin_date_mgmt vx_exp_date_mgmt;
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
						refresh_obs_gen_date refresh_provider_date
						refresh_hash_token_date refresh_lds_address_hx_date
						refresh_immunization_date ;
		array rfn refresh_demographic_daten refresh_enrollment_daten 
					refresh_encounter_daten refresh_diagnosis_daten 
					refresh_procedures_daten refresh_vital_daten 
					refresh_dispensing_daten refresh_lab_result_cm_daten 
					refresh_condition_daten refresh_pro_cm_daten 
					refresh_prescribing_daten refresh_pcornet_trial_daten
					refresh_death_daten refresh_death_cause_daten
					refresh_med_admin_daten refresh_obs_clin_daten
					refresh_obs_gen_daten refresh_provider_daten
					refresh_hash_token_daten refresh_lds_address_hx_datn
					refresh_immunization_daten ;
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
						refresh_obs_gen_daten, refresh_provider_daten,
						refresh_hash_token_daten, refresh_lds_address_hx_datn,
						refresh_immunization_daten),yymmdd10.);
	
		array v networkid network_name datamartid datamart_name datamart_platform
				datamart_claims datamart_ehr birth_date_mgmt enr_start_date_mgmt 
				enr_end_date_mgmt admit_date_mgmt discharge_date_mgmt 
				px_date_mgmt rx_order_date_mgmt rx_start_date_mgmt rx_end_date_mgmt 
				dispense_date_mgmt lab_order_date_mgmt specimen_date_mgmt 
				result_date_mgmt measure_date_mgmt onset_date_mgmt report_date_mgmt
				resolve_date_mgmt pro_date_mgmt death_date_mgmt obsgen_start_date_mgmt
				obsgen_stop_date_mgmt medadmin_start_date_mgmt medadmin_stop_date_mgmt
				obsclin_start_date_mgmt obsclin_stop_date_mgmt
				vx_record_date_mgmt vx_admin_date_mgmt vx_exp_date_mgmt;
			do v1 = 1 to dim(v);
				if v{v1}^=' ' then v{v1}=v{v1};
				else if v{v1}=' ' then v{v1}="NULL or missing";
			end;
	
		if vsops^=" " then operating_system=strip(vsops);
		if vsas^=" " then sas_version=strip(vsas);
	
		if %upcase("&_mtyped")=1 and %upcase("&_mtypev")=1 then datastore="MIX";
		else if %upcase("&_mtyped")=1 then datastore="SAS";
		else if %upcase("&_mtypev")=1 then datastore="RDBMS";
	
		lookback_date=put(&lookback_dt,yymmdd10.);
		sas_ets_installed=propcase("&_ets_installed");
	run;

	* Normalize data;
	proc transpose data=data out=query(rename=(_name_=name col1=value));
		var networkid network_name datamartid datamart_name datamart_platform
			cdm_version datamart_claims datamart_ehr birth_date_mgmt 
			enr_start_date_mgmt enr_end_date_mgmt admit_date_mgmt 
			discharge_date_mgmt px_date_mgmt rx_order_date_mgmt rx_start_date_mgmt
			rx_end_date_mgmt dispense_date_mgmt lab_order_date_mgmt 
			specimen_date_mgmt result_date_mgmt measure_date_mgmt onset_date_mgmt
			report_date_mgmt resolve_date_mgmt pro_date_mgmt death_date_mgmt 
			medadmin_start_date_mgmt medadmin_stop_date_mgmt obsclin_start_date_mgmt
			obsclin_stop_date_mgmt obsgen_start_date_mgmt obsgen_stop_date_mgmt 
			dx_date_mgmt address_period_start_mgmt
			address_period_end_mgmt vx_record_date_mgmt vx_admin_date_mgmt
			vx_exp_date_mgmt refresh_demographic_date refresh_enrollment_date
			refresh_encounter_date refresh_diagnosis_date refresh_procedures_date 
			refresh_vital_date refresh_dispensing_date refresh_lab_result_cm_date 
			refresh_condition_date refresh_pro_cm_date refresh_prescribing_date
			refresh_pcornet_trial_date refresh_death_date refresh_death_cause_date
			refresh_med_admin_date refresh_obs_clin_date refresh_obs_gen_date 
			refresh_hash_token_date refresh_lds_address_hx_date
			refresh_immunization_date refresh_provider_date
			refresh_max operating_system query_package lookback_months 
			lookback_date sas_ets_installed response_date sas_version 
			sas_base sas_graph sas_stat sas_ets sas_af sas_iml sas_connect sas_mysql
			sas_odbc sas_oracle sas_postgres sas_sql sas_teradata datastore;
	run;
	
	* Assign attributes;
	data query;
		length name $125;
		set query;
	
		name=upcase(name);
		attrib name label="NAME"
				value label="VALUE"
		;
	run;
	
	* Order variables;
	proc sql;
		create table dmlocal.xtbl_l3_metadata as select
			name, value
		from query;
    quit;

    * Delete intermediary datasets;
    proc delete data=access data xtbl_mdata_idsn query cdm_version;
    quit;
        
    %elapsed(end);
    
%end;

******************************************************************************;
* XTBL_L3_REFRESH_DATE
******************************************************************************;
%if &_yharvest=1 and %upcase(&_part1)=YES %then %do;
    
	%let qname=xtbl_l3_refresh_date;
	%elapsed(begin);
	
	* Transpose HARVEST to create one date variable;
	proc transpose data=pcordata.harvest out=refresh;
		var refresh:;
	run;
	
	* Convert refresh variable name to the dataset name;
	data refresh;
		length dataset $32 refresh_date $10;
		set refresh;
	
		* create dataset name from date variable name;
		dataset=substr(reverse(substr(reverse(strip(_name_)),6)),9);
		if dataset="LDS_ADDRESS_HX" then dataset="LDS_ADDRESS_HISTORY";
	
		* Convert date to character;
		if col1^=. then refresh_date=put(col1,date9.);
	
		keep dataset refresh_date;
	run;
	
	proc sort data=refresh;
		by dataset;
	run;
	
	* Need only one observation per dataset;
	proc sort data=nobs_all (keep=memname nobs rename=(memname=dataset)) out=datamart nodupkey;
		by dataset;
	run;
	
	* Bring obs and refresh date data together;
	data query;
		merge
			refresh
			datamart
			;
		by dataset;
		
		if refresh_date=" " then refresh_date="N/A";
		
		if dataset in ("DEMOGRAPHIC", "ENROLLMENT", "ENCOUNTER", "DIAGNOSIS", 
					"PROCEDURES", "VITAL", "LAB_RESULT_CM", "PRESCRIBING", 
					"DISPENSING", "DEATH", "CONDITION", "PRO_CM", 
					"PCORNET_TRIAL", "DEATH_CAUSE", "MED_ADMIN", "OBS_CLIN",
					"OBS_GEN", "PROVIDER", "HASH_TOKEN", "LDS_ADDRESS_HISTORY",
					"IMMUNIZATION");
	
		%stdvar;
	run;
	
	* Order variables;
	proc sql;
		create table dmlocal.&qname as select
			datamartid, response_date, query_package, dataset, nobs, refresh_date
		from query;
	quit;
	
	* Delete intermediary datasets;
	proc delete data=nobs_all refresh query datamart;
	quit;
	
	%elapsed(end);

%end;

*************************************************************************************************;
*************************************************************************************************;
* 1. Bring in, compress, process, and drop all datasets not used in any combined-datasets queries;
*************************************************************************************************;
*************************************************************************************************;


*******************************************************************************;
* Bring in and compress HASH_TOKEN
*******************************************************************************;
%if &_yhash_token=1 and %upcase(&_part1)=YES %then %do;

	%let qname=hash_token;
	%elapsed(begin);

	* Determine concatenated length of variables used to determine HASHID;
	proc contents data=pcordata.hash_token out=cont_hsh noprint;
	run;

	data _null_;
		 set cont_hsh end=eof;
		 retain ulength 0;
		 if name in ('PATID' 'TOKEN_ENCRYPTION_KEY') then ulength=ulength+length;
		
		 * Add ULENGTH (1 delimiters);
		 if eof then call symputx('_hshlength',ulength+1);
	run;

	* Delete intermediary datasets;
	proc delete data=cont_hsh;
	quit;

	data hash_token (compress=yes);
        set pcordata.hash_token (keep=patid token_encryption_key token_01-token_09 token_12 token_14-token_18 token_23-token_26 token_29 token_30 token_101-token_111) end=eof;

        length hashid $&_hshlength;
        
        hashid=strip(patid) || '-' || strip(token_encryption_key);
	run;

	%elapsed(end);

		*******************************************************************************;
		* HASH_TOKEN - All Part 1 summaries
		*******************************************************************************;
        
            /***HASH_L3_N***/
				
			* Start timer outside of macro in order to capture grouping processing time;
			%let qname=hash_l3_n;
            %elapsed(begin);

            * Create VALID versions of all variables;
            data hash_token (compress=yes);
                set hash_token;

                length valid_patid valid_token_encryption_key valid_hashid valid_token_01-valid_token_09 valid_token_12 valid_token_14-valid_token_18 valid_token_23-valid_token_26
                    valid_token_29 valid_token_30 valid_token_101-valid_token_111 $1;
                
                array vr (*) patid token_encryption_key hashid token_01-token_09 token_12 token_14-token_18 token_23-token_26 token_29 token_30 token_101-token_111;
                array vld (*) valid_patid valid_token_encryption_key valid_hashid valid_token_01-valid_token_09 valid_token_12 valid_token_14-valid_token_18 valid_token_23-valid_token_26
                    valid_token_29 valid_token_30 valid_token_101-valid_token_111;

                do i=1 to dim(vr);
                    if length(strip(vr(i)))>=3 and substr(upcase(strip(vr(i))),1,3) ne 'XXX' then vld(i)='Y';
                end;

                drop i;
            run;        

            %tag(dset=HASH_TOKEN, varlist=PATID TOKEN_ENCRYPTION_KEY HASHID TOKEN_01 TOKEN_02 TOKEN_03 TOKEN_04 TOKEN_05 TOKEN_06 TOKEN_07 TOKEN_08 TOKEN_09 TOKEN_12 TOKEN_14 TOKEN_15 TOKEN_16 TOKEN_17 TOKEN_18 TOKEN_23 TOKEN_24 TOKEN_25 TOKEN_26 TOKEN_29 TOKEN_30 TOKEN_101 TOKEN_102 TOKEN_103 TOKEN_104 TOKEN_105 TOKEN_106 TOKEN_107 TOKEN_108 TOKEN_109 TOKEN_110 TOKEN_111, qnam=hash_l3_n, timer=n, distinct=y, validall=y, distvalid=y);

            * Set VALID columns related to PATID, TOKEN_ENCRYPTION_KEY, and HASHID to N/A and calculate DISTINCT_N_PCT and VALID_DISTINCT_N_PCT;
            data dmlocal.hash_l3_n;
                retain datamartid response_date query_package dataset tag all_n distinct_n null_n valid_n valid_distinct_n distinct_n_pct valid_distinct_n_pct;
                set dmlocal.hash_l3_n;

                length distinct_n_pct valid_distinct_n_pct $5;

                if tag in ('PATID' 'TOKEN_ENCRYPTION_KEY' 'HASHID') then do;
                    valid_n='N/A';
                    valid_distinct_n='N/A';
                    distinct_n_pct='N/A';
                    valid_distinct_n_pct='N/A';
                end;
                else do;
                    if strip(all_n) not in ('' '0') then distinct_n_pct=strip(put(input(distinct_n,best12.)/input(all_n,best12.)*100,6.1));
                    else distinct_n_pct='0';

                    if strip(valid_n) not in ('' '0') then valid_distinct_n_pct=strip(put(input(valid_distinct_n,best12.)/input(valid_n,best12.)*100,6.1));
                    else valid_distinct_n_pct='0';
                end;
            run;
          
            %elapsed(end);

            /***End of HASH_L3_N***/
     
            /***HASH_L3_TOKEN_AVAILABILITY***/
				
			* Start timer outside of macro in order to capture grouping processing time;
			%let qname=hash_l3_token_availability;
			%elapsed(begin);

            * Create concatenated token variable;
            data hash_token (compress=yes);
                set hash_token (keep=patid token_encryption_key token_01-token_05 token_16 where=(not missing(patid)));

                * Exclude er_ror code encryption keys;
                if missing(token_encryption_key) or substr(upcase(strip(token_encryption_key)),1,3)='XXX' then delete;
                
                * Set er_ror values (XXX) as empty;
                array t (*) token_01 token_02 token_03 token_04 token_05 token_16;
                array p (*) t01 t02 t03 t04 t05 t16;

                do i=1 to dim(t);
                    if t(i)='' or substr(upcase(t(i)),1,3)='XXX' then p(i)=0;
                    else p(i)=1;
                end;
                
                * Create combination variable;
                ord=sum(t01, t02, t03, t04, t05, t16);

                array f $25 f01 f02 f03 f04 f05 f16 ('T01' 'T02' 'T03' 'T04' 'T05' 'T16');
                
                do j=1 to dim(p);
                    if p(j)=1 then do;
                        if ord=1 then token=f(j);
                        else if ord>1 then token=strip(token) || " " || f(j);
                    end;
                    else token=token;
                end;

                if token='' then token='All Missing';
                
                keep patid token_encryption_key token;
            run;

             * Create custom reference file for all possible TOKEN combinations;
            data token_ref;
                table_name='HASH_TOKEN';
                field_name='TOKEN';
                valueset_item_order=0;

                length valueset_item $50;

                array t (6) _temporary_ (1 2 3 4 5 16);

                valueset_item='All Missing';
                output;

                * Single-token;
                do i=1 to 6;
                    valueset_item_order=i;
                    valueset_item='T' || strip(put(t(i),z2.));
                    output;

                    * Two tokens;
                    do j=1 to 6;
                        if j>i then do;
                            valueset_item_order=i*10+j;
                            valueset_item='T' || strip(put(t(i),z2.)) || ' T' || strip(put(t(j),z2.));
                            output;

                            * Three tokens;
                            do k=1 to 6;
                                if k>j then do;
                                    valueset_item_order=i*100+j*10+k;
                                    valueset_item='T' || strip(put(t(i),z2.)) || ' T' || strip(put(t(j),z2.)) || ' T' || strip(put(t(k),z2.));
                                    output;

                                    * Four tokens;
                                    do l=1 to 6;
                                        if l>k then do;
                                            valueset_item_order=i*1000+j*100+k*10+l;
                                            valueset_item='T' || strip(put(t(i),z2.)) || ' T' || strip(put(t(j),z2.)) || ' T' || strip(put(t(k),z2.)) || ' T' || strip(put(t(l),z2.));
                                            output;

                                            * Five tokens;
                                            do m=1 to 6;
                                                if m>l then do;
                                                    valueset_item_order=i*10000+j*1000+k*100+l*10+m;
                                                    valueset_item='T' || strip(put(t(i),z2.)) || ' T' || strip(put(t(j),z2.)) || ' T' || strip(put(t(k),z2.)) || ' T' || strip(put(t(l),z2.)) || ' T' || strip(put(t(m),z2.));
                                                    output;

                                                    * Six tokens;
                                                    do n=1 to 6;
                                                        if n>m then do;
                                                            valueset_item_order=i*100000+j*10000+k*1000+l*100+m*10+n;
                                                            valueset_item='T' || strip(put(t(i),z2.)) || ' T' || strip(put(t(j),z2.)) || ' T' || strip(put(t(k),z2.)) || ' T' || strip(put(t(l),z2.)) || ' T' || strip(put(t(m),z2.)) || ' T' || strip(put(t(n),2.));
                                                            output;
                                                        end;
                                                    end;
                                                end;
                                            end;
                                        end;
                                    end;
                                end;
                            end;
                        end;                
                    end;
                end;

                drop i j k l m n;
            run;
                
            %n_pct_multilev(dset=HASH_TOKEN, var1=TOKEN_ENCRYPTION_KEY, var2=TOKEN, ref2=token_ref, cdmref2=n, alphasort2=n, qnam=hash_l3_token_availability, distinct=y, timer=n);

            * Drop RECORD_N, add DATASET, and exclude NULL or missing rows;
            data dmlocal.hash_l3_token_availability;
                retain datamartid response_date query_package dataset token_encryption_key token distinct_patid_n;
                set dmlocal.hash_l3_token_availability;

                dataset='HASH_TOKEN';

                if token_encryption_key='NULL or missing' or token='NULL or missing' then delete;

                drop record_n;
            run;

            %elapsed(end);
           
            /***End of HASH_L3_TOKEN_AVAILABILITY***/
                
		* End of Part 1 HASH_TOKEN queries;

    * Delete intermediary dataset and HASH_TOKEN dataset;
    proc delete data=hash_token token_ref;
    run;     

********************************************************************************;
* END HASH_TOKEN QUERY
********************************************************************************;
%end;

*******************************************************************************;
* Bring in and compress PCORNET_TRIAL
*******************************************************************************;
%if &_yptrial=1 and %upcase(&_part1)=YES %then %do;

	%let qname=pcornet_trial;
	%elapsed(begin);

	* Determine concatenated length of variables used to determine TRIAL_KEY;
	proc contents data=pcordata.pcornet_trial out=cont_trl noprint;
	run;

	data _null_;
		 set cont_trl end=eof;
		 retain ulength 0;
		 if name in ('PATID' 'TRIALID' 'PARTICIPANTID') then ulength=ulength+length;
		
		 * Add ULENGTH (2 delimiters);
		 if eof then call symputx('_trllength',ulength+2);
	run;

	* Delete intermediary datasets;
	proc delete data=cont_trl;
	quit;

	data pcornet_trial (compress=yes);
		set pcordata.pcornet_trial (keep=patid trialid participantid) end=eof;

		length trial_key $&_trllength;
		trial_key=strip(patid) || '-' || strip(trialid) || '-' || strip(participantid);
	run;

	%elapsed(end);

		*******************************************************************************;
		* PCORNET_TRIAL - All Part 1 summaries
		*******************************************************************************;
		
			%tag(dset=PCORNET_TRIAL, varlist=PATID TRIALID PARTICIPANTID TRIAL_KEY, qnam=trial_l3_n);

		* End of Part 1 PCORNET_TRIAL queries;

    * Delete PCORNET_TRIAL dataset;
    proc delete data=pcornet_trial;
    run;    
    
********************************************************************************;
* END PCORNET_TRIAL QUERY
********************************************************************************;
%end;	

*******************************************************************************;
* Bring in and compress LAB_HISTORY
*******************************************************************************;
%if &_ylab_history=1 %then %do;

	%let qname=lab_history;
	%elapsed(begin);

	data lab_history (compress=yes);
        set pcordata.lab_history (keep=
            %if %upcase(&_part1)=YES %then %do;
                norm_modifier_high lab_loinc norm_modifier_low labhistoryid lab_facilityid race sex result_unit                  
            %end;
            %if %upcase(&_part2)=YES %then %do;
                age_max_wks age_min_wks period_end period_start rename=(period_end=period_end_fulldt period_start=period_start_fulldt)
            %end;
            ) end=eof;
                
        %if %upcase(&_part2)=YES %then %do;
    		if not missing (period_end_fulldt) then period_end=strip(put(year(period_end_fulldt),4.));
    		if not missing(period_start_fulldt) then period_start=strip(put(year(period_start_fulldt),4.));
        %end;    
	run;

	%elapsed(end);

		*******************************************************************************;
		* LAB_HISTORY - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=LAB_HISTORY,      var=NORM_MODIFIER_HIGH, qnam=labhist_l3_high);
			%n_pct_noref(dset=LAB_HISTORY, var=LAB_LOINC,         qnam=labhist_l3_loinc)
			%n_pct(dset=LAB_HISTORY,      var=NORM_MODIFIER_LOW, qnam=labhist_l3_low);    
			%tag(dset=LAB_HISTORY,        varlist=LABHISTORYID LAB_FACILITYID, qnam=labhist_l3_n);
			%n_pct(dset=LAB_HISTORY,      var=RACE,              qnam=labhist_l3_racedist); 
			%n_pct(dset=LAB_HISTORY,      var=SEX,               qnam=labhist_l3_sexdist); 
			%n_pct(dset=LAB_HISTORY,      var=RESULT_UNIT,       qnam=labhist_l3_unit);    

		%end; * End of Part 1 LAB_HISTORY queries;

		*******************************************************************************;
		* LAB_HISTORY - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;

			%cont(dset=LAB_HISTORY, var=age_max_wks, qnam=labhist_l3_max_wks, stats=min p5 median p95 max n nmiss, dec=0);
			%cont(dset=LAB_HISTORY, var=age_min_wks, qnam=labhist_l3_min_wks, stats=min p5 median p95 max n nmiss, dec=0);        
			%n_pct_noref(dset=LAB_HISTORY, var=PERIOD_END,  qnam=labhist_l3_pdend_y);
			%n_pct_noref(dset=LAB_HISTORY, var=PERIOD_START, qnam=labhist_l3_pdstart_y);  

		%end; * End of Part 2 LAB_HISTORY queries;

    * Delete LAB_HISTORY dataset;
    proc delete data=lab_history;
    run;    

********************************************************************************;
* END LAB_HISTORY QUERIES
********************************************************************************;
%end;	

*******************************************************************************;
* Bring in and compress DEATH_CAUSE
*******************************************************************************;
%if &_ydeathc=1 and %upcase(&_part1)=YES %then %do;

	%let qname=death_cause;
	%elapsed(begin);

	* Determine concatenated length of variables used to determine DEATHID;
	proc contents data=pcordata.death_cause out=cont_deathc noprint;
	run;

	data _null_;
		 set cont_deathc end=eof;
		 retain ulength 0;
		 if name in ('PATID' 'DEATH_CAUSE' 'DEATH_CAUSE_CODE' 'DEATH_CAUSE_TYPE' 'DEATH_CAUSE_SOURCE') then ulength=ulength+length;
		
		 * Add ULENGTH (5 delimiters);
		 if eof then call symputx('_dculength',ulength+5);
	run;

	* Delete intermediary datasets;
	proc delete data=cont_deathc;
	quit;

	data death_cause (compress=yes);
		set pcordata.death_cause (keep=patid death_cause_code death_cause_confidence death_cause_source death_cause_type
			death_cause) end=eof;

		length deathcid $&_dculength;
		deathcid=strip(patid) || '_' || strip(death_cause) || '_' || strip(death_cause_code) || '_' || strip(death_cause_type) || '_' || strip(death_cause_source);
	run;        

	%elapsed(end);

		*******************************************************************************;
		* DEATH_CAUSE - All Part 1 summaries
		*******************************************************************************;
		
			%n_pct(dset=DEATH_CAUSE, var=DEATH_CAUSE_CODE,      qnam=deathc_l3_code);
			%n_pct(dset=DEATH_CAUSE, var=DEATH_CAUSE_CONFIDENCE, qnam=deathc_l3_conf);
			%tag(dset=DEATH_CAUSE,  varlist=PATID DEATH_CAUSE DEATHCID, qnam=deathc_l3_n);
			%n_pct(dset=DEATH_CAUSE, var=DEATH_CAUSE_SOURCE,    qnam=deathc_l3_source);
			%n_pct(dset=DEATH_CAUSE, var=DEATH_CAUSE_TYPE,      qnam=deathc_l3_type);    

		* End of Part 1 DEATH_CAUSE queries;

    * Delete DEATH_CAUSE dataset;
    proc delete data=death_cause;
    run;  
    
********************************************************************************;
* END DEATH_CAUSE QUERIES
********************************************************************************;
%end;

**********************************************************************************;
**********************************************************************************;
*** 2. Bring in and compress all datasets needed for combined-datasets queries ***;
**********************************************************************************;
**********************************************************************************;


*******************************************************************************;
* Bring in and compress PAT_RELATIONSHIP
*******************************************************************************;
%if &_ypat_relationship=1 %then %do;

	%let qname=pat_relationship;
	%elapsed(begin);

	* Determine concatenated length of variables used to determine PATRELID and ELIG_RECORD;
	proc contents data=pcordata.pat_relationship out=cont_patrel noprint;
	run;

	data _null_;
		 set cont_patrel end=eof;

		retain ulength 0;

		if name in ('PATID_1' 'PATID_2' 'RELATIONSHIP_TYPE') then ulength=ulength+length;
		
		* Add ULENGTH (2 delimiters);
		if eof then call symputx('_patrellength',ulength+2);
	run;

	* Delete intermediary datasets;
	proc delete data=cont_patrel;
	quit;

	data pat_relationship (compress=yes);
        set pcordata.pat_relationship (keep=patid_1 patid_2 relationship_start
            %if %upcase(&_part1)=YES %then %do;
                relationship_type relationship_end                
            %end;
            ) end=eof;

        %if %upcase(&_part1)=YES %then %do; 
            length patrelid elig_record $&_patrellength;

            patrelid=strip(patid_1) || '_' || strip(patid_2) || '_' || strip(relationship_type);

            if not missing(patid_1) and not missing(patid_2) and not missing(relationship_type) then elig_record=patrelid;                
        %end;
                
        %if %upcase(&_part2)=YES %then %do;           
            length relstart_date_ym $7;

            if not missing(relationship_start) then relstart_date_ym=put(year(relationship_start),4.) || '_' || put(month(relationship_start),z2.);

            if not missing(relationship_start) then relstart_date_y=put(year(relationship_start),4.);
        %end;    
	run;

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress DEATH
*******************************************************************************;
%if &_ydeath=1 %then %do;

	%let qname=death;
    %elapsed(begin);

    %if %upcase(&_part1)=YES %then %do;

        * Determine concatenated length of variables used to determine DEATHID;
        proc contents data=pcordata.death out=cont_death noprint;
        run;

        data _null_;
            set cont_death end=eof;
            retain ulength 0;
            if name in ('PATID' 'DEATH_SOURCE') then ulength=ulength+length;
		
            * Add ULENGTH (1 delimiter);
            if eof then call symputx('_dclength',ulength+1);
        run;

        * Delete intermediary datasets;
        proc delete data=cont_death;
        quit;

    %end;        
        
	data death (compress=yes);
        set pcordata.death (keep=patid death_date death_source
            %if %upcase(&_part1)=YES %then %do;
                 death_date_impute death_match_confidence
            %end;
            ) end=eof;
        
        %if %upcase(&_part1)=YES %then %do;
        	length deathid $&_dclength;
            deathid=strip(patid) || '_' || strip(death_source);
        %end;

        %if %upcase(&_part2)=YES %then %do;            
            length death_date_ym $7;

            if not missing(death_date) then death_date_ym=put(year(death_date),4.) || '_' || put(month(death_date),z2.);

            if not missing(death_date) then death_date_y=put(year(death_date),4.);
        %end;
	run;

	* Sort for later use in DEM_L3_AGEYRSDIST2 (age derivation);
	proc sort data=death;
		by patid death_date;
	run;

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress ENROLLMENT
*******************************************************************************;
%if &_yenrollment=1 and %upcase(&_part1)=YES %then %do;

	%let qname=enrollment;
	%elapsed(begin);

	* Determine concatenated length of variables used to determine ENROLLID;
	proc contents data=pcordata.enrollment out=cont_enr noprint;
	run;

	data _null_;
		 set cont_enr end=eof;
		 retain ulength 0;
		 if name in ('PATID' 'ENR_BASIS') then ulength=ulength+length;
		
		 * Add ULENGTH (2 delimiters and length of 9 for formatted ENR_START_DATE);
		 if eof then call symputx('_enrlength',ulength+2+9);
	run;

	* Delete intermediary datasets;
	proc delete data=cont_enr;
	quit;

	data enrollment (compress=yes);
		set pcordata.enrollment (keep=patid enr_start_date enr_end_date enr_basis chart) end=eof;

		* Restrict data to within lookback period;
		if enr_start_date>=&lookback_dt or enr_end_date>=&lookback_dt or (missing(enr_start_date) and missing(enr_end_date));

		length enrollid $&_enrlength;
		enrollid=strip(patid) || '_' || put(enr_start_date,date9.) || '_' || strip(enr_basis);
	run;        

    %elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress IMMUNIZATION
*******************************************************************************;
%if &_yimmunization=1 %then %do;

	%let qname=immunization;
	%elapsed(begin);

	data immunization (compress=yes);
        set pcordata.immunization (keep=patid vx_record_date vx_admin_date vx_code_type
            %if %upcase(&_part1)=YES %then %do;
                encounterid vx_exp_date vx_body_site vx_dose_unit vx_manufacturer immunizationid proceduresid vx_providerid vx_route vx_source vx_status vx_status_reason vx_code
            %end;
            %if %upcase(&_part2)=YES %then %do;
                vx_dose vx_lot_num  
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if vx_record_date>=&lookback_dt or missing(vx_record_date);

        %if %upcase(&_part1)=YES %then %do;
            * For mismatch query;
            providerid=vx_providerid; 
        %end;
        %if %upcase(&_part2)=YES %then %do;
            length vx_admin_date_ym vx_record_date_ym $7;

            if not missing(vx_admin_date) then vx_admin_date_ym=put(year(vx_admin_date),4.) || '_' || put(month(vx_admin_date),z2.);

            if not missing(vx_record_date) then vx_record_date_ym=put(year(vx_record_date),4.) || '_' || put(month(vx_record_date),z2.);
		
            if not missing(vx_admin_date) then vx_admin_date_y=put(year(vx_admin_date),4.);

            if not missing(vx_record_date) then vx_record_date_y=put(year(vx_record_date),4.); 
        %end;          
	run;        

    %elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress LDS_ADDRESS_HISTORY
*******************************************************************************;
%if &_yldsadrs=1 %then %do;

	%let qname=lds_address_history;
    %elapsed(begin);

    %if %upcase(&_part2)=YES %then %do;

        * Create format from ADDRESS_RANK reference dataset to be used in the creation of ADDRESS_RANK;
        data address_rank;
            set address_rank (keep=address_use address_type address_rank);

            fmtname='$ADD_RANK';
        
            length label start $10;
        
            label=address_rank;
            start=strip(address_use) || '_' || strip(address_type);
        run;

        * Create format;
        proc format library=work cntlin=address_rank fmtlib;
        run;

    %end;

    data
        lds_address_history (compress=yes)
        %if %upcase(&_part2)=YES %then %do;
            lds_address_history_f2f (compress=yes keep=patid address_zip5 address_rank)
            lds_address_history_f2f_current (compress=yes keep=patid address_zip5 county_fips state_fips ruca_zip valid_address_zip5 valid_county_fips valid_state_fips valid_ruca_zip)
        %end;
        ;
		set pcordata.lds_address_history (keep=patid address_zip5 address_zip9 address_use address_type address_preferred address_period_end state_fips county_fips ruca_zip current_address_flag
            %if %upcase(&_part1)=YES %then %do;
                address_period_start address_state addressid address_county  
            %end;
            %if %upcase(&_part2)=YES %then %do;
                address_city
            %end;
            ) end=eof;
        
        * Flag valid values;
        if notdigit(strip(address_zip5))=0 and length(address_zip5)=5 then valid_address_zip5='Y';
        if notdigit(strip(county_fips))=0 and length(county_fips)=5 then valid_county_fips='Y';
        if notdigit(strip(state_fips))=0 and length(state_fips)=2 then valid_state_fips='Y';
        if notdigit(strip(ruca_zip))=0 and 1<=length(ruca_zip)<=2 then valid_ruca_zip='Y';

        check=length(ruca_zip);

        %if %upcase(&_part1)=YES %then %do;
            * Flag valid values;
            if notdigit(strip(address_zip9))=0 and length(address_zip9)=9 then valid_address_zip9='Y';
        %end;

        output lds_address_history;
      
        %if %upcase(&_part2)=YES %then %do;
            * Create ADDRESS_RANK;
            address_rank=input(put(strip(address_use) || '_' || strip(address_type),$add_rank.),??best8.);
            if address_rank=. then address_rank=999;
            
            * Output records to be used in zip code queries for patients with F2F visits within a certain timeframe;
            if (valid_address_zip5='Y' or missing(address_zip5)) and address_preferred='Y' and address_use in ('HO' 'OT' 'NI' 'UN' '') and missing(address_period_end) then output lds_address_history_f2f;
            if ((valid_address_zip5='Y' or missing(address_zip5)) or (valid_county_fips='Y' or missing(county_fips)) or (valid_state_fips='Y' or missing(state_fips)) or (valid_ruca_zip='Y' or missing(ruca_zip)))  
                and current_address_flag='Y' then output lds_address_history_f2f_current;
        %end;
    run;

    %if %upcase(&_part2)=YES %then %do;
        
        * Sort by PATID and ADDRESS_RANK, as needed for zip code queries for patients with F2F visits within a certain timeframe;
        proc sort data=lds_address_history_f2f;
            by patid address_rank;
        run;

        * Re-compress LDS_ADDRESS_HISTORY_F2F;
        data lds_address_history_f2f (compress=yes);
            set lds_address_history_f2f;
        run;
        
        * Sort by PATID as needed for zip code queries for patients with F2F visits within a certain timeframe;
        proc sort data=lds_address_history_f2f_current;
            by patid;
        run;

        * Re-compress LDS_ADDRESS_HISTORY_F2F_CURRENT;
        data lds_address_history_f2f_current (compress=yes);
            set lds_address_history_f2f_current;
        run;

    %end;
    
    * Delete intermediary datasets;
    proc delete data=address_rank;
    quit;
    
	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress OBS_GEN
*******************************************************************************;
%if &_yobs_gen=1 %then %do;

	%let qname=obs_gen;
	%elapsed(begin);

	data obs_gen (compress=yes);
        set pcordata.obs_gen (keep=patid obsgen_start_date obsgen_stop_date obsgen_type obsgen_result_unit obsgen_result_modifier obsgen_result_qual
            %if %upcase(&_part1)=YES %then %do;
                encounterid obsgen_start_time obsgen_stop_time obsgen_abn_ind obsgenid obsgen_providerid obsgen_source obsgen_table_modified 
            %end;
            %if %upcase(&_part2)=YES %then %do;
                obsgen_code obsgen_result_num obsgen_result_text
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
        if obsgen_start_date>=&lookback_dt or missing(obsgen_start_date);

        %if %upcase(&_part1)=YES %then %do;
            * For mismatch query;
            providerid=obsgen_providerid;
        %end;

        %if %upcase(&_part2)=YES %then %do;		
            length obsgen_start_date_ym $7;

            if not missing(obsgen_start_date) then obsgen_start_date_ym=put(year(obsgen_start_date),4.) || '_' || put(month(obsgen_start_date),z2.);

            if not missing(obsgen_start_date) then obsgen_start_date_y=put(year(obsgen_start_date),4.);

            if not missing(obsgen_code) then known_test=1;
            if not missing(obsgen_code) and ((not missing(obsgen_result_num) and obsgen_result_modifier not in ('' 'NI' 'UN' 'OT')) or obsgen_result_qual not in ('' 'NI' 'UN' 'OT')
                or not missing(obsgen_result_text)) then known_test_result=1;
            if not missing(obsgen_code) and not missing(obsgen_result_num) and obsgen_result_modifier not in ('' 'NI' 'UN' 'OT') then known_test_result_num=1;
            if not missing(obsgen_code) and not missing(obsgen_result_num) and obsgen_result_modifier not in ('' 'NI' 'UN' 'OT') and obsgen_result_unit not in ('' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
            if not missing(obsgen_code) and .<obsgen_start_date<=&mxrefreshn and ((not missing(obsgen_result_num) and obsgen_result_modifier not in ('' 'NI' 'UN' 'OT')) or obsgen_result_qual not in ('' 'NI' 'UN' 'OT'))
                then known_test_result_plausible=1;

            drop obsgen_result_num obsgen_result_text;
        %end;                 
    run;

    * Include creation of OBSGEN_TYPE value value set in dataset processing time, since it is used for multiple queries, both in Part 1 and Part 2;        
    * Get list of all user-defined/PCORnet-reserved OBSGEN_TYPE values that occur in CDM data;
    proc sort data=obs_gen (keep=obsgen_type where=(substr(obsgen_type,1,3) in ('UD_' 'PC_'))) out=obsgen_type nodupkey;
        by obsgen_type;
    run;

    data obsgen_type_valuesets;
        set
            valuesets (where=(field_name='OBSGEN_TYPE' and substr(valueset_item,1,3) not in ('UD_' 'PC_')))
            obsgen_type (in=ino rename=(obsgen_type=valueset_item))
            ;

        table_name='OBS_GEN';
        field_name='OBSGEN_TYPE';
        if ino then valueset_item_order=1;
    run;              
                
    * Delete intermediary datasets;
	proc delete data=obsgen_type;
	quit;
            
    %elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress PRO_CM
*******************************************************************************;
%if &_ypro_cm=1 %then %do;

	%let qname=pro_cm;
	%elapsed(begin);

	data pro_cm (compress=yes);
        set pcordata.pro_cm (keep=patid pro_date 
            %if %upcase(&_part1)=YES %then %do;
                encounterid pro_time pro_cat pro_method pro_mode pro_cm_id pro_source pro_type pro_code
            %end;
            %if %upcase(&_part2)=YES %then %do;
                pro_fullname pro_name pro_code
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if pro_date>=&lookback_dt or missing(pro_date);

        %if %upcase(&_part2)=YES %then %do;
    		length pro_date_ym $7;

        	if not missing(pro_date) then pro_date_ym=put(year(pro_date),4.) || '_' || put(month(pro_date),z2.);

            if not missing(pro_date) then pro_date_y=put(year(pro_date),4.);
        %end;
	run;        

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress VITAL
*******************************************************************************;
%if &_yvital=1 %then %do;

	%let qname=vital;
	%elapsed(begin);

	data vital (compress=yes);
        set pcordata.vital (keep=patid measure_date
            %if %upcase(&_part1)=YES %then %do;
                encounterid measure_time bp_position vitalid smoking tobacco tobacco_type vital_source
            %end;
            %if %upcase(&_part2)=YES %then %do;
                original_bmi diastolic ht systolic wt
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if measure_date>=&lookback_dt or missing(measure_date);

        %if %upcase(&_part2)=YES %then %do;
            length measure_date_ym $7;

            if not missing(measure_date) then measure_date_ym=put(year(measure_date),4.) || '_' || put(month(measure_date),z2.);

            if not missing(measure_date) then measure_date_y=put(year(measure_date),4.);
        %end;
	run;        

    * VITAL_PERIOD created here as it is needed for both Part 1 and Part 2 _DASH1 queries;
    data vital_period (compress=yes);
        set vital (keep=patid measure_date);

        length period $5;

        * Identify periods for VIT_L#_DASH1 and XTBL_L3_DASH#;
        %dash(datevar=measure_date);

        drop measure_date;
    run;

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress ENCOUNTER
*******************************************************************************;
%if &_yencounter=1 %then %do;

	%let qname=encounter;
	%elapsed(begin);

	data
        encounter (compress=yes)
        %if %upcase(&_part2)=YES %then %do;                
            encounterid (compress=yes keep=encounterid enc_type) /*To be sorted and used for queries combining other CDM dataset to ENCOUNTER by ENCOUNTERID*/
        %end;
        %if %upcase(&_part1)=YES %then %do;    
            enc2012 (compress=yes keep=patid)
        %end;
		;
		set pcordata.encounter (keep=patid admit_date discharge_date encounterid enc_type admitting_source discharge_disposition discharge_status facility_location facility_type payer_type_primary
            %if %upcase(&_part1)=YES %then %do;
                admit_time discharge_time drg_type providerid facilityid payer_type_secondary
            %end;
            %if %upcase(&_part2)=YES %then %do;
                drg
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
        if admit_date>=&lookback_dt or missing(admit_date);
        
        %if %upcase(&_part1)=YES %then %do;
            * Flag valid facility locations;
            if notdigit(facility_location)=0 and length(facility_location)=5 then valid_facility_location='Y';
        %end;
        
        %if %upcase(&_part2)=YES %then %do;
            length admit_date_ym discharge_date_ym $7;

            if not missing(admit_date) then admit_date_ym=put(year(admit_date),4.) || '_' || put(month(admit_date),z2.);
		
            if not missing(discharge_date) then discharge_date_ym=put(year(discharge_date),4.) || '_' || put(month(discharge_date),z2.);

            if not missing(admit_date) then admit_date_y=put(year(admit_date),4.);

            if not missing(discharge_date) then discharge_date_y=put(year(discharge_date),4.);

        %end;
    
        output encounter;
        %if %upcase(&_part2)=YES %then %do; 
            output encounterid;
        %end;
        %if %upcase(&_part1)=YES %then %do;
            if not missing(admit_date) then do;
                if year(admit_date)>=2012 then output enc2012;
            end;
        %end;
	run;

    %if %upcase(&_part2)=YES %then %do;
        
        * Sort in order to merge ENCOUNTER onto other CDM datasets by ENCOUNTERID for other queries below;
        proc sort data=encounterid;
            by encounterid;
        run;

        * Re-compress ENCOUNTERID after sorting;
        data encounterid (compress=yes);
            set encounterid;
        run; 

    %end;
    
    * Create ENCOUNTER_PERIOD here, as it is used in both Part 1 and Part 2 queries;
    data encounter_period (compress=yes);
        set encounter (keep=patid admit_date enc_type where=(not missing(admit_date) and enc_type in ('ED' 'EI' 'IP' 'OS' 'AV')));

        length period $5;
                    
        * Identify periods for ENC_L3_DASH2;
        %dash(datevar=admit_date);

        keep patid period;
    run;

    * Only need one record per period per PATID per PERIOD from ENCOUNTER;
    proc sort data=encounter_period (keep=patid period where=(not missing(period))) nodupkey;
        by patid period;
    run;
        
	%elapsed(end);

%end;
            
*******************************************************************************;
* Bring in and compress DEMOGRAPHIC
*******************************************************************************;
%if &_ydemographic=1 and %upcase(&_part1)=YES %then %do;

	%let qname=demographic;
	%elapsed(begin);

	data demographic (compress=yes);
        set pcordata.demographic (keep=patid birth_date birth_time gender_identity hispanic sexual_orientation pat_pref_language_spoken race sex biobank_flag
			race_eth_missing race_eth_ai_an race_eth_asian race_eth_black race_eth_hispanic race_eth_me_na race_eth_nh_pi race_eth_white) end=eof;
	run;

	* Sort for use in DEM_L3_AGEYRSDIST2 (age derivation);
	proc sort data=demographic;
		by patid birth_date;
    run;

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress EXTERNAL_MEDS
*******************************************************************************;
%if &_yexternal_meds=1 %then %do;

	%let qname=external_meds;
	%elapsed(begin);

	data external_meds (compress=yes);
        set pcordata.external_meds (keep=patid ext_record_date ext_dose
            %if %upcase(&_part1)=YES %then %do;
                extmedid ext_dose_form ext_dose_ordered_unit ext_route ext_basis extmed_source ext_pat_start_date ext_pat_end_date ext_end_date
            %end;
            %if %upcase(&_part2)=YES %then %do;
                rxnorm_cui
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if ext_record_date>=&lookback_dt or missing(ext_record_date);
		
        rxnorm_cui=strip(upcase(rxnorm_cui));

        %if %upcase(&_part2)=YES %then %do;		
            length ext_record_date_ym $7;

            if not missing(ext_record_date) then ext_record_date_ym=put(year(ext_record_date),4.) || '_' || put(month(ext_record_date),z2.);

            if not missing(ext_record_date) then ext_record_date_y=put(year(ext_record_date),4.);
        %end;            
    run;

	%elapsed(end);    

%end;

*******************************************************************************;
* Bring in and compress PRESCRIBING
*******************************************************************************;
%if &_yprescribing=1 %then %do;

	%let qname=prescribing;
	%elapsed(begin);

	data prescribing (compress=yes);
        set pcordata.prescribing (keep=patid rx_order_date rxnorm_cui encounterid
            %if %upcase(&_part1)=YES %then %do;
                rx_start_date rx_end_date rx_order_time rx_basis rx_dispense_as_written rx_frequency prescribingid rx_providerid rx_prn_flag rx_route rx_dose_form rx_dose_ordered_unit rx_source 
            %end;
            %if %upcase(&_part2)=YES %then %do;
                rx_days_supply rx_dose_ordered rx_quantity rx_refills
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if rx_order_date>=&lookback_dt or missing(rx_order_date);
		
        rxnorm_cui=strip(upcase(rxnorm_cui));

        %if %upcase(&_part1)=YES %then %do;
            * For mismatch query;
            providerid=rx_providerid;

            * _PLUS variable, only created for subset of records;
            if (.<rx_start_date<=&mxrefreshn or .<rx_order_date<=&mxrefreshn) and not missing(rxnorm_cui) then prescribingid_plus=prescribingid;
        %end;

        %if %upcase(&_part2)=YES %then %do;		
            length rx_order_date_ym $7;

            if not missing(rx_order_date) then rx_order_date_ym=put(year(rx_order_date),4.) || '_' || put(month(rx_order_date),z2.);

            if not missing(rx_order_date) then rx_order_date_y=put(year(rx_order_date),4.);
        %end;            
    run;

	%elapsed(end);    

%end;

*******************************************************************************;
* Bring in and compress CONDITION
*******************************************************************************;
%if &_ycondition=1 %then %do;

	%let qname=condition;
	%elapsed(begin);

	data condition (compress=yes);
        set pcordata.condition (keep=patid report_date condition
            %if %upcase(&_part1)=YES %then %do;
                encounterid onset_date resolve_date conditionid condition_source condition_status condition_type
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if report_date>=&lookback_dt or missing(report_date);

        %if %upcase(&_part2)=YES %then %do;		
            length report_date_ym $7;

            if not missing(report_date) then report_date_ym=put(year(report_date),4.) || '_' || put(month(report_date),z2.);

            if not missing(report_date) then report_date_y=put(year(report_date),4.);
        %end;
	run;        

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress DISPENSING
*******************************************************************************;
%if &_ydispensing=1 %then %do;

	%let qname=dispensing;
	%elapsed(begin);

	data dispensing (compress=yes);
        set pcordata.dispensing (keep=patid dispense_date ndc
            %if %upcase(&_part1)=YES %then %do;
                dispense_dose_disp_unit dispensingid prescribingid dispense_route dispense_source
            %end;
            %if %upcase(&_part2)=YES %then %do;
                dispense_amt dispense_dose_disp dispense_sup
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if dispense_date>=&lookback_dt or missing(dispense_date);

        %if %upcase(&_part1)=YES %then %do;
        	* Flag valid NDC values;
            if notdigit(ndc)=0 and length(ndc)=11 then valid_ndc='Y';
        %end;

        %if %upcase(&_part2)=YES %then %do;
            length dispense_date_ym $7;

            if not missing(dispense_date) then dispense_date_ym=put(year(dispense_date),4.) || '_' || put(month(dispense_date),z2.);

            if not missing(dispense_date) then dispense_date_y=put(year(dispense_date),4.);
        %end;
	run;        

	%elapsed(end);

%end;
    
*******************************************************************************;
* Bring in and compress PROCEDURES
*******************************************************************************;
%if &_yprocedures=1 %then %do;

	%let qname=procedures;
	%elapsed(begin);

	data procedures (compress=yes);
        set pcordata.procedures (keep=patid admit_date px_date encounterid enc_type px px_type
            %if %upcase(&_part1)=YES %then %do;
                proceduresid providerid ppx px_source
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if admit_date>=&lookback_dt or missing(admit_date);

        %if %upcase(&_part1)=YES %then %do;
            * _PLUS variable, only created for subset of records;
            if (.<admit_date<=&mxrefreshn or .<px_date<=&mxrefreshn) and not missing(encounterid) and px_type not in ('NI' 'UN' 'OT' '') then proceduresid_plus=proceduresid;
        %end;
            
        %if %upcase(&_part2)=YES %then %do;
            length admit_date_ym $7;

            if not missing(admit_date) then admit_date_ym=put(year(admit_date),4.) || '_' || put(month(admit_date),z2.);

            if not missing(admit_date) then admit_date_y=put(year(admit_date),4.);
		
            if not missing (px_date) then px_date_y=put(year(px_date),4.);
        %end;        		
	run;        

	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress DIAGNOSIS
*******************************************************************************;
%if &_ydiagnosis=1 %then %do;

	%let qname=diagnosis;
	%elapsed(begin);

	data diagnosis (compress=yes);
        set pcordata.diagnosis (keep=patid admit_date dx_date encounterid dx dx_type enc_type pdx
            %if %upcase(&_part1)=YES %then %do;
                dx_poa dx_source diagnosisid providerid
            %end;
            %if %upcase(&_part2)=YES %then %do;
                dx_origin
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if admit_date>=&lookback_dt or missing(admit_date);

        %if %upcase(&_part1)=YES %then %do;
            * _PLUS variable, only created for subset of records;
            if (.<admit_date<=&mxrefreshn or .<dx_date<=&mxrefreshn) and not missing(encounterid) and dx_type not in ('NI' 'UN' 'OT' '') then diagnosisid_plus=diagnosisid;	
        %end;
        
        %if %upcase(&_part2)=YES %then %do;
            length admit_date_ym dx_date_ym $7;

            if not missing(admit_date) then admit_date_ym=put(year(admit_date),4.) || '_' || put(month(admit_date),z2.);
		
            if not missing(dx_date) then dx_date_ym=put(year(dx_date),4.) || '_' || put(month(dx_date),z2.);

            if not missing(admit_date) then admit_date_y=put(year(admit_date),4.);
		
            if not missing(dx_date) then dx_date_y=put(year(dx_date),4.);
        %end;        
    run; 

    * Sort by ENCOUNTERID for below query;
    proc sort data=diagnosis;
        by encounterid;
    run;           
    
	%elapsed(end);

%end;

*******************************************************************************;
* Bring in and compress MED_ADMIN
*******************************************************************************;
%if &_ymed_admin=1 %then %do;

	%let qname=med_admin;
	%elapsed(begin);

	data med_admin (compress=yes);
        set pcordata.med_admin (keep=patid medadmin_start_date encounterid medadmin_code medadmin_type
            %if %upcase(&_part1)=YES %then %do;
                medadmin_stop_date medadmin_start_time medadmin_stop_time medadmin_dose_admin_unit medadminid medadmin_providerid prescribingid medadmin_route medadmin_source
            %end;
            %if %upcase(&_part2)=YES %then %do;
                medadmin_dose_admin
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if medadmin_start_date>=&lookback_dt or missing(medadmin_start_date);

        %if %upcase(&_part1)=YES %then %do;
            * For mismatch query;
            providerid=medadmin_providerid;
            
            * _PLUS variable, only created for subset of records;
            if .<medadmin_start_date<=&mxrefreshn and not missing(medadmin_code) and medadmin_type not in ('NI' 'UN' 'OT' '') then medadminid_plus=medadminid;
        %end;
        
        %if %upcase(&_part2)=YES %then %do;
    		length medadmin_start_date_ym $7;

        	if not missing(medadmin_start_date) then medadmin_start_date_ym=put(year(medadmin_start_date),4.) || '_' || put(month(medadmin_start_date),z2.);

            if not missing(medadmin_start_date) then medadmin_start_date_y=put(year(medadmin_start_date),4.);        
        %end;		        
	run;        

	%elapsed(end);

%end;
    
*******************************************************************************;
* Bring in and compress LAB_RESULT_CM   
*******************************************************************************;
%if &_ylab_result_cm=1 %then %do;

	%let qname=lab_result_cm;
	%elapsed(begin);

	data 
        lab_result_cm (compress=yes)
        lab_result_cm_titer (compress=yes keep=result_qual)
        ;
        set pcordata.lab_result_cm (keep=patid result_date encounterid lab_loinc result_unit lab_px lab_px_type result_modifier result_qual specimen_source norm_modifier_low norm_modifier_high
            %if %upcase(&_part1)=YES %then %do;
                lab_order_date specimen_date result_time specimen_time abn_ind result_loc lab_loinc_source lab_result_cm_id
                priority lab_result_source  
            %end;
            %if %upcase(&_part2)=YES %then %do;
                result_num result_snomed norm_range_low norm_range_high
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
		if result_date>=&lookback_dt or missing(result_date);

        %if %upcase(&_part2)=YES %then %do;
            length result_date_ym $7;

            if not missing(result_date) then result_date_ym=put(year(result_date),4.) || '_' || put(month(result_date),z2.);

            if not missing(result_date) then result_date_y=put(year(result_date),4.);  

            if not missing(lab_loinc) then known_test=1;
            if not missing(lab_loinc) and ((not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT')) or result_qual not in ('' 'NI' 'UN' 'OT')) then known_test_result=1;
            if not missing(lab_loinc) and not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT') then known_test_result_num=1;
            if not missing(lab_loinc) and not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT') and specimen_source not in ('' 'NI' 'UN' 'OT' 'UNK_SUB' 'SMPLS' 'SPECIMEN') then known_test_result_num_source=1;        
            if not missing(lab_loinc) and not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT') and result_unit not in ('' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
            if not missing(lab_loinc) and not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT') and specimen_source not in ('' 'NI' 'UN' 'OT' 'UNK_SUB' 'SMPLS' 'SPECIMEN') and result_unit not in ('' 'NI' 'UN' 'OT')
                then known_test_result_num_srce_unit=1;        
            if not missing(lab_loinc) and not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT') and
                (
                    (norm_modifier_low='EQ' and norm_modifier_high='EQ' and not missing(norm_range_low) and not missing(norm_range_high)) or
                    (norm_modifier_low in ('GT' 'GE') and norm_modifier_high='NO' and not missing(norm_range_low) and missing(norm_range_high)) or
                    (norm_modifier_high in ('LE' 'LT') and norm_modifier_low='NO' and not missing(norm_range_high) and missing(norm_range_low))                
                ) then known_test_result_num_range=1;
            if not missing(lab_loinc) and .<result_date<=&mxrefreshn and ((not missing(result_num) and result_modifier not in ('' 'NI' 'UN' 'OT')) or result_qual not in ('' 'NI' 'UN' 'OT')) then known_test_result_plausible=1;

            drop norm_range_low norm_range_high;
        %end; 

        output lab_result_cm;

        %if %upcase(&_part1)=YES %then %do;
            if index(result_qual,":")>0 and index(result_qual,":0")=0 and substr(strip(result_qual),1,1) ne '0' and substr(result_qual,1,1) ne ' ' and
                compress(tranwrd(strip(result_qual),' ',':'), '0123456789')=':' then output lab_result_cm_titer;
        %end;              
    run;

	%elapsed(end);
    
%end;

*******************************************************************************;
* Bring in and compress OBS_CLIN
*******************************************************************************;
%if &_yobs_clin=1 %then %do;

	%let qname=obs_clin;
	%elapsed(begin);

	data obs_clin (compress=yes);
        set pcordata.obs_clin (keep=patid obsclin_start_date obsclin_result_unit obsclin_type obsclin_result_modifier obsclin_result_qual
            %if %upcase(&_part1)=YES %then %do;
                obsclin_stop_date encounterid obsclin_start_time obsclin_stop_time obsclin_abn_ind obsclinid obsclin_providerid obsclin_source 
            %end;
            %if %upcase(&_part2)=YES %then %do;
                obsclin_code obsclin_result_num obsclin_result_text
            %end;
            ) end=eof;

		* Restrict data to within lookback period;
        if obsclin_start_date>=&lookback_dt or missing(obsclin_start_date);

        %if %upcase(&_part1)=YES %then %do;
            * For mismatch query;
            providerid=obsclin_providerid;
        %end;                

        %if %upcase(&_part2)=YES %then %do;
            length obsclin_start_date_ym $7;

            if not missing(obsclin_start_date) then obsclin_start_date_ym=put(year(obsclin_start_date),4.) || '_' || put(month(obsclin_start_date),z2.); 

            if not missing(obsclin_start_date) then obsclin_start_date_y=put(year(obsclin_start_date),4.); 

            if not missing(obsclin_code) then known_test=1;
            if not missing(obsclin_code) and ((not missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT')) or obsclin_result_qual not in ('' 'NI' 'UN' 'OT')
                or not missing(obsclin_result_text)) then known_test_result=1;
            if not missing(obsclin_code) and not missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT') then known_test_result_num=1;
            if not missing(obsclin_code) and not missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT') and obsclin_result_unit not in ('' 'NI' 'UN' 'OT') then known_test_result_num_unit=1;
            if not missing(obsclin_code) and .<obsclin_start_date<=&mxrefreshn and ((not missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT')) or obsclin_result_qual not in ('' 'NI' 'UN' 'OT'))
                then known_test_result_plausible=1;

            drop obsclin_result_num obsclin_result_text;
        %end;
	run;        

	%elapsed(end);

%end;


***********************************************************************************;
***********************************************************************************;
********************* 3. Process large combined queries ***************************;
***********************************************************************************;
***********************************************************************************;


******************************************************************************;
* XTBL_L3_DATE_LOGIC
******************************************************************************;
%if %upcase(&_part1)=YES %then %do;

	%let qname=xtbl_l3_date_logic;
    %elapsed(begin);

    *- Macro to compare dates against birth and death -*;
    %macro xmin(idsn,var,ord);

        proc means data=&idsn nway noprint;
            class patid;
            var &var;
            output out=min&idsn min=m&var;
            where not missing(patid);
        run;

        data min&var;
            length compvar $20;
            merge
                min&idsn (in=m)
                demographic (keep=patid birth_date where=(not missing(patid))) 
                death (keep=patid death_date where=(not missing(patid))) end=eof
                ;
            by patid;
     
            ord=&ord;
            compvar=upcase("&var");
            
            retain b_cnt d_cnt 0;
            
            if m and not missing(birth_date) and not missing(m&var) and m&var<birth_date then b_cnt=b_cnt+1;
            if m and not missing(death_date) and not missing(m&var) and m&var>death_date then d_cnt=d_cnt+1;

            keep compvar b_cnt d_cnt ord;
            
            if eof then output;
        run;

        proc append base=data data=min&var;               
        run;

        * Delete intermediary datasets;
        proc delete data=min&idsn min&var;
        quit;

    %mend xmin;
        
    %if &_yencounter=1 %then %do;
        proc sort data=encounter (keep=patid discharge_date admit_date) out=encounter_dl;
             by patid; 
        run;
            
        %xmin(idsn=encounter_dl,var=admit_date,ord=1)
        %xmin(idsn=encounter_dl,var=discharge_date,ord=2)
    %end;
    %if &_yprocedures=1 %then %do;
        proc sort data=procedures (keep=patid px_date admit_date) out=procedures_dl;
             by patid; 
        run;
            
        %xmin(idsn=procedures_dl,var=px_date,ord=3)
    %end;
    %if &_ydiagnosis=1 %then %do;
        proc sort data=diagnosis (keep=patid dx_date admit_date) out=diagnosis_dl;
             by patid; 
        run;
            
        %xmin(idsn=diagnosis_dl,var=dx_date,ord=13)
    %end;
    %if &_yvital=1 %then %do;
        %xmin(idsn=vital,var=measure_date,ord=4)
    %end;
    %if &_ydispensing=1 %then %do;
        %xmin(idsn=dispensing,var=dispense_date,ord=5)
    %end;
    %if &_yprescribing=1 %then %do;
        %xmin(idsn=prescribing,var=rx_start_date,ord=6)
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %xmin(idsn=lab_result_cm,var=result_date,ord=7)
    %end;
    %if &_ydeath=1 %then %do;
        %xmin(idsn=death,var=death_date,ord=8)
    %end;
    %if &_ymed_admin=1 %then %do;
        %xmin(idsn=med_admin,var=medadmin_start_date,ord=9)
    %end;
    %if &_yobs_clin=1 %then %do; 
        proc sort data=obs_clin (keep=patid obsclin_start_date obsclin_stop_date) out=obs_clin_dl;
             by patid; 
        run;
                      
        %xmin(idsn=obs_clin_dl,var=obsclin_start_date,ord=10)
    %end;
    %if &_yobs_gen=1 %then %do;
        proc sort data=obs_gen (keep=patid obsgen_start_date obsgen_stop_date) out=obs_gen_dl;
             by patid; 
        run;
           
        %xmin(idsn=obs_gen_dl,var=obsgen_start_date,ord=11)
    %end;
    %if &_yimmunization=1 %then %do;
        %xmin(idsn=immunization,var=vx_record_date,ord=12)
    %end;
    %if &_ypat_relationship=1 %then %do; 
        proc sort data=pat_relationship (keep=patrelid relationship_start relationship_end rename=(patrelid=patid)) out=pat_relationship_dl;
             by patid; 
        run;
    %end;
    %if &_yexternal_meds=1 %then %do; 
        proc sort data=external_meds (keep=patid ext_pat_start_date ext_pat_end_date) out=external_meds_dl;
             by patid; 
        run;
    %end;

    *- Macro to compare dates within a data table -*;
    %macro within(idsn,var,var2,adjp,adjm,ord);

        %if &idsn=pat_relationship_dl %then %do;
            data patrel&var;
        %end;
        %else %if &idsn=external_meds_dl %then %do;
            data extmeds&var;
        %end;
        %else %do;
            data &idsn&var;
        %end;
             set &idsn (keep=patid &var &var2);
             by patid;
     
             retain cnt_within;
             
             if first.patid then cnt_within=0;
             
             if not missing(&var) and not missing(&var2) then do;
                if &var &adjp<&var2 &adjm then cnt_within=1;
             end;
				
             if last.patid;

             keep patid cnt_within;
        run;

        proc means 
            %if &idsn=pat_relationship_dl %then %do;
                data=patrel&var
            %end;
            %else %if &idsn=external_meds_dl %then %do;
                data=extmeds&var
            %end;
            %else %do;
                data=&idsn&var 
            %end;
            nway noprint;
            var cnt_within;
            output  
                %if &idsn=pat_relationship_dl %then %do;
                    out=sumpatrel&var
                %end;
                %else %if &idsn=external_meds_dl %then %do;
                    out=sumextmeds&var
                %end;
                %else %do;
                    out=sum&idsn&var 
                %end;
                sum=sum_within;
        run;
        
        data 
            %if &idsn=pat_relationship_dl %then %do;
                sumpatrel&var
            %end;
            %else %if &idsn=external_meds_dl %then %do;
                sumextmeds&var
            %end;
            %else %do;
                sum&idsn&var
            %end;
            ; 
            %if &idsn=pat_relationship_dl %then %do;
                set sumpatrel&var;
            %end;
            %else %if &idsn=external_meds_dl %then %do;
                set sumextmeds&var;
            %end;
            %else %do;
                set sum&idsn&var ;
            %end;
            ord=&ord;
        run;

        * Delete intermediary datasets;
        proc delete data=&idsn 
            %if &idsn=pat_relationship_dl %then %do;
                patrel&var 
            %end;
            %else %if &idsn=external_meds_dl %then %do;
                extmeds&var 
            %end;
            %else %do;
                &idsn&var 
            %end;
            ;
        quit;

    %mend within;
        
    %if &_yencounter=1 %then %do;
        %within(idsn=encounter_dl,var=discharge_date,var2=admit_date,ord=28)
    %end;
    %if &_yprocedures=1 %then %do;
        %within(idsn=procedures_dl,var=px_date,var2=admit_date,adjm=-5,ord=27)
    %end;
    %if &_ydiagnosis=1 %then %do;
        %within(idsn=diagnosis_dl,var=dx_date,var2=admit_date,adjm=-5,ord=29)
    %end;
    %if &_yobs_clin=1 %then %do;
        %within(idsn=obs_clin_dl,var=obsclin_stop_date,var2=obsclin_start_date,ord=31)
    %end;
    %if &_yobs_gen=1 %then %do;
        %within(idsn=obs_gen_dl,var=obsgen_stop_date,var2=obsgen_start_date,ord=32)
    %end;
    %if &_ypat_relationship=1 %then %do;
        %within(idsn=pat_relationship_dl,var=relationship_end,var2=relationship_start,ord=33)
    %end;
    %if &_yexternal_meds=1 %then %do;
        %within(idsn=external_meds_dl,var=ext_pat_end_date,var2=ext_pat_start_date,ord=34)
    %end;

    *- Macro to compare dates across data tables -*;
    %macro across(idsn,mdsn,mvar,var,var2,adjp,adjm,ord);

        proc sort data=&idsn (keep=patid &mvar &var2) out=&idsn&var nodupkey;
             by patid &mvar;
        run;

        proc sort data=&mdsn (keep=patid &mvar &var) out=&mdsn&var nodupkey;
             by patid &mvar;
        run;

        data &idsn&var;
             merge &idsn&var &mdsn&var;
             by patid &mvar;
     
             retain cnt_across;

             if first.patid then cnt_across=0;
             
             if not missing(&var) and not missing(&var2) then do;
				if &var &adjp<&var2 &adjm then cnt_across=1;
			 end;
			 
             if last.patid;

             keep patid cnt_across;
        run;

        proc means data=&idsn&var nway noprint;
             var cnt_across;
             output out=sum&idsn&var sum=sum_across;
        run;

        data sum&idsn&var;
             set sum&idsn&var;
             ord=&ord;
        run;

        * Delete intermediary datasets;
        proc delete data=&idsn&var &mdsn&var;
        quit;

    %mend across;
        
    %if &_yencounter=1 and &_yprocedures=1 %then %do;
        %across(idsn=procedures,mdsn=encounter,mvar=encounterid,var=discharge_date,var2=px_date,adjp=+5,ord=26)
    %end;
    %if &_yencounter=1 and &_ydiagnosis=1 %then %do;
        %across(idsn=diagnosis,mdsn=encounter,mvar=encounterid,var=discharge_date,var2=dx_date,adjp=+5,ord=30)
    %end;

    data query;
        length date_comparison $50 distinct_patid_n $20;
        set data (in=b rename=(b_cnt=distinct_patid))
            data (in=d rename=(d_cnt=distinct_patid))
            sumproceduresdischarge_date (in=pd rename=(sum_across=distinct_patid))
            sumprocedures_dlpx_date (in=pp rename=(sum_within=distinct_patid))
            sumencounter_dldischarge_date (in=ed rename=(sum_within=distinct_patid))
            sumdiagnosis_dldx_date (in=dd rename=(sum_within=distinct_patid))
            sumdiagnosisdischarge_date (in=ddi rename=(sum_across=distinct_patid))
            sumobs_clin_dlobsclin_stop_date (in=oc rename=(sum_within=distinct_patid))
            sumobs_gen_dlobsgen_stop_date (in=gen rename=(sum_within=distinct_patid))
            sumpatrelrelationship_end (in=pr rename=(sum_within=distinct_patid))
            sumextmedsext_pat_end_date (in=em rename=(sum_within=distinct_patid))
            ;

         * Drop death-death check;
         if d and compvar='DEATH_DATE' then delete;

         if d and ord<=7 then ord=ord+13;
         else if d and ord>=7 then ord=ord+12;

         * Call standard variables;
         %stdvar;

         * Create comparision variable;
         if b then date_comparison=strip(compvar) || " < BIRTH_DATE";
         else if d then date_comparison=strip(compvar) || " > DEATH_DATE";
         else if pd then date_comparison="PX_DATE > DISCHARGE_DATE";
         else if pp then date_comparison="PX_DATE < ADMIT_DATE";
         else if ed then date_comparison="ADMIT_DATE > DISCHARGE_DATE";
         else if dd then date_comparison="DX_DATE < ADMIT_DATE";
         else if ddi then date_comparison="DX_DATE > DISCHARGE_DATE";
         else if oc then date_comparison="OBSCLIN_START_DATE > OBSCLIN_STOP_DATE";
         else if gen then date_comparison="OBSGEN_START_DATE > OBSGEN_STOP_DATE";
         else if pr then date_comparison="RELATIONSHIP_START > RELATIONSHIP_END";
         else if em then date_comparison="EXT_PAT_START_DATE > EXT_PAT_END_DATE";

         distinct_patid_n=strip(put(distinct_patid,16.));
    run;

    proc sort data=query;
         by ord;
    run;

    * Order variables;
    proc sql;
        create table dmlocal.xtbl_l3_date_logic as
        select datamartid, response_date, query_package, date_comparison, distinct_patid_n
        from query;
    quit;

	* Delete intermediary datasets;
	proc delete data=data sumproceduresdischarge_date sumprocedures_dlpx_date sumencounter_dldischarge_date sumdiagnosis_dldx_date sumdiagnosisdischarge_date
            sumobs_clin_dlobsclin_stop_date sumobs_gen_dlobsgen_stop_date sumpatrelrelationship_end sumextmedsext_pat_end_date query;
	quit;

    %elapsed(end);
    
    %end;

******************************************************************************;
* XTBL_L3_MISMATCH
******************************************************************************;
%if %upcase(&_part1)=YES %then %do;

    %let qname=xtbl_l3_mismatch;
    %elapsed(begin);

    * Orphan ENCOUNTERID records from each appropriate table ;
    proc sql noprint;
         create table orphan_encid as select unique encounterid, enc_type, admit_date
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
                &_orph=OBS_CLIN or &_orph=OBS_GEN or &_orph=IMMUNIZATION %then %do;

                * Orphan rows;
                proc sql noprint;
                     create table orph_&_orph as select unique encounterid
                        from pcordata.&_orph
                        where encounterid^=" "
                        order by encounterid;
                quit;

                data orph_&_orph (keep=memname ord recordn);
                     length encounterid $ &maxlength_enc memname $32;
                     merge
                         orph_&_orph (in=o) 
                         orphan_encid (in=oe rename=(enc_type=etype admit_date=adate)) end=eof
                         ;
                     by encounterid;
                     
                     retain recordn 0;
                     if o and not oe then recordn=recordn+1;
                     
                     if eof then do;
                         memname="&_orph";
                         ord=put(upcase(memname),$ord.);
                         output;
                    end;
                run;

                * Compile into one dataset;
                proc append base=orph_encounterid data=orph_&_orph;
                run;

                * Delete intermediary datasets;
                proc delete data=orph_&_orph;
                quit;

                * Mismatch rows - ENCOUNTER;
                %if &_orph=DIAGNOSIS or &_orph=PROCEDURES %then %do;
                    
                    proc sort data=&_orph (keep=encounterid enc_type admit_date) out=mismatch_&_orph;
                         by encounterid;
                         where encounterid^=" ";
                    run;

                    data mismatch_&_orph(keep=memname ord mis:);
                         length encounterid $ &maxlength_enc memname $32;
                         merge
                             mismatch_&_orph (in=o) 
                             orphan_encid (in=oe rename=(enc_type=etype admit_date=adate)) end=eof
                             ;
                         by encounterid;

                         retain mis_etype_n mis_adate_n tap 0;
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

                    * Compile into one dataset;
                    proc append base=mismatch_encounterid data=mismatch_&_orph;
                    run;

                    * Delete intermediary datasets;
                    proc delete data=mismatch_&_orph;
                    quit;
                        
                %end;
                
            %end;
        
            * Mismatch rows - PROVIDER;
            %if &_yprovider=1 %then %do;
            
                %if &_orph=DIAGNOSIS or &_orph=ENCOUNTER or &_orph=MED_ADMIN or 
                    &_orph=OBS_CLIN or &_orph=OBS_GEN or &_orph=PRESCRIBING or 
                    &_orph=PROCEDURES or &_orph=IMMUNIZATION %then %do;
                    
                    proc sort data=&_orph (keep=providerid) out=mismatch_providerid_&_orph;
                         by providerid;
                         where providerid^=" ";
                    run;

                    data mismatch_providerid_&_orph(keep=memname ord mis:);
                         length providerid $ &maxlength_prov memname $32;
                         merge
                             mismatch_providerid_&_orph (in=o) 
                             orphan_provid (in=op) end=eof
                             ;
                         by providerid;
                         
                         retain mis_n 0;
                         if o and not op and first.providerid then mis_n=mis_n+1;

                         if eof then do;
                             memname="&_orph";
                             ord=put(upcase(memname),$ord.);
                             output;
                         end;
                    run;

                    * Compile into one dataset;
                    proc append base=mismatch_providerid data=mismatch_providerid_&_orph;
                    run;

                    * Delete intermediary datasets;
                    proc delete data=mismatch_providerid_&_orph;
                    quit;
                       
                %end;
                
            %end;
        
        %end;

        * Delete intermediary datasets;
        proc delete data=orphan_encid orphan_provid;
        quit;
    
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

    * Orphan PATID records from each appropriate table;
    proc sql noprint;
         create table orphan_patid as select unique patid
            from pcordata.demographic
            order by patid;
         create table orphan_hash as select unique patid
            from pcordata.hash_token
            order by patid;
    quit;

    %macro orph_pat;
            
        %do op=1 %to &workdata_count;
            
            %let _orphp=%scan(&workdata,&op,"|");

            %if &_orphp=ENROLLMENT or &_orphp=ENCOUNTER or &_orphp=DIAGNOSIS or 
                &_orphp=PROCEDURES or &_orphp=VITAL or &_orphp=LAB_RESULT_CM or 
                &_orphp=PRESCRIBING or &_orphp=DISPENSING or &_orphp=DEATH or
                &_orphp=CONDITION or &_orphp=DEATH_CAUSE or &_orphp=PRO_CM or
                &_orphp=PCORNET_TRIAL or &_orphp=MED_ADMIN or &_orphp=OBS_CLIN or 
                &_orphp=OBS_GEN or &_orphp=HASH_TOKEN or 
                &_orphp=LDS_ADDRESS_HISTORY or &_orphp=IMMUNIZATION or 
                &_orphp=DEMOGRAPHIC or &_orphp=EXTERNAL_MEDS %then %do;

                proc sql noprint;
                     create table orph_&_orphp as select unique patid
                        from pcordata.&_orphp
                        order by patid;
                quit;

                data orph_&_orphp(keep=memname ord recordn patidnum);
                     length memname $32;
                     merge
                         orph_&_orphp(in=o) 
                         %if &_orphp^=DEMOGRAPHIC %then %do;
                               orphan_patid (in=oe) end=eof;
                         %end;
                         %else %if &_orphp=DEMOGRAPHIC %then %do;
                               orphan_hash (in=oe) end=eof;
                         %end;
                     by patid;
                     
                     retain recordn 0;
                     if o and not oe then recordn=recordn+1;

                     if eof then do;
                         memname="&_orphp";
                         ord=put(upcase(memname),$ord.);
                         patidnum=.;
                         output;
                    end;
                run;

                * Compile into one dataset;
                proc append base=orph_patid data=orph_&_orphp;
                run;

                * Delete intermediary datasets;
                proc delete data=orph_&_orphp;
                quit;
                    
            %end;
            %else %if &_orphp=PAT_RELATIONSHIP %then %do;

                * Loop through both PATIDs;
                %do i=1 %to 2;

                    proc sql noprint;
                        create table orph_&_orphp as select unique patid_&i
                            from pcordata.&_orphp
                            order by patid_&i;
                    quit;

                    data orph_&_orphp(keep=memname ord recordn patidnum);
                        length memname $32;
                        merge
                            orph_&_orphp(in=o rename=(patid_&i=patid)) 
                            orphan_patid (in=oe) end=eof;
                        by patid;
                        
                        retain recordn 0;
                        if o and not oe then recordn=recordn+1;

                        if eof then do;
                            memname="&_orphp";
                            ord=put(upcase(memname),$ord.);
                            patidnum=&i;
                            output;
                        end;
                    run;
    
                    * Compile into one dataset;
                    proc append base=orph_patid data=orph_&_orphp;
                    run;

                    * Delete intermediary datasets;
                    proc delete data=orph_&_orphp;
                    quit;

                %end;

            %end;
            
        %end;

        * Delete intermediary datasets;
        proc delete data=orphan_patid orphan_hash;
        quit;
    
    %mend orph_pat;
        
    %orph_pat;

    proc sort data=orph_patid;
         by ord memname;
    run;

    data query;
        length dataset $100 tag $50;
        set
            orph_encounterid (in=oe)
            orph_patid (in=op)
            mismatch_encounterid (in=mde where=(memname="DIAGNOSIS") rename=(mis_etype_n=recordn))
            mismatch_encounterid (in=mda where=(memname="DIAGNOSIS") rename=(mis_adate_n=recordn))
            mismatch_encounterid (in=mpe where=(memname="PROCEDURES") rename=(mis_etype_n=recordn))
            mismatch_encounterid (in=mpa where=(memname="PROCEDURES") rename=(mis_adate_n=recordn))
            mismatch_providerid (in=mpre where=(memname="ENCOUNTER") rename=(mis_n=recordn))
            mismatch_providerid (in=mprd where=(memname="DIAGNOSIS") rename=(mis_n=recordn))
            mismatch_providerid (in=mprp where=(memname="PROCEDURES") rename=(mis_n=recordn))
            mismatch_providerid (in=mprpr where=(memname="PRESCRIBING") rename=(mis_n=recordn))
            mismatch_providerid (in=mprma where=(memname="MED_ADMIN") rename=(mis_n=recordn))
            mismatch_providerid (in=mproc where=(memname="OBS_CLIN") rename=(mis_n=recordn))
            mismatch_providerid (in=mprog where=(memname="OBS_GEN") rename=(mis_n=recordn))
            mismatch_providerid (in=mimmu where=(memname="IMMUNIZATION") rename=(mis_n=recordn))
            ;

         * Call standard variables;
         %stdvar;

         distinct_n=strip(put(recordn,16.));
         if oe then do;
            dataset="ENCOUNTER and " || strip(upcase(memname));
            tag="ENCOUNTERID Orphan";
         end;
         else if op then do;
            if memname^="DEMOGRAPHIC" 
               then dataset="DEMOGRAPHIC and " || strip(upcase(memname));
            else if memname="DEMOGRAPHIC" 
               then dataset="HASH_TOKEN and " || strip(upcase(memname));
            
            if patidnum=1 then tag="PATID_1 Orphan";
            else if patidnum=2 then tag="PATID_2 Orphan";
            else tag="PATID Orphan";
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
         else if mimmu then do;
            dataset="PROVIDER and IMMUNIZATION";
            tag="PROVIDERID Orphan";
         end;

         keep datamartid response_date query_package dataset tag distinct_n;
    run;

    * Order variables;
    proc sql;
         create table dmlocal.xtbl_l3_mismatch as select
             datamartid, response_date, query_package, dataset, tag, distinct_n
         from query;
    quit; 

    * Delete intermediary datasets;
    proc delete data=orph_encounterid orph_patid mismatch_encounterid mismatch_providerid query;
    quit;   
    
    %elapsed(end);

%end;

******************************************************************************;
* XTBL_L3_NON_UNIQUE
******************************************************************************;
%if %upcase(&_part1)=YES %then %do;

    %let qname=xtbl_l3_non_unique;
    %elapsed(begin);

	%macro nonuni(udsn,uvar);
	
        %if &&&_y&udsn=1 %then %do;
            
            proc sort data=&udsn (keep=patid &uvar) out=_nonu_&udsn nodupkey;
                by &uvar patid;
                where not missing(&uvar);
            run;
	
            * Check to see if any obs after subset;
            proc sql noprint;
                select count(&uvar) into :nobs from _nonu_&udsn;
            quit;
	
            * If no obs, create dummy data set;
            %if &nobs=0 %then %do;

                data _nonu_&udsn (keep=dataset tag cnt);
                    length dataset $15 tag $25;
                    dataset=upcase("&udsn");
                    tag=upcase("&uvar");
                    cnt=0;
                    output;
                run;
                    
            %end;
            %else %do;
            
                data _nonu_&udsn (keep=dataset tag cnt);
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

        * Delete intermediary datasets;
        proc delete data=_nonu_&udsn;
        quit;                    
                
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
	%nonuni(udsn=immunization,uvar=encounterid);
	
	* Derive appropriate counts and variables;
	data query;
		length distinct_n $20;
		set data;
	
		* Call standard variables;
		%stdvar;
	
		* Count;
		distinct_n=strip(put(cnt,16.));
	
		keep datamartid response_date query_package dataset tag distinct_n;
	run;
	
	* Order variables;
	proc sql;
		create table dmlocal.xtbl_l3_non_unique as select
			datamartid, response_date, query_package, dataset, tag, distinct_n
		from query;
	quit;

    * Delete intermediary datasets;
    proc delete data=data query;
    quit; 
	
    %elapsed(end);

%end;

******************************************************************************;
* XTBL_L3_DATES
******************************************************************************;
%if %upcase(&_part1)=YES %then %do;

    %let qname=xtbl_l3_dates;
    %elapsed(begin);

    %macro xminmax(idsn);
    
        * Append each variable into base dataset;
        proc append base=query data=&idsn;
        run;

        * Delete them if they are not needed for the _TIMES version of this query;
        %if &idsn=diagnosis or &idsn=procedures or &idsn=enrollment or &idsn=death or &idsn=dispensing or &idsn=condition or &idsn=lds_address_history 
            or &idsn=immunization or &idsn=pat_relationship or &idsn=external_meds %then %do;

            * Delete intermediary datasets;
            proc delete data=&idsn;
            quit;

        %end;

    %mend xminmax;

    %macro _dates(dsn,_vn,dtvlist,tmvlist1,tmvlist);
            
      %if &dsn=lds_address_history %then %do;
            data lds_address_dates;
      %end;
      %else %if &dsn=pat_relationship %then %do;
            data pat_rel_dates;
      %end;
      %else %do;
            data &dsn._dates;
      %end;
            length futureflag1-futureflag&_vn preflag1-preflag&_vn 3.;
            format time1-time&_vn time5.;
            
            set &dsn(keep=&dtvlist &tmvlist1);

            array _dt &dtvlist;
            array _tm &tmvlist;
            array _time time1 - time&_vn;
            array _ff futureflag1 - futureflag&_vn;
            array _pf preflag1 - preflag&_vn;
            array _id invdtflag1 - invdtflag&_vn;
            array _im imdtflag1 - imdtflag&_vn;
            array _it invtmflag1 - invtmflag&_vn;
            
            do i=1 to &_vn;
                * Create a time variable, which will be missing if time is not needed;
                _time{i}=_tm{i};

                * Flag for future date records;
                if _dt{i}^=. and _dt{i}>&mxrefreshn then _ff{i}=1;
                else _ff{i}=0;

                * Flag for records pre-01JAN2010;
                if .<_dt{i}<'01JAN2010'd then _pf{i}=1;
                else _pf{i}=0;

                * Flag for records with invalid dates (b/f 01JAN1582 and post 31DEC20001);
                if .<_dt{i}<-138061 or _dt{i}>6589335 then _id{i}=1;
                else _id{i}=0;

                * Flag for records implausible dates;
                if .<_dt{i}<'01JAN1900'd then _im{i}=1;
                else _im{i}=0;

                * Flag for records with invalid times;
                if .<_tm{i}<0 or _tm{i}>86400 then _it{i}=1;
                else _it{i}=0;
            end;
        
            keep &dtvlist time: futureflag: preflag: invdtflag: imdtflag: invtmflag:;
        run;
            
    %mend _dates;

    %if &_ydemographic=1 %then %do;
        %_dates(dsn=demographic,_vn=1,dtvlist=birth_date,tmvlist1=birth_time,tmvlist=birth_time);
        %minmax(idsn=demographic,var=birth_date,var_tm=birth_time,_n=1);
        %xminmax(idsn=demographic_birth_date);

        * Delete intermediary datasets;
        proc delete data=demographic_dates;
        quit;
    %end;
    %if &_ydiagnosis=1 %then %do;
        %_dates(dsn=diagnosis,_vn=2,dtvlist=admit_date dx_date,tmvlist1=,tmvlist=admit_time dx_time);
        %minmax(idsn=diagnosis,var=admit_date,var_tm=.,_n=1);
        %minmax(idsn=diagnosis,var=dx_date,var_tm=.,_n=2);
        %xminmax(idsn=diagnosis_admit_date);
        %xminmax(idsn=diagnosis_dx_date);

        * Delete intermediary datasets;
        proc delete data=diagnosis_dates;
        quit;
    %end;
    %if &_yencounter=1 %then %do;
        %_dates(dsn=encounter,_vn=2,dtvlist=admit_date discharge_date,tmvlist1=admit_time discharge_time,tmvlist=admit_time discharge_time);
        %minmax(idsn=encounter,var=admit_date,var_tm=admit_time,_n=1);
        %minmax(idsn=encounter,var=discharge_date,var_tm=discharge_time,_n=2);
        %xminmax(idsn=encounter_admit_date);
        %xminmax(idsn=encounter_discharge_date);

        * Delete intermediary datasets;
        proc delete data=encounter_dates;
        quit;
    %end;
    %if &_yprocedures=1 %then %do;
        %_dates(dsn=procedures,_vn=2,dtvlist=admit_date px_date,tmvlist1=,tmvlist=admit_time px_time);
        %minmax(idsn=procedures,var=admit_date,var_tm=.,_n=1);
        %minmax(idsn=procedures,var=px_date,var_tm=.,_n=2);
        %xminmax(idsn=procedures_admit_date);
        %xminmax(idsn=procedures_px_date);

        * Delete intermediary datasets;
        proc delete data=procedures_dates;
        quit;
    %end;
    %if &_yvital=1 %then %do;
        %_dates(dsn=vital,_vn=1,dtvlist=measure_date,tmvlist1=measure_time,tmvlist=measure_time);
        %minmax(idsn=vital,var=measure_date,var_tm=measure_time,_n=1);
        %xminmax(idsn=vital_measure_date);

        * Delete intermediary datasets;
        proc delete data=vital_dates;
        quit;
    %end;
    %if &_yenrollment=1 %then %do;
        %_dates(dsn=enrollment,_vn=2,dtvlist=enr_start_date enr_end_date,tmvlist1=,tmvlist=enr_start_time enr_end_time);
        %minmax(idsn=enrollment,var=enr_start_date,var_tm=.,_n=1);
        %minmax(idsn=enrollment,var=enr_end_date,var_tm=.,_n=2);
        %xminmax(idsn=enrollment_enr_end_date);
        %xminmax(idsn=enrollment_enr_start_date);

        * Delete intermediary datasets;
        proc delete data=enrollment_dates;
        quit;
    %end;
    %if &_ydeath=1 %then %do;
        %_dates(dsn=death,_vn=1,dtvlist=death_date,tmvlist1=,tmvlist=death_time);
        %minmax(idsn=death,var=death_date,var_tm=.,_n=1);
        %xminmax(idsn=death_death_date);

        * Delete intermediary datasets;
        proc delete data=death_dates;
        quit;
    %end;
    %if &_ydispensing=1 %then %do;
        %_dates(dsn=dispensing,_vn=1,dtvlist=dispense_date,tmvlist1=,tmvlist=dispense_time);
        %minmax(idsn=dispensing,var=dispense_date,var_tm=.,_n=1);
        %xminmax(idsn=dispensing_dispense_date);

        * Delete intermediary datasets;
        proc delete data=dispensing_dates;
        quit;
    %end;
    %if &_yprescribing=1 %then %do;
        %_dates(dsn=prescribing,_vn=3,dtvlist=rx_order_date rx_start_date rx_end_date,tmvlist1=rx_order_time,tmvlist=rx_order_time rx_start_time rx_end_time);
        %minmax(idsn=prescribing,var=rx_order_date,var_tm=rx_order_time,_n=1);
        %minmax(idsn=prescribing,var=rx_start_date,var_tm=.,_n=2);
        %minmax(idsn=prescribing,var=rx_end_date,var_tm=.,_n=3);
        %xminmax(idsn=prescribing_rx_end_date);
        %xminmax(idsn=prescribing_rx_order_date);
        %xminmax(idsn=prescribing_rx_start_date);

        * Delete intermediary datasets;
        proc delete data=prescribing_dates;
        quit;
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %_dates(dsn=lab_result_cm,_vn=3,dtvlist=lab_order_date specimen_date result_date,tmvlist1=specimen_time result_time,tmvlist=lab_order_time specimen_time result_time);
        %minmax(idsn=lab_result_cm,var=lab_order_date,var_tm=.,_n=1);
        %minmax(idsn=lab_result_cm,var=specimen_date,var_tm=specimen_time,_n=2);
        %minmax(idsn=lab_result_cm,var=result_date,var_tm=result_time,_n=3);
        %xminmax(idsn=lab_result_cm_lab_order_date);
        %xminmax(idsn=lab_result_cm_specimen_date);
        %xminmax(idsn=lab_result_cm_result_date);

        * Delete intermediary datasets;
        proc delete data=lab_result_cm_dates;
        quit;
    %end;
    %if &_ycondition=1 %then %do;
        %_dates(dsn=condition,_vn=3,dtvlist=report_date resolve_date onset_date,tmvlist1=,tmvlist=report_order_time specimen_time result_time);
        %minmax(idsn=condition,var=report_date,var_tm=.,_n=1);
        %minmax(idsn=condition,var=resolve_date,var_tm=.,_n=2);
        %minmax(idsn=condition,var=onset_date,var_tm=.,_n=3);
        %xminmax(idsn=condition_onset_date);
        %xminmax(idsn=condition_report_date);
        %xminmax(idsn=condition_resolve_date);

        * Delete intermediary datasets;
        proc delete data=condition_dates;
        quit;
    %end;
    %if &_ypro_cm=1 %then %do;
        %_dates(dsn=pro_cm,_vn=1,dtvlist=pro_date,tmvlist1=pro_time,tmvlist=pro_time);
        %minmax(idsn=pro_cm,var=pro_date,var_tm=pro_time,_n=1);
        %xminmax(idsn=pro_cm_pro_date);

        * Delete intermediary datasets;
        proc delete data=pro_cm_dates;
        quit;
    %end;
    %if &_ymed_admin=1 %then %do;
        %_dates(dsn=med_admin,_vn=2,dtvlist=medadmin_start_date medadmin_stop_date,tmvlist1=medadmin_start_time medadmin_stop_time,tmvlist=medadmin_start_time medadmin_stop_time);
        %minmax(idsn=med_admin,var=medadmin_start_date,var_tm=medadmin_start_time,_n=1);
        %minmax(idsn=med_admin,var=medadmin_stop_date,var_tm=medadmin_stop_time,_n=2);
        %xminmax(idsn=med_admin_medadmin_stop_date);
        %xminmax(idsn=med_admin_medadmin_start_date);

        * Delete intermediary datasets;
        proc delete data=med_admin_dates;
        quit;
    %end;
    %if &_yobs_clin=1 %then %do;
        %_dates(dsn=obs_clin,_vn=2,dtvlist=obsclin_start_date obsclin_stop_date,tmvlist1=obsclin_start_time obsclin_stop_time,tmvlist=obsclin_start_time obsclin_stop_time);
        %minmax(idsn=obs_clin,var=obsclin_start_date,var_tm=obsclin_start_time,_n=1);
        %minmax(idsn=obs_clin,var=obsclin_stop_date,var_tm=obsclin_stop_time,_n=2);
        %xminmax(idsn=obs_clin_obsclin_start_date);
        %xminmax(idsn=obs_clin_obsclin_stop_date);

        * Delete intermediary datasets;
        proc delete data=obs_clin_dates;
        quit;
    %end;
    %if &_yobs_gen=1 %then %do;
        %_dates(dsn=obs_gen,_vn=2,dtvlist=obsgen_start_date obsgen_stop_date,tmvlist1=obsgen_start_time obsgen_stop_time,tmvlist=obsgen_start_time obsgen_stop_time);
        %minmax(idsn=obs_gen,var=obsgen_start_date,var_tm=obsgen_start_time,_n=1);
        %minmax(idsn=obs_gen,var=obsgen_stop_date,var_tm=obsgen_stop_time,_n=2);
        %xminmax(idsn=obs_gen_obsgen_start_date);
        %xminmax(idsn=obs_gen_obsgen_stop_date);

        * Delete intermediary datasets;
        proc delete data=obs_gen_dates;
        quit;
    %end;
    %if &_yldsadrs=1 %then %do;
        %_dates(dsn=lds_address_history,_vn=2,dtvlist=address_period_start address_period_end,tmvlist1=,tmvlist=address_period_start_time address_period_end_time);
        %minmax(idsn=lds_address,var=address_period_start,var_tm=.,_n=1);
        %minmax(idsn=lds_address,var=address_period_end,var_tm=.,_n=2);
        %xminmax(idsn=lds_address_address_period_start);
        %xminmax(idsn=lds_address_address_period_end);

        * Delete intermediary datasets;
        proc delete data=lds_address_dates;
        quit;
    %end;
    %if &_yimmunization=1 %then %do;
        %_dates(dsn=immunization,_vn=3,dtvlist=vx_record_date vx_admin_date vx_exp_date,tmvlist1=,tmvlist=vx_record_time vx_admin_time vx_exp_time);
        %minmax(idsn=immunization,var=vx_record_date,var_tm=.,_n=1);
        %minmax(idsn=immunization,var=vx_admin_date,var_tm=.,_n=2);
        %minmax(idsn=immunization,var=vx_exp_date,var_tm=.,_n=3);
        %xminmax(idsn=immunization_vx_record_date);
        %xminmax(idsn=immunization_vx_admin_date);
        %xminmax(idsn=immunization_vx_exp_date);

        * Delete intermediary datasets;
        proc delete data=immunization_dates;
        quit;
    %end;
    %if &_ypat_relationship=1 %then %do;
        %_dates(dsn=pat_relationship,_vn=2,dtvlist=relationship_start relationship_end,tmvlist1=,tmvlist=relationship_start_time relationship_end_time);
        %minmax(idsn=pat_rel,var=relationship_start,var_tm=.,_n=1);
        %minmax(idsn=pat_rel,var=relationship_end,var_tm=.,_n=2);
        %xminmax(idsn=pat_rel_relationship_start);
        %xminmax(idsn=pat_rel_relationship_end);

        * Delete intermediary datasets;
        proc delete data=pat_rel_dates;
        quit;
    %end;
    %if &_yexternal_meds=1 %then %do;
        %_dates(dsn=external_meds,_vn=4,dtvlist=ext_record_date ext_pat_start_date ext_pat_end_date ext_end_date,tmvlist1=,tmvlist=ext_record_time ext_pat_start_time ext_pat_end_time ext_end_time);
        %minmax(idsn=external_meds,var=ext_record_date,var_tm=.,_n=1);
        %minmax(idsn=external_meds,var=ext_pat_start_date,var_tm=.,_n=2);
        %minmax(idsn=external_meds,var=ext_pat_end_date,var_tm=.,_n=3);
        %minmax(idsn=external_meds,var=ext_end_date,var_tm=.,_n=4);
        %xminmax(idsn=external_meds_ext_record_date);
        %xminmax(idsn=external_meds_ext_pat_start_date);
        %xminmax(idsn=external_meds_ext_pat_end_date);
        %xminmax(idsn=external_meds_ext_end_date);

        * Delete intermediary datasets;
        proc delete data=external_meds_dates;
        quit;
    %end;

    data query;
         set query;
         if dataset="LDS_ADDRESS" then dataset="LDS_ADDRESS_HISTORY";
         else if dataset="PAT_REL" then dataset="PAT_RELATIONSHIP";
    run;

    * Order variables;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, dataset, tag, min, p5, 
             median, p95, max, n, nmiss, future_dt_n, pre2010_n, invalid_n, impl_n
         from query;
    quit;

	* Delete intermediary datasets;
	proc delete data=query;
    quit;

    %elapsed(end);
        
%end;

******************************************************************************;
* XTBL_L3_TIMES
******************************************************************************;
%if %upcase(&_part1)=YES %then %do;

	%let qname=xtbl_l3_times;
    %elapsed(begin);

    %macro xminmax(idsn);
    
        * Append each variable into base dataset;
        proc append base=query data=&idsn (keep=datamartid response_date 
                    query_package dataset tag_tm min_tm mean_tm median_tm max_tm
                    n_tm nmiss_tm invalid_n_tm);
        run;

        * Delete intermediary datasets;
        proc delete data=&idsn;
        quit;

    %mend xminmax;
        
    %if &_ydemographic=1 %then %do;
        %xminmax(idsn=demographic_birth_date);
    %end;
    %if &_yencounter=1 %then %do;
        %xminmax(idsn=encounter_admit_date)
        %xminmax(idsn=encounter_discharge_date);
    %end;
    %if &_yvital=1 %then %do;
        %xminmax(idsn=vital_measure_date);
    %end;
    %if &_ylab_result_cm=1 %then %do;
        %xminmax(idsn=lab_result_cm_result_date);
        %xminmax(idsn=lab_result_cm_specimen_date);
    %end;
    %if &_yprescribing=1 %then %do;
        %xminmax(idsn=prescribing_rx_order_date);
    %end;
    %if &_ypro_cm=1 %then %do;
        %xminmax(idsn=pro_cm_pro_date);
    %end;
    %if &_ymed_admin=1 %then %do;
        %xminmax(idsn=med_admin_medadmin_start_date);
        %xminmax(idsn=med_admin_medadmin_stop_date);
    %end;
    %if &_yobs_clin=1 %then %do;
        %xminmax(idsn=obs_clin_obsclin_start_date);
        %xminmax(idsn=obs_clin_obsclin_stop_date);
    %end;
    %if &_yobs_gen=1 %then %do;
        %xminmax(idsn=obs_gen_obsgen_start_date);
        %xminmax(idsn=obs_gen_obsgen_stop_date);
    %end;

    * Order variables;
    proc sql;
         create table dmlocal.&qname as select
             datamartid, response_date, query_package, dataset, tag_tm as tag, 
             min_tm as min, median_tm as median, max_tm as max, 
             n_tm as n, nmiss_tm as nmiss, invalid_n_tm as invalid_n
         from query;
    quit; 

    * Delete intermediary datasets;
    proc delete data=query;
    quit;

    %elapsed(end);
    
%end;

*********************************************************************************************************************;
*********************************************************************************************************************;
* 4. Process individual dataset queries not used in any combined queries other than the large ones, then drop dataset;
*********************************************************************************************************************;
*********************************************************************************************************************;

    
********************************************************************************;
* PROCESS OBS_CLIN QUERIES
********************************************************************************;
%if &_yobs_clin=1 %then %do;
        
		*******************************************************************************;
		* OBS_CLIN - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_ABN_IND, qnam=obsclin_l3_abn);
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_RESULT_MODIFIER, qnam=obsclin_l3_mod);
			%tag(dset=OBS_CLIN,  varlist=PATID OBSCLINID ENCOUNTERID OBSCLIN_PROVIDERID, qnam=obsclin_l3_n);
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_RESULT_QUAL, qnam=obsclin_l3_qual);
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_RESULT_UNIT, qnam=obsclin_l3_runit);
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_SOURCE, qnam=obsclin_l3_source);
			%n_pct(dset=OBS_CLIN, var=OBSCLIN_TYPE, qnam=obsclin_l3_type);    

		%end; * End of Part 1 OBS_CLIN queries;
		
		*******************************************************************************;
		* OBS_CLIN - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
            %n_pct_multilev(dset=OBS_CLIN, var1=OBSCLIN_CODE, var2=OBSCLIN_TYPE, ref2=valuesets, qnam=obsclin_l3_code_type, distinct=y, observedonly=y);
            %n_pct_multilev(dset=OBS_CLIN, var1=OBSCLIN_CODE, var2=OBSCLIN_RESULT_UNIT, qnam=obsclin_l3_code_unit, subvar=obsclin_type, subset=%str(obsclin_code_old ne '' and obsclin_type='LC'), 
                ref2=valuesets, observedonly=y); 

            /***OBSCLIN_L3_LOINC***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=obsclin_l3_loinc;
            %elapsed(begin);

            %let nobs_obs_clin_loinc = 0;

            data obs_clin_loinc (compress=yes);
                set obs_clin (keep=patid obsclin_code obsclin_type where=(obsclin_type='LC')) end=eof;

                * Need macro value for # of observations in this input dataset for use in macro;
                if eof then call symputx('nobs_obs_clin_loinc',_n_);
                
                drop obsclin_type;              
            run;             
            
            %n_pct_noref(dset=OBS_CLIN_LOINC, var=OBSCLIN_CODE, qnam=obsclin_l3_loinc, distinct=y, timer=n);

            * Determine length of OBSCLIN_CODE; 
            proc contents data=dmlocal.obsclin_l3_loinc noprint out=obsclin_l3_loinc_length (keep=name length where=(name='OBSCLIN_CODE') rename=(length=obsclin_l3_loinc_length));
            run;

            data _null_;
                set loinc_num_length;
                if _n_=1 then set obsclin_l3_loinc_length;

                call symputx('loinc_num_length',max(loinc_num_length,obsclin_l3_loinc_length));
            run;

            * Preserve final desired sort order;
            data obsclin_l3_loinc;
                set dmlocal.obsclin_l3_loinc;

                sortord=_n_;
            run;

            * Sort by LOINC code for merging with reference file;
            proc sort data=obsclin_l3_loinc;
                by obsclin_code;
            run;

            data obsclin_l3_loinc;
                length obsclin_code $&loinc_num_length;
                merge
                    obsclin_l3_loinc (in=inp)
                    loinc (keep=loinc_num panel_type rename=(loinc_num=obsclin_code))
                    ;
                by obsclin_code;
                if inp;

                format obsclin_code;
                informat obsclin_code;
                label obsclin_code=' ';
            run;

            * Sort by final sort order;
            proc sort data=obsclin_l3_loinc out=dmlocal.obsclin_l3_loinc (drop=sortord);
                by sortord;
            run;

            * Delete intermediary dataset;
            proc delete data=obs_clin_loinc obsclin_l3_loinc obsclin_l3_loinc_length;
            quit;

            %elapsed(end);

            /***End of OBSCLIN_L3_LOINC***/
               
			%tag(dset=OBS_CLIN,         varlist=KNOWN_TEST KNOWN_TEST_RESULT KNOWN_TEST_RESULT_NUM KNOWN_TEST_RESULT_NUM_UNIT KNOWN_TEST_RESULT_PLAUSIBLE, qnam=obsclin_l3_recordc, distinct=n, null=n);
			%n_pct_noref(dset=OBS_CLIN, var=OBSCLIN_START_DATE_Y, qnam=obsclin_l3_sdate_y, varlbl=OBSCLIN_START_DATE, distinct=y);
			%n_pct_noref(dset=OBS_CLIN, var=OBSCLIN_START_DATE_YM, qnam=obsclin_l3_sdate_ym, varlbl=OBSCLIN_START_DATE, nopct=y, distinct=y); 

		%end; * End of Part 2 OBS_CLIN queries;

	* Delete OBS_CLIN dataset;
	proc delete data=obs_clin;
	quit;
    
********************************************************************************;
* END OBS_CLIN QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS PROCEDURES QUERIES
********************************************************************************;
%if &_yprocedures=1 %then %do;
    
		*******************************************************************************;
		* PROCEDURES - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=PROCEDURES,      var=ENC_TYPE, qnam=pro_l3_enctype, distinct=y);
			%tag(dset=PROCEDURES,  varlist=ENCOUNTERID PATID PROCEDURESID PROVIDERID PROCEDURESID_PLUS, qnam=pro_l3_n); 
			%n_pct(dset=PROCEDURES,      var=PPX,      qnam=pro_l3_ppx);
			%n_pct_noref(dset=PROCEDURES, var=PX,       qnam=pro_l3_px, dec=2);
			%n_pct(dset=PROCEDURES,      var=PX_SOURCE, qnam=pro_l3_pxsource);
            %n_pct_multilev(dset=PROCEDURES, var1=PX_TYPE, var2=ENC_TYPE, ref1=valuesets, ref2=valuesets, qnam=pro_l3_pxtype_enctype);

		%end; * End of Part 1 PROCEDURES queries;

		*******************************************************************************;
		* PROCEDURES - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=PROCEDURES, var=ADMIT_DATE_Y, qnam=pro_l3_adate_y, varlbl=ADMIT_DATE, distinct=y, distinctenc=y);
            %n_pct_noref(dset=PROCEDURES, var=ADMIT_DATE_YM, qnam=pro_l3_adate_ym, nopct=y, varlbl=ADMIT_DATE);
            %n_pct_multilev(dset=PROCEDURES, var1=ENC_TYPE, var2=ADMIT_DATE_YM, ref1=valuesets, qnam=pro_l3_enctype_adate_ym, var2lbl=ADMIT_DATE, distinct=y, distinctenc=y);
            %n_pct_multilev(dset=PROCEDURES, var1=PX, var2=PX_TYPE, ref2=valuesets, qnam=pro_l3_px_pxtype, distinct=y, observedonly=y);
            %n_pct_multilev(dset=PROCEDURES, var1=PX_TYPE, var2=ADMIT_DATE_Y, ref1=valuesets, qnam=pro_l3_pxtype_adate_y, var2lbl=ADMIT_DATE, distinct=y);
			%n_pct_noref(dset=PROCEDURES, var=PX_DATE_Y,    qnam=pro_l3_pxdate_y, varlbl=PX_DATE, distinct=y, distinctenc=y);
			%n_pct(dset=PROCEDURES,      var=PX_TYPE,      qnam=pro_l3_pxtype);

		%end; * End of Part 2 PROCEDURES queries;

	* Delete PROCEDURES dataset;
	proc delete data=procedures;
	quit;

********************************************************************************;
* END PROCEDURES QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS DISPENSING QUERIES
********************************************************************************;
%if &_ydispensing=1 %then %do;
    
		*******************************************************************************;
		* DISPENSING - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=DISPENSING, var=DISPENSE_DOSE_DISP_UNIT, qnam=disp_l3_doseunit);
			%tag(dset=DISPENSING, varlist=PATID DISPENSINGID PRESCRIBINGID NDC, qnam=disp_l3_n, valid1=4);
			%n_pct(dset=DISPENSING, var=DISPENSE_ROUTE,         qnam=disp_l3_route);
			%n_pct(dset=DISPENSING, var=DISPENSE_SOURCE,        qnam=disp_l3_source);    

		%end; * End of Part 1 DISPENSING queries;

		*******************************************************************************;
		* DISPENSING - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=DISPENSING, var=DISPENSE_DATE_Y,   qnam=disp_l3_ddate_y, varlbl=DISPENSE_DATE, distinct=y);
			%n_pct_noref(dset=DISPENSING, var=DISPENSE_DATE_YM,  qnam=disp_l3_ddate_ym, varlbl=DISPENSE_DATE, nopct=y, distinct=y);
			%cont(dset=DISPENSING,       var=DISPENSE_AMT,      qnam=disp_l3_dispamt_dist, stats=min mean median max n nmiss);
			%cont(dset=DISPENSING,       var=DISPENSE_DOSE_DISP, qnam=disp_l3_dose_dist, stats=min mean median max n nmiss);
			%n_pct_noref(dset=DISPENSING, var=NDC,               qnam=disp_l3_ndc, distinct=y, dec=2);
			
			/***DISP_L3_SUPDIST2***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=disp_l3_supdist2;
            %elapsed(begin);

            * Create custom format for categorizing DISPENSE_SUP;
            proc format;
                value dispsup
                    low-<1 = '<1 day'
                    1-15 = '1-15 days'
                    16-30 = '16-30 days'
                    31-60 = '31-60 days'
                    61-90 = '61-90 days'
                    90<-high = '>90 days'
                    ;
            run;                
			
			* Categorization of DISPENSE_SUP for DISP_L3_SUPDIST2;
			data dispensing_sup (compress=yes);
				set dispensing (keep=dispense_sup);

				length dispense_sup_group $10;

                if not missing(dispense_sup) then dispense_sup_group=strip(put(dispense_sup,dispsup.));

                drop dispense_sup;
			run;    
			
			* Create custom decode list for DISP_L3_SUPDIST2 categories;
			proc format;
				value sup_days
					1 = '<1 day'
					2 = '1-15 days'
					3 = '16-30 days'
					4 = '31-60 days'
					5 = '61-90 days'
					6 = '>90 days'
					;
			run;
			
			data dispense_sup_group_ref;
				table_name='DISPENSING_SUP';
				field_name='DISPENSE_SUP_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 6; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,sup_days.));
					output;
				end;            
			run;
			
			%n_pct(dset=DISPENSING_SUP, var=DISPENSE_SUP_GROUP, qnam=disp_l3_supdist2, ref=dispense_sup_group_ref, cdmref=n, alphasort=n, timer=n);

			* Delete intermediary datasets;
			proc delete data=dispensing_sup dispense_sup_group_ref;
            quit;

            %elapsed(end);

			/***End of DISP_L3_SUPDIST2***/
	 
		%end; * End of Part 2 DISPENSING queries;

	* Delete DISPENSING dataset;
	proc delete data=dispensing;
	quit;

********************************************************************************;
* END DISPENSING QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS CONDITION QUERIES
********************************************************************************;
%if &_ycondition=1 %then %do;
    
		*******************************************************************************;
		* CONDITION - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%tag(dset=CONDITION,  varlist=PATID ENCOUNTERID CONDITIONID CONDITION, qnam=cond_l3_n);
			%n_pct(dset=CONDITION, var=CONDITION_SOURCE, qnam=cond_l3_source);
			%n_pct(dset=CONDITION, var=CONDITION_STATUS, qnam=cond_l3_status);
			%n_pct(dset=CONDITION, var=CONDITION_TYPE,  qnam=cond_l3_type);    

		%end; * End of Part 1 CONDITION queries;

		*******************************************************************************;
		* CONDITION - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=CONDITION, var=CONDITION, qnam=cond_l3_condition, distinct=y);
			%n_pct_noref(dset=CONDITION, var=REPORT_DATE_Y, qnam=cond_l3_rdate_y, varlbl=REPORT_DATE, distinct=y);
			%n_pct_noref(dset=CONDITION, var=REPORT_DATE_YM, qnam=cond_l3_rdate_ym, varlbl=REPORT_DATE, nopct=y);

		%end; * End of Part 2 CONDITION queries;

	* Delete CONDITION dataset;
	proc delete data=condition;
	quit;

********************************************************************************;
* END CONDITION QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS PRO_CM QUERIES
********************************************************************************;
%if &_ypro_cm=1 %then %do;
    
		*******************************************************************************;
		* PRO_CM - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=PRO_CM, var=PRO_CAT,   qnam=procm_l3_cat);
			%n_pct_multilev(dset=PRO_CM, var1=PRO_CODE, var2=PRO_TYPE, ref2=valuesets, qnam=procm_l3_code_type, distinct=y, observedonly=y);
			%n_pct(dset=PRO_CM, var=PRO_METHOD, qnam=procm_l3_method);
			%n_pct(dset=PRO_CM, var=PRO_MODE,  qnam=procm_l3_mode);
			%tag(dset=PRO_CM,  varlist=PATID ENCOUNTERID PRO_CM_ID, qnam=procm_l3_n); 
			%n_pct(dset=PRO_CM, var=PRO_SOURCE, qnam=procm_l3_source);
			%n_pct(dset=PRO_CM, var=PRO_TYPE,  qnam=procm_l3_type);        

		%end; * End of Part 1 PRO_CM queries;

		*******************************************************************************;
		* PRO_CM - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=PRO_CM, var=PRO_FULLNAME,   qnam=procm_l3_fullname);
            %n_pct_noref(dset=PRO_CM, var=PRO_NAME,       qnam=procm_l3_name);

            /***PROCM_L3_LOINC***/

			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=procm_l3_loinc;
			%elapsed(begin);
           
            %n_pct_noref(dset=PRO_CM, var=PRO_CODE, qnam=procm_l3_loinc, distinct=y, timer=n);

            * Determine length of PRO_CODE; 
            proc contents data=dmlocal.procm_l3_loinc noprint out=procm_l3_loinc_length (keep=name length where=(name='PRO_CODE') rename=(length=procm_l3_loinc_length));
            run;

            data _null_;
                set loinc_num_length;
                if _n_=1 then set procm_l3_loinc_length;

                call symputx('loinc_num_length',max(loinc_num_length,procm_l3_loinc_length));
            run;

            * Preserve final desired sort order;
            data procm_l3_loinc;
                set dmlocal.procm_l3_loinc;

                sortord=_n_;
            run;

            * Sort by LOINC code for merging with reference file;
            proc sort data=procm_l3_loinc;
                by pro_code;
            run;

            data procm_l3_loinc;
                length pro_code $&loinc_num_length;
                merge
                    procm_l3_loinc (in=inp)
                    loinc (keep=loinc_num panel_type rename=(loinc_num=pro_code))
                    ;
                by pro_code;
                if inp;

                format pro_code;
                informat pro_code;
                label pro_code=' ';
            run;

            * Sort by final sort order;
            proc sort data=procm_l3_loinc out=dmlocal.procm_l3_loinc (drop=sortord);
                by sortord;
            run;

            * Delete intermediary dataset;
            proc delete data=procm_l3_loinc procm_l3_loinc_length;
            quit;

            %elapsed(end);

            /***End of PROCM_L3_LOINC***/
            
			%n_pct_noref(dset=PRO_CM, var=PRO_DATE_Y,          qnam=procm_l3_pdate_y, varlbl=PRO_DATE, distinct=y);
			%n_pct_noref(dset=PRO_CM, var=PRO_DATE_YM,         qnam=procm_l3_pdate_ym, nopct=y, varlbl=PRO_DATE);    

		%end; * End of Part 2 PRO_CM queries;

    * Delete PRO_CM dataset;
    proc delete data=pro_cm;
    quit;

********************************************************************************;
* END PRO_CM QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS OBS_GEN QUERIES
********************************************************************************;
%if &_yobs_gen=1 %then %do;
        
		*******************************************************************************;
		* OBS_GEN - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=OBS_GEN, var=OBSGEN_ABN_IND,        qnam=obsgen_l3_abn);
			%n_pct(dset=OBS_GEN, var=OBSGEN_RESULT_MODIFIER, qnam=obsgen_l3_mod);
			%tag(dset=OBS_GEN,  varlist=PATID OBSGENID ENCOUNTERID OBSGEN_PROVIDERID, qnam=obsgen_l3_n);
			%n_pct(dset=OBS_GEN, var=OBSGEN_RESULT_QUAL,    qnam=obsgen_l3_qual);
			%n_pct(dset=OBS_GEN, var=OBSGEN_RESULT_UNIT,    qnam=obsgen_l3_runit);
			%n_pct(dset=OBS_GEN, var=OBSGEN_SOURCE,         qnam=obsgen_l3_source);
            %n_pct(dset=OBS_GEN, var=OBSGEN_TABLE_MODIFIED, qnam=obsgen_l3_tmod);            
            %n_pct(dset=OBS_GEN, var=OBSGEN_TYPE, qnam=obsgen_l3_type, ref=obsgen_type_valuesets);

		%end; * End of Part 1 OBS_GEN queries;
		
		*******************************************************************************;
		* OBS_GEN - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;                        

            %n_pct_multilev(dset=OBS_GEN, var1=OBSGEN_CODE, var2=OBSGEN_TYPE, ref2=obsgen_type_valuesets, qnam=obsgen_l3_code_type, distinct=y, observedonly=y);
            %n_pct_multilev(dset=OBS_GEN, var1=OBSGEN_CODE, var2=OBSGEN_RESULT_UNIT, ref2=valuesets, qnam=obsgen_l3_code_unit, subvar=obsgen_type, subset=%str(obsgen_code_old ne '' and obsgen_type='LC'), 
                observedonly=y);
			%tag(dset=OBS_GEN,        varlist=KNOWN_TEST KNOWN_TEST_RESULT KNOWN_TEST_RESULT_NUM KNOWN_TEST_RESULT_NUM_UNIT KNOWN_TEST_RESULT_PLAUSIBLE, qnam=obsgen_l3_recordc, distinct=n, null=n);
			%n_pct_noref(dset=OBS_GEN, var=OBSGEN_START_DATE_Y, qnam=obsgen_l3_sdate_y, varlbl=OBSGEN_START_DATE, distinct=y);
			%n_pct_noref(dset=OBS_GEN, var=OBSGEN_START_DATE_YM, qnam=obsgen_l3_sdate_ym, varlbl=OBSGEN_START_DATE, nopct=y, distinct=y);            			

		%end; * End of Part 2 OBS_GEN queries;

    * Delete intermediary and OBS_GEN datasets;
    proc delete data=obsgen_type_valuesets obs_gen;
	quit; 
           
********************************************************************************;
* END OBS_GEN QUERIES
********************************************************************************;
%end;
    
********************************************************************************;
* PROCESS LDS_ADDRESS_HISTORY QUERIES
********************************************************************************;
%if &_yldsadrs=1 %then %do;
    
		*******************************************************************************;
		* LDS_ADDRESS_HISTORY - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_PREFERRED, qnam=ldsadrs_l3_adrspref);
			%n_pct(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_STATE,    qnam=ldsadrs_l3_adrsstate);
			%n_pct(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_TYPE,     qnam=ldsadrs_l3_adrstype);
			%n_pct(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_USE,      qnam=ldsadrs_l3_adrsuse);
			%tag(dset=LDS_ADDRESS_HISTORY,  varlist=PATID ADDRESSID ADDRESS_ZIP5 ADDRESS_ZIP9 ADDRESS_COUNTY RUCA_ZIP COUNTY_FIPS STATE_FIPS, qnam=ldsadrs_l3_n, valid1=3, valid2=4, valid3=7, valid4=8);  
			%n_pct(dset=LDS_ADDRESS_HISTORY, var=CURRENT_ADDRESS_FLAG,      qnam=ldsadrs_l3_current); 
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=STATE_FIPS,      qnam=ldsadrs_l3_statefips);  
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=COUNTY_FIPS,      qnam=ldsadrs_l3_countyfips);   
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=RUCA_ZIP,      qnam=ldsadrs_l3_rucazip);        

		%end; * End of Part 1 LDS_ADDRESS_HISTORY queries;

		*******************************************************************************;
		* LDS_ADDRESS_HISTORY - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_CITY, qnam=ldsadrs_l3_adrscity);
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_ZIP5, qnam=ldsadrs_l3_adrszip5);
			%n_pct_noref(dset=LDS_ADDRESS_HISTORY, var=ADDRESS_ZIP9, qnam=ldsadrs_l3_adrszip9);

		%end; * End of Part 2 LDS_ADDRESS_HISTORY queries;

    * Delete LDS_ADDRESS_HISTORY dataset;
    proc delete data=lds_address_history;
    run;  

********************************************************************************;
* END LDS_ADDRESS_HISTORY QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS IMMUNIZATION QUERIES
********************************************************************************;
%if &_yimmunization=1 %then %do;
    
		*******************************************************************************;
		* IMMUNIZATION - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=IMMUNIZATION, var=VX_BODY_SITE,    qnam=immune_l3_bodysite);
			%n_pct(dset=IMMUNIZATION, var=VX_DOSE_UNIT,    qnam=immune_l3_doseunit);
			%n_pct(dset=IMMUNIZATION, var=VX_MANUFACTURER, qnam=immune_l3_manufacturer);    
			%tag(dset=IMMUNIZATION,  varlist=PATID IMMUNIZATIONID ENCOUNTERID PROCEDURESID VX_PROVIDERID, qnam=immune_l3_n); 
			%n_pct(dset=IMMUNIZATION, var=VX_ROUTE,        qnam=immune_l3_route);  
			%n_pct(dset=IMMUNIZATION, var=VX_SOURCE,       qnam=immune_l3_source);  
			%n_pct(dset=IMMUNIZATION, var=VX_STATUS,       qnam=immune_l3_status);  
			%n_pct(dset=IMMUNIZATION, var=VX_STATUS_REASON, qnam=immune_l3_statusreason);
            %n_pct_multilev(dset=IMMUNIZATION, var1=VX_CODE, var2=VX_CODE_TYPE, ref2=valuesets, qnam=immune_l3_code_codetype, nopct=n, distinct=y, observedonly=y);     

		%end; * End of Part 1 IMMUNIZATION queries;

		*******************************************************************************;
		* IMMUNIZATION - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=IMMUNIZATION, var=VX_ADMIN_DATE_Y,   qnam=immune_l3_adate_y, varlbl=VX_ADMIN_DATE, distinct=y);
			%n_pct_noref(dset=IMMUNIZATION, var=VX_ADMIN_DATE_YM,  qnam=immune_l3_adate_ym, varlbl=VX_ADMIN_DATE, nopct=y, distinct=y);
			%cont(dset=IMMUNIZATION,       var=VX_DOSE,           qnam=immune_l3_dose_dist, stats=min mean median max n nmiss);        
			%n_pct_noref(dset=IMMUNIZATION, var=VX_RECORD_DATE_Y,  qnam=immune_l3_rdate_y, varlbl=VX_RECORD_DATE, distinct=y);
			%n_pct_noref(dset=IMMUNIZATION, var=VX_RECORD_DATE_YM, qnam=immune_l3_rdate_ym, varlbl=VX_RECORD_DATE, nopct=y, distinct=y);     
			%n_pct(dset=IMMUNIZATION,      var=VX_CODE_TYPE,      qnam=immune_l3_codetype);
			%n_pct_noref(dset=IMMUNIZATION, var=VX_LOT_NUM,        qnam=immune_l3_lotnum);    

		%end; * End of Part 2 IMMUNIZATION queries;

    * Delete IMMUNIZATION dataset;
    proc delete data=immunization;
    run;  

********************************************************************************;
* END IMMUNIZATION QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS ENROLLMENT QUERIES
********************************************************************************;
%if &_yenrollment=1 %then %do;
        
		*******************************************************************************;
		* ENROLLMENT - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=ENROLLMENT, var=ENR_BASIS, qnam=enr_l3_basedist);
			%n_pct(dset=ENROLLMENT, var=CHART,    qnam=enr_l3_chart);     
			%tag(dset=ENROLLMENT,  varlist=PATID ENR_START_DATE ENROLLID, qnam=enr_l3_n);

            * Delete ENROLLMENT dataset;
            proc delete data=enrollment;
            run;    

		%end; * End of Part 1 ENROLLMENT queries;

********************************************************************************;
* END ENROLLMENT QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS PAT_RELATIONSHIP QUERIES
********************************************************************************;
%if &_ypat_relationship=1 %then %do;
        
		*******************************************************************************;
		* PAT_RELATIONSHIP - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		   
			%tag(dset=PAT_RELATIONSHIP, varlist=PATID_1 PATID_2 RELATIONSHIP_TYPE PATRELID, qnam=patrel_l3_n);
            %n_pct(dset=PAT_RELATIONSHIP, var=RELATIONSHIP_TYPE, elig=y, distinct1=y, distinct2=y, qnam=patrel_l3_type); 

		%end; * End of Part 1 PAT_RELATIONSHIP queries;

		*******************************************************************************;
		* PAT_RELATIONSHIP - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;

			%n_pct_noref(dset=PAT_RELATIONSHIP, var=RELSTART_DATE_Y,  distinct1=y, distinct2=y, varlbl=RELATIONSHIP_START, qnam=patrel_l3_sdate_y);
			%n_pct_noref(dset=PAT_RELATIONSHIP, var=RELSTART_DATE_YM, distinct1=y, distinct2=y, varlbl=RELATIONSHIP_START, nopct=y, qnam=patrel_l3_sdate_ym);  

		%end; * End of Part 2 PAT_RELATIONSHIP queries;

    * Delete PAT_RELATIONSHIP dataset;
    proc delete data=pat_relationship;
    run;   

********************************************************************************;
* END PAT_RELATIONSHIP QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS EXTERNAL_MEDS QUERIES
********************************************************************************;
%if &_yexternal_meds=1 %then %do;
        
		*******************************************************************************;
		* EXTERNAL_MEDS - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=EXTERNAL_MEDS, var=EXT_BASIS, qnam=extmed_l3_basis);
			%tag(dset=EXTERNAL_MEDS,  varlist=PATID EXTMEDID, qnam=extmed_l3_n);
			%n_pct(dset=EXTERNAL_MEDS, var=EXT_ROUTE,    qnam=extmed_l3_route);
			%n_pct(dset=EXTERNAL_MEDS, var=EXT_DOSE_ORDERED_UNIT,    qnam=extmed_l3_rxdose_unit);
			%n_pct(dset=EXTERNAL_MEDS, var=EXT_DOSE_FORM,    qnam=extmed_l3_rxdoseform);
			%n_pct(dset=EXTERNAL_MEDS, var=EXTMED_SOURCE,    qnam=extmed_l3_source);

		%end; * End of Part 1 EXTERNAL_MEDS queries;
        
		*******************************************************************************;
		* EXTERNAL_MEDS - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=EXTERNAL_MEDS, var=EXT_RECORD_DATE_Y,   qnam=extmed_l3_rdate_y, varlbl=EXT_RECORD_DATE, distinct=y);
			%n_pct_noref(dset=EXTERNAL_MEDS, var=EXT_RECORD_DATE_YM,  qnam=extmed_l3_rdate_ym, varlbl=EXT_RECORD_DATE, nopct=y, distinct=y);
            %cont(dset=EXTERNAL_MEDS,       var=EXT_DOSE,           qnam=extmed_l3_rxdose_dist, stats=min mean median max n nmiss);        

            /***EXTMED_L3_RXCUI***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=extmed_l3_rxcui;
            %elapsed(begin);            

            * Create format for RXNORM_CUI_TTY from RXNORM_CUI_REF reference file;
            data rxnorm_cui_tty;
                length rxnorm_cui_tty $15;
                set rxnorm_cui_ref (keep=code rxnorm_cui_tty) end=eof;

                hlo='';
                fmtname='$CUI_TTY';
                
                rename
                    rxnorm_cui_tty=label
                    code=start
                    ;

                * Find longest length of RXNORM_CUI_TTY in reference file (length must be at least $15 to account for NULL or missing);
                retain rxnorm_cui_tty_l;
                if _n_=1 then rxnorm_cui_tty_l=15;
                if length(rxnorm_cui_tty)>rxnorm_cui_tty_l then rxnorm_cui_tty_l=length(rxnorm_cui_tty);
                if eof then call symputx('rxnorm_cui_tty_l',rxnorm_cui_tty_l);

                output;
                if eof then do;
                    rxnorm_cui_tty='NULL or missing';
                    code='other';
                    hlo='o';
                    output;
                end;
            run;
             
            * Create format;
            proc format library=work cntlin=rxnorm_cui_tty;
            run;
     
            %n_pct_noref(dset=EXTERNAL_MEDS, var=RXNORM_CUI, qnam=extmed_l3_rxcui, distinct=y, timer=n, dec=2);

            * Create RXNORM_CUI_TTY on final file using format;
            data dmlocal.extmed_l3_rxcui;
                set dmlocal.extmed_l3_rxcui;

                length rxnorm_cui_tty $&rxnorm_cui_tty_l..;

                rxnorm_cui_tty=put(rxnorm_cui,$cui_tty.);              
            run;

            * Reorder columns;       
            data dmlocal.extmed_l3_rxcui;
            retain datamartid response_date query_package rxnorm_cui rxnorm_cui_tty record_n record_pct distinct_patid_n;
                set dmlocal.extmed_l3_rxcui;
            run;
           
            * Delete intermediary datasets;
			proc delete data=rxnorm_cui_tty;
            quit;

            %elapsed(end); 
            
            /***End of EXTMED_L3_RXCUI***/

            /***EXTMED_L3_RXCUI_TIER***/
					
			%let qname=extmed_l3_rxcui_tier;
            %elapsed(begin);

            * Create format for all categories;
            proc format;
                invalue $rxtier
                    'Tier 1' = 1
                    'Tier 2' = 2
                    'Tier 3' = 3
                    'Tier 4' = 4
                    'NULL or missing' = 5
                    ;
                value $rxcats
                    'Tier 1' = 'Tier 1'
                    'Tier 2' = 'Tier 2'
                    'Tier 3' = 'Tier 3'
                    'Tier 4' = 'Tier 4'
                    'NULL or missing' = 'NULL or missing'
                    ;
            run;

            * Sort by RXCUI code for merging with reference file;
            proc sort data=dmlocal.extmed_l3_rxcui (keep=rxnorm_cui record_n) out=extmed_l3_rxcui;
                by rxnorm_cui;
            run;

            * Merge PRES_L3_CODE_TYPE records onto reference file for categorizing into tiers;
            data extmed_l3_code_type_tiers;
                length rxnorm_cui_tier $15;
                merge
                    extmed_l3_rxcui (in=inm)
                    rxnorm_cui_ref (keep=rxnorm_cui rxnorm_cui_tier)
                    ;
                by rxnorm_cui;
                if inm;

                if rxnorm_cui_tier='' then rxnorm_cui_tier='NULL or missing';

                record_n_n=input(record_n,best12.);

                drop rxnorm_cui record_n;                
            run;

            proc means data=extmed_l3_code_type_tiers completetypes noprint;
                class rxnorm_cui_tier/preloadfmt;
                var record_n_n;
                output out=extmed_l3_rxcui_tier sum=record_n_n;
                format rxnorm_cui_tier $rxcats.;
            run;

            data extmed_l3_rxcui_tier;
                set extmed_l3_rxcui_tier (where=(_type_=1));
                if _n_=1 then set extmed_l3_rxcui_tier (keep=_type_ record_n_n where=(_type_denom=0) rename=(record_n_n=denom _type_=_type_denom));

                %stdvar;

                length record_n $20 record_pct $6;

                if record_n_n ne . then do;
                    record_n=strip(put(record_n_n,12.));
                    record_pct=put(record_n_n/denom*100,6.2);
                end;
                
                ord=input(rxnorm_cui_tier,$rxtier.);

                if record_n_n=. then do;
                    record_n='0';
                    record_pct='  0.00';
                end;
                
                drop _freq_ _type_ _type_denom record_n_n denom;
            run;
            
            proc sort data=extmed_l3_rxcui_tier;
                by ord;
            run;        

            * Set final variable order;
            data dmlocal.extmed_l3_rxcui_tier;
                retain datamartid response_date query_package rxnorm_cui_tier record_n record_pct;
                set extmed_l3_rxcui_tier;

                format rxnorm_cui_tier;

                drop ord;
            run;

            * Delete intermediary datasets;
            proc delete data=extmed_l3_rxcui extmed_l3_code_type_tiers extmed_l3_rxcui_tier;
            quit;
            
            %elapsed(end);

            /***End of EXTMED_L3_RXCUI_TIER***/

		%end; * End of Part 2 EXTERNAL_MEDS queries;

    * Delete EXTERNAL_MEDS dataset;
    proc delete data=external_meds;
    run;  

********************************************************************************;
* END EXTERNAL_MEDS QUERIES
********************************************************************************;
%end;


***********************************************************************************;
***********************************************************************************;
************************** 4. Process remaining queries ***************************;
***********************************************************************************;
***********************************************************************************;


********************************************************************************;
* PROCESS ENCOUNTER QUERIES
********************************************************************************;
%if &_yencounter=1 %then %do;
    
		*******************************************************************************;
		* ENCOUNTER - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
            %n_pct(dset=ENCOUNTER, var=ADMITTING_SOURCE, qnam=enc_l3_admsrc);
            				
			/***ENC_L3_DASH2***/
					
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_dash2;
            %elapsed(begin);

            * Create custom decode list for ENC_L3_DASH2 values;
            data dash_ref;
                length table_name $30;
                
                table_name='ENCOUNTER_PERIOD';
                field_name='PERIOD';

                length valueset_item $5;

                do valueset_item_order=1 to 5;
                    if valueset_item_order=1 then valueset_item=put(valueset_item_order,1.) || ' yr';
                    else valueset_item=put(valueset_item_order,1.) || ' yrs';                    
                    output;
                end;
            run;
		
            %n_pct(dset=ENCOUNTER_PERIOD, var=PERIOD, qnam=enc_l3_dash2, ref=dash_ref, cdmref=n, timer=n, alphasort=n, distinct=y);

            * Delete RECORD_N and RECORD_PCT columns and NULL or missing row from this query;
            data dmlocal.enc_l3_dash2;                
                set dmlocal.enc_l3_dash2;

                if period='NULL or missing' then delete;

                drop record_n record_pct;
            run;
         
            * Delete intermediary datasets;
			proc delete data=dash_ref
                    /*Delete ENCOUNTER_PERIOD here if Part 2 is not processed*/
                    %if %upcase(&_part2) ne YES %then %do;
                        encounter_period
                    %end;
                    ;
            quit;

            %elapsed(end);

            /***End of ENC_L3_DASH2***/
                    
			%n_pct(dset=ENCOUNTER, var=DISCHARGE_DISPOSITION, qnam=enc_l3_disdisp, dec=2);
			%n_pct(dset=ENCOUNTER, var=DISCHARGE_STATUS,     qnam=enc_l3_disstat); 
            %n_pct(dset=ENCOUNTER, var=DRG_TYPE,             qnam=enc_l3_drg_type);

            /***ENC_L3_ENCTYPE***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_enctype;
            %elapsed(begin);

            * Determine concatenated length of vars used to determine UNIQUE_VISIT and ELIG_RECORD;
            proc contents data=encounter out=cont_enc noprint;
            run;

            data _null_;
                set cont_enc end=eof;
                
                retain ulength elength 0;
                
                if name in ('PATID' 'PROVIDERID') then ulength=ulength+length;
                if name in ('PATID' 'ENCOUNTERID' 'PROVIDERID') then elength=elength+length;
                
                * Add 12 to ULENGTH (9 for date and 3 for delimiter);
                if eof then call symputx('_ulength',ulength+12);

                * Add 13 to ELENGTH (9 for date and 4 for delimiter);
                if eof then call symputx('_elength',elength+13);
            run;

            %global length_uvis;

            data encounter_enctype (compress=yes);
                set encounter (keep=patid enc_type admit_date providerid encounterid) end=eof;

                * Create UNIQUE_VISIT and determine the max length needed;
                
                length unique_visit $&_ulength elig_record $&_elength;
                
                retain maxlength_uv maxlength_er 0;
                
                if patid ne '' and enc_type ne '' and admit_date ne . and providerid ne '' then do;
                    unique_visit=compress(patid)|| '/' || compress(enc_type) || '/' || put(admit_date,date9.) || '/' || compress(providerid);                
                    maxlength_uv=max(maxlength_uv,length(unique_visit));
                end;
                
                if patid ne '' and enc_type ne '' and admit_date ne . and providerid ne '' and encounterid ne '' then do;
                    elig_record=compress(patid)|| '/' || compress(enc_type) || '/' || put(admit_date,date9.) || '/' || compress(providerid) || '/' || compress(encounterid);                
                    maxlength_er=max(maxlength_er,length(elig_record));
                end;
                
                * Create macro variables to re-size UNIQUE_VISIT and ELIG_RECORD within macro;
                if eof then call symputx('length_uvis',max(maxlength_uv,1));
                if eof then call symputx('length_erec',max(maxlength_er,1));            

                drop maxlength_uv maxlength_er;
            run;

            * Create reference file for ENC_TYPE with new input dataset name;
            data encounter_enctype_ref;
                set valuesets (where=(table_name='ENCOUNTER' and field_name='ENC_TYPE'));

                table_name='ENCOUNTER_ENCTYPE';
            run;
            
            %n_pct(dset=ENCOUNTER_ENCTYPE, var=ENC_TYPE, ref=encounter_enctype_ref, qnam=enc_l3_enctype, distinct=y, distinctvis=y, elig=y, timer=n);

            * Delete intermediary datasets;
			proc delete data=cont_enc encounter_enctype encounter_enctype_ref;
            quit;
            
            %elapsed(end);

            /***End of ENC_L3_ENCTYPE***/
            
            /***ENC_L3_FACILITYTYPE***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_facilitytype;
            %elapsed(begin);            

            * Create format for FACILITY_TYPE_GRP from FACILITY_TYPE reference file;
            data facility_type_grp;
                length facility_type_grp $100;
                set facility_type (keep=code facility_type_grp) end=eof;

                fmtname='$FAC_GRP';

                rename
                    facility_type_grp=label
                    code=start
                    ; 

                * Find longest length of FACILITY_TYPE_GRP in reference file (length must be at least $36 to accont for Values outside of CDM specifications);
                retain facility_type_grp_l;
                if _n_=1 then facility_type_grp_l=36;
                if length(facility_type_grp)>facility_type_grp_l then facility_type_grp_l=length(facility_type_grp);
                if eof then call symputx('facility_type_grp_l',facility_type_grp_l);

                output;
                if eof then do;
                    facility_type_grp='NULL or missing';
                    code='NULL or missing';
                    output;
                    
                    facility_type_grp='Values outside of CDM specifications';
                    code='other';
                    hlo='o';
                    output;
                end;                
            run;
                
            * Create format;
            proc format library=work cntlin=facility_type_grp;
            run;
            
            %n_pct(dset=ENCOUNTER, var=FACILITY_TYPE, qnam=enc_l3_facilitytype, distinct=y, timer=n);

            * Create FACILITY_TYPE_GRP on final file using format;
            data dmlocal.enc_l3_facilitytype;
                set dmlocal.enc_l3_facilitytype;

                length facility_type_grp $&facility_type_grp_l..;

                facility_type_grp=put(facility_type,$fac_grp.);
            run;

            * Reorder columns;       
            data dmlocal.enc_l3_facilitytype;
            retain datamartid response_date query_package facility_type_grp facility_type record_n record_pct distinct_patid_n;
                set dmlocal.enc_l3_facilitytype;
            run;
         
            * Delete intermediary datasets;
			proc delete data=facility_type facility_type_grp;
            quit;

            %elapsed(end); 
            
            /***End of ENC_L3_FACILITYTYPE***/
                      
			%tag(dset=ENCOUNTER, varlist=ENCOUNTERID PATID PROVIDERID FACILITYID FACILITY_LOCATION, qnam=enc_l3_n, valid1=5);

            /***ENC_L3_PAYERTYPE1***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_payertype1;
            %elapsed(begin);            

            * Create format for PAYER_TYPE_PRIMARY_GRP from PAYER_TYPE reference file;
            data payer_type_primary_grp;
                length payer_type_grp $100;
                set payer_type (keep=code payer_type_grp) end=eof;

                fmtname='$PAY_GRP';

                rename
                    payer_type_grp=label
                    code=start
                    ;
                
                * Find longest length of PAYER_TYPE_GRP in reference file (length must be at least $36 to accont for Values outside of CDM specifications);
                retain payer_type_grp_l;
                if _n_=1 then payer_type_grp_l=36;
                if length(payer_type_grp)>payer_type_grp_l then payer_type_grp_l=length(payer_type_grp);
                if eof then call symputx('payer_type_grp_l',payer_type_grp_l);

                output;
                if eof then do;
                    payer_type_grp='NULL or missing';
                    code='NULL or missing';
                    output;
                    
                    payer_type_grp='Values outside of CDM specifications';
                    code='other';
                    hlo='o';
                    output;
                end;                
            run;
            
            * Create format;
            proc format library=work cntlin=payer_type_primary_grp;
            run;
            
            %n_pct(dset=ENCOUNTER, var=PAYER_TYPE_PRIMARY, qnam=enc_l3_payertype1, distinct=y, timer=n);

            * Create PAYER_TYPE_PRIMARY_GRP on final file using format;
            data dmlocal.enc_l3_payertype1;
                set dmlocal.enc_l3_payertype1;

                length payer_type_primary_grp $&payer_type_grp_l..;

                payer_type_primary_grp=put(payer_type_primary,$pay_grp.);               
            run;

            * Reorder columns;       
            data dmlocal.enc_l3_payertype1;
            retain datamartid response_date query_package payer_type_primary_grp payer_type_primary record_n record_pct distinct_patid_n;
                set dmlocal.enc_l3_payertype1;
            run;
            
            * Delete intermediary datasets;
			proc delete data=payer_type payer_type_primary_grp;
            quit;

            %elapsed(end); 
            
            /***End of ENC_L3_PAYERTYPE1***/

            /***ENC_L3_PAYERTYPE2***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_payertype2;
            %elapsed(begin);

            %n_pct(dset=ENCOUNTER, var=PAYER_TYPE_SECONDARY, qnam=enc_l3_payertype2, distinct=y, timer=n);

            * Create PAYER_TYPE_PRIMARY_GRP on final file using format;
            data dmlocal.enc_l3_payertype2;
                set dmlocal.enc_l3_payertype2;

                length payer_type_secondary_grp $&payer_type_grp_l..;

                payer_type_secondary_grp=put(payer_type_secondary,$pay_grp.);
                if payer_type_secondary_grp='NUL' then payer_type_secondary_grp='NULL or missing';
                else if payer_type_secondary_grp='Val' then payer_type_secondary_grp='Values outside of CDM specifications';
            run;

            * Reorder columns;       
            data dmlocal.enc_l3_payertype2;
            retain datamartid response_date query_package payer_type_secondary_grp payer_type_secondary record_n record_pct distinct_patid_n;
                set dmlocal.enc_l3_payertype2;
            run;

            %elapsed(end); 
            
            /***End of ENC_L3_PAYERTYPE2***/

		%end; * End of Part 1 ENCOUNTER queries;

		*******************************************************************************;
		* ENCOUNTER - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
            %n_pct_noref(dset=ENCOUNTER, var=ADMIT_DATE_Y, qnam=enc_l3_adate_y, varlbl=ADMIT_DATE, distinct=y);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=ADMIT_DATE_Y, ref1=valuesets, qnam=enc_l3_enctype_adate_y, var2lbl=ADMIT_DATE, distinct=y);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=ADMIT_DATE_YM, ref1=valuesets, qnam=enc_l3_enctype_adate_ym, var2lbl=ADMIT_DATE, distinct=y);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=ADMITTING_SOURCE, ref1=valuesets, ref2=valuesets, qnam=enc_l3_enctype_admsrc, nopct=n, dec=2);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=DISCHARGE_DATE_YM, ref1=valuesets, qnam=enc_l3_enctype_ddate_ym, var2lbl=DISCHARGE_DATE, distinct=y);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=DISCHARGE_DISPOSITION, ref1=valuesets, ref2=valuesets, qnam=enc_l3_enctype_disdisp, nopct=n, dec=2);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=DISCHARGE_STATUS, ref1=valuesets, ref2=valuesets, qnam=enc_l3_enctype_disstat, nopct=n, dec=2);
            %n_pct_multilev(dset=ENCOUNTER, var1=ENC_TYPE, var2=DRG, ref1=valuesets, qnam=enc_l3_enctype_drg, dec=2, nopct=n);
			%n_pct_noref(dset=ENCOUNTER, var=FACILITY_LOCATION, qnam=enc_l3_facilityloc, distinct=y);
            %n_pct_multilev(dset=ENCOUNTER, var1=FACILITY_TYPE, var2=FACILITY_LOCATION, ref1=valuesets, qnam=enc_l3_facilitytype_facilityloc, nopct=n, observedonly=y, distinct=y);
			%n_pct_noref(dset=ENCOUNTER, var=ADMIT_DATE_YM,    qnam=enc_l3_adate_ym, nopct=y, varlbl=ADMIT_DATE);
			%n_pct_noref(dset=ENCOUNTER, var=DISCHARGE_DATE_Y, qnam=enc_l3_ddate_y, distinct=y, varlbl=DISCHARGE_DATE);
			%n_pct_noref(dset=ENCOUNTER, var=DISCHARGE_DATE_YM, qnam=enc_l3_ddate_ym, nopct=y, varlbl=DISCHARGE_DATE);

            /***ENC_L3_LOS_DIST***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=enc_l3_los_dist;
            %elapsed(begin);    
			
			* Create custom format for categorizing date differences;
			proc format;
				value los_cat
					0 = '0 days (Admit_Date=Discharge_Date)'
					1 = '1 day (Admit_Date < Discharge_Date)'
					2-high = '>=2 days (Admit_Date < Discharge_Date)'
					;
			run;

            * Categorization of date differences for ENC_L3_LOS_DIST;
			data encounter_los (compress=yes);
				set encounter (keep=enc_type admit_date discharge_date where=(.<admit_date<=discharge_date or missing(discharge_date)));

                length los_group $38;
        
                if not missing(discharge_date) then los_group=strip(put(discharge_date-admit_date,los_cat.));
                else los_group='Missing (Null Discharge Date)';
                if los_group not in ('Missing (Null Discharge Date)' '0 days (Admit_Date=Discharge_Date)' '1 day (Admit_Date < Discharge_Date)' '>=2 days (Admit_Date < Discharge_Date)') then los_group='';

                if los_group ne '';

                drop admit_date discharge_date;
            run;

            * Create reference file for ENC_TYPE;
            data los_enc_type_ref;
                set valuesets (where=(table_name='ENCOUNTER' and field_name='ENC_TYPE'));
				table_name='ENCOUNTER_LOS';
			run;
				
			* Create custom decode list for ENC_L3_LOS_DIST categories;
			proc format;
				value losdist
					1 = 'Missing (Null Discharge Date)'
					2 = '0 days (Admit_Date=Discharge_Date)'
					3 = '1 day (Admit_Date < Discharge_Date)'
                    4 = '>=2 days (Admit_Date < Discharge_Date)'
                    ;
    		run;
				
			data los_group_ref;
				table_name='ENCOUNTER_LOS';
				field_name='LOS_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 4; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,losdist.));
					output;
				end;
			run;            
				  				            
            %n_pct_multilev(dset=ENCOUNTER_LOS, var1=ENC_TYPE, var2=LOS_GROUP, ref1=los_enc_type_ref, ref2=los_group_ref, alphasort2=n, cdmref2=n, qnam=enc_l3_los_dist, nopct=n, dec=2, timer=n);

            * Drop macro-created NULL or missing row since it does not apply to this variable;
            data dmlocal.enc_l3_los_dist;
                set dmlocal.enc_l3_los_dist;

                if los_group='NULL or missing' then delete;
            run;
           
            * Delete intermediary datasets;
			proc delete data=encounter_los los_enc_type_ref los_group_ref;
            quit;

            %elapsed(end); 

            /***End of ENC_L3_LOS_DIST***/
        
            %n_pct_multilev(dset=ENCOUNTER, var1=PAYER_TYPE_PRIMARY, var2=ADMIT_DATE_Y, ref1=valuesets, qnam=enc_l3_payertype_y, var2lbl=ADMIT_DATE, distinct=y);
            				                
			*******************************************************************************;
			* ENCOUNTER and LDS_ADDRESS_HISTORY combined queries
			*******************************************************************************; 
            %if &_yldsadrs=1 %then %do;
                    
                /***XTBL_L3_ZIP5_1Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_zip5_1y;
                %elapsed(begin);

                %let nobs_zip_1y = 0;

                * Subset to only subjects within 1yr dash period;
                data zip_1y (compress=yes);
                    merge
                        lds_address_history_f2f  (in=inl)
                        encounter_period (in=ine where=(period='1 yr'))
                        ;
                    by patid;
                    if inl and ine;
                    
                    drop period;
                run;

                data zip_1y_highest (compress=yes);
                    set zip_1y;
                    by patid address_rank;

                    if first.patid;

                    keep patid address_rank;
                run;

                data zip_1y (compress=yes);
                    merge 
                        zip_1y
                        zip_1y_highest (in=inh)
                        ;
                    by patid address_rank;
                    if inh;

                    keep patid address_rank address_zip5;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set zip_1y end=eof;
                    
                    if eof then call symputx('nobs_zip_1y',_n_);
                run;

                %n_pct_noref(dset=ZIP_1Y, var=ADDRESS_ZIP5, qnam=xtbl_l3_zip5_1y, distinct=y, nopct=y, timer=n);
               
                * Delete intermediary datasets;
                proc delete data=zip_1y zip_1y_highest;
                quit;

                %elapsed(end);

                /***End of XTBL_L3_ZIP5_1Y***/
                    
                /***XTBL_L3_ZIP5_5Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_zip5_5y;
                %elapsed(begin);
                
                %let nobs_zip_5y = 0;

                * Subset to only subjects within 1yr dash period;
                data zip_5y (compress=yes);
                    merge
                        lds_address_history_f2f  (in=inl)
                        encounter_period (in=ine where=(period='5 yrs'))
                        ;
                    by patid;
                    if inl and ine;

                    drop period;
                run;

                data zip_5y_highest (compress=yes);
                    set zip_5y;
                    by patid address_rank;

                    if first.patid;

                    keep patid address_rank;
                run;

                data zip_5y (compress=yes);
                    merge 
                        zip_5y
                        zip_5y_highest (in=inh)
                        ;
                    by patid address_rank;
                    if inh;

                    keep patid address_rank address_zip5;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set zip_5y end=eof;
                    
                    if eof then call symputx('nobs_zip_5y',_n_);
                run;

                %n_pct_noref(dset=ZIP_5Y, var=ADDRESS_ZIP5, qnam=xtbl_l3_zip5_5y, distinct=y, nopct=y, timer=n);
                
                * Delete intermediary datasets;
                proc delete data=lds_address_history_f2f zip_5y zip_5y_highest;
                quit;
                
                %elapsed(end);

                /***End of XTBL_L3_ZIP5_5Y***/    
                    
                /***XTBL_L3_ZIP5C_1Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_zip5c_1y;
                %elapsed(begin);

                %let nobs_zipc_1y = 0;

                * Subset to only subjects within 1yr dash period;
                data zipc_1y (compress=yes);
                    merge
                        lds_address_history_f2f_current  (in=inl where=(valid_address_zip5='Y' or missing(address_zip5)))
                        encounter_period (in=ine where=(period='1 yr'))
                        ;
                    by patid;
                    if inl and ine;
                    
                    drop period;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set zipc_1y end=eof;
                    
                    if eof then call symputx('nobs_zipc_1y',_n_);
                run;

                %n_pct_noref(dset=ZIPC_1Y, var=ADDRESS_ZIP5, qnam=xtbl_l3_zip5c_1y, distinct=y, nopct=y, timer=n);
               
                * Delete intermediary datasets;
                proc delete data=zipc_1y;
                quit;

                %elapsed(end);

                /***End of XTBL_L3_ZIP5C_1Y***/   
                    
                /***XTBL_L3_ZIP5C_5Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_zip5c_5y;
                %elapsed(begin);
                
                %let nobs_zipc_5y = 0;

                * Subset to only subjects within 5yr dash period;
                data zipc_5y (compress=yes);
                    merge
                        lds_address_history_f2f_current  (in=inl where=(valid_address_zip5='Y' or missing(address_zip5)))
                        encounter_period (in=ine where=(period='5 yrs'))
                        ;
                    by patid;
                    if inl and ine;

                    drop period;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set zipc_5y end=eof;
                    
                    if eof then call symputx('nobs_zipc_5y',_n_);
                run;

                %n_pct_noref(dset=ZIPC_5Y, var=ADDRESS_ZIP5, qnam=xtbl_l3_zip5c_5y, distinct=y, nopct=y, timer=n);
                
                * Delete intermediary datasets;
                proc delete data=zipc_5y;
                quit;
                
                %elapsed(end);

                /***End of XTBL_L3_ZIP5C_5Y***/    
                    
                /***XTBL_L3_COUNTYFIPSC_5Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_countyfipsc_5y;
                %elapsed(begin);
                
                %let nobs_countyfips_5y = 0;

                * Subset to only subjects within 5yr dash period;
                data countyfips_5y (compress=yes);
                    merge
                        lds_address_history_f2f_current  (in=inl where=(valid_county_fips='Y' or missing(county_fips)))
                        encounter_period (in=ine where=(period='5 yrs'))
                        ;
                    by patid;
                    if inl and ine;

                    drop period;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set countyfips_5y end=eof;
                    
                    if eof then call symputx('nobs_countyfips_5y',_n_);
                run;

                %n_pct_noref(dset=COUNTYFIPS_5Y, var=COUNTY_FIPS, qnam=xtbl_l3_countyfipsc_5y, distinct=y, nopct=y, timer=n); 
                
                * Delete intermediary datasets;
                proc delete data=countyfips_5y;
                quit;
                
                %elapsed(end);

                /***End of XTBL_L3_COUNTYFIPSC_5Y***/    
                    
                /***XTBL_L3_STATEFIPSC_5Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_statefipsc_5y;
                %elapsed(begin);
                
                %let nobs_statefips_5y = 0;

                * Subset to only subjects within 5yr dash period;
                data statefips_5y (compress=yes);
                    merge
                        lds_address_history_f2f_current  (in=inl where=(valid_state_fips='Y' or missing(state_fips)))
                        encounter_period (in=ine where=(period='5 yrs'))
                        ;
                    by patid;
                    if inl and ine;

                    drop period;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set statefips_5y end=eof;
                    
                    if eof then call symputx('nobs_statefips_5y',_n_);
                run; 

                %n_pct_noref(dset=STATEFIPS_5Y, var=STATE_FIPS, qnam=xtbl_l3_statefipsc_5y, distinct=y, nopct=y, timer=n); 
                
                * Delete intermediary datasets;
                proc delete data=statefips_5y;
                quit;
                
                %elapsed(end);

                /***End of XTBL_L3_STATEFIPSC_5Y***/    
                    
                /***XTBL_L3_RUCAC_5Y***/
					
                * Start timer outside of macro in order to capture derivation processing time;
                %let qname=xtbl_l3_rucac_5y;
                %elapsed(begin);
                
                %let nobs_rucac_5y = 0;

                * Subset to only subjects within 5yr dash period;
                data rucac_5y (compress=yes);
                    merge
                        lds_address_history_f2f_current  (in=inl where=(valid_ruca_zip='Y' or missing(ruca_zip)))
                        encounter_period (in=ine where=(period='5 yrs'))
                        ;
                    by patid;
                    if inl and ine;

                    drop period;
                run;

                * Need macro value for # of observations in this input dataset for use in macro;
                data _null_;
                    set rucac_5y end=eof;
                    
                    if eof then call symputx('nobs_rucac_5y',_n_);
                run; 

                %n_pct_noref(dset=RUCAC_5Y, var=RUCA_ZIP, qnam=xtbl_l3_rucac_5y, distinct=y, nopct=y, timer=n);     
                
                * Delete intermediary datasets;
                proc delete data=rucac_5y lds_address_history_f2f_current encounter_period;
                quit;  
                
                %elapsed(end);

            %end;
			/*End of ENCOUNTER query requiring LDS_ADDRESS_HISTORY*/
            
		%end; * End of Part 2 ENCOUNTER queries;

********************************************************************************;
* END ENCOUNTER QUERIES
********************************************************************************;
%end;

*******************************************************************************;
* Bring in and compress PROVIDER
*******************************************************************************;
%if &_yprovider=1 %then %do;

	%let qname=provider;
	%elapsed(begin);

	data provider (compress=yes);
		set pcordata.provider (keep=provider_npi_flag provider_sex provider_specialty_primary providerid provider_npi) end=eof;
	run;        

	%elapsed(end);

%end;

********************************************************************************;
* PROCESS PROVIDER QUERIES
********************************************************************************;
%if &_yprovider=1 %then %do;
    
		*******************************************************************************;
		* PROVIDER - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%tag(dset=PROVIDER,  varlist=PROVIDERID PROVIDER_NPI, qnam=prov_l3_n);
			%n_pct(dset=PROVIDER, var=PROVIDER_NPI_FLAG,         qnam=prov_l3_npiflag);
			%n_pct(dset=PROVIDER, var=PROVIDER_SEX,              qnam=prov_l3_sex);
			%n_pct(dset=PROVIDER, var=PROVIDER_SPECIALTY_PRIMARY, qnam=prov_l3_specialty);
			
			/***PROV_L3_SPECIALTY_GROUP***/
				
			* Start timer outside of macro in order to capture grouping processing time;
			%let qname=prov_l3_specialty_group;
			%elapsed(begin);

			* Create format for PROVIDER_SPECIALTY_PRIMARY grouping;
			data prov_grp;
				 length fmtname $9 start label $100;
				 set provider_specialty_primary (keep=code grouping) end=eof;
				 
				 fmtname='$PROV_GRP';

				 start=code;
				 label=grouping;

				 * Identify longest GROUPING length;
				 retain grp_length;
				 if _n_=1 then grp_length=length(grouping);
				 if length(grouping)>grp_length then grp_length=length(grouping);
				 if eof then call symput('prov_grp_length',strip(put(grp_length,12.)));
			run;
			
			* Create format;
			proc format library=work cntlin=prov_grp;
			run;

			* Create formatted version of PROVIDER_SPECIALTY_PRIMARY;
			data provider_spec_prim (compress=yes);
				set provider (keep=providerid provider_specialty_primary);

				length provider_specialty_group $&prov_grp_length;

				if provider_specialty_primary not in ('' 'NI' 'UN' 'OT') then provider_specialty_group=put(provider_specialty_primary,$prov_grp.);
                else provider_specialty_group=provider_specialty_primary;
			run;

			* Knock grouping values down to one record per group for reference file;
			proc sort data=prov_grp nodupkey;
				by grouping;
			run;
			
			* Create reference file for PROV_L3_SPECIALTY_GROUP groupings;
			data provider_specialty_group_ref;
				set prov_grp (keep=grouping) end=eof;

				table_name='PROVIDER_SPEC_PRIM';
				field_name='PROVIDER_SPECIALTY_GROUP';

				valueset_item_order=_n_;

				length valueset_item $100;

				valueset_item=grouping;
				output;

                if eof then do;
                    valueset_item='NI';
                    valueset_item_order=30;
                    output;
                    
                    valueset_item='UN';
                    valueset_item_order=31;
                    output;
                    
                    valueset_item='OT';
                    valueset_item_order=32;                    
					output;            
				end;
				
				drop grouping;
			run;        

			%n_pct(dset=PROVIDER_SPEC_PRIM, var=PROVIDER_SPECIALTY_GROUP, qnam=prov_l3_specialty_group, ref=provider_specialty_group_ref, timer=n);   

			* Delete intermediary and PROVIDER datasets;
			proc delete data=prov_grp provider_specialty_primary provider;
            quit;

            %elapsed(end);
		
            /***End of PROV_L3_SPECIALTY_GROUP***/
                
			*******************************************************************************;
			* PROVIDER and ENCOUNTER combined queries
			*******************************************************************************; 
			%if &_yencounter=1 %then %do;
			
                /***PROV_L3_SPECIALTY_ENCTYPE***/
				
                * Start timer outside of macro in order to capture grouping processing time;
                %let qname=prov_l3_specialty_enctype;
                %elapsed(begin);

                proc sort data=encounter (keep=providerid enc_type) out=encounter_type;
                    by providerid;
                run;

                proc sort data=provider_spec_prim (keep=providerid provider_specialty_primary provider_specialty_group where=(not missing(providerid) and not missing(provider_specialty_primary))) out=provider_specialty;
                    by providerid;
                run;

                data provider_enctype (compress=yes);
                    merge
                        encounter_type (in=ine where=(not missing(providerid)))
                        provider_specialty (in=inp)
                        ;
                    by providerid;
                    if ine and inp;
                run;

                * Create reference file for PROVIDER_SPECIALTY_GROUP with new input dataset name;
                data provider_specialty_group_ref;
                    set provider_specialty_group_ref (drop=table_name);

                    table_name='PROVIDER_ENCTYPE';
                run;

                * Create reference file for PROVIDER_SPECIALTY_PRIMARY with new input dataset name;
                data provider_specialty_primary_ref;
                    set valuesets (where=(table_name='PROVIDER' and field_name='PROVIDER_SPECIALTY_PRIMARY'));

                    table_name='PROVIDER_ENCTYPE';
                run; 

                * Create reference file for ENC_TYPE with new input dataset name;
                data provider_enctype_ref;
                    set valuesets (where=(table_name='ENCOUNTER' and field_name='ENC_TYPE'));

                    table_name='PROVIDER_ENCTYPE';
                run;

                %n_pct_multilev(dset=PROVIDER_ENCTYPE, var1=PROVIDER_SPECIALTY_GROUP, var2=PROVIDER_SPECIALTY_PRIMARY, var3=ENC_TYPE, ref1=provider_specialty_group_ref,
                    ref2=provider_specialty_primary_ref, ref3=provider_enctype_ref, qnam=prov_l3_specialty_enctype, distinctprov=y, observedonly=y, timer=n);

                * Delete intermediary datasets;
                proc delete data=encounter_type provider_enctype provider_specialty_group_ref provider_specialty_primary_ref provider_enctype_ref provider_specialty provider_spec_prim;
                quit;

                %elapsed(end);

                /***End of PROV_L3_SPECIALTY_ENCTYPE***/

            %end;
			/*End of PROVIDER query requiring ENCOUNTER*/
				
		%end; * End of Part 1 PROVIDER queries;           

********************************************************************************;
* END PROVIDER QUERIES
********************************************************************************;
%end;

********************************************************************************;
* PROCESS DEMOGRAPHIC QUERIES
********************************************************************************;
%if &_ydemographic=1 %then %do;
    
		*******************************************************************************;
		* DEMOGRAPHIC - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			* Can only derive age for summaries if DEATH is available;
			%if &_ydeath=1 %then %do;

				/***DEM_L3_AGEYRSDIST2***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=dem_l3_ageyrsdist2;
				%elapsed(begin);

				* Get earliest death date;                
				data earliest_death;
					set death (where=(not missing(death_date)));
					by patid death_date;

					if first.patid;

					keep patid death_date;
				run;
				
                * Create custom format for categorizing AGE;
                proc format;
                    value agegrp
                        low-<0 = '<0 yrs'
                        0-1 = '0-1 yrs'
                        2-4 = '2-4 yrs'
                        5-9 = '5-9 yrs'
                        10-14 = '10-14 yrs'
                        15-18 = '15-18 yrs'
                        19-21 = '19-21 yrs'
                        22-44 = '22-44 yrs'
                        45-64 = '45-64 yrs'
                        65-74 = '65-74 yrs'
                        75-89 = '75-89 yrs'
                        89<-high = '>89 yrs'
                        ;
                run;

				* Categorization of age for DEM_L3_AGEYRSDIST2;
				data demographic (compress=yes);
					merge
						demographic (in=ind)
						earliest_death
						;
					by patid;
					if ind;

					if not missing(birth_date) then age=int(yrdif(birth_date,min(death_date,today())));

					length age_group $10;

					if not missing(age) then age_group=strip(put(age,agegrp.));
				run;    
				
				* Create custom decode list for DEM_L3_AGEYRSDIST2 categories;
				proc format;
					value age_group
						1 = '<0 yrs'
						2 = '0-1 yrs'
						3 = '2-4 yrs'
						4 = '5-9 yrs'
						5 = '10-14 yrs'
						6 = '15-18 yrs'
						7 = '19-21 yrs'
						8 = '22-44 yrs'
						9 = '45-64 yrs'
						10 = '65-74 yrs'
						11 = '75-89 yrs'
						12 = '>89 yrs'
						;
                run;                
                
                * Create reference file for AGE_GROUP;                				
				data age_group_ref;
					table_name='DEMOGRAPHIC';
					field_name='AGE_GROUP';

					length valueset_item $100;

					do valueset_item_order=1 to 12; * Custom sort to be used in final dataset;
						valueset_item=strip(put(valueset_item_order,age_group.));
						output;
					end;
                run;          
				  
				%n_pct(dset=DEMOGRAPHIC, var=AGE_GROUP, qnam=dem_l3_ageyrsdist2, ref=age_group_ref, cdmref=n, timer=n, alphasort=n);

				* Delete intermediary datasets;
				proc delete data=earliest_death;
                quit;

                %elapsed(end);
							
				/***End of DEM_L3_AGEYRSDIST2***/
		
				%cont(dset=DEMOGRAPHIC, var=AGE, qnam=dem_l3_ageyrsdist1, stats=min mean median max n nmiss, dec=0); 
			
			%end;
			/*End of DEMOGRAPHIC queries requiring DEATH*/
			
			%n_pct(dset=DEMOGRAPHIC, var=BIOBANK_FLAG,         		qnam=dem_l3_biobank);
			%n_pct(dset=DEMOGRAPHIC, var=GENDER_IDENTITY,         	qnam=dem_l3_genderdist);
			%n_pct(dset=DEMOGRAPHIC, var=HISPANIC,                	qnam=dem_l3_hispdist);    
			%tag(dset=DEMOGRAPHIC,   varlist=PATID,               	qnam=dem_l3_n);
			%n_pct(dset=DEMOGRAPHIC, var=SEXUAL_ORIENTATION,      	qnam=dem_l3_orientdist);
			%n_pct(dset=DEMOGRAPHIC, var=PAT_PREF_LANGUAGE_SPOKEN, 	qnam=dem_l3_patpreflang);
			%n_pct(dset=DEMOGRAPHIC, var=RACE,                    	qnam=dem_l3_racedist);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_MISSING,         	qnam=dem_l3_raceethmiss);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_AI_AN,         	qnam=dem_l3_raceethaian);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_ASIAN,         	qnam=dem_l3_raceethasian);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_BLACK,         	qnam=dem_l3_raceethblack);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_HISPANIC,         qnam=dem_l3_raceethhispanic);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_ME_NA,         	qnam=dem_l3_raceethmena);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_NH_PI,         	qnam=dem_l3_raceethnhpi);
			%n_pct(dset=DEMOGRAPHIC, var=RACE_ETH_WHITE,         	qnam=dem_l3_raceethwhite);
			%n_pct(dset=DEMOGRAPHIC, var=SEX,                     	qnam=dem_l3_sexdist);   
		   
			*******************************************************************************;
			* DEMOGRAPHIC and ENCOUNTER combined queries
			*******************************************************************************; 
			%if &_yencounter=1 %then %do;	
			
				/***XTBL_L3_RACE_ENC***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_race_enc;
				%elapsed(begin);
				
				proc sort data=enc2012 nodupkey;
					by patid;
				run;

				data race2012 (compress=yes);
					merge
						demographic (in=ind keep=patid race)
						enc2012 (in=ine keep=patid)
						;
					by patid;
					if ind and ine;
				run;

				* Create reference file for RACE2012 based on same versions used for DEMOGRAPHICS summary;
				data race2012_valuesets;
					set valuesets (where=(table_name='DEMOGRAPHIC'));
					
					table_name='RACE2012';
				run;

				%n_pct(dset=RACE2012, var=RACE, qnam=xtbl_l3_race_enc, ref=race2012_valuesets, distinct=y, timer=n);

				* Delete intermediary datasets;
				proc delete data=race2012 race2012_valuesets enc2012;
                quit;

                %elapsed(end);

				/***End of XTBL_L3_RACE_ENC***/			
			
                /***DEM_OBS_L3_GENDERDIST***/
					
                * Start timer outside of macro in order to capture derivation processing time (all DEM_OBS dataset processing overall is put into the runtime of this query);
                %let qname=dem_obs_l3_genderdist;
                %elapsed(begin);

                * Derive DEM_OBS dataset for use in DEMO_OBS queries;  
                %if &_yencounter=1 %then %do;
        
                    * Identify PATIDs with an encounter of interest in the last five years;
                    data enc_obs (compress=yes);
                        set encounter (keep=patid admit_date enc_type where=(enc_type in ('AV' 'ED' 'EI' 'IP' 'OS') and &cutoff_dashyr5<=admit_date<=&cutoff_dash));

                        keep patid;
                    run;

                    * Only keep each PATID once;
                    proc sort data=enc_obs nodupkey;
                        by patid;
                    run;  

                    * Merge ENC_OBS onto DEMOGRAPHIC to subset population;
                    data dem_obs;
                        merge
                            demographic (in=in1)
                            enc_obs (in=in2)
                            ;
                        by patid;
                        if in1 and in2;
                    run;

                    * Delete intermediary and ENCOUNTER and DEMOGRAPHIC datasets;
                    proc delete data=enc_obs encounter demographic;
                    quit;        

                %end;
			
                * Create reference file for DEM_OBS based on same versions used for DEMOGRAPHIC summary;
                data dem_obs_valuesets;
                    set valuesets (where=(table_name='DEMOGRAPHIC'));
				
                    table_name='DEM_OBS';
                run;
		
                %n_pct(dset=DEM_OBS, var=GENDER_IDENTITY, qnam=dem_obs_l3_genderdist, ref=dem_obs_valuesets, timer=n);

                %elapsed(end);

                /***End of DEM_OBS_L3_GENDERDIST***/
                
                %n_pct(dset=DEM_OBS, var=HISPANIC,                qnam=dem_obs_l3_hispdist,   ref=dem_obs_valuesets);
                %n_pct(dset=DEM_OBS, var=SEXUAL_ORIENTATION,      qnam=dem_obs_l3_orientdist, ref=dem_obs_valuesets);
                %n_pct(dset=DEM_OBS, var=PAT_PREF_LANGUAGE_SPOKEN, qnam=dem_obs_l3_patpreflang, ref=dem_obs_valuesets);
                %n_pct(dset=DEM_OBS, var=RACE,                    qnam=dem_obs_l3_racedist,   ref=dem_obs_valuesets);
                %n_pct(dset=DEM_OBS, var=SEX,                     qnam=dem_obs_l3_sexdist,    ref=dem_obs_valuesets); 

                * Can only derive age for summaries if DEATH is available;
                %if &_ydeath=1 %then %do;

                    %cont(dset=DEM_OBS, var=AGE, qnam=dem_obs_l3_ageyrsdist1, stats=min mean median max n nmiss, dec=0);  			
			
                    /***DEM_OBS_L3_AGEYRSDIST2***/
					
                    * Start timer outside of macro in order to capture derivation processing time;
                    %let qname=dem_obs_l3_ageyrsdist2;
                    %elapsed(begin);

                    * Create reference file for AGE_GROUP;                				
                    data dem_obs_age_group_ref;
                        set age_group_ref;
                        table_name='DEM_OBS';
                        field_name='AGE_GROUP';
                    run;
				  
                    %n_pct(dset=DEM_OBS, var=AGE_GROUP, qnam=dem_obs_l3_ageyrsdist2, ref=dem_obs_age_group_ref, cdmref=n, timer=n, alphasort=n);

                    * Delete intermediary datasets;
                    proc delete data=dem_obs_age_group_ref age_group_ref;
                    quit;

                    %elapsed(end);

                    /***End of DEM_OBS_L3_AGEYRSDIST2***/          
			
                %end;
                /*End of DEM_OBS queries requiring DEATH*/       

            %end; 
            /*End of DEMOGRAPHIC queries requiring ENCOUNTER*/
                
            * Delete intermediary datasets;
            proc delete data=dem_obs dem_obs_valuesets;
            quit;
            
        %end; * End of Part 1 DEMOGRAPHIC queries;

********************************************************************************;
* END DEMOGRAPHIC QUERIES
********************************************************************************;
%end; 

*********************************************************************************************************;
* VIT_L3_DASH1 (Used in a downstream query that uses DIAGNOSIS, so processed above rest of VITAL queries);
*********************************************************************************************************;
%if &_yvital=1 and %upcase(&_part2)=YES %then %do;
    
    * Start timer outside of macro in order to capture derivation processing time;
    %let qname=vit_l3_dash1;
    %elapsed(begin);
            
    * Create custom decode list for VIT_L3_DASH1 values;
    data dash_ref;
        length table_name $30;
                
        table_name='VITAL_PERIOD';
        field_name='PERIOD';

        length valueset_item $5;

        do valueset_item_order=1 to 5;
            if valueset_item_order=1 then valueset_item=put(valueset_item_order,1.) || ' yr';
            else valueset_item=put(valueset_item_order,1.) || ' yrs';                    
            output;
        end;
    run;
		
    %n_pct(dset=VITAL_PERIOD, var=PERIOD, qnam=vit_l3_dash1, ref=dash_ref, cdmref=n, timer=n, alphasort=n, distinct=y);

    * Delete RECORD_N and RECORD_PCT columns and NULL or missing row from this query;
    data dmlocal.vit_l3_dash1;                
        set dmlocal.vit_l3_dash1;

        if period='NULL or missing' then delete;

        drop record_n record_pct;
    run;
               
	* Delete intermediary datasets;
	proc delete data=dash_ref;
    quit;

    %elapsed(end);

%end;

*********************************************************************************************************;
****************************************** End of VIT_L3_DASH1 ******************************************;
*********************************************************************************************************;

********************************************************************************;
* PROCESS DIAGNOSIS QUERIES
********************************************************************************;
%if &_ydiagnosis=1 %then %do;
    
		*******************************************************************************;
		* DIAGNOSIS - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct_noref(dset=DIAGNOSIS, var=DX,       qnam=dia_l3_dx, dec=2);
			%n_pct(dset=DIAGNOSIS,      var=DX_POA,   qnam=dia_l3_dxpoa, distinct=y);
			%n_pct(dset=DIAGNOSIS,      var=DX_SOURCE, qnam=dia_l3_dxsource);
            %n_pct_multilev(dset=DIAGNOSIS, var1=DX_TYPE, var2=DX_SOURCE, ref1=valuesets, ref2=valuesets, qnam=dia_l3_dxtype_dxsource);
            %n_pct_multilev(dset=DIAGNOSIS, var1=DX_TYPE, var2=ENC_TYPE, ref1=valuesets, ref2=valuesets, qnam=dia_l3_dxtype_enctype);
			%n_pct(dset=DIAGNOSIS,      var=ENC_TYPE, qnam=dia_l3_enctype, distinct=y);    
			%tag(dset=DIAGNOSIS,        varlist=ENCOUNTERID PATID DIAGNOSISID PROVIDERID DIAGNOSISID_PLUS, qnam=dia_l3_n);
			%n_pct(dset=DIAGNOSIS,      var=PDX,      qnam=dia_l3_pdx); 
		   
			*******************************************************************************;
			* DIAGNOSIS and VITAL combined queries
			*******************************************************************************; 
			%if &_yvital=1 %then %do;	
			
				/***XTBL_L3_DASH1***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_dash1;
                %elapsed(begin);

                data diagnosis_period (compress=yes);
                    set diagnosis (keep=patid admit_date dx enc_type where=(not missing(admit_date) and not missing(dx) and enc_type in ('ED' 'EI' 'IP' 'OS' 'AV')));

                    length period $5;
                    
                    * Identify periods for XTBL_L3_DASH#;
                    %dash(datevar=admit_date);

                    keep patid period;
                run;

                * Only need one record per period per PATID per PERIOD from DIAGNOSIS;
				proc sort data=diagnosis_period (keep=patid period where=(not missing(period))) nodupkey;
					by patid period;
				run;

                * Only need one record per period per PATID per PERIOD from VITAL;
				proc sort data=vital_period (keep=patid period where=(not missing(period))) nodupkey;
					by patid period;
				run;

                data dash1 (compress=yes);
                    length period $15;
					merge
						diagnosis_period (in=ind)
						vital_period (in=inv)
						;
					by patid period;
					if ind and inv;
                run;

                * Create reference file for use in XTBL_L3_DASH1;
                data dash_ref;
                    length table_name $30;
                
                    table_name='DASH1';
                    field_name='PERIOD';

                    length valueset_item $5;

                    do valueset_item_order=1 to 5;
                        if valueset_item_order=1 then valueset_item=put(valueset_item_order,1.) || ' yr';
                        else valueset_item=put(valueset_item_order,1.) || ' yrs';                    
                        output;
                    end;
                run;
		
                %n_pct(dset=DASH1, var=PERIOD, qnam=xtbl_l3_dash1, ref=dash_ref, cdmref=n, timer=n, alphasort=n, distinct=y);

                * Delete RECORD_N and RECORD_PCT columns and NULL or missing row from this query;
                data dmlocal.xtbl_l3_dash1;                
                    set dmlocal.xtbl_l3_dash1;

                    if period='NULL or missing' then delete;

                    drop record_n record_pct;
                run;
               
				* Delete intermediary datasets;
				proc delete data=diagnosis_period vital_period;
                quit;

                %elapsed(end);

				/***End of XTBL_L3_DASH1***/
				
			%end;
			/*End of DIAGNOSIS query requiring VITAL*/ 

            %if %upcase(&_part2) ne YES %then %do;
                
                * Delete DIAGNOSIS dataset if Part 2 is not processed;
                proc delete data=diagnosis;
                quit;

            %end;                
           
		%end; * End of Part 1 DIAGNOSIS queries;

		*******************************************************************************;
		* DIAGNOSIS - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=DIAGNOSIS, var=ADMIT_DATE_Y, qnam=dia_l3_adate_y, varlbl=ADMIT_DATE, distinct=y, distinctenc=y);
			%n_pct_noref(dset=DIAGNOSIS, var=ADMIT_DATE_YM, qnam=dia_l3_adate_ym, nopct=y, varlbl=ADMIT_DATE);
            %n_pct_multilev(dset=DIAGNOSIS, var1=DX, var2=DX_TYPE, ref2=valuesets, qnam=dia_l3_dx_dxtype, distinct=y, observedonly=y);
			%n_pct(dset=DIAGNOSIS,      var=DX_TYPE,      qnam=dia_l3_dxtype);
            %n_pct_multilev(dset=DIAGNOSIS, var1=DX_TYPE, var2=ADMIT_DATE_Y, ref1=valuesets, qnam=dia_l3_dxtype_adate_y, var2lbl=ADMIT_DATE, distinct=y);
			%n_pct_noref(dset=DIAGNOSIS, var=DX_DATE_Y,    qnam=dia_l3_dxdate_y, varlbl=DX_DATE, distinct=y, distinctenc=y);
            %n_pct_noref(dset=DIAGNOSIS, var=DX_DATE_YM,   qnam=dia_l3_dxdate_ym, nopct=y, varlbl=DX_DATE);
            %n_pct_multilev(dset=DIAGNOSIS, var1=ENC_TYPE, var2=ADMIT_DATE_YM, ref1=valuesets, qnam=dia_l3_enctype_adate_ym, var2lbl=ADMIT_DATE, distinct=y, distinctenc=y); 
			%n_pct(dset=DIAGNOSIS,      var=DX_ORIGIN,    qnam=dia_l3_origin, distinct=y);
            %n_pct_multilev(dset=DIAGNOSIS, var1=PDX, var2=ENC_TYPE, var3=DX_ORIGIN, ref1=valuesets, ref2=valuesets, ref3=valuesets, qnam=dia_l3_pdx_enctype, distinct=y, distinctenc=y);			       

            /***DIA_L3_PDXGRP_ENCTYPE***/
					
			%let qname=dia_l3_pdxgrp_enctype;
            %elapsed(begin);

            * Derive PDXGRP - One-per-subject flag identifying if the ENCOUNTERID ever had a records with PDX=P;
            data pdxgrp (compress=yes);
                set diagnosis (keep=encounterid pdx);
                by encounterid;
                
                retain pdxgrp;
                
                if first.encounterid then pdxgrp='U';
                if pdx='P' then pdxgrp='P';

                if last.encounterid;

                drop pdx;
            run;

            * Merge flag back onto DIAGNOSIS by ENCOUNTERID;
            data diagnosis (compress=yes);
				length encounterid $&maxlength_enc;
                merge
                    pdxgrp
                    diagnosis
                    ;
                by encounterid;
            run;

            * Create custom reference file for PDXGRP;
            data pdxgrp_valueset;
				table_name='DIAGNOSIS';
				field_name='PDXGRP';

				length valueset_item $1;

                valueset_item_order=1;
				valueset_item='P';
				output;      

                valueset_item_order=2;
				valueset_item='U';
				output;          
			run;  
      
            %n_pct_multilev(dset=DIAGNOSIS, var1=PDXGRP, var2=ENC_TYPE, var3=DX_ORIGIN, ref1=pdxgrp_valueset, ref2=valuesets, ref3=valuesets, qnam=dia_l3_pdxgrp_enctype, distinctenc=y, timer=n);

            * Drop RECORD_N and PDXGRP=NULL or missing rows that come out of macro for this special case;
            data dmlocal.dia_l3_pdxgrp_enctype;
                set dmlocal.dia_l3_pdxgrp_enctype;

                if pdxgrp in ('NULL or missing' 'Values outside of CDM specifications') then delete;

                drop record_n;
            run;                
         
            * Delete intermediary and DIAGNOSIS datasets;
            proc delete data=pdxgrp diagnosis pdxgrp_valueset;
            quit;

            %elapsed(end);

            /***End of DIA_L3_PDXGRP_ENCTYPE***/
                
		%end; * End of Part 2 DIAGNOSIS queries;

********************************************************************************;
* END DIAGNOSIS QUERIES
********************************************************************************;
%end;    

********************************************************************************;
* PROCESS MED_ADMIN QUERIES
********************************************************************************;
%if &_ymed_admin=1 %then %do;
    
		*******************************************************************************;
		* MED_ADMIN - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=MED_ADMIN, var=MEDADMIN_DOSE_ADMIN_UNIT, qnam=medadm_l3_doseadmunit);
			%tag(dset=MED_ADMIN,  varlist=PATID MEDADMINID MEDADMIN_PROVIDERID ENCOUNTERID PRESCRIBINGID MEDADMINID_PLUS, qnam=medadm_l3_n);
			%n_pct(dset=MED_ADMIN, var=MEDADMIN_ROUTE,          qnam=medadm_l3_route);
			%n_pct(dset=MED_ADMIN, var=MEDADMIN_SOURCE,         qnam=medadm_l3_source);
			%n_pct(dset=MED_ADMIN, var=MEDADMIN_TYPE,           qnam=medadm_l3_type);
                  		   
			*******************************************************************************;
			* MED_ADMIN, PRESCRIBING, DIAGNOSIS, and VITAL combined queries
			*******************************************************************************; 
			%if &_yvital=1 and &_ydiagnosis=1 and &_yprescribing and &_ymed_admin=1 %then %do;	
			
				/***XTBL_L3_DASH2***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_dash2;
                %elapsed(begin);

                data med_admin_period (compress=yes);
                    set med_admin (keep=patid medadmin_code medadmin_start_date where=(not missing(medadmin_code) and not missing(medadmin_start_date)));

                    length period $5;
                    
                    * Identify periods for XTBL_L3_DASH#;
                    %dash(datevar=medadmin_start_date);

                    keep patid period;
                run;

                * Only need one record per period per PATID per PERIOD from MED_ADMIN;
				proc sort data=med_admin_period (keep=patid period where=(not missing(period))) nodupkey;
					by patid period;
				run;

                data prescribing_period (compress=yes);
                    set prescribing (keep=patid rxnorm_cui rx_order_date where=(not missing(rxnorm_cui) and not missing(rx_order_date)));

                    length period $5;
                    
                    * Identify periods for XTBL_L3_DASH#;
                    %dash(datevar=rx_order_date);

                    keep patid period;
                run;

                * Only need one record per period per PATID per PERIOD from PRESCRIBING;
				proc sort data=prescribing_period (keep=patid period where=(not missing(period))) nodupkey;
					by patid period;
				run;

                data dash2 (compress=yes);
                    length period $15;
                    merge
                        dash1 (in=in1) /*DASH1 is PATIDs with a record with both vital and face-to-face diagnosis records*/
						med_admin_period (in=inm)
						prescribing_period (in=inp)
						;
					by patid period;
					if in1 and (inm or inp);
                run;

                * Create reference file for use in XTBL_L3_DASH2;
                data dash_ref;
                    set dash_ref;

                    table_name='DASH2';
                run;
		
                %n_pct(dset=DASH2, var=PERIOD, qnam=xtbl_l3_dash2, ref=dash_ref, cdmref=n, timer=n, alphasort=n, distinct=y);

                * Delete RECORD_N and RECORD_PCT columns and NULL or missing row from this query;
                data dmlocal.xtbl_l3_dash2;                
                    set dmlocal.xtbl_l3_dash2;

                    if period='NULL or missing' then delete;

                    drop record_n record_pct;
                run;
             
				* Delete intermediary datasets;
				proc delete data=dash1 med_admin_period prescribing_period;
                quit;

                %elapsed(end);

				/***End of XTBL_L3_DASH2***/
				
			%end;
			/*End of MED_ADMIN query requiring PRESCRIBING, DIAGNOSIS, and VITAL*/

            %if %upcase(&_part2) ne YES %then %do;
                
                * Delete MED_ADMIN dataset if Part 2 is not processed;
                proc delete data=med_admin;
                quit;

            %end;				
            
		%end; * End of Part 1 MED_ADMIN queries;
		
		*******************************************************************************;
		* MED_ADMIN - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
            
            %n_pct_multilev(dset=MED_ADMIN, var1=MEDADMIN_CODE, var2=MEDADMIN_TYPE, ref2=valuesets, qnam=medadm_l3_code_type, distinct=y, observedonly=y);
            %cont(dset=MED_ADMIN,       var=MEDADMIN_DOSE_ADMIN,   qnam=medadm_l3_doseadm, stats=min mean median max n nmiss);			       

            /***MEDADM_L3_RXCUI_TIER***/
					
			%let qname=medadm_l3_rxcui_tier;
            %elapsed(begin);

            * Determine length of MEDADMIN_CODE;            
            proc contents data=rxnorm_cui_ref noprint out=medadm_l3_code_type_length1 (keep=name length where=(name='RXNORM_CUI') rename=(length=length1));
            run;

            proc contents data=dmlocal.medadm_l3_code_type noprint out=medadm_l3_code_type_length2 (keep=name length where=(name='MEDADMIN_CODE') rename=(length=length2));
            run;

            data _null_;
                set medadm_l3_code_type_length1;
                if _n_=1 then set medadm_l3_code_type_length2;

                call symputx('medadmin_code_length',max(length1,length2));
            run;
       
            * Merge MEDADM_L3_CODE_TYPE RX records onto reference file for categorizing into tiers;
            data medadm_l3_code_type_tiers;
                length rxnorm_cui_tier $15 medadmin_code $&medadmin_code_length;
                merge
                    dmlocal.medadm_l3_code_type (in=inm keep=medadmin_code medadmin_type record_n where=(medadmin_type='RX' and medadmin_code ne 'NULL or missing'))
                    rxnorm_cui_ref (keep=rxnorm_cui rxnorm_cui_tier rename=(rxnorm_cui=medadmin_code))
                    ;
                by medadmin_code;
                if inm;

                if rxnorm_cui_tier='' then rxnorm_cui_tier='NULL or missing';

                record_n_n=input(record_n,best12.);

                drop medadmin_type record_n;                
            run;

            proc means data=medadm_l3_code_type_tiers noprint completetypes;
                class rxnorm_cui_tier/preloadfmt;
                var record_n_n;
                output out=medadm_l3_rxcui_tier sum=record_n_n;
                format rxnorm_cui_tier $rxcats.;
            run;

            data medadm_l3_rxcui_tier;
                set medadm_l3_rxcui_tier (where=(_type_=1));
                if _n_=1 then set medadm_l3_rxcui_tier (keep=_type_ record_n_n where=(_type_denom=0) rename=(record_n_n=denom _type_=_type_denom));

                %stdvar;

                length record_n $20 record_pct $6;
                
                if record_n_n ne . then do;
                    record_n=strip(put(record_n_n,12.));
                    record_pct=strip(put(record_n_n/denom*100,6.1));
                end;
                
                ord=input(rxnorm_cui_tier,$rxtier.);

                if record_n_n=. then do;
                    record_n='0';
                    record_pct='0.0';
                end;
                
                drop _freq_ _type_ _type_denom record_n_n denom;
            run;
            
            proc sort data=medadm_l3_rxcui_tier;
                by ord;
            run;        

            * Set final variable order;
            data dmlocal.medadm_l3_rxcui_tier;
                retain datamartid response_date query_package rxnorm_cui_tier record_n record_pct;
                set medadm_l3_rxcui_tier;

                format _all_;

                drop ord;
            run;
                
            * Delete intermediary datasets;
            proc delete data=medadm_l3_code_type_tiers medadm_l3_rxcui_tier medadm_l3_code_type_length1 medadm_l3_code_type_length2;
            quit;
            
            %elapsed(end);

            /***End of MEDADM_L3_RXCUI_TIER***/

			%n_pct_noref(dset=MED_ADMIN, var=MEDADMIN_START_DATE_Y, qnam=medadm_l3_sdate_y, varlbl=MEDADMIN_START_DATE, distinct=y);
			%n_pct_noref(dset=MED_ADMIN, var=MEDADMIN_START_DATE_YM, qnam=medadm_l3_sdate_ym, varlbl=MEDADMIN_START_DATE, nopct=y, distinct=y); 

			*******************************************************************************;
			* MED_ADMIN and ENCOUNTER combined queries
			*******************************************************************************;    
			%if &_yencounter=1 %then %do;			       

				/***XTBL_L3_MEDADM_ENCTYPE***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_medadm_enctype;
				%elapsed(begin);
				
				proc sort data=med_admin (keep=patid encounterid) out=medadm_enctype;
					by encounterid;
				run;
			
				data medadm_enctype (compress=yes);
					length encounterid $&maxlength_enc;
					merge
						medadm_enctype (in=inm)
						encounterid (in=ine keep=encounterid enc_type)
						;
					by encounterid;
					if inm and ine;
                run;
      
				* Create reference file for MEDADM_ENCTYPE based on same versions used for ENCOUNTER summary;
				data medadm_enctype_valuesets;
					set valuesets (where=(table_name='ENCOUNTER'));
					
					table_name='MEDADM_ENCTYPE';
				run;

				%n_pct(dset=MEDADM_ENCTYPE, var=ENC_TYPE, qnam=xtbl_l3_medadm_enctype, ref=medadm_enctype_valuesets, distinct=y, timer=n);

				* Delete intermediary and MED_ADMIN datasets;
				proc delete data=medadm_enctype medadm_enctype_valuesets med_admin;
                quit;

                %elapsed(end);
				
				/***End of XTBL_L3_MEDADM_ENCTYPE***/
				
			%end;
            /*End of MED_ADMIN query requiring ENCOUNTER*/
                
		%end; * End of Part 2 MED_ADMIN queries;

********************************************************************************;
* END MED_ADMIN QUERIES
********************************************************************************;
%end;
            
********************************************************************************;
* PROCESS LAB_RESULT_CM QUERIES
********************************************************************************;
%if &_ylab_result_cm=1 %then %do;
    
		*******************************************************************************;
		* LAB_RESULT_CM - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=LAB_RESULT_CM, var=ABN_IND,           qnam=lab_l3_abn);
			%n_pct(dset=LAB_RESULT_CM, var=NORM_MODIFIER_HIGH, qnam=lab_l3_high);
			%n_pct(dset=LAB_RESULT_CM, var=RESULT_LOC,        qnam=lab_l3_loc);
			%n_pct(dset=LAB_RESULT_CM, var=NORM_MODIFIER_LOW, qnam=lab_l3_low);
			%n_pct(dset=LAB_RESULT_CM, var=LAB_LOINC_SOURCE,  qnam=lab_l3_lsource);
			%n_pct(dset=LAB_RESULT_CM, var=RESULT_MODIFIER,   qnam=lab_l3_mod);
			%tag(dset=LAB_RESULT_CM,  varlist=PATID LAB_RESULT_CM_ID ENCOUNTERID, qnam=lab_l3_n);
			%n_pct(dset=LAB_RESULT_CM, var=PRIORITY,          qnam=lab_l3_priority);
			%n_pct(dset=LAB_RESULT_CM, var=LAB_PX_TYPE,       qnam=lab_l3_px_type);

            /***LAB_L3_QUAL***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=lab_l3_qual;
                %elapsed(begin);

                * Reduce LAB_RESULT_CM_TITER down to one record per unique titer result value;
                proc sort data=lab_result_cm_titer nodupkey;
                    by result_qual;
                run;

                * Determine length of VALUESET_ITEM/RESULT_QUAL;            
                proc contents data=valuesets (where=(table_name='LAB_RESULT_CM' and field_name='RESULT_QUAL')) noprint 
                    out=lab_l3_qual_length1 (keep=name length where=(name='VALUESET_ITEM') rename=(length=length1));
                run;

                proc contents data=lab_result_cm_titer noprint out=lab_l3_qual_length2 (keep=name length where=(name='RESULT_QUAL') rename=(length=length2));
                run;

                data _null_;
                    set lab_l3_qual_length1;
                    if _n_=1 then set lab_l3_qual_length2;

                    call symputx('result_qual_length',max(length1,length2));
                run;

                * Create custom reference file based on VALUESETS that also adds titer results in;
				data result_qual_valuesets;
                    length valueset_item $&result_qual_length;
					set                         
                        lab_result_cm_titer (in=int rename=(result_qual=valueset_item))
                        valuesets (where=(table_name='LAB_RESULT_CM' and field_name='RESULT_QUAL'))
                        ;
					
					if int then do;
                        valueset_item_order=_n_;                    
                        table_name='LAB_RESULT_CM';
                        field_name='RESULT_QUAL';
                    end;
				run; 
           
				* Delete intermediary datasets;
				proc delete data=lab_result_cm_titer lab_l3_qual_length1 lab_l3_qual_length2;
                quit;

			    %n_pct(dset=LAB_RESULT_CM, var=RESULT_QUAL, ref=result_qual_valuesets, qnam=lab_l3_qual, timer=n);
             
				* Delete intermediary datasets;
				proc delete data=result_qual_valuesets;
                quit;

            %elapsed(end);

			/***End of LAB_L3_QUAL***/

			%n_pct(dset=LAB_RESULT_CM, var=LAB_RESULT_SOURCE, qnam=lab_l3_rsource);
			%n_pct(dset=LAB_RESULT_CM, var=SPECIMEN_SOURCE,   qnam=lab_l3_source);
			%n_pct(dset=LAB_RESULT_CM, var=RESULT_UNIT,       qnam=lab_l3_unit);

   			*******************************************************************************;
			* LAB_RESULT_CM, MED_ADMIN, PRESCRIBING, DIAGNOSIS, and VITAL combined queries
			*******************************************************************************; 
			%if &_yvital=1 and &_ydiagnosis=1 and &_yprescribing and &_ymed_admin=1 and &_ylab_result_cm %then %do;	
			
				/***XTBL_L3_DASH3***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_dash3;
                %elapsed(begin);

                data lab_result_cm_period (compress=yes);
                    set lab_result_cm (keep=patid lab_loinc result_date where=(not missing(lab_loinc) and not missing(result_date)));

                    length period $5;
                    
                    * Identify periods for XTBL_L3_DASH#;
                    %dash(datevar=result_date);

                    keep patid period;
                run;

                * Only need one record per period per PATID per PERIOD from LAB_RESULT_CM;
				proc sort data=lab_result_cm_period (keep=patid period where=(not missing(period))) nodupkey;
					by patid period;
                run;
                
                data dash3 (compress=yes);
                    length period $15;
                    merge
                        dash2 (in=in2) /*DASH2 is PATIDs with a record with both vital and face-to-face diagnosis records and either a PRECSRIBING or MED_ADMIN record*/
						lab_result_cm_period (in=inl)
						;
					by patid period;
					if in2 and inl;
                run;

                * Create reference file for use in XTBL_L3_DASH3;
                data dash_ref;
                    set dash_ref;

                    table_name='DASH3';
                run;
		
                %n_pct(dset=DASH3, var=PERIOD, qnam=xtbl_l3_dash3, ref=dash_ref, cdmref=n, timer=n, alphasort=n, distinct=y);

                * Delete RECORD_N and RECORD_PCT columns and NULL or missing row from this query;
                data dmlocal.xtbl_l3_dash3;                
                    set dmlocal.xtbl_l3_dash3;

                    if period='NULL or missing' then delete;

                    drop record_n record_pct;
                run;
             
				* Delete intermediary datasets;
				proc delete data=dash2 dash3 lab_result_cm_period dash_ref;
                quit;

                %elapsed(end);

				/***End of XTBL_L3_DASH3***/
				
			%end;
			/*End of LAB_RESULT_CM query requiring MED_ADMIN, PRESCRIBING, DIAGNOSIS, and VITAL*/  

            %if %upcase(&_part2) ne YES %then %do;
                
                * Delete LAB_RESULT_CM dataset if Part 2 is not processed;
                proc delete data=lab_result_cm;
                quit;

            %end;              
            
		%end; * End of Part 1 LAB_RESULT_CM queries;
		
		*******************************************************************************;
		* LAB_RESULT_CM - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;

            /***LAB_L3_LOINC***/ 
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=lab_l3_loinc;
            %elapsed(begin);

            %n_pct_noref(dset=LAB_RESULT_CM, var=LAB_LOINC, qnam=lab_l3_loinc, distinct=y, timer=n);

            * Determine length of LAB_LOINC; 
            proc contents data=dmlocal.lab_l3_loinc noprint out=lab_l3_loinc_length (keep=name length where=(name='LAB_LOINC') rename=(length=lab_l3_loinc_length));
            run;

            data _null_;
                set loinc_num_length;
                if _n_=1 then set lab_l3_loinc_length;

                call symputx('loinc_num_length',max(loinc_num_length,lab_l3_loinc_length));
            run;

            * Preserve final desired sort order;
            data lab_l3_loinc;
                set dmlocal.lab_l3_loinc;

                sortord=_n_;
            run;

            * Sort by LOINC code for merging with reference file;
            proc sort data=lab_l3_loinc;
                by lab_loinc;
            run;

            data lab_l3_loinc;
                length lab_loinc $&loinc_num_length;
                merge
                    lab_l3_loinc (in=inp)
                    loinc (keep=loinc_num panel_type rename=(loinc_num=lab_loinc))
                    ;
                by lab_loinc;
                if inp;

                format lab_loinc;
                informat lab_loinc;
                label lab_loinc=' ';
            run;

            * Sort by final sort order;
            proc sort data=lab_l3_loinc out=dmlocal.lab_l3_loinc (drop=sortord);
                by sortord;
            run;

            * Delete intermediary dataset;
            proc delete data=loinc lab_l3_loinc lab_l3_loinc_length loinc_num_length;
            quit;

            %elapsed(end);

            /***End of LAB_L3_LOINC***/
            
            %n_pct_multilev(dset=LAB_RESULT_CM, var1=LAB_LOINC, var2=RESULT_UNIT, qnam=lab_l3_loinc_unit, observedonly=y);
            %t_cont(dset=LAB_RESULT_CM, var=RESULT_NUM, qnam=lab_l3_loinc_result_num, byvar=LAB_LOINC, stats=min p1 p5 p25 median p75 p95 p99 max n nmiss);
            %n_pct_multilev(dset=LAB_RESULT_CM, var1=LAB_PX, var2=LAB_PX_TYPE, ref2=valuesets, qnam=lab_l3_px_pxtype, distinct=y, observedonly=y);            			                                          
			%n_pct_noref(dset=LAB_RESULT_CM, var=RESULT_DATE_Y, qnam=lab_l3_rdate_y, varlbl=RESULT_DATE, distinct=y);
			%n_pct_noref(dset=LAB_RESULT_CM, var=RESULT_DATE_YM, qnam=lab_l3_rdate_ym, varlbl=RESULT_DATE, nopct=y, distinct=y);
            %tag(dset=LAB_RESULT_CM, varlist=KNOWN_TEST KNOWN_TEST_RESULT KNOWN_TEST_RESULT_NUM KNOWN_TEST_RESULT_NUM_SOURCE KNOWN_TEST_RESULT_NUM_UNIT KNOWN_TEST_RESULT_NUM_SRCE_UNIT KNOWN_TEST_RESULT_NUM_RANGE KNOWN_TEST_RESULT_PLAUSIBLE,
                qnam=lab_l3_recordc, distinct=n, null=n);
			%n_pct_noref(dset=LAB_RESULT_CM, var=RESULT_SNOMED, qnam=lab_l3_snomed, distinct=y);  
		   
			*******************************************************************************;
			* LAB_RESULT_CM and ENCOUNTER combined queries
			*******************************************************************************;   
			%if &_yencounter=1 %then %do;			                  

				/***XTBL_L3_LAB_ENCTYPE***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_lab_enctype;
				%elapsed(begin);
				
				proc sort data=lab_result_cm (keep=patid encounterid) out=lab_enctype;
					by encounterid;
				run;

				data lab_enctype (compress=yes);				
					length encounterid $&maxlength_enc;
					merge
						lab_enctype (in=inp)
						encounterid (in=ine keep=encounterid enc_type)
						;
					by encounterid;
					if inp and ine;
				run;

				* Create reference file for PRES_ENCTYPE based on same versions used for ENCOUNTER summary;
				data lab_enctype_valuesets;
					set valuesets (where=(table_name='ENCOUNTER'));
					
					table_name='LAB_ENCTYPE';
				run;

				%n_pct(dset=LAB_ENCTYPE, var=ENC_TYPE, qnam=xtbl_l3_lab_enctype, ref=lab_enctype_valuesets, distinct=y, timer=n);

				* Delete intermediary and LAB_RESULT_CM datasets;
				proc delete data=lab_enctype lab_enctype_valuesets lab_result_cm;
                quit;

                %elapsed(end);

				/***End of XTBL_L3_LAB_ENCTYPE***/
				
			%end;
            /*End of LAB_RESULT_CM query requiring ENCOUNTER*/

		%end; * End of Part 2 LAB_RESULT_CM queries;
		
********************************************************************************;
* END LAB_RESULT_CM QUERIES
********************************************************************************;
%end;	

********************************************************************************;
* PROCESS PRESCRIBING QUERIES
********************************************************************************;
%if &_yprescribing=1 %then %do;
    
		*******************************************************************************;
		* PRESCRIBING - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=PRESCRIBING, var=RX_BASIS,              qnam=pres_l3_basis);
			%n_pct(dset=PRESCRIBING, var=RX_DISPENSE_AS_WRITTEN, qnam=pres_l3_dispaswrtn);
			%n_pct(dset=PRESCRIBING, var=RX_FREQUENCY,          qnam=pres_l3_freq);    
			%tag(dset=PRESCRIBING,  varlist=PATID PRESCRIBINGID ENCOUNTERID RX_PROVIDERID PRESCRIBINGID_PLUS, qnam=pres_l3_n);  
			%n_pct(dset=PRESCRIBING, var=RX_PRN_FLAG,           qnam=pres_l3_prnflag);    
			%n_pct(dset=PRESCRIBING, var=RX_ROUTE,              qnam=pres_l3_route);     
			%n_pct(dset=PRESCRIBING, var=RX_DOSE_FORM,          qnam=pres_l3_rxdoseform);      
			%n_pct(dset=PRESCRIBING, var=RX_DOSE_ORDERED_UNIT,  qnam=pres_l3_rxdoseodrunit);      
			%n_pct(dset=PRESCRIBING, var=RX_SOURCE,             qnam=pres_l3_source);  

            %if %upcase(&_part2) ne YES %then %do;
                
                * Delete PRESCRIBING dataset if Part 2 is not processed;
                proc delete data=prescribing;
                quit;     

            %end;
                
		%end; * End of Part 1 PRESCRIBING queries;
		
		*******************************************************************************;
		* PRESCRIBING - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=PRESCRIBING, var=RX_ORDER_DATE_Y, qnam=pres_l3_odate_y, varlbl=RX_ORDER_DATE, distinct=y);
            %n_pct_noref(dset=PRESCRIBING, var=RX_ORDER_DATE_YM, qnam=pres_l3_odate_ym, varlbl=RX_ORDER_DATE, nopct=y, distinct=y);            

            /***PRES_L3_RXCUI***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=pres_l3_rxcui;
            %elapsed(begin);  
     
            %n_pct_noref(dset=PRESCRIBING, var=RXNORM_CUI, qnam=pres_l3_rxcui, distinct=y, timer=n, dec=2);

            * Create RXNORM_CUI_TTY on final file using format;
            data dmlocal.pres_l3_rxcui;
                set dmlocal.pres_l3_rxcui;

                length rxnorm_cui_tty $&rxnorm_cui_tty_l..;

                rxnorm_cui_tty=put(rxnorm_cui,$cui_tty.);              
            run;

            * Reorder columns;       
            data dmlocal.pres_l3_rxcui;
            retain datamartid response_date query_package rxnorm_cui rxnorm_cui_tty record_n record_pct distinct_patid_n;
                set dmlocal.pres_l3_rxcui;
            run;

            %elapsed(end); 
            
            /***End of PRES_L3_RXCUI***/

            %t_cont(dset=PRESCRIBING, var=RX_DAYS_SUPPLY, qnam=pres_l3_rxcui_rxsup, byvar=RXNORM_CUI, stats=min mean max n nmiss, dec=0);

            /***PRES_L3_RXCUI_TIER***/
					
			%let qname=pres_l3_rxcui_tier;
            %elapsed(begin);

            * Sort by RXCUI code for merging with reference file;
            proc sort data=dmlocal.pres_l3_rxcui (keep=rxnorm_cui record_n) out=pres_l3_rxcui;
                by rxnorm_cui;
            run;

            * Merge PRES_L3_CODE_TYPE records onto reference file for categorizing into tiers;
            data pres_l3_code_type_tiers;
                length rxnorm_cui_tier $15;
                merge
                    pres_l3_rxcui (in=inm)
                    rxnorm_cui_ref (keep=rxnorm_cui rxnorm_cui_tier)
                    ;
                by rxnorm_cui;
                if inm;

                if rxnorm_cui_tier='' then rxnorm_cui_tier='NULL or missing';

                record_n_n=input(record_n,best12.);

                drop rxnorm_cui record_n;                
            run;

            proc means data=pres_l3_code_type_tiers completetypes noprint;
                class rxnorm_cui_tier/preloadfmt;
                var record_n_n;
                output out=pres_l3_rxcui_tier sum=record_n_n;
                format rxnorm_cui_tier $rxcats.;
            run;

            data pres_l3_rxcui_tier;
                set pres_l3_rxcui_tier (where=(_type_=1));
                if _n_=1 then set pres_l3_rxcui_tier (keep=_type_ record_n_n where=(_type_denom=0) rename=(record_n_n=denom _type_=_type_denom));

                %stdvar;

                length record_n $20 record_pct $6;

                if record_n_n ne . then do;
                    record_n=strip(put(record_n_n,12.));
                    record_pct=put(record_n_n/denom*100,6.2);
                end;
                
                ord=input(rxnorm_cui_tier,$rxtier.);

                if record_n_n=. then do;
                    record_n='0';
                    record_pct='  0.00';
                end;
                
                drop _freq_ _type_ _type_denom record_n_n denom;
            run;
            
            proc sort data=pres_l3_rxcui_tier;
                by ord;
            run;        

            * Set final variable order;
            data dmlocal.pres_l3_rxcui_tier;
                retain datamartid response_date query_package rxnorm_cui_tier record_n record_pct;
                set pres_l3_rxcui_tier;

                format rxnorm_cui_tier;

                drop ord;
            run;

            * Delete intermediary datasets;
            proc delete data=pres_l3_rxcui pres_l3_code_type_tiers pres_l3_rxcui_tier rxnorm_cui_ref;
            quit;
            
            %elapsed(end);

            /***End of PRES_L3_RXCUI_TIER***/
			
			/***PRES_L3_SUPDIST2***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=pres_l3_supdist2;
			%elapsed(begin);
				
            * Create custom format for categorizing RX_DAYS_SUPPLY;
            proc format;
                value rxdays
                    low-<1 = '<1 day'
                    1-15 = '1-15 days'
                    16-30 = '16-30 days'
                    31-60 = '31-60 days'
                    61-90 = '61-90 days'
                    90<-high = '>90 days'
                    ;
            run;
			
			* Categorization of RX_DAYS_SUPPLY for PRES_L3_SUPDIST2;
			data prescribing_sup (compress=yes);
				set prescribing (keep=rx_days_supply);

				length rx_days_supply_group $10;

                if not missing(rx_days_supply) then rx_days_supply_group=strip(put(rx_days_supply,rxdays.));

                drop rx_days_supply;
			run;    
			
			* Create custom decode list for PRES_L3_SUPDIST2 categories;
			proc format;
				value rx_days
					1 = '<1 day'
					2 = '1-15 days'
					3 = '16-30 days'
					4 = '31-60 days'
					5 = '61-90 days'
					6 = '>90 days'
					;
			run;
			
			data rx_days_supp_group_ref;
				table_name='PRESCRIBING_SUP';
				field_name='RX_DAYS_SUPPLY_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 6; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,rx_days.));
					output;
				end;            
			run;            
			  
			%n_pct(dset=PRESCRIBING_SUP, var=RX_DAYS_SUPPLY_GROUP, qnam=pres_l3_supdist2, ref=rx_days_supp_group_ref, cdmref=n, timer=n, alphasort=n);

			* Delete intermediary datasets;
			proc delete data=prescribing_sup rx_days_supp_group_ref;
            quit;

            %elapsed(end);

			/*** End of PRES_L3_SUPDIST2***/
				
			%cont(dset=PRESCRIBING, var=RX_DOSE_ORDERED, qnam=pres_l3_rxdoseodr_dist, stats=min mean median max n nmiss);
			%cont(dset=PRESCRIBING, var=RX_QUANTITY,    qnam=pres_l3_rxqty_dist, stats=min mean median max n nmiss);
			%cont(dset=PRESCRIBING, var=RX_REFILLS,     qnam=pres_l3_rxrefill_dist, stats=min mean median max n nmiss);
		   
			*******************************************************************************;
			* PRESCRIBING and ENCOUNTER combined queries
			*******************************************************************************; 
			%if &_yencounter=1 %then %do;			

				/***XTBL_L3_PRES_ENCTYPE***/
					
				* Start timer outside of macro in order to capture derivation processing time;
				%let qname=xtbl_l3_pres_enctype;
				%elapsed(begin);
				
				proc sort data=prescribing (keep=patid encounterid) out=pres_enctype;
					by encounterid;
				run;
			
				data pres_enctype (compress=yes);
					length encounterid $&maxlength_enc;
					merge
						pres_enctype (in=inp)
						encounterid (in=ine keep=encounterid enc_type)
						;
					by encounterid;
					if inp and ine;
				run;

				* Create reference file for PRES_ENCTYPE based on same versions used for ENCOUNTER summary;
				data pres_enctype_valuesets;
					set valuesets (where=(table_name='ENCOUNTER'));
					
					table_name='PRES_ENCTYPE';
				run;

				%n_pct(dset=PRES_ENCTYPE, var=ENC_TYPE, qnam=xtbl_l3_pres_enctype, ref=pres_enctype_valuesets, distinct=y, timer=n);

				* Delete intermediary and PRESCRIBING datasets;
				proc delete data=pres_enctype pres_enctype_valuesets prescribing encounterid;
                quit;

                %elapsed(end);
				
				/***End of XTBL_L3_PRES_ENCTYPE***/
				
			%end;
			/*End of PRESCRIBING query requiring ENCOUNTER*/
				
		%end; * End of Part 2 PRESCRIBING queries;

********************************************************************************;
* END PRESCRIBING QUERIES
********************************************************************************;
%end;
            
********************************************************************************;
* PROCESS VITAL QUERIES
********************************************************************************;
%if &_yvital=1 %then %do;
    
		*******************************************************************************;
		* VITAL - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
		
			%n_pct(dset=VITAL, var=BP_POSITION, qnam=vit_l3_BP_position_type);
			%tag(dset=VITAL,  varlist=ENCOUNTERID PATID VITALID, qnam=vit_l3_n);
			%n_pct(dset=VITAL, var=SMOKING,     qnam=vit_l3_smoking, distinct=y, dec=2);
			%n_pct(dset=VITAL, var=TOBACCO,     qnam=vit_l3_tobacco, distinct=y, dec=2);
			%n_pct(dset=VITAL, var=TOBACCO_TYPE, qnam=vit_l3_tobacco_type, dec=2);
			%n_pct(dset=VITAL, var=VITAL_SOURCE, qnam=vit_l3_vital_source, dec=2);

		%end; * End of Part 1 VITAL queries;

		*******************************************************************************;
		* VITAL - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;

			/***VIT_L3_BMI***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=vit_l3_BMI;
			%elapsed(begin);
				
            * Create custom format for categorizing BMI;
            proc format;
                value bmi
                    low-<0 = '<0'
                    0-1 = '0-1'
                    2-5 = '2-5'
                    6-10 = '6-10'
                    11-15 = '11-15'
                    16-20 = '16-20'
                    21-25 = '21-25'
                    26-30 = '26-30'
                    31-35 = '31-35'
                    36-40 = '36-40'
                    41-45 = '41-45'
                    46-50 = '46-50'
                    50<-high = '>50'
                    ;
            run;
			
			* Categorization of BMI for VIT_L3_BMI;
			data vital_bmi (compress=yes);
				set vital (keep=patid original_bmi);

				length bmi_group $5;

                if not missing(original_bmi) then bmi_group=strip(put(round(original_bmi,1),bmi.));

                drop original_bmi;
			run;    
			
			* Create custom decode list for VIT_L3_BMI categories;
			proc format;
				value bmigrp
					1 = '<0'
					2 = '0-1'
					3 = '2-5'
					4 = '6-10'
					5 = '11-15'
					6 = '16-20'
					7 = '21-25'
					8 = '26-30'
					9 = '31-35'
					10 = '36-40'
					11 = '41-45'
					12 = '46-50'
					13 = '>50'
					;
			run;
			
			data bmi_group_ref;
				table_name='VITAL_BMI';
				field_name='BMI_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 13; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,bmigrp.));
					output;
				end;            
			run;  
		
			%n_pct(dset=VITAL_BMI, var=BMI_GROUP, qnam=vit_l3_BMI, ref=bmi_group_ref, cdmref=n, distinct=y, timer=n, alphasort=n, dec=2);

			* Delete intermediary datasets;
			proc delete data=vital_bmi bmi_group_ref;
            quit;

            %elapsed(end);

			/*** End of VIT_L3_BMI***/

			/***VIT_L3_DIASTOLIC***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=vit_l3_diastolic;
			%elapsed(begin);
				
            * Create custom format for categorizing DBP;
            proc format;
                value dbp
                    low-<40 = '<40'
                    40-60 = '40-60'
                    61-75 = '61-75'
                    76-80 = '76-80'
                    81-90 = '81-90'
                    91-100 = '91-100'
                    101-110 = '101-110'
                    111-120 = '111-120'
                    120<-high = '>120'
                    ;
            run;
			
			* Categorization of DBP for VIT_L3_DIASTOLIC;
			data vital_dbp (compress=yes);
				set vital (keep=patid diastolic);

				length diastolic_group $7;

                if not missing(diastolic) then diastolic_group=strip(put(diastolic,dbp.));

                drop diastolic;
			run;    
			
			* Create custom decode list for VIT_L3_DIASTOLIC categories;
			proc format;
				value diagrp
					1 = '<40'
					2 = '40-60'
					3 = '61-75'
					4 = '76-80'
					5 = '81-90'
					6 = '91-100'
					7 = '101-110'
					8 = '111-120'
					9 = '>120'
					;
			run;
			
			data diastolic_group_ref;
				table_name='VITAL_DBP';
				field_name='DIASTOLIC_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 9; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,diagrp.));
					output;
				end;            
			run; 
			
			%n_pct(dset=VITAL_DBP, var=DIASTOLIC_GROUP, qnam=vit_l3_diastolic, ref=diastolic_group_ref, cdmref=n, distinct=y, timer=n, alphasort=n, dec=2);

			* Delete intermediary datasets;
			proc delete data=vital_dbp diastolic_group_ref;
            quit;

            %elapsed(end);

			/*** End of VIT_L3_DIASTOLIC***/

			/***VIT_L3_HT***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=vit_l3_ht;
			%elapsed(begin);
				
            * Create custom format for categorizing HT;
            proc format;
                value ht
                    low-<0 = '<0'
                    0-10 = '0-10'
                    11-20 = '11-20'
                    21-45 = '21-45'
                    46-52 = '46-52'
                    53-58 = '53-58'
                    59-64 = '59-64'
                    65-70 = '65-70'
                    71-76 = '71-76'
                    77-82 = '77-82'
                    83-88 = '83-88'
                    89-94 = '89-94'
                    95-high = '>=95'
                    ;
            run;
			
			* Categorization of SBP for VIT_L3_HT;
			data vital_ht (compress=yes);
				set vital (keep=patid ht);

				length ht_group $7;

                if not missing(ht) then ht_group=strip(put(round(ht,1),ht.));

                drop ht;
			run;    
			
			* Create custom decode list for VIT_L3_HT categories;
			proc format;
				value htgrp
					1 = '<0'
					2 = '0-10'
					3 = '11-20'
					4 = '21-45'
					5 = '46-52'
					6 = '53-58'
					7 = '59-64'
					8 = '65-70'
					9 = '71-76'
					10 = '77-82'
					11 = '83-88'
					12 = '89-94'
					13 = '>=95'
					;
			run;
			
			data ht_group_ref;
				table_name='VITAL_HT';
				field_name='HT_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 13; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,htgrp.));
					output;
				end;            
			run;
				
			%n_pct(dset=VITAL_HT, var=HT_GROUP, qnam=vit_l3_ht, ref=ht_group_ref, cdmref=n, timer=n, alphasort=n, distinct=y);
			
			* Delete intermediary datasets;
			proc delete data=vital_ht ht_group_ref;
            quit;

            %elapsed(end);

			/*** End of VIT_L3_HT***/
				
			%cont(dset=VITAL,       var=HT,             qnam=vit_l3_ht_dist, stats=min mean median max n nmiss);            
			%n_pct_noref(dset=VITAL, var=MEASURE_DATE_Y, qnam=vit_l3_mdate_y, varlbl=MEASURE_DATE, distinct=y);
			%n_pct_noref(dset=VITAL, var=MEASURE_DATE_YM, qnam=vit_l3_mdate_ym, varlbl=MEASURE_DATE, nopct=y, distinct=y);       

			/***VIT_L3_SYSTOLIC***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=vit_l3_systolic;
			%elapsed(begin);
				
            * Create custom format for categorizing SBP;
            proc format;
                value sbp
                    low-<40 = '<40'
                    40-50 = '40-50'
                    51-60 = '51-60'
                    61-70 = '61-70'
                    71-80 = '71-80'
                    81-90 = '81-90'
                    91-100 = '91-100'
                    101-110 = '101-110'
                    111-120 = '111-120'
                    121-130 = '121-130'
                    131-140 = '131-140'
                    141-150 = '141-150'
                    151-160 = '151-160'
                    161-170 = '161-170'
                    171-180 = '171-180'
                    181-190 = '181-190'
                    191-200 = '191-200'
                    201-210 = '101-210'
                    210<-high = '>210'
                    ;
            run;
			
			* Categorization of SBP for VIT_L3_SYSTOLIC;
			data vital_sbp (compress=yes);
				set vital (keep=patid systolic);

				length systolic_group $7;

                if not missing(systolic) then systolic_group=strip(put(systolic,sbp.));

                drop systolic;
			run;    
			
			* Create custom decode list for VIT_L3_SYSTOLIC categories;
			proc format;
				value sysgrp
					1 = '<40'
					2 = '40-50'
					3 = '51-60'
					4 = '61-70'
					5 = '71-80'
					6 = '81-90'
					7 = '91-100'
					8 = '101-110'
					9 = '111-120'
					10 = '121-130'
					11 = '131-140'
					12 = '141-150'
					13 = '151-160'
					14 = '161-170'
					15 = '171-180'
					16 = '181-190'
					17 = '191-200'
					18 = '201-210'
					19 = '>210'
					;
			run;
			
			data systolic_group_ref;
				table_name='VITAL_SBP';
				field_name='SYSTOLIC_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 19; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,sysgrp.));
					output;
				end;            
			run;
				
			%n_pct(dset=VITAL_SBP, var=SYSTOLIC_GROUP, qnam=vit_l3_systolic, ref=systolic_group_ref, cdmref=n, distinct=y, timer=n, alphasort=n, dec=2);
			
			* Delete intermediary datasets;
			proc delete data=vital_sbp systolic_group_ref;
            quit;

            %elapsed(end);

			/*** End of VIT_L3_SYSTOLIC***/

			/***VIT_L3_WT***/
				
			* Start timer outside of macro in order to capture derivation processing time;
			%let qname=vit_l3_wt;
			%elapsed(begin);
				
            * Create custom format for categorizing WT;
            proc format;
                value wt
                    low-<0 = '<0'
                    0-1 = '0-1'
                    2-6 = '2-6'
                    7-12 = '7-12'
                    13-20 = '13-20'
                    21-35 = '21-35'
                    36-50 = '36-50'
                    51-75 = '51-75'
                    76-100 = '76-100'
                    101-125 = '101-125'
                    126-150 = '126-150'
                    151-175 = '151-175'
                    176-200 = '176-200'
                    201-225 = '201-225'
                    226-250 = '226-250'
                    251-275 = '251-275'
                    276-300 = '276-300'
                    301-350 = '301-350'
                    350<-high = '>350'
                    ;
            run;
			
			* Categorization of WT for VIT_L3_WT;
			data vital_wt (compress=yes);
                set vital (keep=patid wt);                

				length wt_group $7;

                if not missing(wt) then wt_group=strip(put(round(wt,1),wt.));

                drop wt;
			run;    
			
			* Create custom decode list for VIT_L3_WT categories;
			proc format;
				value wtgrp
					1 = '<0'
					2 = '0-1'
					3 = '2-6'
					4 = '7-12'
					5 = '13-20'
					6 = '21-35'
					7 = '36-50'
					8 = '51-75'
					9 = '76-100'
					10 = '101-125'
					11 = '126-150'
					12 = '151-175'
					13 = '176-200'
					14 = '201-225'
					15 = '226-250'
					16 = '251-275'
					17 = '276-300'
					18 = '301-350'
					19 = '>350'
					;
			run;
			
			data wt_group_ref;
				table_name='VITAL_WT';
				field_name='WT_GROUP';

				length valueset_item $100;

				do valueset_item_order=1 to 19; * Custom sort to be used in final dataset;
					valueset_item=strip(put(valueset_item_order,wtgrp.));
					output;
				end;            
			run;
				
			%n_pct(dset=VITAL_WT, var=WT_GROUP, qnam=vit_l3_wt, ref=wt_group_ref, cdmref=n, timer=n, alphasort=n, distinct=y);
			
			* Delete intermediary datasets;
			proc delete data=vital_wt wt_group_ref;
            quit;

            %elapsed(end);

			/*** End of VIT_L3_WT***/
				
			%cont(dset=VITAL, var=WT, qnam=vit_l3_wt_dist, stats=min mean median max n nmiss);       

        %end; * End of Part 2 VITAL queries;
        
        * Delete VITAL dataset;
		proc delete data=vital;
        quit;

********************************************************************************;
* END VITAL QUERIES
********************************************************************************;
%end;      
    
********************************************************************************;
* PROCESS DEATH QUERIES
********************************************************************************;
%if &_ydeath=1 %then %do;
    
		*******************************************************************************;
		* DEATH - All Part 1 summaries
		*******************************************************************************;
		%if %upcase(&_part1)=YES %then %do;
			
			%n_pct(dset=DEATH, var=DEATH_DATE_IMPUTE,     qnam=death_l3_impute);
			%n_pct(dset=DEATH, var=DEATH_MATCH_CONFIDENCE, qnam=death_l3_match);
			%tag(dset=DEATH,  varlist=PATID DEATHID,     qnam=death_l3_n);

            /***DEATH_L3_SOURCE_YM***/
				
			* Start timer outside of macro, because it is the last query processed (if Part 2 is not requested), and so must use the last=Y parameter;
			%let qname=death_l3_source;
            %elapsed(begin);
            
			%n_pct(dset=DEATH, var=DEATH_SOURCE, qnam=death_l3_source, timer=n); 

            %elapsed(b_or_e=end
                %if %upcase(&_part2) ne YES %then %do;
                    , last=Y
                %end;
                );   

		%end; * End of Part 1 DEATH queries;

		*******************************************************************************;
		* DEATH - All Part 2 summaries
		*******************************************************************************;
		%if %upcase(&_part2)=YES %then %do;
		
			%n_pct_noref(dset=DEATH, var=DEATH_DATE_Y, qnam=death_l3_date_y, varlbl=DEATH_DATE, distinct=y);
            %n_pct_noref(dset=DEATH, var=DEATH_DATE_YM, qnam=death_l3_date_ym, varlbl=DEATH_DATE, nopct=y, distinct=y);

            /***DEATH_L3_SOURCE_YM***/
				
			* Start timer outside of macro, because it is the last query processed (if Part 2 is requested), and so must use the last=Y parameter;
			%let qname=death_l3_source_ym;
			%elapsed(begin);
            
            %n_pct_multilev(dset=DEATH, var1=DEATH_SOURCE, var2=DEATH_DATE_YM, ref1=valuesets, qnam=death_l3_source_ym, var2lbl=DEATH_DATE, distinct=y, timer=n);

            %elapsed(b_or_e=end, last=Y);
            
		%end; * End of Part 2 DEATH queries;

        * Delete DEATH dataset;
		proc delete data=death;
        quit;

********************************************************************************;
* END DEATH QUERIES
********************************************************************************;
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
