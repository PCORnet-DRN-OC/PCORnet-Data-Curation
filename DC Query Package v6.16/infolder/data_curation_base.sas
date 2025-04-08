/*******************************************************************************
*  $Source: data_curation_base $;
*    $Date: 2025/02/28
*    Study: PCORnet
*
*  Purpose: Produce PCORnet Data Curation Query Package V6.16 - 
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
options validvarname=upcase errors=0 nomprint missing='' minoperator;
ods html close;

********************************************************************************;

********************************************************************************;
*- Flush anything/everything that might be in WORK
*******************************************************************************;
proc datasets kill noprint;
quit;

********************************************************************************;
*- Set LIBNAMES for data and output
*******************************************************************************;
libname pcordata "&dpath" access=readonly;
libname drnoc "&qpath.drnoc";
libname dmlocal "&qpath.dmlocal";
filename dcref "&qpath./infolder/dc_reference.cpt";
filename loincref "&qpath./infolder/loinc.cpt";
libname cptdata cvp "&qpath.infolder";

********************************************************************************;
*- Set version number
********************************************************************************;
%let dc = DC V6.16;

*- Create macro variable to determine lookback cutoff -*;
data _null_;
     today="&sysdate"d;
     lookback=mdy(month(today),day(today),year(today)-&lookback);
     call symputx('lookback_dt',put(lookback,8.));

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

*- Read log file and create SAS dataset records to be in XTBL_L3_METADATA;
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
* Create an external DRNOC log file
*******************************************************************************;
filename qlog "&qpath.drnoc/&dmid._&tday._data_curation_base.log" lrecl=200;
proc printto log=qlog new;
run;

********************************************************************************;
* Create standard macros used in multiple queries
********************************************************************************;

*- Macro to create DATAMARTID, RESPONSE_DATE and QUERY_PACKAGE in every query;
%macro stdvar;
     length datamartid $20 query_package $8 response_date 4.;
     format response_date date9.;
     datamartid="&dmid";
     response_date="&sysdate"d;
     query_package="&dc";
%mend stdvar;

*- Macro for vertical continuous statistics;
%macro cont(dset=,var=,qnam=,timer=,stats=,dec=1);

    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;

    * Reset count of observations in input dataset for each macro run;
    %let num_obs = 0;

    * Get number of records in input dataset;
    data _null_;
        set &dset end=eof;

        if eof then call symputx('num_obs',_n_);
    run;

    * Create dummy data if input dataset is empty;
    %if &num_obs=0 %then %do;

        data dummy;
            &var=0;
        run;
        
    %end;

    * Use dummy dataset if input dataset is empty;
    proc means data=        
        %if &num_obs=0 %then %do; 
            dummy
        %end;
        %else %do;
            &dset
        %end;
        noprint;
        var &var;
        output out=&qnam (drop=_: keep=&stats) min=min p5=p5 mean=mean median=median p95=p95 max=max n=n nmiss=nmiss;
    run;

    proc transpose data=&qnam out=t_&qnam (rename=(_name_=stat));        
    run;

    data t_&qnam;
        length stat $15 record_n $20;
        set t_&qnam;

        %stdvar;

        if stat='NMISS' then stat='NULL or missing';

        record_n=strip(put(col1,20.&dec));

        * Blank out values if input dataset is empty;
        %if &num_obs=0 %then %do;

            record_n='';                

        %end;
            
        label stat=' ';

        drop col1;
    run;

    data dmlocal.&qnam;
        retain datamartid response_date query_package stat record_n;
        set t_&qnam;
    run;

    * Delete intermediary datasets;
    proc delete data=&qnam t_&qnam
            %if &num_obs=0 %then %do;
                dummy
            %end;
            ;
    quit; 

    %elapsed(end);       

%mend cont;

*- Macro for horizontal continuous statistics;
%macro t_cont(dset=,var=,byvar=,qnam=,timer=,stats=,dec=1,subset=,local=y,invalid=,impl=,future_dt=,pre2010=);

    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;

    * Keep only needed variables;
    data &dset._t_cont (compress=yes);
        set &dset (keep=&var &byvar);
        
        %if %upcase(&invalid)=Y %then %do;
            %if %index(%upcase(&qnam),TIMES)>0 %then %do;
                if not missing(&var) and (&var<0 or &var>86400) then invalid_&var=1;
            %end;
            %else %if %index(%upcase(&qnam),DATES)>0 %then %do;
                if not missing(&var) and (&var<-138061 or &var>6589335) then invalid_&var=1;
            %end;
        %end;

        %if %upcase(&impl)=Y %then %do;
            if not missing(&var) and &var<'01JAN1900'd then impl_&var=1;
        %end;

        %if %upcase(&future_dt)=Y %then %do;
            if not missing(&var) and &mxrefreshn<&var then future_dt_&var=1;
        %end;

        %if %upcase(&pre2010)=Y %then %do;
            if not missing(&var) and &var<'01JAN2010'd then pre2010_&var=1;
        %end;
    run;

    * Reset count of observations in input dataset for each macro run;
    %let num_obs = 0;

    * Get number of records in input dataset;
    data _null_;
        set &dset._t_cont            
            %if &subset ne %then %do;
                (where=(&subset=1))
            %end;
            end=eof;

        if eof then call symputx('num_obs',_n_);
    run;

    * Check to see if all &byvar values are missing;
    proc sql noprint;
        select count(*) into :num_byvar_values from &dset._t_cont
            %if &byvar ne %then %do;
                where not missing(&byvar)
            %end;
        ;
    quit;

    * Create dummy data if input dataset is empty or all &byvar values are missing;
    %if &num_obs=0 or &num_byvar_values=0 %then %do;

        data dummy;
			%if &byvar ne %then %do;
				&byvar='fake';
			%end;
            &var=0;
            %if %upcase(&invalid)=Y %then %do;
                invalid_&var=0;
            %end;
            %if %upcase(&impl)=Y %then %do;
                impl_&var=0;
            %end;
            %if %upcase(&future_dt)=Y %then %do;
                future_dt_&var=0;
            %end;
            %if %upcase(&pre2010)=Y %then %do;
                pre2010_&var=0;
            %end;
            %if &subset ne %then %do;
                &subset=1;
            %end;
        run;
        
    %end;

    * Get counts. Use dummy dataset if input dataset is empty or all &byvar values are missing;
    proc means data=        
        %if &num_obs=0 or &num_byvar_values=0 %then %do; 
            dummy
        %end;               
        %else %do;
            &dset._t_cont
        %end;
        noprint missing;
        %if &subset ne %then %do;
            where &subset=1;
        %end;
        %if &byvar ne %then %do;
            class &byvar;
        %end;
        var &var
            %if %upcase(&invalid)=Y %then %do;
                invalid_&var
            %end;
            %if %upcase(&impl)=Y %then %do;
                impl_&var
            %end;
            %if %upcase(&future_dt)=Y %then %do;
                future_dt_&var
            %end;
            %if %upcase(&pre2010)=Y %then %do;
                pre2010_&var
            %end;
            ;
        output out=&qnam 
            %if &byvar ne %then %do;
                (where=(_type_=1))
            %end;
            min=minn
            p1=p1n
            p5=p5n
            p25=p25n
            median=mediann
            p75=p75n
            p95=p95n
            p99=p99n
            mean=meann
            max=maxn
            
            n=nn
                %if %upcase(&invalid)=Y %then %do;
                    ninv
                %end;
                %if %upcase(&impl)=Y %then %do;
                    nimpl
                %end;
                %if %upcase(&future_dt)=Y %then %do;
                    nfut
                %end;
                %if %upcase(&pre2010)=Y %then %do;
                    npre
                %end;
                
            nmiss=nmissn
            ;
    run;

    * If null values present in &byvar, set ord value to force to bottom;
    %if &byvar ne %then %do;
        
        data &qnam;
            set &qnam;

            if &byvar='' then ord=1;
        run;
    
        * Template dataset to force NULL or missing row in if there are no missing &byvar values;
        data misstemp;
            ord=1;
        run;

        proc sort data=&qnam;
            by ord;
        run;
        
    %end;

    data &qnam;
        length min p1 p5 p25 median p75 p95 p99 max n nmiss
            %if %upcase(&invalid)=Y %then %do;
                invalid_n
            %end;
            %if %upcase(&impl)=Y %then %do;
                impl_n
            %end;
            %if %upcase(&future_dt)=Y %then %do;
                future_dt_n
            %end;
            %if %upcase(&pre2010)=Y %then %do;
                pre2010_n
            %end;
            $16
            %if &byvar ne %then %do;
                &byvar $25
            %end;
            ;
        /*If there is no by-variable (as in XTBL_L3_DATES and XTBL_L3_TIMES), no need to force a NULL or missing row for it*/ 
        %if &byvar ne %then %do;
            merge
                &qnam                
                misstemp
                ;
            by ord;
        %end;
        %else %do;
            set &qnam;                    
        %end;
        
        %stdvar;

        * Create DATASET column for time and date queries;
        %if %index(%upcase(&qnam),TIMES)>0 or %index(%upcase(&qnam),DATES)>0 %then %do;
            length dataset $25;
            dataset="&dset";
        %end;
	
		* Only need NULL or missing row for	summaries with by groups;
        %if &byvar ne %then %do;
            if &byvar='' then &byvar='NULL or missing';
        %end;
		/* Otherwise assign TAG as summary variable name*/
		%else %do;
			length tag $25;
			
			tag="&var";
		%end;

        array nm (*) minn p1n p5n p25n mediann p75n p95n p99n meann maxn;
        array ch (*) min p1 p5 p25 median p75 p95 p99 mean max;

        do i=1 to dim(nm);
            %if %index(%upcase(&qnam),TIMES)>0 %then %do; /*Format times as HH:MM*/
                ch(i)=put(nm(i),time5.);
            %end;
            %else %if %index(%upcase(&qnam),DATES)>0 %then %do; /*Format dates as YYYYMM*/
                ch(i)=put(year(nm(i)),4.) || '_' || put(month(nm(i)),z2.);                
            %end;
            %else %do;
                ch(i)=strip(put(nm(i),20.&dec));
            %end;
        end;

        * Always present Ns as integers;
        n=strip(put(nn,20.));
        nmiss=strip(put(nmissn,20.));

        %if %upcase(&invalid)=Y %then %do;
            invalid_n=strip(put(ninv,20.));
        %end;

        %if %upcase(&impl)=Y %then %do;
            impl_n=strip(put(nimpl,20.));
        %end;

        %if %upcase(&future_dt)=Y %then %do;
            future_dt_n=strip(put(nfut,20.));
        %end;

        %if %upcase(&pre2010)=Y %then %do;
            pre2010_n=strip(put(npre,20.));
        %end;

        *If input dataset is empty or all &byvar values are missing, query dataset will be empty;
        %if &num_obs=0 or &num_byvar_values=0 %then %do;
            delete;
        %end;

        keep datamartid response_date query_package &byvar &stats
            %if %upcase(&invalid)=Y %then %do;
                invalid_n
            %end; 
            %if %upcase(&impl)=Y %then %do;
                impl_n
            %end; 
            %if %upcase(&future_dt)=Y %then %do;
                future_dt_n
            %end;
            %if %upcase(&pre2010)=Y %then %do;
                pre2010_n
            %end;
            %if %index(%upcase(&qnam),TIMES)>0 or %index(%upcase(&qnam),DATES)>0 %then %do;
                dataset
            %end;
            %if &byvar ne %then %do;
                ord
            %end;
            %if &byvar= %then %do;
                tag
            %end;
            ;
    run;

    data
        %if %upcase(&local)=Y %then %do;
            dmlocal.&qnam
        %end;
        %else %do;
            &qnam
        %end;
        ;
            
        retain datamartid response_date query_package 
            %if %index(%upcase(&qnam),TIMES)>0 or %index(%upcase(&qnam),DATES)>0 %then %do;
                dataset
            %end;
            &byvar 
			%if &byvar= %then %do;
                tag
            %end;
			&stats
            %if %upcase(&invalid)=Y %then %do;
                invalid_n
            %end;
            %if %upcase(&impl)=Y %then %do;
                impl_n
            %end;
            %if %upcase(&future_dt)=Y %then %do;
                future_dt_n
            %end;
            %if %upcase(&pre2010)=Y %then %do;
                pre2010_n
            %end;
            ;
        set &qnam;

        * Special handling of PRES_L3_RXCUI_RXSUP and LAB_L3_LOINC_RESULT_NUM, which do not need NULL or missing rows;
        %if %upcase(&qnam)=PRES_L3_RXCUI_RXSUP or %upcase(&qnam)=LAB_L3_LOINC_RESULT_NUM %then %do;
            if &byvar='NULL or missing' then delete;
        %end;

        %if &byvar ne %then %do;
            drop ord;
        %end;
    run;

    * Delete intermediary datasets;
    proc delete data=&dset._t_cont
            %if %upcase(&local)=Y %then %do;
                &qnam
            %end;
            %if &num_obs=0 or &num_byvar_values=0 %then %do;
                dummy
            %end;
            %if &byvar ne %then %do;
                misstemp
            %end;
            ;
    quit;

    %if &timer= %then %do;
        %elapsed(end);
    %end;

%mend t_cont; 

*- Macro for ALL_N/DISTINCT_N/NULL_N/VALID_N;
%macro tag(dset=,varlist=,qnam=,timer=,subset=,validall=,valid1=,valid2=,distinct=y,distvalid=,null=y);

    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;

    * Process counts if input dataset is not empty;
    %if &&nobs_&dset>0 %then %do;
    
        %let numvars = %sysfunc(countw(&varlist));

        %do i=1 %to &numvars;

            %let var=%scan(&varlist,&i,' ');

            proc sort data=&dset (keep=&var
                %if &subset ne %then %do;
                    &subset
                %end;
                /* Also keep corresponding _VALID flag if variable is indicated as bein the column with a _VALID check;*/
                %if &valid1=&i or &valid2=&i or %upcase(&validall)=Y %then %do; 
                    valid_&var
                %end;
                ) out=invar_sorted;
                by &var               
                    %if %upcase(&distvalid)=Y %then %do; /*Also sort by VALID_ version of variable if VALID_DISTINCT_N is needed*/
                        valid_&var
                    %end;
                    ;
            run;

            %if &subset ne %then %do;
            
                data invar_sorted;
                    set invar_sorted;

                    if &subset=1;

                    drop &subset;
                run;
                
            %end;

            data invar_sorted;
                set invar_sorted end=eof;
                by &var;

                length dataset tag $35 all_n
                    %if %upcase(&distinct)=Y %then %do;
                        distinct_n
                    %end;
                    %if %upcase(&null)=Y %then %do;
                        null_n
                    %end;
                    %if &valid1 ne or %upcase(&validall)=Y %then %do;
                        valid_n
                    %end;
                    %if %upcase(&distvalid)=Y %then %do;
                        valid_distinct_n
                    %end;
                    $20;

                dataset="&dset";
                tag="&var";

                if _n_=1 then do;
                    all_n_n=0;
                    
                    %if %upcase(&distinct)=Y %then %do;
                        distinct_n_n=0;
                    %end;
                    %if %upcase(&null)=Y %then %do;
                        null_n_n=0;
                    %end;

                    %if &valid1 ne or %upcase(&validall)=Y %then %do;
                        valid_n_n=0;
                    %end;
                    
                    %if %upcase(&distvalid)=Y %then %do;
                        valid_distinct_n_n=0;
                    %end;
                end;

                if not missing(&var) then all_n_n+1;

                %if %upcase(&distinct)=Y %then %do;
                    if first.&var and not missing(&var) then distinct_n_n+1;                
                %end;
                
                %if %upcase(&null)=Y %then %do;
                    if missing(&var) then null_n_n+1;
                %end;

                * Only find counts if the variable of interest is indicated as the one with the corresponding _VALID flag;
                %if &valid1=&i or &valid2=&i or %upcase(&validall)=Y %then %do;
                    if not missing(valid_&var) then valid_n_n+1;
                %end;

                %if %upcase(&distvalid)=Y %then %do;
                    if last.&var and not missing(&var) and valid_&var='Y' then valid_distinct_n_n+1;                
                %end;

                if eof then do;
                    all_n=strip(put(all_n_n,best20.));

                    %if %upcase(&distinct)=Y %then %do;
                        distinct_n=strip(put(distinct_n_n,best20.));
                    %end;

                    %if %upcase(&null)=Y %then %do;
                        null_n=strip(put(null_n_n,best20.));
                    %end;

                    * Only fill in counts if the variable of interest is indicated as the one with the corresponding _VALID flag;
                    %if &valid1=&i or &valid2=&i or %upcase(&validall)=Y %then %do;
                        valid_n=strip(put(valid_n_n,best20.));
                    %end;
                    %else %if &valid1 ne or %upcase(&validall)=Y %then %do; * Otherwise set as n/a for queries that include the VALID_N column;
                        valid_n='n/a';
                    %end;

                    %if %upcase(&distvalid)=Y %then %do;
                        valid_distinct_n=strip(put(valid_distinct_n_n,best20.));
                    %end;
                
                    output;
                end;
            
                drop &var all_n_n                     
                    %if %upcase(&distinct)=Y %then %do;
                        distinct_n_n
                    %end;
                    %if %upcase(&null)=Y %then %do;
                        null_n_n
                    %end;
                    %if &valid1 ne or %upcase(&validall)=Y %then %do;
                        valid_n_n
                    %end;
                    %if &valid1=&i or &valid2=&i or %upcase(&validall)=Y %then %do;
                        valid_&var
                    %end;                    
                    %if %upcase(&distvalid)=Y %then %do;
                        valid_distinct_n_n
                    %end;
                ;
            run;

            proc datasets;
                append base=&qnam data=invar_sorted;
            quit;

            * Delete intermediary datasets;
            proc delete data=invar_sorted;
            quit;
        
        %end;

    %end;
    %else %do; * Create dummy all-zero dataset if input dataset is empty;

        * Insert commas between variable names;
        %let varlist_str = %sysfunc(tranwrd(&varlist,%str( ),%str(,)));

        data _null_;
            length varlist_str $450;
            varlist_str='"' || tranwrd("&varlist_str",',','","') || '"';

            * Create macro value list of variables to be included;
            call symput('varlist_str',varlist_str);
        run;

        * Output dummy row for each variable;
        data &qnam;                        
            length dataset tag $35 all_n                     
                %if %upcase(&distinct)=Y %then %do;
                    distinct_n
                %end;
                %if %upcase(&null)=Y %then %do;
                    null_n
                %end;
                %if &valid1 ne or %upcase(&validall)=Y %then %do;
                    valid_n
                %end;                    
                %if %upcase(&distvalid)=Y %then %do;
                    valid_distinct_n
                %end;
                $20;                

            dataset="&dset";

            all_n='0';

            %if %upcase(&distinct)=Y %then %do;
                distinct_n='0';
            %end;
                
            %if %upcase(&null)=Y %then %do;                    
                null_n='0';
            %end;                    

            %if &valid1 ne or %upcase(&validall)=Y %then %do;
                valid_n='0';
            %end;

            %if %upcase(&distvalid)=Y %then %do;
                valid_distinct_n='0';
            %end;
            
            do tag=&varlist_str;
                output;
            end;
        run;
        
    %end;

    data dmlocal.&qnam;
        retain datamartid response_date query_package dataset tag all_n                   
                %if %upcase(&distinct)=Y %then %do;
                    distinct_n
                %end;
                %if %upcase(&null)=Y %then %do;
                    null_n
                %end;
                %if &valid1 ne or %upcase(&validall)=Y %then %do;
                    valid_n
                %end;                   
                %if %upcase(&distvalid)=Y %then %do;
                    valid_distinct_n
                %end;
                ;
        set &qnam;

        %stdvar;
    run;

    * Delete intermediary datasets;
    proc delete data=&qnam;
    quit;  

    %if &timer= %then %do;
        %elapsed(end);
    %end;
    
%mend tag;    

*- Macro for RECORD_N/RECORD_PCT queries (CAT (N & %)) that have reference files -*;
%macro n_pct(dset=,var=,qnam=,subset=,ref=valuesets,cdmref=y,timer=,alphasort=y,dec=1,distinct=,distinctvis=,elig=);

    /*** Note: Because of text-string-matching to the VALUESET code lists, any dataset or variable names (DSET/VAR) must be in all caps when the macro is
        called if it uses VALUESET (which is the default code list unless specified otherwise with the REF parameter);***/
    
    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;      
    
    * Bring in CDM value set for variable;
    data &var._vals;
        length valueset_item label $200;
        set &ref (where=(table_name="&dset" and field_name="&var")) end=eof;

        hlo='';
        fmtname='$' || "&var";

        * Add ordering to front of value for sorting purposes;
        label=strip(put(valueset_item_order,12.)) || '@' || strip(valueset_item);        
        
        * Add NULL or missing and Value outside of CDM specifications into value set for super-population of all categories;
        output;
        if eof then do;
            label='88888@NULL or missing';
            valueset_item='';
            output;

            label='99999@Values outside of CDM specifications';
            valueset_item='other';
            hlo='o';
            output;
        end;

        rename valueset_item=start;

        keep fmtname valueset_item label hlo;
    run;

    * Create format;
    proc format library=work cntlin=&var._vals;
    run; 

    * Determine maximum length of all possible values of summary variable;
    data _null_;
        set &ref (where=(table_name="&dset" and field_name="&var")) end=eof;

        retain varlength varlength_final;

        * Set working length to a minimum of $43 to account for 99999@Values outside of CDM specifications;
        if _n_=1 then varlength=43;

        if length(valueset_item)>varlength then varlength=length(valueset_item);
        
        * Set final length to a minimum of $36 to account for Values outside of CDM specifications (with no order attached) if using a CDM-specified value set;
        * Set final length to a minimum of $15 to account for NULL or missing (with no order attached) if using a custom value set (hence no CDM-specified values);
        %if &cdmref=y %then %do;
            if _n_=1 then varlength_final=36;
        %end;
        %else %do;       
            if _n_=1 then varlength_final=15;
        %end;

        if length(valueset_item)>varlength_final then varlength_final=length(valueset_item);

        * Create macro variable containing the lengths;
        if eof then do;
            call symputx('length',varlength);
            call symputx('length_final',varlength_final);
        end;
    run;

    * Reset count of observations in input dataset for each macro run;
    %let subset_obs = 0;

    * Keep only summary variable on input dataset;
    data &dset._subset;        
        set &dset (keep=&var rename=(&var=&var._old)) end=eof;
		
		length &var $&length;
		
		&var=&var._old;

        if eof then call symputx('subset_obs',_n_);
		
        drop &var._old;
    run;

    * Only do the proc means if input dataset is not empty;
    %if &subset_obs>0 %then %do;

        * Get counts;
        proc means data=&dset._subset completetypes noprint missing;
            class &var/preloadfmt;
            output out=stats;
            format &var $&var..;
        run;

        * If any extra counts requested, cut down on any extra length on summary variable;
        %if %upcase(&distinct)=Y or %upcase(&elig)=Y %then %do;

            * Cut down on any extra length on summary variable;
            data &dset._subset (compress=yes);
                set &dset (keep=&var
                    %if %upcase(&distinct)=Y %then %do;
                        patid
                    %end;
                    %if %upcase(&distinctvis)=Y %then %do;
                        unique_visit
                    %end;
                    %if %upcase(&elig)=Y %then %do;
                        elig_record 
                    %end;
                    rename=(&var=&var._old));
				
                length &var $&length_final;
					
				&var=&var._old;
				
				drop &var._old;				
            run;

        %end;
            
        * Count distinct PATIDs within group;
        %if %upcase(&distinct)=Y %then %do;
            
            * Reduce input data down to one record per summary variable type per PATID;
            proc sort data=&dset._subset (keep=patid &var) out=&qnam._oneper (drop=patid) nodupkey;
                by &var patid;
            run;
    
            * Get counts by summary variable;
            proc means data=&qnam._oneper noprint missing;
                class &var;
                output out=stats_oneper;
            run;

            data stats;
                merge
                    stats
                    stats_oneper (keep=&var _type_ _freq_ where=(_type_patid=1) rename=(_type_=_type_patid _freq_=_freq_patid))
                    ;
                by &var;

                drop _type_patid;
            run;

        %end; 

        * Count distinct visits within group;
        %if %upcase(&distinctvis)=Y %then %do;
            
            * Reduce input data down to one record per summary variable type per PATID/ADMIT_DATE/PROVIDERID;
            proc sort data=&dset._subset (keep=unique_visit &var where=(unique_visit ne '')) out=&qnam._onepervis (drop=unique_visit) nodupkey;
                by unique_visit;
            run;
    
            * Get counts by summary variable;
            proc means data=&qnam._onepervis noprint missing;
                class &var;
                output out=stats_onepervis;
            run;

            data stats;
                merge
                    stats
                    stats_onepervis (keep=&var _type_ _freq_ where=(_type_visit=1) rename=(_type_=_type_visit _freq_=_freq_visit))
                    ;
                by &var;

                drop _type_visit;
            run;

        %end; 

        * Count eligible records within group;
        %if %upcase(&elig)=Y %then %do;
            
            * Reduce input data down to one record per summary variable type per PATID/ADMIT_DATE/PROVIDERID;
            proc sort data=&dset._subset (keep=elig_record &var where=(elig_record ne '')) out=&qnam._elig (drop=elig_record) nodupkey;
                by elig_record;
            run;
    
            * Get counts by summary variable;
            proc means data=&qnam._elig noprint missing;
                class &var;
                output out=stats_elig;
            run;

            data stats;
                merge
                    stats
                    stats_elig (keep=&var _type_ _freq_ where=(_type_elig=1) rename=(_type_=_type_elig _freq_=_freq_elig))
                    ;
                by &var;

                drop _type_elig;
            run;

        %end;  
    
        * Create final variables and sort order;
        data &qnam;        
            set stats (where=(_type_=1));
            if _n_=1 then set stats (keep=_type_ _freq_ 
                    %if %upcase(&distinct)=Y %then %do;
                        _freq_patid
                    %end;
                    %if %upcase(&distinctvis)=Y %then %do;
                        _freq_visit
                    %end;
                    %if %upcase(&elig)=Y %then %do;
                        _freq_elig
                    %end;
                where=(_type_=0) rename=(_freq_=denom));

            * Sort alphabetically by default, use custom sort (set in custom reference file) if otherwise specified with empty alphasort parameter;
            %if %sysfunc(upcase(&alphasort))=Y %then %do;
                if scan(put(&var,$&var..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord=input(scan(put(&var,$&var..),1,'@'),12.);
            %end;
            %else %do;
                ord=input(scan(put(&var,$&var..),1,'@'),12.);
            %end;

            %stdvar;

            length record_n $20 record_pct $6;
                
            record_n=strip(put(_freq_,12.));
            record_pct=put(_freq_/denom*100,6.&dec);

            %if %upcase(&distinct)=Y %then %do;
                * Fill in zero for missing category if needed;
                if _freq_patid=. then _freq_patid=0;
                
                length distinct_patid_n $20;
                
                distinct_patid_n=strip(put(_freq_patid,12.));

                drop _freq_patid;
            %end;

            %if %upcase(&distinctvis)=Y %then %do;
                * Fill in zero for missing category if needed;
                if _freq_visit=. then _freq_visit=0;
                
                length distinct_visit_n $20;
                
                distinct_visit_n=strip(put(_freq_visit,12.));

                drop _freq_visit;
            %end;

            %if %upcase(&elig)=Y %then %do;
                * Fill in zero for missing category if needed;
                if _freq_elig=. then _freq_elig=0;
                
                length elig_record_n $20;
                
                elig_record_n=strip(put(_freq_elig,12.));

                drop _freq_elig;
            %end;

            * Strip ordering info off of summary values;
            &var=scan(put(&var,$&var..),2,'@');

            * Only output this row for CDM-specified value sets (delete for custom value sets);
            %if &cdmref ne y %then %do;
                if &var='Values outside of CDM specifications' then delete;
            %end;

            format &var;

            drop _type_ _freq_ denom;
        run;

        * Delete intermediary datasets;
        proc delete data=stats
                %if %upcase(&distinct)=Y %then %do;
                    &qnam._oneper stats_oneper
                %end;
                %if %upcase(&distinctvis)=Y %then %do;
                    &qnam._onepervis stats_onepervis
                %end;
                %if %upcase(&elig)=Y %then %do;
                    &qnam._elig stats_elig
                %end;
                ;
        quit;

    %end;
    %else %do; /*Create dummy all-zero dataset if input dataset is empty*/

        data &qnam;
            set &var._vals (keep=label);

            length &var $&length_final;

            * Sort alphabetically by default, use custom sort (set in custom reference file) if otherwise specified with empty alphasort parameter;
            %if %sysfunc(upcase(&alphasort))=Y %then %do;
                if scan(label,2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord=input(scan(label,1,'@'),12.);
            %end;
            %else %do;
                ord=input(scan(label,1,'@'),12.);
            %end;

            %stdvar;

            length record_n $20 record_pct $6;
                
            record_n='0';
            record_pct='0.0';

            %if %upcase(&distinct)=Y %then %do;
                length distinct_patid_n $20;
                
                distinct_patid_n='0';
            %end;
            
            %if %upcase(&distinctvis)=Y %then %do;
                length distinct_visit_n $20;
                
                distinct_visit_n='0';
            %end;
            
            %if %upcase(&elig)=Y %then %do;
                length elig_record_n $20;
                
                elig_record_n='0';
            %end;

            * Strip ordering info off of summary values;
            &var=scan(label,2,'@');
        
            * Only output this row for CDM-specified value sets (delete for custom value sets);
            %if &cdmref ne y %then %do;
                if &var='Values outside of CDM specifications' then delete;
            %end;

            drop label;
        run;            

    %end;        

    * Reset final summary variable length;
    data &qnam;
        set &qnam (rename=(&var=&var._old));
		
        length &var $&length_final;
		
		&var=&var._old;
		
		drop &var._old;
    run;        

    data &qnam;
        retain datamartid response_date query_package &var record_n record_pct
            %if %upcase(&distinctvis)=Y %then %do;
                distinct_visit_n
            %end;
            %if %upcase(&distinct)=Y %then %do;
                distinct_patid_n
            %end;
            %if %upcase(&elig)=Y %then %do;
                elig_record_n
            %end;
            ;
        set &qnam;
    run;

    proc sort data=&qnam out=dmlocal.&qnam (drop=ord);
        by ord &var;
    run;

    * Delete intermediary datasets;
    proc delete data=&qnam &var._vals &dset._subset;
    quit;

    * Option to start timer outside of macro if needed;
    %if %upcase(&timer)= %then %do;
        %elapsed(end);
    %end;                    

%mend n_pct;

    
*- Macro for 2- or 3-level RECORD_N/RECORD_PCT queries (CAT (N & %)) that have reference files -*;
%macro n_pct_multilev(dset=,var1=,var2=,var3=,qnam=,subvar=,subset=,ref1=,cdmref1=y,ref2=,cdmref2=y,ref3=,cdmref3=y,timer=,alphasort1=y,alphasort2=y,alphasort3=y,
    dec=1,nopct=y,distinct=,var1lbl=,var2lbl=,var3lbl=,distinctenc=,distinctprov=,observedonly=n);

    /*** Note: Because of text-string-matching to the VALUESET code lists, any dataset or variable names (DSET/VAR) must be in all caps when the macro is
        called if it uses VALUESET (which is the default code list unless specified otherwise with the REF parameter);***/
    
    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;

    %if &var3 ne %then %do;
        %let numvars = 3;
    %end;
    %else %do;
        %let numvars = 2;
    %end;

    * Loop through each summary variable to check for value set processing;
    %do i=1 %to &numvars;

        * Process value set for variable from reference file, if applicable;
        %if &&ref&i ne %then %do;
    
            * Bring in CDM value set for variable 1;
            data &&var&i.._vals;
                length valueset_item label $200;
                set &&ref&i (where=(table_name="&dset" and field_name="&&var&i")) end=eof;

                hlo='';

                * SAS formats cannot end in numbers;
                fmtname="$&&var&i";

                * Add ordering to front of value for sorting purposes;
                label=strip(put(valueset_item_order,12.)) || '@' || strip(valueset_item);        
        
                * Add NULL or missing and Value outside of CDM specifications into value set for super-population of all categories;
                output;
                if eof then do;
                    label='88888@NULL or missing';
                    valueset_item='';
                    output;

                    label='99999@Values outside of CDM specifications';
                    valueset_item='other';
                    hlo='o';
                    output;
                end;

                rename valueset_item=start;

                keep fmtname valueset_item label hlo;
            run;

            * Create format;
            proc format library=work cntlin=&&var&i.._vals;
            run; 

            * Determine maximum length of all possible values of summary variable;
            data _null_;
                set &&ref&i (where=(table_name="&dset" and field_name="&&var&i")) end=eof;

                retain varlength&i varlength_final&i;

                * Set working length to a minimum of $43 to account for 99999@Values outside of CDM specifications;
                if _n_=1 then varlength&i=43;

                if length(valueset_item)>varlength&i then varlength&i=length(valueset_item);
        
                * Set final length to a minimum of $36 to account for Values outside of CDM specifications (with no order attached) if using a CDM-specified value set;
                * Set final length to a minimum of $15 to account for NULL or missing (with no order attached) if using a custom value set (hence no CDM-specified values);
                %if &cdmref1=y %then %do;
                    if _n_=1 then varlength_final&i=36;
                %end;
                %else %do;       
                    if _n_=1 then varlength_final&i=15;
                %end;

                if length(valueset_item)>varlength_final&i then varlength_final&i=length(valueset_item);

                * Create macro variable containing the lengths;
                if eof then do;
                    call symputx("length&i",varlength&i);
                    call symputx("length_final&i",varlength_final&i);
                end;
            run;

        %end;        
        %else %do; /* If no reference value set for variable, determine length based on contents of variable itself*/
            
            * Set final summary variable length to $30 in case of empty input dataset;
            %let length_final&i = 30;

            * If summary variable is a YYYY or YYYY_MM date, length does not need to be checked from data, it can stay at $30;
            %if %index(%upcase(&qnam),_Y)=0 %then %do;

                * Determine maximum length of all possible values of summary variable;
                data _null_;
                    set &dset (keep=&&var&i) end=eof;

                    retain varlength&i;

                    * Set final length to a minimum of $30 to account for NULL or missing;
                    if _n_=1 then varlength&i=30;

                    if length(&&var&i)>varlength&i then varlength&i=length(&&var&i);

                    * Create macro variable containing the lengths;
                    if eof then call symputx("length_final&i",varlength&i);
                run;

            %end;
            
        %end;
            
    %end;

    * Reset count of observations in input dataset for each macro run;
    %let subset_obs = 0;

    * Keep only summary variables on input dataset;
    data &dset._subset (compress=yes);
        set &dset (keep=&var1 &var2 
            %if &var3 ne %then %do;
                &var3
            %end;
            %if %upcase(&distinct)=Y %then %do;
                patid
            %end;
            %if %upcase(&distinctenc)=Y %then %do;
                encounterid
            %end;
            %if %upcase(&distinctprov)=Y %then %do;
                providerid
            %end; 
            %if &subvar ne %then %do;
                &subvar
            %end;             
            %if &subset ne %then %do;
                where=(&subset)
            %end;
            rename=(&var1=&var1._old &var2=&var2._old				
				%if &var3 ne %then %do;
					&var3=&var3._old
				%end;
				)
			)
            end=eof;
			
        length &var1 $&length_final1 &var2 $&length_final2
            %if &var3 ne %then %do;
                &var3 $&length_final3
            %end;
            ;
			
		&var1=&var1._old;
		&var2=&var2._old;
		%if &var3 ne %then %do;
			&var3=&var3._old;
		%end;
		
        * If no reference value set for variable, fill in null values with ZZZZZZZZZZZZZZZZZZZZZZZ to force it to bottom of sort;
        %if &ref1= %then %do;
            if &var1='' then &var1='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ';
        %end;
        %if &ref2= %then %do;
            if &var2='' then &var2='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ';
        %end;
        %if &var3 ne and &ref3= %then %do;
            if &var3='' then &var3='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ';
        %end;
        
        if eof then call symputx('subset_obs',_n_);
		
		drop &var1._old &var2._old		
			%if &var3 ne %then %do;
				&var3._old;
			%end;
			;
    run;

    * Create format to force missing row in if no value set and no null values in data;
    proc format;
        value $miss
            'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ' = 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'
            ;
    run;        

    * Only do the proc means if input dataset is not empty;
    %if &subset_obs>0 %then %do;

        * Get counts;
        proc means data=&dset._subset noprint missing
            %if %upcase(&observedonly) ne Y %then %do; /* If spec indicates we should keep all possible combinations, use COMPLETETYPES option*/
                completetypes
            %end;
            ;
            class &var1
                %if %upcase(&observedonly) ne Y %then %do;
                    /preloadfmt
                %end;
                ;
            class &var2
                %if %upcase(&observedonly) ne Y %then %do;
                    /preloadfmt
                %end;
                ;
            %if &var3 ne %then %do;
                class &var3                    
                    %if %upcase(&observedonly) ne Y %then %do;
                        /preloadfmt
                    %end;
                    ;
            %end;
            output out=stats;
            %if &ref1 ne %then %do;
                format &var1 $&var1..;
            %end;
            %else %do;
                format &var1 $miss.;
            %end;
            
            %if &ref2 ne %then %do;
                format &var2 $&var2..;
            %end;
            %else %do;
                format &var2 $miss.;
            %end;

            %if &var3 ne %then %do;
                %if &ref3 ne %then %do;
                    format &var3 $&var3..;
                %end;
                %else %do;
                    format &var3 $miss.;
                %end;
            %end;
        run;

        * Special handling of ENC_L3_LOS_DIST to add RECORD_PCT_CAT column;
        %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;

            * Get denominator for RECORD_PCT_CAT;
            proc means data=&dset._subset completetypes noprint missing;
                class &var1/preloadfmt;
                where &var1 ne '';
                output out=stats_record_pct_cat (where=(_type_=1));
                %if &ref1 ne %then %do;
                    format &var1 $&var1..;
                %end;
                %else %do;
                    format &var1 $miss.;
                %end;
            run;

            proc sort data=stats;
                by &var1;
            run;

            data stats;
                merge
                    stats (in=ins)
                    stats_record_pct_cat (keep=&var1 _type_ _freq_ rename=(_type_=_type_record_pct_cat _freq_=record_pct_cat_denom))
                    ;
                by &var1;
                if ins;

                drop _type_record_pct_cat;
            run;

        %end; 

        * If any DISTINCT stat requested, sort STATS by summary variables for merging purposes;
        %if %upcase(&distinct)=Y or %upcase(&distinctenc)=Y or %upcase(&distinctprov)=Y %then %do;

            proc sort data=stats;
                by &var1 &var2 &var3;
            run;
            
        %end;            

        %if %upcase(&distinct)=Y %then %do;
                
            * Reduce input data down to one record per summary variable type per patid;
            proc sort data=&dset._subset (keep=patid &var1 &var2 &var3) out=stats_oneper (drop=patid) nodupkey;
                by &var1 &var2 &var3 patid;
            run;
    
            * Get counts by summary variable;
            proc means data=stats_oneper nway noprint missing;
                class &var1 &var2 &var3;
                output out=stats_oneper;
            run;

            data stats;
                merge
                    stats (in=ins)
                    stats_oneper (keep=&var1 &var2 &var3 _type_ _freq_ rename=(_type_=_type_patid _freq_=_freq_patid))
                    ;
                by &var1 &var2 &var3;
                if ins;

                drop _type_patid;
            run;

        %end;  

        %if %upcase(&distinctenc)=Y %then %do;

            * Reduce input data down to one record per summary variable type per encounter id;
            proc sort data=&dset._subset (keep=encounterid &var1 &var2 &var3) out=stats_oneperenc (drop=encounterid) nodupkey;
                by &var1 &var2 &var3 encounterid;
            run;
    
            * Get counts by summary variable;
            proc means data=stats_oneperenc nway noprint missing;
                class &var1 &var2 &var3;
                output out=stats_oneperenc;
            run;

            data stats;
                merge
                    stats (in=ins)
                    stats_oneperenc (keep=&var1 &var2 &var3 _type_ _freq_ rename=(_type_=_type_encounterid _freq_=_freq_encounterid))
                    ;
                by &var1 &var2 &var3;
                if ins;

                drop _type_encounterid;
            run;

        %end;  

        %if %upcase(&distinctprov)=Y %then %do;

            * Reduce input data down to one record per summary variable type per encounter id;
            proc sort data=&dset._subset (keep=providerid &var1 &var2 &var3) out=stats_oneperprov (drop=providerid) nodupkey;
                by &var1 &var2 &var3 providerid;
            run;
    
            * Get counts by summary variable;
            proc means data=stats_oneperprov nway noprint missing;
                class &var1 &var2 &var3;
                output out=stats_oneperprov;
            run;

            data stats;
                merge
                    stats (in=ins)
                    stats_oneperprov (keep=&var1 &var2 &var3 _type_ _freq_ rename=(_type_=_type_providerid _freq_=_freq_providerid))
                    ;
                by &var1 &var2 &var3;
                if ins;

                drop _type_providerid;
            run;

        %end;
       
        * If second summary variable is a year/month summary, super-populate all year/months between the first and last available year/months;
        * Note that at the time of programming, any _YM variables are always the second summary variable in a 2-level query, and the associated first summary variables are also ones with reference value sets.;
        * Any deviations from these assumptions in the future will require edits.;    
        %if %index(%upcase(&qnam),_YM)>0 %then %do;

            data ym_&qnam;
                set stats (keep=&var2 where=(&var2 not in ('' 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'))) end=eof;

                * Retain first year/month down to last record;
                retain firstyr firstmn;

                if _n_=1 then do;
                    firstyr=input(scan(&var2,1,'_'),4.);
                    firstmn=input(scan(&var2,2,'_'),2.);
                end;
            
                if eof then do;
                    lastyr=input(scan(&var2,1,'_'),4.);
                    lastmn=input(scan(&var2,2,'_'),2.);
                end;

                if eof;

                keep firstyr firstmn lastyr lastmn;
            run;

            * Create template of unique first summary variable values;
            proc sort data=stats (keep=&var1) out=&var1._values nodupkey;
                by &var1;
            run;

            data ym_&qnam;
                set &var1._values;
                if _n_=1 then set ym_&qnam;
            run;

            data ym_&qnam;
                set ym_&qnam end=eof;
                by &var1;
           
                * Loop through all years in available dates;        
                do year=firstyr to lastyr;
                    * Loop through all months in available years; 
                    do month=firstmn to 12;
                        * Only output years/months that are not past the last available year/month;
                        if year<lastyr or (year=lastyr and month<=lastmn) then do;
                            &var2=put(year,4.) || '_' || put(month,z2.);
                            output;
                        end;
                    end;
                end;               

                keep &var1 &var2;
            run;
     
            proc sort data=stats;
                by &var1 &var2;
            run;

            data stats;
                merge
                    stats (in=ins)
                    ym_&qnam (in=iny)
                    ;
                by &var1 &var2;
                if ins;

                * Fill in _type_=3 for template-only records so that they will be kept in the final dataset;
                if iny and _type_=. then _type_=3;
                
                if _freq_=. then _freq_=0;

                %if %upcase(&distinct)=Y %then %do;
                    if distinct_patid_n='' then distinct_patid_n='0';
                %end;

                %if %upcase(&distinctenc)=Y %then %do;
                    if distinct_encid_n='' then distinct_encid_n='0';
                %end;

                %if %upcase(&distinctprov)=Y %then %do;
                    if distinct_providerid_n='' then distinct_providerid_n='0';
                %end;
            run;            

            * Delete template dataset;
            proc delete data=&var1._values ym_&qnam;
            quit;
        
        %end;

        * Create final variables and sort order;
        data &qnam;        
            set stats (keep=&var1 &var2 &var3 _type_ _freq_ 
                %if %upcase(&distinct)=Y %then %do;
                    _freq_patid
                %end; 
                %if %upcase(&distinctenc)=Y %then %do;
                    _freq_encounterid
                %end; 
                %if %upcase(&distinctprov)=Y %then %do;
                    _freq_providerid
                %end;
                %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                    record_pct_cat_denom
                %end;

                %if &numvars=2 %then %do;
                    where=(_type_=3)
                %end;
                %else %if &numvars=3 %then %do;
                    where=(_type_=7)
                %end;
                )
                ;
            if _n_=1 then set stats (keep=_type_ _freq_ where=(_type_=0) rename=(_freq_=denom));
            
            * Set sort orders to missing for if there is no associated reference file. Sort will be purely alphabetical based on variable values in that case;
            ord1=.;
            ord2=.;
            ord3=.;

            * If associated reference file, sort alphabetically (except for null flavor values) by default, use custom sort (set in custom reference file) if otherwise specified with non-Y alphasort parameter;
            %if &ref1 ne %then %do;
                %if %sysfunc(upcase(&alphasort1))=Y %then %do;
                    if scan(put(&var1,$&var1..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord1=input(scan(put(&var1,$&var1..),1,'@'),12.);
                %end;
                %else %do;
                    ord1=input(scan(put(&var1,$&var1..),1,'@'),12.);
                %end;
            %end;

            %if &ref2 ne %then %do;
                %if %sysfunc(upcase(&alphasort2))=Y %then %do;
                    if scan(put(&var2,$&var2..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord2=input(scan(put(&var2,$&var2..),1,'@'),12.);
                %end;
                %else %do;
                    ord2=input(scan(put(&var2,$&var2..),1,'@'),12.);
                %end;
            %end;

            %if &ref3 ne %then %do;
                %if %sysfunc(upcase(&alphasort3))=Y %then %do;
                    if scan(put(&var3,$&var3..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord3=input(scan(put(&var3,$&var3..),1,'@'),12.);
                %end;
                %else %do;
                    ord3=input(scan(put(&var3,$&var3..),1,'@'),12.);
                %end;
            %end;
        
            %stdvar;
            
            length record_n $20 record_pct $6;
                
            record_n=strip(put(_freq_,12.));
            record_pct=put(_freq_/denom*100,6.&dec);

            %if %upcase(&distinct)=Y %then %do;
                * Fill in zero for missing category if needed;
                if _freq_patid=. then _freq_patid=0;
                
                length distinct_patid_n $20;
                
                distinct_patid_n=strip(put(_freq_patid,12.));

                drop _freq_patid;
            %end;

            %if %upcase(&distinctenc)=Y %then %do;
                * Fill in zero if needed;
                if _freq_encounterid=. then _freq_encounterid=0;
                
                length distinct_encid_n $20;
                
                distinct_encid_n=strip(put(_freq_encounterid,12.));

                drop _freq_encounterid;
            %end;

            %if %upcase(&distinctprov)=Y %then %do;
                * Fill in zero if needed;
                if _freq_providerid=. then _freq_providerid=0;
                
                length distinct_providerid_n $20;
                
                distinct_providerid_n=strip(put(_freq_providerid,12.));

                drop _freq_providerid;
            %end;

            * Special handling of ENC_L3_LOS_DIST to add RECORD_PCT_CAT column;
            %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                * Fill in zero if needed;
                if record_pct_cat_denom not in (. 0) then record_pct_catn=_freq_/record_pct_cat_denom*100;
                
                length record_pct_cat $20;
                
                record_pct_cat=put(record_pct_catn,6.&dec);

                drop record_pct_catn record_pct_cat_denom;
            %end;

            * Strip ordering info off of summary values, if applicable;
            %if &ref1 ne %then %do;
                &var1=scan(put(&var1,$&var1..),2,'@');
            %end;

            %if &ref2 ne %then %do;
                &var2=scan(put(&var2,$&var2..),2,'@');
            %end;

            %if &ref3 ne %then %do;
                &var3=scan(put(&var3,$&var3..),2,'@');
            %end;

            * Only output this row for CDM-specified value sets (delete for custom value sets);
            %if %sysfunc(upcase(&cdmref1))=N %then %do;
                if &var1='Values outside of CDM specifications' then delete;
            %end;
            %if %sysfunc(upcase(&cdmref2))=N %then %do;
                if &var2='Values outside of CDM specifications' then delete;
            %end;
            %if %sysfunc(upcase(&cdmref3))=N %then %do;
                if &var3='Values outside of CDM specifications' then delete;
            %end;

            format &var1 &var2 &var3;

            drop _type_ _freq_ denom;
        run;      

        * Delete intermediary datasets;
        proc delete data=&dset._subset stats
                %if &ref1 ne %then %do;
                    &var1._vals
                %end;
                %if &ref2 ne %then %do;
                    &var2._vals
                %end;
                %if &ref3 ne %then %do;
                    &var3._vals
                %end;
                    
                %if %upcase(&distinct)=Y %then %do;
                    stats_oneper
                %end;
                %if %upcase(&distinctenc)=Y %then %do;
                    stats_oneperenc
                %end;
                %if %upcase(&distinctprov)=Y %then %do;
                    stats_oneperprov
                %end;
                %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                    stats_record_pct_cat
                %end;
                ;
        quit;  

    %end;  
    %else %do; 

        * If all summary variables have reference lists and we are not keeping only observed combos, use proc means to create dummy crosstab (assumes that if there is a third summary variable, it always has a reference list);
        %if &ref1 ne and &ref2 ne and %upcase(&observedonly) ne Y %then %do;

            * Create dummy dataset to go into proc means;
            data &dset._dummy;
                %if &ref1 ne %then %do;
                    length &var1 $&length_final1;
                %end;
                %if &ref2 ne %then %do;
                    length &var2 $&length_final2;
                %end;
                %if &ref3 ne %then %do;
                    length &var3 $&length_final3;
                %end;
                
                &var1='0';
                &var2='0';
                
                %if &var3 ne %then %do;
                    &var3='0';
                %end;

                output;
            run;
        
            * Get counts;
            proc means data=&dset._dummy noprint missing completetypes;
                class &var1/preloadfmt;
                class &var2/preloadfmt;
                %if &var3 ne %then %do;
                    class &var3/preloadfmt
                %end;                
                output out=stats;
                format &var1 $&var1..;
                format &var2 $&var2..;
                %if &var3 ne %then %do;
                    format &var3 $&var3..;
                %end;
            run;

            * Create final variables and sort order;
            data &qnam;        
                set stats (keep=&var1 &var2 &var3 _type_ _freq_
                    %if &numvars=2 %then %do;
                        where=(_type_=3)
                    %end;
                    %else %if &numvars=3 %then %do;
                        where=(_type_=7)
                    %end;
                    )
                    ;
            
                * Set sort orders to missing for if there is no associated refernce file. Sort will be purely alphabetical based on variable values in that case;
                ord1=.;
                ord2=.;
                ord3=.;

                * If associated reference file, sort alphabetically (except for null flavor values) by default, use custom sort (set in custom reference file) if otherwise specified with non-Y alphasort parameter;
                %if %sysfunc(upcase(&alphasort1))=Y %then %do;
                    if scan(put(&var1,$&var1..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord1=input(scan(put(&var1,$&var1..),1,'@'),12.);
                %end;
                %else %do;
                    ord1=input(scan(put(&var1,$&var1..),1,'@'),12.);
                %end;
                
                %if %sysfunc(upcase(&alphasort2))=Y %then %do;
                    if scan(put(&var2,$&var2..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord2=input(scan(put(&var2,$&var2..),1,'@'),12.);
                %end;
                %else %do;
                    ord2=input(scan(put(&var2,$&var2..),1,'@'),12.);
                %end;                

                %if &var3 ne %then %do;
                    %if %sysfunc(upcase(&alphasort3))=Y %then %do;
                        if scan(put(&var3,$&var3..),2,'@') in ('NI' 'UN' 'OT' 'NULL or missing' 'Values outside of CDM specifications') then ord3=input(scan(put(&var3,$&var3..),1,'@'),12.);
                    %end;
                    %else %do;
                        ord3=input(scan(put(&var3,$&var3..),1,'@'),12.);
                    %end;
                %end;
        
                %stdvar;
            
                length record_n $20 record_pct $6;
                
                record_n='0';
                record_pct=put(0,6.&dec);

                %if %upcase(&distinct)=Y %then %do;
                    distinct_patid_n='0';
                %end;

                %if %upcase(&distinctenc)=Y %then %do;
                    distinct_encid_n='0';
                %end;

                %if %upcase(&distinctprov)=Y %then %do;                
                    distinct_providerid_n='0';
                %end;
                * Special handling of ENC_L3_LOS_DIST to add RECORD_PCT_CAT column;
                %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                    record_pct_cat=' ';
                %end;

                * Strip ordering info off of summary values, if applicable;
                &var1=scan(put(&var1,$&var1..),2,'@');
                &var2=scan(put(&var2,$&var2..),2,'@');
                %if &var3 ne %then %do;
                    &var3=scan(put(&var3,$&var3..),2,'@');
                %end;

                * Only output this row for CDM-specified value sets (delete for custom value sets);
                %if %sysfunc(upcase(&cdmref1))=N %then %do;
                    if &var1='Values outside of CDM specifications' then delete;
                %end;
                %if %sysfunc(upcase(&cdmref2))=N %then %do;
                    if &var2='Values outside of CDM specifications' then delete;
                %end;
                %if %sysfunc(upcase(&cdmref3))=N %then %do;
                    if &var3='Values outside of CDM specifications' then delete;
                %end;

                format &var1 &var2 &var3;

                drop _type_ _freq_;
            run;      

            * Delete intermediary datasets;
            proc delete data=stats &dset._dummy;  
            run;

        %end;
        /* Else if either summary variable has no reference list, create empty dummy dataset*/
        %else %do;

            data &qnam;

                &var1='0';
                &var2='0';
                %if &var3 ne %then %do;
                    &var3='0';
                %end;
                
                %stdvar;

                ord1=.;
                ord2=.;
                ord3=.;

                record_n='0';
                record_pct='0';

                %if %upcase(&distinct)=Y %then %do;
                    distinct_patid_n='0';
                %end;

                %if %upcase(&distinctenc)=Y %then %do;
                    distinct_encid_n='0';
                %end;

                %if %upcase(&distinctprov)=Y %then %do;                
                    distinct_providerid_n='0';
                %end;
                * Special handling of ENC_L3_LOS_DIST to add RECORD_PCT_CAT column;
                %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                    record_pct_cat=' ';
                %end;

                delete;
            run;  

        %end; 

    %end;

    * Reset final summary variable length;
    data &qnam;
        set &qnam
			%if &ref1 ne or &ref2 ne or &ref3 ne %then %do;
			(rename=(
				%if &ref1 ne %then %do;
					&var1=&var1._old
				%end;
				%if &ref2 ne %then %do;
					&var2=&var2._old
				%end;
				%if &ref3 ne %then %do;
					&var3=&var3._old
				%end;
			))			
			%end;
		;
		
        %if &ref1 ne %then %do;
            length &var1 $&length_final1;
			
			&var1=&var1._old;
			
			drop &var1._old;
        %end;
        %if &ref2 ne %then %do;
            length &var2 $&length_final2;
			
			&var2=&var2._old;
			
			drop &var2._old;
        %end;
        %if &ref3 ne %then %do;
            length &var3 $&length_final3;
			
			&var3=&var3._old;
			
			drop &var3._old;
        %end;
    run;        

    data &qnam;
        retain datamartid response_date query_package &var1 &var2 &var3 record_n record_pct
            %if %upcase(&distinct)=Y %then %do;
                distinct_patid_n
            %end;
            %if %upcase(&distinctenc)=Y %then %do;
                distinct_encid_n
            %end;
            %if %upcase(&distinctprov)=Y %then %do;
                distinct_providerid_n
            %end;
            %if %upcase(&qnam)=ENC_L3_LOS_DIST %then %do;
                record_pct_cat
            %end;                   
            ;
        set &qnam;
                
        %if %upcase(&nopct)=Y %then %do;
            drop record_pct;
        %end; 
    run;

    proc sort data=&qnam;
        by ord1 &var1 ord2 &var2 ord3 &var3;
    run;

    * Rename summary variable(s) if needed;
    data dmlocal.&qnam;
        set &qnam;

        * Reassign null value label after proper sorting;
        if &var1='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ' then &var1='NULL or missing';
        if &var2='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ' then &var2='NULL or missing';
        %if &var3 ne %then %do;
            if &var3='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ' then &var3='NULL or missing';
        %end;

        * Rename summary variable to match spec if needed;
        %if &var1lbl ne %then %do;
            rename &var1=&var1lbl;
        %end;
        %if &var2lbl ne %then %do;
            rename &var2=&var2lbl;
        %end;
        %if &var3lbl ne %then %do;
            rename &var3=&var3lbl;
        %end;

        drop ord1 ord2 ord3;
    run;        

    * Delete intermediary datasets;
    proc delete data=&qnam;
    quit;  
    
    * Option to start timer outside of macro if needed;
    %if %upcase(&timer)= %then %do;
        %elapsed(end);
    %end; 
       
%mend n_pct_multilev;    

*- Macro for RECORD_N/RECORD_PCT queries (CAT (N & %)) that do not have reference files -*;
%macro n_pct_noref(dset=,var=,qnam=,timer=,dec=1,nopct=,varlbl=,distinct=,distinctenc=);

    * Option to start timer outside of macro if needed;
    %if &timer= %then %do;
        %let qname=&qnam;
        %elapsed(begin);
    %end;

    %if &&nobs_&dset>0 %then %do;

        * Keep only summary variable;
        data &qnam;
            set &dset;
                
            keep &var;
        run;
    
        * Get counts by summary variable;
        proc means data=&qnam noprint missing;
            class &var;
            output out=&qnam;
        run;

        * Create template for missing row in case there are none;
        data miss;
            &var='';
            _type_=1;
        run;

        * Merge missing template onto counts;
        data &qnam;
            merge
                &qnam
                miss
                ;
            by _type_ &var;
        run;  

        %if %upcase(&distinct)=Y %then %do;
                
            * Reduce input data down to one record per summary variable type per patid;
            proc sort data=&dset (keep=patid &var where=(not missing(patid))) out=&qnam._oneper (drop=patid) nodupkey;
                by &var patid;
            run;
    
            * Get counts by summary variable;
            proc means data=&qnam._oneper noprint missing;
                class &var;
                output out=&qnam._oneper;
            run;

            data &qnam;
                merge
                    &qnam
                    &qnam._oneper (keep=&var _type_ _freq_ where=(_type_patid=1) rename=(_type_=_type_patid _freq_=_freq_patid))
                    ;
                by &var;

                drop _type_patid;
            run;

        %end;  

        %if %upcase(&distinctenc)=Y %then %do;
                
            * Reduce input data down to one record per summary variable type per encounter id;
            proc sort data=&dset (keep=encounterid &var where=(not missing(encounterid))) out=&qnam._oneperenc (drop=encounterid) nodupkey;
                by &var encounterid;
            run;
    
            * Get counts by summary variable;
            proc means data=&qnam._oneperenc noprint missing;
                class &var;
                output out=&qnam._oneperenc;
            run;

            data &qnam;
                merge
                    &qnam
                    &qnam._oneperenc (keep=&var _type_ _freq_ where=(_type_encounterid=1) rename=(_type_=_type_encounterid _freq_=_freq_encounterid))
                    ;
                by &var;

                drop _type_encounterid;
            run;

        %end;  

        * Special handling of pres_l3_odate_y - Counts of available RXNORM_CUI by year;
        %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                
            * Keep only summary variables;
            data &qnam._rxnorm_cui;
                set &dset (keep=&var rxnorm_cui where=(not missing(rxnorm_cui)));
            run;                    
    
            * Get counts by summary variable;
            proc means data=&qnam._rxnorm_cui noprint missing;
                class &var;
                output out=&qnam._rxnorm_cui;
            run;

            data &qnam;
                merge
                    &qnam
                    &qnam._rxnorm_cui (keep=&var _type_ _freq_ where=(_type_rxnorm_cui=1) rename=(_type_=_type_rxnorm_cui _freq_=_freq_rxnorm_cui))
                    ;
                by &var;

                drop _type_rxnorm_cui;
            run;

        %end;     

        data &qnam;
            set &qnam (where=(_type_=1) keep=&var _type_ _freq_
                %if %upcase(&distinct)=Y %then %do;
                    _freq_patid
                %end;
                %if %upcase(&distinctenc)=Y %then %do;
                    _freq_encounterid
                %end;
                %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                    _freq_rxnorm_cui
                %end;                        
                rename=(&var=&var._old));                
            if _n_=1 then set &qnam (keep=_type_ _freq_ where=(_type_=0) rename=(_freq_=denom));

            %stdvar;
			
            length &var $200; 
			&var=&var._old;

            if missing(&var) then do;
                &var='NULL or missing';
                ord=999999;
            end;

            * Fill in zero if needed;
            if _freq_=. then _freq_=0;

            length record_n $20 record_pct $6;
                
            record_n=strip(put(_freq_,12.));
            record_pct=put(_freq_/denom*100,6.&dec);

            %if %upcase(&distinct)=Y %then %do;
                * Fill in zero if needed;
                if _freq_patid=. then _freq_patid=0;
                
                length distinct_patid_n $20;
                
                distinct_patid_n=strip(put(_freq_patid,12.));

                drop _freq_patid;
            %end;

            %if %upcase(&distinctenc)=Y %then %do;
                * Fill in zero if needed;
                if _freq_encounterid=. then _freq_encounterid=0;
                
                length distinct_encid_n $20;
                
                distinct_encid_n=strip(put(_freq_encounterid,12.));

                drop _freq_encounterid;
            %end;

            %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                * Fill in zero if needed;
                if _freq_rxnorm_cui=. then _freq_rxnorm_cui=0;
                
                length record_n_rxcui $16;
                
                record_n_rxcui=strip(put(_freq_rxnorm_cui,12.));

                drop _freq_rxnorm_cui;
            %end;
        
            drop &var._old _type_ _freq_ denom;
        run;

        * Set final summary variable length to $15 in case of empty input dataset;
        %let length_final = 15;

        * Determine maximum length of all possible values of summary variable;
        data _null_;
            set &qnam end=eof;

            retain varlength_final;

            * Set final length to a minimum of $15 to account for NULL or missing;
            if _n_=1 then varlength_final=15;

            if length(&var)>varlength_final then varlength_final=length(&var);

            * Create macro variable containing the lengths;
            if eof then call symputx('length_final',varlength_final);
        run;        

        * If it is a year/month summary, super-populate all year/months between the first and last available year/months;
        %if %index(%upcase(&qnam),_YM)>0 %then %do;

            data year_months_&qnam;
                set &qnam (where=(&var ne 'NULL or missing')) end=eof;

                * Retain first year/month down to last record;
                retain firstyr firstmn;

                if _n_=1 then do;
                    firstyr=input(scan(&var,1,'_'),4.);
                    firstmn=input(scan(&var,2,'_'),2.);
                end;
            
                if eof then do;
                    lastyr=input(scan(&var,1,'_'),4.);
                    lastmn=input(scan(&var,2,'_'),2.);
                end;
            
                if eof then do;
                    * Loop through all years in available dates;        
                    do year=firstyr to lastyr;
                        * Loop through all months in available years; 
                        do month=firstmn to 12;
                            * Only output years/months that are not past the last available year/month;
                            if year<lastyr or (year=lastyr and month<=lastmn) then do;
                                &var=put(year,4.) || '_' || put(month,z2.);
                                output;
                            end;
                        end;
                    end;
                end;

                keep &var;
            run;

            proc sort data=&qnam;
                by &var;
            run;

            data &qnam;
                merge
                    &qnam
                    year_months_&qnam
                    ;
                by &var;

                if record_n='' then record_n='0';
                if record_pct='' then record_pct='0.0';

                %if %upcase(&distinct)=Y %then %do;
                    if distinct_patid_n='' then distinct_patid_n='0';
                %end;

                %if %upcase(&distinctenc)=Y %then %do;
                    if distinct_encid_n='' then distinct_encid_n='0';
                %end;
            run;            

            * Delete template dataset;
            proc delete data=year_months_&qnam;
            quit;
        
        %end;    

		* Delete intermediary datasets;
		proc delete data=miss
			%if %upcase(&distinct)=Y %then %do;
                &qnam._oneper
            %end;
            %if %upcase(&distinctenc)=Y %then %do;
                &qnam._oneperenc
            %end;
            %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                &qnam._rxnorm_cui
            %end;
            ;
		quit;

        * Reset final summary variable length;
        data &qnam;
            set &qnam (rename=(&var=&var._old));
			
            length &var $&length_final;			
			&var=&var._old;
			
			drop &var._old;
        run;

    %end;
    %else %do;

        * Create null dataset if no observations in source dataset;
        data &qnam;
            %stdvar;

            length &var $15;
            &var='NULL or missing';
            record_n='0';
            record_pct='0.0';
            ord=.;

            %if %upcase(&distinct)=Y %then %do;
                distinct_patid_n='0';
            %end;

            %if %upcase(&distinctenc)=Y %then %do;
                distinct_encid_n='0';
            %end;

            %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                record_n_rxcui='0';        
            %end;

            delete;
        run;
            
    %end;

    * Final renames/drops as needed;
    data &qnam;
        retain datamartid response_date query_package &var record_n record_pct
            %if %upcase(&qnam)=PRES_L3_ODATE_Y %then %do;
                record_n_rxcui
            %end;
            %if %upcase(&distinctenc)=Y %then %do;
                distinct_encid_n
            %end;
            %if %upcase(&distinct)=Y %then %do;
                distinct_patid_n
            %end;
            ;
        set &qnam;

        * Rename summary variable to match spec if needed;
        %if &varlbl ne %then %do;
            rename &var=&varlbl;
        %end;
                
        %if %upcase(&nopct)=Y %then %do;
            drop record_pct;
        %end;        
    run;

    proc sort data=&qnam out=dmlocal.&qnam (drop=ord);
        by ord;
    run;    

    * Delete intermediary datasets;
    proc delete data=&qnam;
    quit;

    * Option to start timer outside of macro if needed;
    %if %upcase(&timer)= %then %do;
        %elapsed(end);
    %end;       

%mend n_pct_noref;

*- Macro to determine minimum/maximum date;
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

	* Delete intermediary datasets;
	proc delete data=m&idsn;
    quit;
    
%mend minmax;

*- Macro to print each query result -*;
%macro prnt(pdsn=,_obs=max,dropvar=datamartid response_date query_package,_svar=,_suppvar=,_recordn=record_n,orient=portrait);

     options orientation=&orient;

     * if not printing entire dataset, sort so that top frequencies are printed *;
     %if &_obs^=max %then %do;
        data &pdsn;
             set dmlocal.&pdsn(rename=(&_recordn=record_nc));

             if strip(record_nc) in ("0") then delete;
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
         title "Query name:  %upcase(&pdsn) (Limited to most frequent &_obs observations)";
     %end;
     %if &_obs^=max %then %do;
         proc print width=min data=&pdsn(drop=&dropvar obs=&_obs);
              var &_svar &_suppvar;
         run;
         proc delete data=&pdsn;
         run;
     %end;
     %else %do;
         proc print width=min data=dmlocal.&pdsn(drop=&dropvar obs=&_obs);
         run;
     %end;
     ods listing close;
%mend prnt;

*- Macro for DASH queries -*;
%macro dash(datevar);
        length period $5;
    
         if &cutoff_dashyr5<=&datevar<=&cutoff_dash then do;
            period='5 yrs';
            output;
            if &cutoff_dashyr4<=&datevar<=&cutoff_dash then do;
               period='4 yrs';
               output;
               if &cutoff_dashyr3<=&datevar<=&cutoff_dash then do;
                  period='3 yrs';
                  output;
                  if &cutoff_dashyr2<=&datevar<=&cutoff_dash then do;
                     period='2 yrs';
                     output;
                     if &cutoff_dashyr1<=&datevar<=&cutoff_dash then do;
                        period='1 yr';
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

        * Append current run-time to elapsed dataset *;
        proc append base=elapsed data=_qe(where=(dflag=0) drop=description);
        run;

        * Append current run-time to progress dataset *;
        proc append base=progress data=_qe(drop=dflag);
        run;

        * Print data set of progress *;
        ods html close;
        ods listing;
        ods path sashelp.tmplmst(read) library.templat(read); 
        
        *******************************************************************************;
        * Close DRNOC log
        *******************************************************************************;
        proc printto log=qlog;
        run;

        *******************************************************************************;
        * Re-direct to default log
        *******************************************************************************;
        proc printto log=log;
        run;

        ods rtf file="&qpath.drnoc/&dmid._&tday._data_curation_progress_report%lowcase(&_dcpart).rtf" style=journal;
        
        *******************************************************************************;
        * Close default log
        *******************************************************************************;
        proc printto log=log;
        run;

        *******************************************************************************;
        * Re-direct to DRNOC log
        *******************************************************************************;
        proc printto log=qlog;
        run;

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
* Macro to prevent open code
********************************************************************************;
%macro dc_base;

%global _ycondition _ydeath _ydeathc _ydemographic _ydiagnosis _ydispensing 
        _yencounter _yenrollment _yharvest _yhash_token _yimmunization 
        _ylab_result_cm _ylab_history _yldsadrs _ymed_admin _yobs_clin _yobs_gen 
        _yprescribing _yprocedures _ypro_cm _yprovider _yvital _yptrial 
        _part1_mstring _part2_mstring
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

* Delete intermediary dataset;
proc delete data=pcordata;
quit;

********************************************************************************;
*- Import transport files for all reference data sets
********************************************************************************;
/* This reference file is only used in Part 2 queries*/
%if %upcase(&_part2)=YES %then %do;
        
    *- LOINC reference data sets -*;
    proc cimport library=work infile=loincref;
    run;

%end;

*- DC reference data sets -*;
proc cimport library=work infile=dcref;
    select dc_tables valuesets address_rank
        /* These reference files are only used in Part 2 queries*/
        %if %upcase(&_part1)=YES %then %do;
            provider_specialty_primary facility_type payer_type
        %end;
        /* This reference file is only used in Part 2 queries*/
        %if %upcase(&_part2)=YES %then %do;
            rxnorm_cui_ref
        %end;
        ;            
run;

%if %upcase(&_part2)=YES %then %do;
    
    *- Sort RXNORM_CUI_REF for future merging -*;
    proc sort data=rxnorm_cui_ref out=rxnorm_cui_ref;
        by rxnorm_cui;
    run;
%end;      

%if %upcase(&_part1)=YES %then %do;
    
    *- Add quotes around each version -*;
    data cdm_version;
        length code_w_str $20;
        set valuesets (keep=field_name valueset_item where=(field_name='CDM_VERSION'));

        code_w_str="'"||strip(valueset_item)||"'";

        drop field_name valueset_item;
    run;

%end;

* Create reference file for final sort order of ELAPSED data in SAS dataset and PDF print;
data elapsed_order;
    length query $100;

    array txt (294) $100 _temporary_ ('DC PROGRAM' 'DEATH' 'DEATH_L3_N' 'DEATH_L3_IMPUTE' 'DEATH_L3_SOURCE' 'DEATH_L3_MATCH' 'DEATH_L3_DATE_Y' 
        'DEATH_L3_DATE_YM' 'DEATH_L3_SOURCE_YM' 'DEATH_CAUSE' 'DEATHC_L3_N' 'DEATHC_L3_CODE' 'DEATHC_L3_TYPE' 'DEATHC_L3_SOURCE' 'DEATHC_L3_CONF' 
        'ENCOUNTER' 'ENC_L3_N' 'ENC_L3_ADMSRC' 'ENC_L3_DISDISP' 'ENC_L3_DISSTAT' 'ENC_L3_DRG_TYPE' 'ENC_L3_ENCTYPE' 'ENC_L3_DASH2' 'ENC_L3_PAYERTYPE1' 
        'ENC_L3_PAYERTYPE2' 'ENC_L3_FACILITYTYPE' 'ENC_L3_LOS_DIST' 'ENC_L3_ENCTYPE_ADMSRC' 'ENC_L3_ADATE_Y' 'ENC_L3_ADATE_YM' 'ENC_L3_PAYERTYPE_Y' 
        'ENC_L3_ENCTYPE_ADATE_Y' 'ENC_L3_ENCTYPE_ADATE_YM' 'ENC_L3_DDATE_Y' 'ENC_L3_DDATE_YM' 'ENC_L3_ENCTYPE_DDATE_YM' 'ENC_L3_ENCTYPE_DISDISP' 
        'ENC_L3_ENCTYPE_DISSTAT' 'ENC_L3_ENCTYPE_DRG' 'ENC_L3_FACILITYLOC' 'ENC_L3_FACILITYTYPE_FACILITYLOC' 'DEMOGRAPHIC' 'DEM_L3_N' 'DEM_L3_AGEYRSDIST1' 
        'DEM_OBS_L3_AGEYRSDIST1' 'DEM_L3_AGEYRSDIST2' 'DEM_OBS_L3_AGEYRSDIST2' 'DEM_L3_HISPDIST' 'DEM_OBS_L3_HISPDIST' 'DEM_L3_RACEDIST' 'DEM_OBS_L3_RACEDIST' 
        'DEM_L3_SEXDIST' 'DEM_OBS_L3_SEXDIST' 'DEM_L3_GENDERDIST' 'DEM_OBS_L3_GENDERDIST' 'DEM_L3_PATPREFLANG' 'DEM_OBS_L3_PATPREFLANG' 'DEM_L3_ORIENTDIST' 
        'DEM_OBS_L3_ORIENTDIST' 'DIAGNOSIS' 'DIA_L3_N' 'DIA_L3_DX' 'DIA_L3_DXPOA' 'DIA_L3_DXSOURCE' 'DIA_L3_DXTYPE_DXSOURCE' 'DIA_L3_PDX' 'DIA_L3_DXTYPE_ENCTYPE' 
        'DIA_L3_ENCTYPE' 'DIA_L3_DX_DXTYPE' 'DIA_L3_PDX_ENCTYPE' 'DIA_L3_PDXGRP_ENCTYPE' 'DIA_L3_ADATE_Y' 'DIA_L3_ADATE_YM' 'DIA_L3_DXDATE_Y' 'DIA_L3_DXDATE_YM' 
        'DIA_L3_ORIGIN' 'DIA_L3_DXTYPE' 'DIA_L3_DXTYPE_ADATE_Y' 'DIA_L3_ENCTYPE_ADATE_YM' 'PROCEDURES' 'PRO_L3_N' 'PRO_L3_PX' 'PRO_L3_PXSOURCE' 'PRO_L3_PPX' 
        'PRO_L3_ENCTYPE' 'PRO_L3_PXTYPE_ENCTYPE' 'PRO_L3_ADATE_Y' 'PRO_L3_ADATE_YM' 'PRO_L3_PXDATE_Y' 'PRO_L3_PXTYPE' 'PRO_L3_PXTYPE_ADATE_Y' 
        'PRO_L3_ENCTYPE_ADATE_YM' 'PRO_L3_PX_PXTYPE' 'ENROLLMENT' 'ENR_L3_N' 'ENR_L3_BASEDIST' 'ENR_L3_CHART' 'VITAL' 'VIT_L3_N' 'VIT_L3_VITAL_SOURCE' 
        'VIT_L3_BP_POSITION_TYPE' 'VIT_L3_SMOKING' 'VIT_L3_TOBACCO' 'VIT_L3_TOBACCO_TYPE' 'VIT_L3_MDATE_Y' 'VIT_L3_MDATE_YM' 'VIT_L3_HT' 'VIT_L3_HT_DIST' 
        'VIT_L3_WT' 'VIT_L3_WT_DIST' 'VIT_L3_DIASTOLIC' 'VIT_L3_SYSTOLIC' 'VIT_L3_BMI' 'VIT_L3_DASH1' 'DISPENSING' 'DISP_L3_N' 'DISP_L3_DOSEUNIT' 
        'DISP_L3_ROUTE' 'DISP_L3_SOURCE' 'DISP_L3_NDC' 'DISP_L3_DDATE_Y' 'DISP_L3_DDATE_YM' 'DISP_L3_SUPDIST2' 'DISP_L3_DISPAMT_DIST' 'DISP_L3_DOSE_DIST' 
        'PRESCRIBING' 'PRES_L3_N' 'PRES_L3_RXDOSEFORM' 'PRES_L3_BASIS' 'PRES_L3_DISPASWRTN' 'PRES_L3_FREQ' 'PRES_L3_PRNFLAG' 'PRES_L3_RXDOSEODRUNIT' 
        'PRES_L3_ROUTE' 'PRES_L3_SOURCE' 'PRES_L3_RXCUI' 'PRES_L3_RXCUI_TIER' 'PRES_L3_ODATE_Y' 'PRES_L3_ODATE_YM' 'PRES_L3_RXCUI_RXSUP' 'PRES_L3_SUPDIST2' 
        'PRES_L3_RXQTY_DIST' 'PRES_L3_RXREFILL_DIST' 'PRES_L3_RXDOSEODR_DIST' 'MED_ADMIN' 'MEDADM_L3_N' 'MEDADM_L3_DOSEADMUNIT' 'MEDADM_L3_ROUTE' 
        'MEDADM_L3_SOURCE' 'MEDADM_L3_TYPE' 'MEDADM_L3_DOSEADM' 'MEDADM_L3_CODE_TYPE' 'MEDADM_L3_RXCUI_TIER' 'MEDADM_L3_SDATE_Y' 'MEDADM_L3_SDATE_YM' 
        'OBS_GEN' 'OBSGEN_L3_N' 'OBSGEN_L3_MOD' 'OBSGEN_L3_TMOD' 'OBSGEN_L3_QUAL' 'OBSGEN_L3_RUNIT' 'OBSGEN_L3_TYPE' 'OBSGEN_L3_ABN' 'OBSGEN_L3_SOURCE' 
        'OBSGEN_L3_RECORDC' 'OBSGEN_L3_CODE_TYPE' 'OBSGEN_L3_SDATE_Y' 'OBSGEN_L3_SDATE_YM' 'OBSGEN_L3_CODE_UNIT' 'CONDITION' 'COND_L3_N' 'COND_L3_STATUS' 
        'COND_L3_TYPE' 'COND_L3_SOURCE' 'COND_L3_CONDITION' 'COND_L3_RDATE_Y' 'COND_L3_RDATE_YM' 'PRO_CM' 'PROCM_L3_N' 'PROCM_L3_METHOD' 'PROCM_L3_MODE' 
        'PROCM_L3_CAT' 'PROCM_L3_TYPE' 'PROCM_L3_SOURCE' 'PROCM_L3_PDATE_Y' 'PROCM_L3_PDATE_YM' 'PROCM_L3_LOINC' 'PROCM_L3_ITEMFULLNAME' 'PROCM_L3_ITEMNM' 
        'PROCM_L3_MEASURE_FULLNAME' 'PROCM_L3_MEASURENM' 'PROCM_L3_MEASURE_LOINC' 'PROVIDER' 'PROV_L3_N' 'PROV_L3_NPIFLAG' 'PROV_L3_SPECIALTY' 
        'PROV_L3_SPECIALTY_GROUP' 'PROV_L3_SPECIALTY_ENCTYPE' 'PROV_L3_SEX' 'LDS_ADDRESS_HISTORY' 'LDSADRS_L3_N' 'LDSADRS_L3_ADRSUSE' 'LDSADRS_L3_ADRSTYPE' 
        'LDSADRS_L3_ADRSPREF' 'LDSADRS_L3_ADRSSTATE' 'LDSADRS_L3_ADRSCITY' 'LDSADRS_L3_ADRSZIP5' 'LDSADRS_L3_ADRSZIP9' 'IMMUNIZATION' 'IMMUNE_L3_N' 
        'IMMUNE_L3_CODE_CODETYPE' 'IMMUNE_L3_STATUS' 'IMMUNE_L3_STATUSREASON' 'IMMUNE_L3_SOURCE' 'IMMUNE_L3_DOSEUNIT' 'IMMUNE_L3_ROUTE' 'IMMUNE_L3_BODYSITE' 
        'IMMUNE_L3_MANUFACTURER' 'IMMUNE_L3_RDATE_Y' 'IMMUNE_L3_RDATE_YM' 'IMMUNE_L3_ADATE_Y' 'IMMUNE_L3_ADATE_YM' 'IMMUNE_L3_CODETYPE' 'IMMUNE_L3_DOSE_DIST' 
        'IMMUNE_L3_LOTNUM' 'OBS_CLIN' 'OBSCLIN_L3_N' 'OBSCLIN_L3_MOD' 'OBSCLIN_L3_ABN' 'OBSCLIN_L3_QUAL' 'OBSCLIN_L3_RUNIT' 'OBSCLIN_L3_TYPE' 'OBSCLIN_L3_SOURCE' 
        'OBSCLIN_L3_RECORDC' 'OBSCLIN_L3_CODE_TYPE' 'OBSCLIN_L3_SDATE_Y' 'OBSCLIN_L3_SDATE_YM' 'OBSCLIN_L3_CODE_UNIT' 'OBSCLIN_L3_LOINC' 'LAB_RESULT_CM' 
        'LAB_L3_N' 'LAB_L3_SOURCE' 'LAB_L3_PRIORITY' 'LAB_L3_QUAL' 'LAB_L3_MOD' 'LAB_L3_LOW' 'LAB_L3_HIGH' 'LAB_L3_PX_TYPE' 'LAB_L3_ABN' 'LAB_L3_RSOURCE' 
        'LAB_L3_LSOURCE' 'LAB_L3_UNIT' 'LAB_L3_LOC' 'LAB_L3_RECORDC' 'LAB_L3_LOINC_UNIT' 'LAB_L3_SNOMED' 'LAB_L3_LOINC' 'LAB_L3_RDATE_Y' 'LAB_L3_RDATE_YM' 
        'LAB_L3_PX_PXTYPE' 'LAB_L3_LOINC_RESULT_NUM' 'LAB_HISTORY' 'LABHIST_L3_N' 'LABHIST_L3_LOINC' 'LABHIST_L3_SEXDIST' 'LABHIST_L3_RACEDIST' 'LABHIST_L3_UNIT' 
        'LABHIST_L3_LOW' 'LABHIST_L3_HIGH' 'LABHIST_L3_MIN_WKS' 'LABHIST_L3_MAX_WKS' 'LABHIST_L3_PDSTART_Y' 'LABHIST_L3_PDEND_Y' 'XTBL_L3_NON_UNIQUE' 
        'XTBL_L3_DATES' 'XTBL_L3_DATE_LOGIC' 'XTBL_L3_TIMES' 'XTBL_L3_MISMATCH' 'XTBL_L3_RACE_ENC' 'XTBL_L3_DASH1' 'XTBL_L3_DASH2' 'XTBL_L3_DASH3' 
        'XTBL_L3_METADATA' 'XTBL_L3_REFRESH_DATE' 'XTBL_L3_LAB_ENCTYPE' 'XTBL_L3_PRES_ENCTYPE' 'XTBL_L3_MEDADM_ENCTYPE' 'XTBL_L3_ZIP5_5Y' 'XTBL_L3_ZIP5_1Y' 
        'HASH_TOKEN' 'HASH_L3_N' 'HASH_L3_TOKEN_AVAILABILITY' 'PCORNET_TRIAL' 'TRIAL_L3_N');

    do order=1 to dim(txt);
        query=txt(order);
        output;
    end;
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
