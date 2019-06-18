/*******************************************************************************
*  $Source: edc_prep $;
*    $Date: 2018/09/13
*    Study: PCORnet
*
*  Purpose: To test the existance of PCORnet Data Curation Query Package v4.0
*              query datasets prior to producing the EDC report
* 
*   Inputs: DC Query Package SAS datasets
*
*  Outputs: a listing stating existing problems to resolve prior to executing
*               the EDC report
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
filename edc_ref "&qpath./infolder/edc_reference.cpt";
    
********************************************************************************;
* Determine if all queries have been run and expected query result tables exist
********************************************************************************;

proc format;
     value problem
      1="Expected query dataset not present"
      ;
run;

*- Import dataset of expected query result tables -*;
proc cimport library=work infile=edc_ref;
run;

*- Match length for merge purposes -*;
data dc_tables;
     length memname source_table $32;
     set dc_tables;
     memname=table;
run;

proc sort data=dc_tables;
     by memname;
run;

*- Get query result tables -*;
proc contents data=dmlocal._all_ out=_dmlocal noprint;
run;

proc sort data=_dmlocal(keep=memname crdate) nodupkey;
     by memname;
run;

*- Merge expected with produced -*;
data _expected;
     merge dc_tables(in=expected) _dmlocal(in=produced);
     by memname;
     if expected;

     * create flag if expected dataset not produced *;
     if expected and not produced then do;
        _expectedflag=1;
        output;
     end;
run;

proc sort data=_expected;
     by source_table memname;
run;

*- Create macro variable of the number of problems -*;
proc sql noprint;
     select count(*) into :_yquerydiag from _expected;
quit;

*- Create dummy dataset if not problems exist -*;
%if &_yquerydiag=0 %then %do;
    data _expected;
        source_table="No datasets are missing";
        array c memname _expectedflag;
    run;
%end;

title 'EDC preparation';
proc print label data=_expected;
     label memname='Query table name'
           source_table='CDM table name'
           _expectedflag='Problem'
     ;
     var source_table memname _expectedflag;
     format _expectedflag problem.;
run;
