/*******************************************************************************
*  $Source: 04_run_edc_report $;
*    $Date: 2020/05/28
*    Study: PCORnet
*
*  Purpose: Run the Empirical Data Curation portion of PCORnet Data Curation Query Package v5.13
* 
*   Inputs: 
*	   1) PCORnet CDM v5.1 SAS data stores (datasets or views) 
*	   2) SAS programs and files: /infolder/normalization.sas, 
*                                     required_structure.cpt,
*                                     required_toc.cpt,
*                                     edc_template.sas,
*                                     edc_report.sas
*
*  Outputs: 
*          1) normalization.sas - SAS dataset normalizing the DC results_
*                                 <DataMart Id>_<response date>_dc_norm.sas7bdat
*          2) required_structure.cpt - None
*          3) edc_template.sas - None
*          4) edc_report.sas - Print of compiled EDC report in RTF file format 
*              stored as (<DataMart Id>_<response date>_EDCRPT.rtf)
*
*  Requirements:  
*                1) Program run in SAS 9.3 or higher with the SAS/GRAPH module ("proc setinit; run" to confirm)
*                2) Each site is required to create the following sub-directory 
*                   structure on its network/server. The subdirectories must 
*                   reside within an outer folder which contains the query name 
*                   from the DRN Query Tool (e.g. PROD_P02_DQA_FDPRO_DCQ_T1D3_r001_v01)
*                   a) /dmlocal: folder containing output generated by the 
*                       request that should be saved locally but not returned to
*                       DRN OC. Output may be used locally or to facilitate 
*                       follow-up queries.
*                   b) /drnoc: folder containing output generated by the request 
*                       that should be returned to the DRN OC via the PCORnet DRN
*                       Query Tool (DRN QT). These tables consist of aggregate 
*                       data/output and transfer the minimum required to answer
*                `       the analytic question.
*                   c) /sasprograms: folder containing the master SAS program 
*                       that must be edited and then executed locally.
*                       (i.e. RUN_EDC_REPORT.SAS)
*                   d) /infolder: folder containing all input and lookup files 
*                       needed to execute request. Input files are created for 
*                       each request by the DRN OC Data Curation team; the 
*                       contents of this folder should not be edited.
*                       (i.e.EDC_REPORT.SAS)
*
*  Actions needed: 
*               1) User provides libname path where data resides (section below)
*                     (Example: /ct/pcornet/data/).  
*				2) User provides root network/server paths where the required
*                     sub-directory structure has been created (section below) 
*                     (Example: /ct/pcornet/queries/PROD_P02_DQA_FDPRO_DCQ_T1D3_r001_v01/) 
*               3) User provides root network server paths where the required
*                     sub-directory structure was created for the most recently 
*                     approved DataMart refresh (section below)
*			    4) User runs the proc product_status procedure (proc product_status; run) 
				to determine if SAS ETS is both licensed and installed.  
				If SAS_ETS is listed, enter "yes" for %let _ets_installed. 
				Otherwise enter "no" (default="yes")

*******************************************************************************/

********************************************************************************
* Provide user defined values
********************************************************************************;
 /*Enter directory where data is stored:*/             %let dpath=;
 /*Enter directory where data from the most recently
     approved DataMart refresh is stored:            */ %let ppath=;
 /*Enter network/server directory:                   */ %let qpath=; 
 /*Enter SAS ETS licensed and installed:             */ %let _ets_installed=yes; 
********************************************************************************
* End of user provided values
*******************************************************************************;


********************************************************************************
* Submit NORMALIZATION program
********************************************************************************;
%include "&qpath.infolder/normalization.sas";
********************************************************************************;

********************************************************************************
* Submit EDC report program
********************************************************************************;
%include "&qpath.infolder/edc_report.sas";
********************************************************************************;