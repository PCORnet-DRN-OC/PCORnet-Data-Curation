/*******************************************************************************/
*  Name: 	01_run_potential_code_errors.sas
*  Date: 	2024/01/11

*  Purpose: Run the Potential Code Errors portion of Data Curation Query Package v6.12 
* 
*   Inputs: 
*	    1) PCORnet CDM v6.1 SAS data stores (datasets or views) 
*	    2) SAS program:  /infolder/potential_code_errors.sas
*       3) SAS dataset:  /infolder/loinc.cpt                            
*
*  Outputs:
**    	1) Up to 13 SAS datasets stored in /dmlocal containing all records which violate one or more of the heuristics
*    		for the given code type, and a summary dataset of the results:
*			bad_dx.sas7bdat
*	    	bad_px.sas7bdat
* 			bad_condition.sas7bdat
*			bad_immunization.sas7bdat
*	    	bad_pres.sas7bdat
*	    	bad_disp.sas7bdat
*	    	bad_lab.sas7bat
*			bad_lab_hist.sas7bat
*			bad_medadmin.sas7bdat
*			bad_obsgen.sas7bdat
*			bad_obsclin.sas7bdat
*			misplaced_loincs.sas7bat
*       	code_summary.sas7bdat (NOTE: this dataset is used by the EDC portion of the query package)
*	 	2) CSV versions of the SAS datasets stored in /dmlocal
*	 	3) Potential_Code_Errors.pdf stored in /drnoc (contains a partial print of key fields from each of the above datasets)
*	 	4) [DataMartID]_[Date]_code_summary.cpt stored in /drnoc
*	 	5) SAS log file of potential code errors stored in /drnoc (<DataMart Id>_<response date>_potential_code_errors.log)

*
*  Requirements:  
*                1) Program run in SAS 9.3 or higher
*                2) Each site is required to create the following sub-directory 
*                   structure on its network/server. The subdirectories must 
*                   reside within an outer folder which contains the query name 
*                  (e.g. PROD_P02_DQA_FDPRO_DCQ_NSD_q610_v01)
*                   a) /dmlocal: folder containing output generated by the 
*                       request that should be saved locally but not returned to
*                       DRN OC. Output may be used locally or to facilitate 
*                       follow-up queries.
*                   b) /drnoc: folder containing output generated by the request 
*                       that should be returned to the DRN OC Data Curation team. These tables 
*						consist of aggregate  data/output and transfer the minimum required to answer
*                       the analytic question.
*                   c) /sasprograms: folder containing the master SAS program 
*                       that must be edited and then executed locally.
*                   d) /infolder: folder containing all input and lookup files 
*                       needed to execute request. Input files are created for 
*                       each request by the DRN OC Data Curation team. The 
*                       contents of this folder should not be edited.
*
*
*  Actions needed: 
*               1) User provides libname path where data resides (section below)
*                     (Example: /ct/pcornet/data/)
*               2) User provides root network/server path where the required
*                     sub-directory structure has been created (section below) 
*                     (Example: /ct/pcornet/queries/PROD_P02_DQA_FDPRO_DCQ_NSD_q610_v01/) 
*                  ###  DO NOT add sub-directory structure (i.e. /dmlocal) ###
/*******************************************************************************/

/********************************************************************************/
* Provide user defined values
/********************************************************************************/;
/********************************************************************************/;
 /*Enter directory where data is stored:*/             %let dpath=;
 /*Enter network/server directory: */                  %let qpath=;
/********************************************************************************
* End of user provided values
*********************************************************************************/


/********************************************************************************
* Submit code errors program
*********************************************************************************/
%include "&qpath.infolder/potential_code_errors.sas";
