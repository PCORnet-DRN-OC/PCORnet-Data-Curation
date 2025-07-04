/*******************************************************************************
*    Name: 02_run_queries.sas
*    Date: 2025/05/27
*    Study: PCORnet
*
*  Purpose: Run the query portion of Data Curation Query Package v7.00
* 
*   Inputs: 
*	    1) PCORnet CDM v7.0 SAS data stores (datasets or views) 
*	    2) SAS programs: 		   /infolder/data_curation_tables.sas
*						 		   /infolder/data_curation_base.sas
*						 		   /infolder/data_curation_print.sas
*       3) SAS reference datasets: /infolder/dc_reference.cpt
*								   /infolder/edc_reference.cpt
*                                  /infolder/loinc.cpt

*
*  Outputs: 
*           1) SAS dataset for each query stored in /dmlocal
*                (e.g. dem_l3_n.sas7bdat)
*           2) Print of each query in PDF file format stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query.pdf)
*           3) SAS transport file of SAS datasets for each query stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_<_part>.cpt)
*           4) SAS log files of query portion stored in /drnoc
*                (<DataMart Id>_<response date>_data_curation_query_base.log)
*                (<DataMart Id>_<response date>_data_curation_query_<part>.log)
*           5) SAS dataset of DataMart metadata stored in /dmlocal
*                (datamart_all.sas7bdat)
*           6) RTF of query run time stored in /drnoc
*				 (<DataMart Id>_<response date>_data_curation_progress_report.rtf)

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
*  Actions needed: 
*               1) User provides libname path where data resides (section below)
*                     (Example: /ct/pcornet/data/)
*               2) User provides root network/server path where the required
*                     sub-directory structure has been created (section below) 
*                     (Example: /ct/pcornet/queries/PROD_P02_DQA_FDPRO_DCQ_NSD_q610_v01/) 
*                  ###  DO NOT add sub-directory structure (i.e. /dmlocal) ###
*				3) User provides the part of the query group to process (section below)
*                     (Example: Yes or No) (default is Yes for both). 
*               4) User provides whether CSV outputs are desired (section below)
*                     (Example: Yes or No) (default is No)
*			    5) User runs the proc product_status procedure (proc product_status; run) 
*				to determine if SAS ETS is both licensed and installed (section below).  
*				If SAS_ETS is listed, enter Yes for %let _ets_installed, otherwise enter No. 
*					(Example: Yes or No) (default is Yes)
*               6) User provides the lookback years if requested to do so (section below)
*                     (Example: 20) (default is 10)

*******************************************************************************/

********************************************************************************
* Provide user defined values
********************************************************************************;
 /*Enter directory where data is stored:*/              %let dpath= ;
 /*Enter network/server directory: */                   %let qpath= ;

 /*Enter whether to process query group: */             %let _part1 = Yes;
                                            			%let _part2 = Yes;

 /*Create CSV files of DMLOCAL data:*/                  %let _csv=No;

 /*Enter SAS ETS licensed and installed:*/              %let _ets_installed=Yes; 

 /*Enter lookback years. DO NOT MODIFY UNLESS 
 INSTRUCTED TO DO SO*/                                  %let lookback = 10;

********************************************************************************
* End of user provided values
*******************************************************************************;

********************************************************************************
* Submit data curation query base program
********************************************************************************;
%include "&qpath.infolder/data_curation_base.sas";


