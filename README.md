### *This README is intended to provide orientation to DC Query Package v6.14.*  
### *Previous versions are provided for historical reference only.*

# PCORnet Data Curation Query Package

### Version 6.14

### Purpose
The purpose of the Data Curation Query Package v6.14 is to characterize the data in PCORnet Common Data Model (CDM) v6.1. This package examines all 23 tables. The package consists of the Potential Code Errors query, a Data Curation query and an Empirical Data Curation Report which summarizes key information from the query output and evaluates the results against PCORnet’s Data Checks v15. Output tables will be produced by running SAS programs against static local DataMarts in PCORnet CDM v6.1 with SAS data types. 
Query results will be used by the PCORnet Coordinating Center’s Distributed Research Network Operations Center (DRN OC) to ensure a foundational level of data quality across the networks. Approved results may be used to provide initial feasibility estimates for prep-to-research queries, inform study planning activities, and to create DataMart-level, CRN-level or network-level reports. Data aggregated at the network level may be shared publicly. DataMart-level results may be published within PCORnet and data from this query can be used to inform the PCORnet Coordinating Center of data availability and fitness for use in response to PCORnet queries, to enable the Coordinating Center to share high-level counts of data variables with requestors, to present PCORnet-level Aggregate Data on public-facing websites, (e.g., PCORnet.org) in manuscripts consistent with the guidelines of the International Committee of Medical Journal Editors (“ICMJE”), and for use in PCORnet marketing materials. 
To provide the DRN OC with additional insight into the query results, the ETL Annotated Data Dictionary (ETL ADD) must be updated prior to submitting the response to this query.  


### PCORnet data partners run code packages distributed by Operations Center
It's crucially important for data partners to follow the process of running the exact code module distributed to your DataMart by the Operations Center. This process ensures that end-to-end provenance is preserved. Complete instructions on how to run this code are provided to PCORnet DataMarts when the query is distributed to a given DataMart. 

### This GitHub repository is a copy for reference
PCORnet's Distributed Research Network Operations Center (DRN OC) is responsible for distribution of the production code package to data partners.

Waves of data partner querying may result in more than one code package being active at any given time. Beta testing is also an important ongoing activity.

Therefore, **this repository is not the production version of the code package**, but instead serves as a copy for reference and knowledge sharing.

### System Requirements
This code is designed to run in SAS versions 9.3 or higher with SAS/GRAPH.

### Scope
Output tables are produced by running the SAS code against static local DataMarts in PCORnet CDM v6.1 with SAS data types. 

CDM specifications are available at [http://www.pcornet.org/pcornet-common-data-model/](http://www.pcornet.org/pcornet-common-data-model/). 

### Acknowledgments
This code package was developed by the Data Curation Development Team at Duke Clinical Research Institute.
