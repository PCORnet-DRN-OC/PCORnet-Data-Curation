### *This README is intended to provide orientation to DC Query Package v4.15.*  
### *Previous versions are provided for historical reference only.*

# PCORnet Data Curation Query Package

### Version 4.15 

### Purpose
The purpose of the Data Curation Query Package v4.15 is to characterize the data in 19 of the PCORnet Common Data Model (CDM) v4.1 tables. The package consists of a code errors query, a data curation query and an Empirical Data Curation Report which summarizes key information from the query output and evaluates the results against PCORnet’s Data Check v6. Output tables will be produced by running SAS programs against static local DataMarts in PCORnet CDM v4.1 with SAS data types.

### PCORnet data partners run code packages distributed through PopMedNet
It's crucially important for data partners to follow the process of running the exact code module distributed to your DataMart by the Operations Center (via PopMedNet). This process ensures that end-to-end provenance is preserved. Complete instructions on how to run this code are provided to PCORnet DataMarts when the query is distributed to a given DataMart. 

### This GitHub repository is a copy for reference
PCORnet's Distributed Research Network Operations Center (DRN OC) is responsible for distribution of the production code package to data partners, which happens within PopMedNet.

Waves of data partner querying may result in more than one code package being active at any given time. Beta testing is also an important ongoing activity.

Therefore, **this repository is not the production version of the code package**, but instead serves as a copy for reference and knowledge sharing.

### System Requirements
This code is designed to run in SAS versions 9.3 or higher with SAS/GRAPH.

### Scope
Output tables are produced by running the SAS code against static local DataMarts in PCORnet CDM v4.1 with SAS data types. 

CDM specifications are available at [http://www.pcornet.org/pcornet-common-data-model/](http://www.pcornet.org/pcornet-common-data-model/). 

### Acknowledgments
This code package was developed by the Data Curation Development Team in the DRN OC, with members from the Duke Clinical Research Institute.
