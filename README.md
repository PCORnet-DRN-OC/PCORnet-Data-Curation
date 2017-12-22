### *This README is intended to provide orientation to DC Query Package v3.12.*  
### *v3.03, v3.04, v3.10, and v.3.11 are provided for historical reference only.*

# PCORnet Data Curation Query Package

### Version 3.12 

### Purpose
The purpose of the Data Curation Query Package v3.12 is to characterize the data in the 15 PCORnet Common Data Model (CDM) v3.1 tables. The package consists of the PCORnet code errors query, the data curation query and an Empirical Data Curation Report which summarizes key information from the data curation query output and evaluates the results against PCORnet’s Data Check v4. Output tables will be produced by running SAS programs against static local DataMarts in PCORnet CDM v3.1 with SAS data types.

### PCORnet data partners run code packages distributed through PopMedNet
It's crucially important for data partners to follow the process of running the exact code module distributed to your DataMart by the Operations Center (via PopMedNet). This process ensures that end-to-end provenance is preserved. Complete instructions on how to run this code are provided to PCORnet DataMarts when the query is distributed to a given DataMart. 

### This GitHub repository is a copy for reference
PCORnet's Distributed Research Network Operations Center (DRN OC) is responsible for distribution of the production code package to data partners, which happens within PopMedNet.

Waves of data partner querying may result in more than one code package being active at any given time. Beta testing is also an important ongoing activity.

Therefore, **this repository is not the production version of the code package**, but instead serves as a copy for reference and knowledge sharing.

### System Requirements
This code is designed to run in SAS versions 9.3 or higher with SAS/GRAPH.

### Scope
Output tables are produced by running the SAS code against static local DataMarts in PCORnet CDM v3.1 with SAS data types. 

CDM specifications are available at [http://www.pcornet.org/pcornet-common-data-model/](http://www.pcornet.org/pcornet-common-data-model/). 

### Acknowledgments
This code package was developed by the Data Curation Development Team in the DRN OC, with members from the Duke Clinical Research Institute. We gratefully acknowledge their work and individual contributions.
