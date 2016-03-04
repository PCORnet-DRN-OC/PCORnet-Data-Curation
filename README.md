# PCORnet Data Characterization Query Package

### Purpose
This code package examines record-level data model conformance against the PCORnet Common Data Model v3.0, and generates output tables of counts and frequencies.

The Data Characterization Query Package is a complement to the Diagnostic Query package. Data Characterization is run **after** the [Diagnostic Query package] (https://github.com/PCORnet-DRN-OC/PCORnet-Diagnostic-Query).

### PCORnet data partners run code packages distributed through PopMedNet
It's crucially important for data partners to follow the process of running the exact code module distributed to your DataMart by the Operations Center (via PopMedNet). This process ensures that end-to-end provenance is preserved. Complete instructions on how to run this code are provided to PCORnet DataMarts when the query is distributed to a given DataMart. 

### This GitHub repository is a copy for reference
PCORnet's Distributed Research Network Operations Center (DRN OC) is responsible for distribution of the production code package to data partners, which happens within PopMedNet.

Waves of data partner querying may result in more than one code package being active at any given time. Beta testing is also an important ongoing activity.

Therefore, **this repository is not the production version of the code package**, but instead serves as a copy for reference and knowledge sharing.

### System Requirements
This code is designed to run in SAS versions 9.3 or higher. No other SAS packages are necessary. 

### Scope
Output tables are produced by running the SAS code against static local DataMarts in PCORnet CDM v3.0 with SAS data types. 

CDM specifications are available at [http://www.pcornet.org/pcornet-common-data-model/] (http://www.pcornet.org/pcornet-common-data-model/). This version of the query allows DataMarts to incorporate [CDM errata] (https://github.com/CDMFORUM/CDM-ERRATA) identified as of January 20, 2016.

### Acknowledgments
This code package was developed by the Data Characterization Development Team in the DRN OC, with members from the Duke Clinical Research Institute. We gratefully acknowledge their work and individual contributions.
