# Covid-19 data analysis

The following analytics has been done:
- Month, Year and countryterritoryCode wise **Infection Rate** and **Death Rate**.

Where,
>Infection Rate = (cases / TestPerformed) * 100%  
Death Rate = (deaths / cases) * 100%

## Setup Used for this analysis
- Number of hadoop cluster = 2
- Python code written on pycharm
- All the outputs is processed in Apache Spark using Standalone Cluster Mode
- The spark job is scheduled to run in every hour at 15 minutes
- Dataset is provided by BdRen.
