[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)  [![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# How fast are countries vaccinating?

This is an ETL pipeline to pull daily vaccinations data from [Our World in Data ](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations) and load it into the data warehouse (Google BigQuery). This project aims to show the vaccination progress per country/region. 

Check out the [dashboard](https://share.streamlit.io/julingc/vaccinations-monitor/main/vaccinations_app.py) that is built using daily vaccinations data. 


## Architecture
![arch](https://github.com/julingc/vaccinations-monitor/blob/main/image/Architecture_diagram.png)

This project uses Airflow to schedule the ETL workflow to run every 24 hours.

## DAG View

![dag](https://github.com/julingc/vaccinations-monitor/blob/main/image/DAG_Graph.png)



## Setup

### Pre-requisites

1. [Google Cloud Platform account](https://cloud.google.com)
2. [Streamlit](https://docs.streamlit.io/en/stable/)

