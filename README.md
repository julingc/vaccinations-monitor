[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/yourGitHubName/yourRepo/yourApp/)

# How fast are countries vaccinating?

This is an ETL pipeline to pull daily vaccinations data from [Our World in Data ](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations) and load it into the data warehouse (Google BigQuery). This project aims to show the vaccination progress per country/region.

Check out the [dashboard](https://share.streamlit.io/julingc/vaccinations-monitor/main/vaccinations_app.py) which is built using daily vaccinations data.

## Architecture

![arch](https://github.com/julingc/vaccinations-monitor/blob/main/image/Architecture_diagram.png)

This project uses Airflow to schedule the ETL workflow to run every 24 hours.

## DAG View

1. Pull vaccinations data from [Our World in Data ](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations) and save it as a local csv file. This function also pushes the number of rows in vaccinations data into an XCom.
2. Get the number of rows in the Google BigQuery warehouse.
3. Compare the number of rows in [Our World in Data ](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations) and Google BigQuery warehouse to
   - Delete the local csv file if there is no new data needs to load into the data warehouse or
   - Load data into the datawarehouse and delete the local csv file

![dag](https://github.com/julingc/vaccinations-monitor/blob/main/image/DAG_Graph.png)

## Setup

### Pre-requisites

1. [Google Cloud Platform account](https://cloud.google.com)
2. [Streamlit](https://docs.streamlit.io/en/stable/)
