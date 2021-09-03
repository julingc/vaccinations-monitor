import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import datetime as dt
from google.oauth2 import service_account
from google.cloud import bigquery


# Create API Client
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


@st.cache()
def load_data(locations, start_date, end_date):
    query = f"""
            SELECT location, date, daily_vaccinations_per_million FROM `vaccination-monitor.vaccinations.daily-vaccinations` 
            WHERE date BETWEEN DATE("{start_date}") AND DATE("{end_date}") AND location IN UNNEST(@selected_countries);
            """
    job_config = bigquery.QueryJobConfig(query_parameters=[
                                         bigquery.ArrayQueryParameter('selected_countries', 'STRING', locations), ])
    return client.query(query, job_config=job_config).result().to_dataframe()


# Retrieve all countries/regions
country_query = "SELECT DISTINCT location FROM `vaccination-monitor.vaccinations.daily-vaccinations`"
countries_df = client.query(country_query).result().to_dataframe()
countries = list(countries_df.location)

# Title & sidebar widgets
st.title('How fast are countries vaccinating?')
st.sidebar.header('Parameter setting')
selected_countries = st.sidebar.multiselect(
    'Select countries/regions', countries, default=['World'])
all_countries = st.sidebar.checkbox('Select all countries/regions')
if all_countries:
    selected_countries = countries

# Earliest obeservation date in vaccinations.csv
start_date = st.sidebar.date_input('Start Date', value=dt.date(
    2020, 12, 2), min_value=dt.date(2020, 12, 2), max_value=dt.date.today())

end_date = st.sidebar.date_input('End Date', value=dt.date.today() - dt.timedelta(
    days=2), min_value=dt.date(2020, 12, 2), max_value=dt.date.today())

# Poltly
data = load_data(selected_countries, start_date, end_date)
fig = go.Figure()
for c in data['location'].unique():
    dfp = data[data['location'] == c].pivot(
        index='date', columns='location', values='daily_vaccinations_per_million')
    fig.add_traces(go.Scatter(x=dfp.index, y=dfp[c], mode='lines', name=c))
st.plotly_chart(fig)

# Bottom text to show data source
st.write(
    'The daily vaccinations per million data (7-day smoothed) is from [Our World in Data](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations)')
st.write('Mathieu, E., Ritchie, H., Ortiz-Ospina, E. et al. A global database of COVID-19 vaccinations. Nat Hum Behav (2021). https://doi.org/10.1038/s41562-021-01122-8')
