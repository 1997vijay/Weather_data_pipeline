"""
Analyze the data on the processed weather dataset..

Dependencies:
    - pandas: A Python library for data analysis.
    - matplotlib: A python library for visualization.
    - seaborn: A library built on top of matplotlib that provide more advance visualization.
    - plotly: A python library for ploting the intreactive and advance graph.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Run the code to perform data analysis on the dataset

    Author Information:
    Name: Vijay Kumar
    Date: 2nd Jan 2024

Abstract/Description:
This script read the data from processed weather file and perform analysis by plotting graphs.

Change Log:
    - 2nd Jan 2024: Initial creation.
"""

# import required libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import config

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

import warnings
warnings.filterwarnings('ignore')


# define variables
filePath=config.DATAFRAME_OUTPUT_PATH+'/'+config.DATAFRAME_OUTPUT_FILE_NAME
fileOutputPath=config.AGGREGATED_FILE_PATH

# read processed weather file
try:
    weather_df=pd.read_csv(filePath)
    print(weather_df.head(5))
except FileNotFoundError as e:
    logging.error('File not found.')
    raise e

# convert required columns to string and integer
weather_df['VisibilityCategory']=weather_df['VisibilityCategory'].astype(str)
weather_df['WindCategory']=weather_df['WindCategory'].astype(str)
weather_df['TEMP']=round(weather_df['TEMP'],2)
weather_df['DATE']=pd.to_datetime(weather_df['DATE'],format='ISO8601')

# aggregate the data based on Date
try:
    # aggregate based on date
    grouped_by_date=weather_df.groupby('DATE').agg({
        'Year':'max',
        "TEMP":"mean",
        'VISIB':'mean',
        'TempChange':"mean",
        'WindCategory':'max',
        'VisibilityCategory':'max'
    }).reset_index()
    grouped_by_date['TEMP']=round(grouped_by_date['TEMP'],2)
    grouped_by_date['VISIB']=round(grouped_by_date['VISIB'],2)
    grouped_by_date['TempChange']=round(grouped_by_date['TempChange'],2)
except Exception as e:
    raise e


# aggregate the data based on Year
try:
    # aggregate based on year
    grouped_by_year=weather_df.groupby('Year').agg({
        "TEMP":"mean",
        'VISIB':'mean',
        'TempChange':"mean",
    }).reset_index()

    # applying the dense rank to create the color for the bar graph
    grouped_by_year['Rank'] = grouped_by_year['TEMP'].rank(ascending=False, method='dense')
    grouped_by_year['TEMP']=round(grouped_by_year['TEMP'],2)
    grouped_by_year['VISIB']=round(grouped_by_year['VISIB'],2)
    grouped_by_year['TempChange']=round(grouped_by_year['TempChange'],2)

    # assing the colors to highlight the highest, second highest and lowest temprature bar
    grouped_by_year['Colors']=grouped_by_year['Rank'].apply(lambda row: 'red' if row == 1 else ('orange' if row == 2 else ('green' if row == grouped_by_year['Rank'].max() else 'gray')))
except Exception as e:
    raise e

# aggregate based on date and seasons
try:
    # aggregate based on date and seasons
    grouped_by_seasons=weather_df.groupby(['Season',"Year"]).agg({
        "TEMP":"mean",
        'VISIB':'mean',
        'TempChange':"mean",
    }).reset_index()
    grouped_by_seasons['TEMP']=round(grouped_by_seasons['TEMP'],2)
    grouped_by_seasons['VISIB']=round(grouped_by_seasons['VISIB'],2)
    grouped_by_seasons['TempChange']=round(grouped_by_seasons['TempChange'],2)
except Exception as e:
    raise e

#----------------------Visualization---------------------------#

# Average temprature for each year
try:
    plt.figure(figsize =(30, 10)) 
    plots=sns.barplot(x="Year", y="TEMP", data=grouped_by_year, palette=grouped_by_year['Colors'].tolist())
    # dispay the value on top of each bar
    for bar in plots.patches: 
        plots.annotate(format(bar.get_height(), ''),  
                    (bar.get_x() + bar.get_width() / 2,  
                    bar.get_height()),ha='center', va='center', 
                    size=6, xytext=(0, 8), 
                    textcoords='offset points')
    plt.title('Average Temprature for each year')
    plt.xticks(rotation=90)
    plt.show()
except Exception as e:
    raise e


# Plot Monthly and seasonal trend.
# Temprature  Distribution
try:
    # Monthly trends
    weather_df['Month'] = pd.to_datetime(weather_df['DATE']).dt.month
    monthly_mean_temp = weather_df.groupby('Month')['TEMP'].mean().reset_index()

    # Seasonal trends 
    seasonal_mean_temp = weather_df.groupby('Season')['TEMP'].mean().reset_index()

    # set the graph size
    plt.figure(figsize=(15, 7))

    # subplot(row, columns, first plot)
    plt.subplot(2, 2, 1)
    sns.lineplot(x='Month', y='TEMP', data=monthly_mean_temp, marker='o')
    plt.title('Monthly Mean Temperature')
    plt.xlabel('Month')
    plt.ylabel('Mean Temperature (°C)')

    plt.subplot(2, 2, 2)
    sns.barplot(x='Season', y='TEMP', data=seasonal_mean_temp)
    plt.title('Seasonal Mean Temperature')
    plt.xlabel('Season')
    plt.ylabel('Mean Temperature (°C)')

    # 2. Temperature Distribution
    plt.subplot(2, 2, 3)
    sns.histplot(weather_df['TEMP'], bins=20, kde=True,color='red')
    plt.title('Temperature Distribution')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Frequency')


    plt.subplots_adjust(hspace=0.5, wspace=0.5)
    plt.show()
except Exception as e:
    raise e



# Plot World Map
geo_df = weather_df[weather_df['Year']==2023].groupby('STATION NAME').agg({'TEMP':'mean','LAT':'max','LON':'max','COUNTRY NAME':'max'}).reset_index()
geo_df['TEMP']=round(geo_df['TEMP'],2)
geo_df['Rank']=geo_df['TEMP'].rank(ascending=False, method='dense')
geo_df['text'] = geo_df['STATION NAME'] + ' ' + geo_df['COUNTRY NAME'] + '-->' + geo_df['TEMP'].astype(str)

fig = go.Figure(data=go.Scattergeo(
        text = geo_df['text'],
        lat = geo_df['LAT'],
        lon = geo_df['LON'],
        mode = 'markers',
        marker_color = geo_df['Rank'],
        ))

fig.update_layout(
        geo_scope='world',
        geo = dict(resolution = 110),
        height=600,  
        width=1400,  
    )
fig.show()


# save the all the dataframe 
grouped_by_date.to_csv(f'{fileOutputPath}/aggregated_by_date.csv',index=False)
grouped_by_year.to_csv(f'{fileOutputPath}/aggregated_by_year.csv',index=False)
grouped_by_seasons.to_csv(f'{fileOutputPath}/aggregated_by_season.csv',index=False)

