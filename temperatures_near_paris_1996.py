#!/usr/bin/env python
# --*-- encoding: utf-8 --*--

import datetime
import os
import plotly.graph_objs as go

from plotly.offline import download_plotlyjs, init_notebook_mode, plot
from databasemanager import DatabaseManager

ORLY_CODE = int("07149")
DATABASENAME = "synop.db"
DATABASE_PATH = os.path.join('db', DATABASENAME)
SYNOP_TABLE = "synop_data"

def convert_datetime_to_timestamp(datetime_object):
	return int((datetime_object - datetime.datetime(1970, 1, 1)).total_seconds())

janv1_1996 = convert_datetime_to_timestamp(datetime.datetime(1996, 1, 1, 0, 0, 1)) #year, month, day, hour, minute, second
dec31_1996 = convert_datetime_to_timestamp(datetime.datetime(1996, 12, 31, 23, 59, 59))
dbmanager = DatabaseManager(DATABASE_PATH)
data = dbmanager.select_data_from_meteo_table_between(SYNOP_TABLE, ORLY_CODE, janv1_1996, dec31_1996)

dates = []
temperatures = []

for line in data:
	if line.temperature != 0:
		temperatures.append(line.temperature-273.15) #because it's in kelvin
		dates.append(datetime.datetime.fromtimestamp(line.date_timestamp))

trace = go.Scatter(
    x = dates,
    y = temperatures
)

plot([trace], filename='graphs/temperature_near_paris_1996.html')
