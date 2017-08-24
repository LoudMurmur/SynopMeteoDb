#!/usr/bin/env python
# --*-- encoding: utf-8 --*--

"""

This script will retrieve all the meteo data from 1996 (furthest available)

https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32

"""
import urllib
import os
import time
import datetime
import gzip
import csv
import shutil
import numpy as np

import sqlite3

from databasemanager import DatabaseManager

CSVS_DIR = "METEODATA"
YEAR_1996 = 1996
MONTH_1 = 1
DATABASENAME = "synop.db"
DATABASE_PATH = os.path.join('db', DATABASENAME)
SYNOP_TABLE = "synop_data"
TEMP_DIR = 'TEMP'

def make_dir_if_not_exist(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def month_year_iter(start_month, start_year, end_month, end_year):
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month
    for ym in range( ym_start, ym_end ):
        y, m = divmod( ym, 12 )
        date = '{}{}'.format(y, str(m+1).zfill(2))
        yield date

def getDateForDataUpdate(dbmanager):
	current_synop_date = time.strftime("%Y%m")
	end_year = int(current_synop_date[:4])
	end_month = int(current_synop_date[4:])
	try:
		highest_ts = dbmanager.get_highest_timestamp_from_meteo_data(SYNOP_TABLE)
		datime_date = datetime.datetime.fromtimestamp(highest_ts)
		synop_date = datime_date.strftime("%Y%m")
		start_year = int(synop_date[:4])
		start_month = int(synop_date[4:])
		return start_month, start_year, end_month, end_year, highest_ts
	except:
		return MONTH_1, YEAR_1996, end_month, end_year, None

def retrieve_csv_data_for(dbmanager, start_month, start_year, end_month, end_year):
	make_dir_if_not_exist(CSVS_DIR)
	for file_date in month_year_iter(start_month, start_year, end_month, end_year):
		file_url = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.{}.csv.gz".format(file_date)
		output_file_name = "{}.csv.gz".format(file_date)
		print "downloading {} to {}".format(file_url, os.path.join(CSVS_DIR, output_file_name))
		time.sleep(2) # without it the website will blacklist you after 20-30 downloads
		urllib.urlretrieve(file_url, os.path.join(CSVS_DIR, output_file_name))

def delete_folder(folder_path):
	shutil.rmtree(folder_path)

def extract_all_csv_to_temp(start_month, start_year, end_month, end_year):

	all_csv_archive_files_names = []
	for synop_date in month_year_iter(start_month, start_year, end_month, end_year):
		all_csv_archive_files_names.append("{}.csv.gz".format(synop_date))

	for i, file_name in enumerate(all_csv_archive_files_names):
		print "extracting {} ({}/{})".format(file_name, i+1, len(all_csv_archive_files_names))
		filename = os.path.join(CSVS_DIR, file_name)
		out_filename = os.path.join(TEMP_DIR, os.path.splitext(file_name)[0])
		inF = gzip.open(filename, 'rb')
		outF = open(out_filename, 'wb')
		outF.write(inF.read())
		inF.close()
		outF.close()

def convert_datetime_to_timestamp(datetime_object):
	return int((datetime_object - datetime.datetime(1970, 1, 1)).total_seconds())

def store_csvs_in_database(dbmanager, start_month, start_year, end_month, end_year, highest_ts):

	def remove_csv_header(csv_file):
		return csv_file[1:]
	
	def get_csv_header(csv_file):
		return csv_file[0]
	
	def get_column_for_name(csv_filename, header, name):
		for i, element in enumerate(header):
			if element == name:
				return i
		raise Exception("{} is not present in csv header for {}".format(csv_filename))
	
	def get_int_data(str_data):
		if str_data == "mq":
			return int(0)
		return int(str_data)
	
	def get_float_data(str_data):
		if str_data == "mq":
			return float(0)
		return float(str_data)
	
	def prepare_data_for_db_insert(csv_filename, csv_header, csv_data):
		data = []
		for line in csv_data:
			char_date = str(line[get_column_for_name(csv_filename, csv_header, 'date')])
			datetime_object = datetime.datetime.strptime(char_date, '%Y%m%d%H%M%S') #http://strftime.org/
			ts = convert_datetime_to_timestamp(datetime_object)
			meteo_data = [
							get_int_data(line[get_column_for_name(csv_filename, csv_header, 'numer_sta')]),
							ts,
							get_int_data(line[get_column_for_name(csv_filename, csv_header, 'pmer')]),
							get_float_data(line[get_column_for_name(csv_filename, csv_header, 'ff')]),
							get_float_data(line[get_column_for_name(csv_filename, csv_header, 't')]),
							get_int_data(line[get_column_for_name(csv_filename, csv_header, 'u')]),
							get_float_data(line[get_column_for_name(csv_filename, csv_header, 'ht_neige')]),
							get_float_data(line[get_column_for_name(csv_filename, csv_header, 'rr24')])
						]
			data.append(meteo_data)
		return data

	all_csv_files_names = []
	for synop_date in month_year_iter(start_month, start_year, end_month, end_year):
		all_csv_files_names.append("{}.csv".format(synop_date))
	
	for i, file_name in enumerate(all_csv_files_names):
		print "processing {} ({}/{})".format(file_name, i+1, len(all_csv_files_names))
		with open(os.path.join(TEMP_DIR, file_name), 'rb') as csvfile:
			csv_file = list(csv.reader(csvfile, delimiter=';'))
			csv_data = remove_csv_header(csv_file)
			csv_header = get_csv_header(csv_file)
			data  = prepare_data_for_db_insert(file_name, csv_header, csv_data)
			try:
				dbmanager.insert_into_meteo_table(SYNOP_TABLE, data)
			except sqlite3.IntegrityError:
				print u"unicity issue, trying to insert datablock line by line to remove duplicated lines"
				dbmanager.insert_into_meteo_table_one_by_one(SYNOP_TABLE, data)

def update_synop_data(dbmanager):
	start_month, start_year, end_month, end_year, highest_ts = getDateForDataUpdate(dbmanager)
	print "Making temp folder"
	make_dir_if_not_exist(TEMP_DIR)
	
	if (start_month == end_month) and (start_year == end_year):
		print "uptading just this month missing data"
	else:
		print "updating data from {}-{} to {}-{}".format(start_year, str(start_month).zfill(2), end_year, str(end_month).zfill(2))

	print "Downloading csv(s)"
	retrieve_csv_data_for(dbmanager, start_month, start_year, end_month, end_year)
	print "Extracting csv in temp folder for parsing and insertion in DB"
	extract_all_csv_to_temp(start_month, start_year, end_month, end_year)
	store_csvs_in_database(dbmanager, start_month, start_year, end_month, end_year, highest_ts)
	delete_folder(TEMP_DIR)


dbmanager = DatabaseManager(DATABASE_PATH)
update_synop_data(dbmanager)
