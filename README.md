
Nick Tulli - nick.tulli95@gmail.com (nat5142@psu.edu)

This repository contains a list of files created during the Spring 2017 semester for Meteo 498a - Scientific Database Management.

Files in this repository:

processStations.py:
	- Students were prompted to extract meteorological data station information from the file 'stations.txt'
	- Regular expressions were utilized to define which lines of 'stations.txt' were to be processed, and which were to be ignored.
	- Extracted data was then inserted into a MySQL database

metarDecoder_nat5142.py:
	- Using data inserted from 'processStations.py', a series of 5-minute interval US surface observations (METARs) were processed and organized for insertion into a new table in the existing database
	- Regular expressions were again utilized for data extraction
	- MySQL connections handled in Python














