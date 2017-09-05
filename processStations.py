##############################################################################################################
# Nicholas Tulli - nick.tulli95@gmail.com
# Script completed in March 2017 for METEO 498a - The Pennsylvania State University
#
# Purpose: Read 'stations.txt', a file containing information for global meteorological observation sites,
#          extract stations' geospatial information, create list of dictionaries containing said data,
#		   and insert created dictionaries into MySQL database hosted by Department of Meteorology and
#		   Atmospheric Science.
#
# File 'stations.txt' authored by Greg Thompson of NCAR and continuously maintained at
#      'http://www.rap.ucar.edu/weather/surface/stations.txt'
#
# File 'stations.txt' provided to course enrollees for use in METEO 498a
#
# Final list of dictionaries to be used at later time in METEO 498a for storage in database hosted on
#       department servers.
#
# Script written for Python 2.7.7 on Department of Meteorology and Atmospheric Science servers
##############################################################################################################

# import libraries
import re
import mysql.connector

# establish connection to MySQL server (credentials removed)
cnx = mysql.connector.connect(user='nat5142', password='#########', host='#########', database='#########')
# mysql connection cursor - database insertions will be done from a list of dictionaries
cursor = cnx.cursor(dictionary=True) 

# relative path to file stations.txt
filename = '../../meteo498aData/DATA/stations.txt'

# open file and read lines
stations = open(filename, 'r')
lines = stations.readlines()

data = [] # an empty list which will be populated later in script

# dictionary 'elements' created
# 		file 'stations.txt' is formatted in a manner so that contents of each column remain constant through document.
# 		thanks to this, we can define 'start' and 'end' values in a nested dictionary to retrieve the desired information
#		from the file.
elements={}
elements['state']=  {'start': 0,  'end': 2}
elements['name']=   {'start': 3,  'end': 19}
elements['id']=     {'start': 20, 'end': 24}
elements['lat']=    {'start': 39, 'end': 45}
elements['lon']=    {'start': 47, 'end': 54}
elements['elev']=   {'start': 55, 'end': 59}
elements['country']={'start': 81, 'end': 83}

# lines matching the 'noDataRE' regular expression will be omitted from processing
# for this assignment, we were instructed to process only stations within the United States
noDataRE = '^(?:!|\s{4,}|\w{3,}|CD|\w{1}(?:\s|\.)|\n|EL)'

# for each line in file 'stations.txt':
for line in lines:

	parse = {} # an empty dictionary to be populated during each cycle of for loop

	# if 'noDataRE' matches the beginning of a line in 'stations.txt', it will be ignored.
	if re.match(noDataRE, line):
		continue

	# for each key in dictionary 'elements':
	for key in elements:
		# the key names of dictionary 'elements' become the key names for dictionary 'parse'
		parse[key] = line[elements[key]['start']:elements[key]['end']].strip() # additional whitespace stripped

		# if the 'elements' key being processed is 'lat' OR 'lon':
		if re.match('lat|lon', key):
			# 'match' is a regular expression function including three capturing groups
			# 		capturing group \d{1,3} will obtain the latitude/longitude degrees
			#		capturing group \d{2} will obtain the latitude/longitude minutes
			# 		capturing group N|S|E|W will obtain the latitude/longitude's direction
			match = re.search('(\d{3})\s(\d{2})(N|S|E|W)', line[elements[key]['start']:elements[key]['end']])

			# our assignment included instructions to convert the latitude/longitude from degrees and minutes to
			# 	decimal degrees. that conversion takes place below
			try:
				parse[key] = float(match.group(1)) + (float(match.group(2)) / 60)
				parse[key] = round((parse[key]),2)

				# if the directional capturing group is either South or West, we must multiply decimal degree value by -1
				if match.group(3) == 'S' or match.group(3) == 'W':
					parse[key]*= -1
			except AttributeError:
				continue
		else:
			# if a matched section of the file contains only white space, make the corresponding key None
			#	this allows 'NULL' to be entered into the database in lieu of blank white space
			if re.match('^\s+$', line[elements[key]['start']:elements[key]['end']]):
				parse[key]=None

	# some stations in the file do not contain ICAO ID values, only white space in columns 20:24
	# 	should the computer encounter one of these records, they should be omitted from final database insertion
	if parse['id'] == None:
		continue
	else:
		# if an ICAO ID value exists for the station in question, append the constructed dictionary to the
		# 	originally empty list 'data'
		data.append(parse)
		# NOTE: variable 'parse' will be wiped clean at the beginning of each instance of the outermost for loop
		#	this ensures data is not overwritten and allows us to store the dictionaries in a list

# a string containing our SQL insert statement
insert_stmt = "INSERT INTO stations(id, name,state, country, lat, lon, elev) VALUES"
insert_stmt += "(%(id)s, %(name)s, %(state)s, %(country)s, %(lat)s, %(lon)s, %(elev)s)"


# for each dictionary in the list 'data':
for dictionary in data:
	# execute the insertion statement
	#	the MySQL/Python connector recognizes key names of each dictionary and inserts correspondingly
	cursor.execute(insert_stmt, dictionary)
	# print the dictionaries being inserted for debugging
	print(dictionary)

# close MySQL connection
cnx.close()



