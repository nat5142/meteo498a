##############################################################################################################
# Nicholas Tulli - nick.tulli95@gmail.com
# Assigned March 2017 for METEO 498a - The Pennsylvania State University (work in progress)
# 
# Purpose: Parse through filenames matching regular expression 'file_RE' within 'filepath' and extract METAR
#		   data from selected files for database entry at a future time
#
# Files within variable 'filepath' hosted on Penn State Dept. of Atmospheric Sciences and Meteorology servers
#
# Script written for Python 2.7.7 on Department of Atmospheric Sciences and Meteorology servers
##############################################################################################################

# module library
import os
import re
import mysql.connector

# establish connection to MySQL server (credentials removed)
cnx = mysql.connector.connect(user='nat5142', password='#########', host='#########', database='#########')
# mysql connection cursor - database insertions/selections will utilize a list of dictionaries
cursor = cnx.cursor(dictionary=True)

# relative path to directory housing numerous METAR record files
filepath = '../../meteo498aData/DATA/asos-fivemin/'

# regular expression which will be used to match files in 'filepath'
file_RE = '\d{5}K(?:Z\w{2}|\w{2}Z)\d{4}06\.dat'

# select id and stationID variables from 'stations' table for stations within the United States
# ----------- NOTE: table 'stations' was originally populated via file 'processStations.py' ----------- #
sitenames = "SELECT id, stationID FROM stations WHERE country='US'"
# execute select statement
cursor.execute(sitenames)
# variable 'sites' is a list of dictionaries, each containing keys 'id' and 'stationID' for US metar sites
sites = cursor.fetchall()


# for each site in list 'sites':
for site in sites:
	# if a file in the directory 'filepath' matches "64010"+site['id']..., it will be added to a list of files to be processed
	# 	all files begin with '64010', followed by a 4-character ICAO ID
	files = [f for f in os.listdir(filepath) if re.match("64010"+site['id'], f)]
	print files

	# regular expression library ------------------------------------------------------------------------- #
	
	#	each of these regular expressions will match a specific value in properly-constructed metars
	station_RE = '(?P<siteid>^K\w{3})'
	zdatetime_RE = '\s+(?P<utc_day>[0-2]?[0-9]|3[0-1])(?P<utc_hour>[0-1]?[0-9]|2[0-3])(?P<utc_min>[0-5][0-9])Z'
	
	# re.compile is used here to convert regular expression patterns into regular expression objects
	wind_RE = re.compile('(?P<wdir>\d{3}|VRB)(?P<wspd>\d{2})(?:KT\s|G)(?:(?P<wgust>\d{2})?KT\s)?')
	temp_RE = re.compile('(?P<temp>M?\d{2})\/(?P<dew>M?\d{2})?$') #matches temp, dew point, and 'M' signaling 'minus' value for each
	
	ldatetime_RE = '\w{3}(?P<lyear>\d{4})(?P<lmonth>\d{2})(?P<lday>\d{2})(?P<lhour>\d{2})(?P<lmin>\d{2})'
	# ---------------------------------------------------------------------------------------------------- #
	
	# database insert statement string, to be used at end of upcoming for loop
	insert_stmt = "INSERT INTO metar_screen_trial (stationID, ldatetime, zdatetime, wspd, wdir, wgust, vrb, temp, dew) "
	insert_stmt += "VALUES(%(stationID)s, %(ldatetime)s, %(zdatetime)s, %(wspd)s, %(wdir)s, %(wgust)s, %(vrb)s, %(temp)s, %(dew)s)"

	# for each file in list files:
	for filename in files:
		# 'linerecords' is a dictionary variable, where each value is initially set to None, except that of key 'stationID', which was previously queried from database
		linerecords = {'stationID':site['stationID'], 'ldatetime':None, 'zdatetime':None, 'wspd':None, 'wdir':None, 'wgust':None, 'vrb':None, 'temp':None, 'dew':None}
		
		# open the file in question
		with open(filepath+filename) as f:
			# create variable 'lines' containing content of file in question
			lines=f.readlines()
		# close the file to reduce processing requirements
		f.close()

		# for each line in the file in question:
		for line in lines:
			# search the line for strings matching the zdatetime and ldatetime regular expressions
			zdatetime = re.search(zdatetime_RE, line)
			ldatetime = re.search(ldatetime_RE, line)
			# if both zdatetime and ldatetime variables are matched in the given line:
			if zdatetime and ldatetime:
				# separate variables zdatetime and ldatetime into their capturing groups
				zdt = zdatetime.groupdict()
				ldt = ldatetime.groupdict()

				# format value for key 'ldatetime'
				linerecords['ldatetime'] = str(ldatetime.group(1) + '-' + ldatetime.group(2) + '-' + ldatetime.group(3) \
							+ ' ' + ldatetime.group(4) + ':' + ldatetime.group(5))

				# because of the discrepancy between UTC and US local times, and the fashion in which each are printed in metars,
				# 	we need to ensure that dates and times are accurate within the last 4-7 hours of each month and year

				# if the UTC date is greater than or equal to the local date, no conversion must be made, but UTC must be structured properly
				if zdt['utc_day'] >= ldt['lday']:
					linerecords['zdatetime'] = str(ldatetime.group(1) + '-' + ldatetime.group(2) + '-' + zdatetime.group(1) \
								+ ' ' + zdatetime.group(2) + ':' + zdatetime.group(3))

				# if the UTC date is LESS THAN the local date (i.e. 01APR/31MAR), conversions are required
				else:
					# if the record corresponds to December 31st, the UTC year must be increased by 1, and month must be reset to 01
					if ldt['lday'] == 31 and ldt['lmonth'] == 12:
						linerecords['zdatetime'] = str((int(ldatetime.group(1))+1) + '-' + str(01) + '-' + str(01) \
								+ ' ' + zdatetime.group(2) + ':' + zdatetime.group(3))
					# if the record corresponds to the last day of a month that is NOT December 31st, only UTC month must be increased by 1
					else:
						linerecords['zdatetime'] = str(ldatetime.group(1) + '-' + str(int(ldatetime.group(2))+1) + '-' + zdt['utc_day'] \
								+ ' ' + zdatetime.group(2) + ':' + zdatetime.group(3))
		
			# split the line in question by white space and give each section the alias 'token'
			for token in re.split('\s',line):
				# use the temperature regular expression to match a temperature value in the line
				temp = temp_RE.match(token)
				# if the line above finds a match, signaling that the token does contain a temperature:
				if temp:
					# 'temps' is a dictionary containing all alised capturing groups found in temp_RE
					temps = temp.groupdict()

					# loop over each key, value pair in dictionary temps
					for k,v in temps.items():
						# if a value exists for the key in question:
						if v:
							# if the two digits representing either temperature or dew point do not contain a leading 'M', no change required
							if re.match('^\d+', v):
								temps[k] = int(v)

							# if the temperature or dew point value DOES contain a leading 'M', value must be turned negaitve, excluding leading 'M'
							if re.match('^M\d+', v):
								temps[k] = -int(v[1:])
						# update dictionary 'linerecords' with the values in the 'temps' dictionary		
						linerecords.update(temps)
				
				# use the wind regular expression to match appropriate string in token
				wind = wind_RE.match(token)	
				# if the line above finds a match, indicating that the token does contain a wind string:
				if wind:
					# if VRB appears in wind string, set linerecords['VRB'] equal to string 'VRB' and make linerecords['wdir'] NULL
					if wind.group(1) == 'VRB':
						linerecords['vrb'] = wind.group(1)
						linerecords['wdir'] = None

					# if match group does not equal 'VRB', set linerecords['wdir'] equal to integer value in captured group, and make linerecords['VRB'] NULL
					else:
						linerecords['wdir'] = int(wind.group(1))
						linerecords['vrb'] = None

					# regardless of VRB matching, set linerecords['wspd'] to value of second capturing group
					linerecords['wspd'] = int(wind.group(2))

					# if a third capturing group exists, set linerecords['wgust'] to the value of that group
					if wind.group(3):
						linerecords['wgust'] = int(wind.group(3))

					# if the third capturing group DOES NOT exist, the wind gust value has been omitted in the metar, which is standard practice under steady winds
					# 	so, make linerecords['wgust'] NULL
					else:
						linerecords['wgust'] = None
				
				# use station regular expression to match to token in question
				siteid = re.match(station_RE, token)

				# if the line above finds a match, set linerecords['siteid'] equal to the value of the capturing group
				if siteid:
					linerecords['siteid'] = siteid.group(1)
			
			# many times while trying to run this script (which took over an hour to complete) errors were encountered for various reasons, including, but not limited to:
			# 		- incorrect times and dates
			#		- improprtly formatted strings/match groups
			# 		- errors in metar data
			# 	printing each dictionary, 'linerecords' provided evidence for where the error occurred, so it could easily identified and debugged
			print linerecords
			
			# try to insert the now-populated 'linerecords' dictionary to the database
			# 	if a duplicate key is inserted, the dictionary will be omitted from database insertion
			try:
				cursor.execute(insert_stmt, linerecords)
			except mysql.connector.errors.IntegrityError:		
				pass

# note that the dictionary 'linerecords' is overwritten each time a new line is processed
			
# close MySQL connection
cnx.close()

