import os
import glob
import pymssql
import re
import time
import datetime
from datetime import datetime as dt

# Connect to the DB
try:
    conn = pymssql.connect(server = 'GCI-PR-DATEAU01\DATEAUBASE', database ='dateaubase2020')
    cursor = conn.cursor()
    print("conn ok")
except:
    print("conn error")


#--------------------------------------------------------------------------------------------------

# THIS IS THE BEGIN FOR ANAPRO IN LAVAL

# Path to the directory with .par files (anaPro)
path = "//10.10.10.13/infpc1_2/"

# List the .par files
listFile = glob.glob(path+'*.par')

# Sort the list from the oldest to the last
listFile.sort

# Index of the oldest file with new data (will change during the execution but the last file always has new data)
index = len(listFile)-1

# Get the timestamp of the last data inserted in the DB for anaPro in Laval
cursor.execute('SELECT ISNULL(MAX(Timestamp),0) FROM dbo.value WHERE Metadata_ID = 1')
#print('Query done')
row = cursor.fetchone()
lastTS = row[0]

# Convert the Timestamp in DateTime format
lastData = str(dt.fromtimestamp(lastTS))

# loop to check all the files
for idx,file in enumerate(listFile):

	# Delete the path from the name	
	actualFile = file.replace(path,'')
	
	# Get the date/time of the first data in the .par file
	dateName = re.split(r'[._-]+', actualFile)
	dateFile = (dateName[0] + '-' +dateName[1] + '-' +dateName[2] + ' ' +dateName[3] + ':' +dateName[4] + ':' +dateName[5] )

	# if a file is more recent than the last data => change the index to begin the file just before this one
	if dateFile>lastData:
		index = max(idx-1,0)
		break

j = 1
for file in listFile[index:]:
	# Open, save the content and close the file
	f = open(file,'r')
	print(file)
	lignes = f.readlines()[2:]
	f.close()

	# Find the highest ID in the Database to increment from it
	cursor.execute('SELECT ISNULL(MAX(VALUE_ID),0) FROM dbo.value')
	#row = cursor.fetchone()
	for rows in cursor:
		lastID = rows[0]
	i = 1

	# For each line on the .par file, split the datas and get pertinent ones. If value = NaN : Value = 0
	for ligne in lignes :
		ligne = ligne.replace(",",".")
		datas = ligne.split("\t")
		# Split date and time
		datetime = datas[0].split("  ")
		# Format date to the DB format
		Date = datetime[0].replace(".","-")
		Time = datetime[1]
		# If last data in the DB oldest than the the actual treated data : insert it
		DateTime = Date + " " + Time
		UTCTimestamp=time.mktime(time.strptime(DateTime,"%Y-%m-%d %H:%M:%S"))
		if lastData<DateTime:
			TSS = datas[2]
			if TSS=="NaN":
				TSS = 0
			NO3N = datas[4]
			if NO3N=="NaN":
				NO3N = 0
			COD = datas[6]
			if COD=="NaN":
				COD = 0
			CODf = datas[8]
			if CODf=="NaN":
				CODf = 0
			NH4N = datas[10]
			if NH4N=="NaN":
				NH4N = 0
			K = datas[12]
			if K=="NaN":
				K = 0
			pH = datas[14]
			if pH=="NaN":
				pH = 0
			Temp = datas[16]
			if Temp=="NaN":
				Temp = 0
		
			# Increment the ID
			ID = lastID + i
	
			# Insert the datas in the DB 
			
			cursor.executemany(
				"INSERT INTO dbo.value(Value_ID, Timestamp, Value, Number_of_experiment, Metadata_ID) VALUES (%d,%d,%d,%d,%d)",
				[(ID, int(UTCTimestamp), float(TSS)/1000, 1,5),
				(ID+1, int(UTCTimestamp), float(NO3N)/1000, 1,6),
				(ID+2, int(UTCTimestamp), float(COD)/1000, 1,7),
				(ID+3, int(UTCTimestamp), float(CODf)/1000, 1,8),
				(ID+4, int(UTCTimestamp), float(NH4N), 1,1),
				(ID+5, int(UTCTimestamp), float(K), 1,2),
				(ID+6, int(UTCTimestamp), float(pH), 1,3),
				(ID+7, int(UTCTimestamp), float(Temp), 1,4)])
			conn.commit()
			i=i+8
			#print(lastID) #(debug)
	j = j + i
print(" %d : number of rows added to datEAUbase" % j)
			
# THIS IS THE END FOR ANAPRO IN LAVAL			
			
#--------------------------------------------------------------------------------------------------

# THIS IS THE BEGIN FOR ANAPRO IN GRANDPILES (Affluent)			
			
# # Path to the directory with .par files (anaPro)
# path = "//10.10.10.11/inflpc/"

# # List the .par files
# listFile = glob.glob(path+'*.par')

# # Sort the list from the oldest to the last
# listFile.sort

# # Index of the oldest file with new data (will change during the execution but the last file always has new data)
# index = len(listFile)-1

# # Get the timestamp of the last data inserted in the DB for AnaPro Grandpiles Affluent
# cursor.execute('SELECT ISNULL(MAX(Timestamp),0) FROM dbo.value WHERE (Metadata_ID>=59 AND Metadata_ID < 63) OR (Metadata_ID >= 75 AND Metadata_ID < 79) ')
# row = cursor.fetchone()
# lastTS = row[0]
# lastData=0
# # Convert the Timestamp in DateTime format
# if lastTS==0:
	# lastData=0
# else:
	# lastData = str(dt.fromtimestamp(lastTS))
	# #(debug)print lastData


# # loop to check all the files
# for idx,file in enumerate(listFile):

	# # Delete the path from the name	
	# actualFile = file.replace(path,'')
	
	# # Get the date/time of the first data in the .par file
	# dateName = re.split(r'[._-]+', actualFile)
	# dateFile = (dateName[0] + '-' +dateName[1] + '-' +dateName[2] + ' ' +dateName[3] + ':' +dateName[4] + ':' +dateName[5] )

	# # if a file is more recent than the last data => change the index to begin the file just before this one
	# if dateFile>lastData:
		# index = max(idx-1,0)
		# break
	
# for file in listFile[index:]:
	# # Open, save the content and close the file
	# f = open(file,'r')
	# lignes = f.readlines()[2:]
	# f.close()

	# # Find the highest ID in the Database to increment from it
	# cursor.execute('SELECT ISNULL(MAX(VALUE_ID),0) FROM dateaubase.dbo.value')
	# row = cursor.fetchone()
	# lastID = row[0]
	# i = 1

	# # For each line on the .par file, split the datas and get pertinent ones. If value = NaN : Value = 0
	# for ligne in lignes :
		# ligne = ligne.replace(",",".")
		# datas = ligne.split("\t")
		# # Split date and time
		# datetime = datas[0].split("  ")
		# # Format date to the DB format
		# Date = datetime[0].replace(".","-")
		# Time = datetime[1]
		# # If last data in the DB oldest than the the actual treated data : insert it
		# DateTime = Date + " " + Time
		# UTCTimestamp=time.mktime(time.strptime(DateTime,"%Y-%m-%d %H:%M:%S"))
		# if lastData<DateTime:
			# TSS = datas[2]
			# if TSS=="NaN":
				# TSS = 0
			# NO3N = datas[4]
			# if NO3N=="NaN":
				# NO3N = 0
			# COD = datas[6]
			# if COD=="NaN":
				# COD = 0
			# CODf = datas[8]
			# if CODf=="NaN":
				# CODf = 0
			# NH4N = datas[16]
			# if NH4N=="NaN":
				# NH4N = 0
			# K = datas[14]
			# if K=="NaN":
				# K = 0
			# pH = datas[12]
			# if pH=="NaN":
				# pH = 0
			# Temp = datas[10]
			# if Temp=="NaN":
				# Temp = 0
		
			# # Increment the ID
			# ID = lastID + i
	
			# # Insert the datas in the DB 
			
			# cursor.executemany(
				# "INSERT INTO dbo.value(Value_ID, Timestamp, Value, Number_of_experiment, Metadata_ID) VALUES (%d,%d,%d,%d,%d)",
				# [(ID, int(UTCTimestamp), float(TSS)/1000, 1,62),
				# (ID+1, int(UTCTimestamp), float(NO3N)/1000, 1,59),
				# (ID+2, int(UTCTimestamp), float(COD)/1000, 1,60),
				# (ID+3, int(UTCTimestamp), float(CODf)/1000, 1,61),
				# (ID+4, int(UTCTimestamp), float(NH4N), 1,76),
				# (ID+5, int(UTCTimestamp), float(K), 1,77),
				# (ID+6, int(UTCTimestamp), float(pH), 1,75),
				# (ID+7, int(UTCTimestamp), float(Temp), 1,78)])
			# conn.commit()
			# i=i+8
			# #debug
			# print (ID)		

# THIS IS THE END FOR ANAPRO IN GRANDPILES (Affluent)			
			
#--------------------------------------------------------------------------------------------------


# THIS IS THE BEGIN FOR ANAPRO IN GRANDPILES (Effluent)			
			
# # Path to the directory with .par files (anaPro)
# path = "//10.10.10.12/gp_eff2/"

# # List the .par files
# listFile = glob.glob(path+'*.par')

# # Sort the list from the oldest to the last
# listFile.sort

# # Index of the oldest file with new data (will change during the execution but the last file always has new data)
# index = len(listFile)-1

# # Get the timestamp of the last data inserted in the DB for AnaPro Grandpiles Affluent
# cursor.execute('SELECT ISNULL(MAX(Timestamp),0) FROM dbo.value WHERE (Metadata_ID>=63 AND Metadata_ID < 66) OR (Metadata_ID >= 79 AND Metadata_ID < 82)')
# row = cursor.fetchone()
# lastTS = row[0]
# lastData=0
# # Convert the Timestamp in DateTime format
# if lastTS==0:
	# lastData=0
# else:
	# lastData = str(dt.fromtimestamp(lastTS))
	# #(debug)print lastData


# # loop to check all the files
# for idx,file in enumerate(listFile):

	# # Delete the path from the name	
	# actualFile = file.replace(path,'')
	
	# # Get the date/time of the first data in the .par file
	# dateName = re.split(r'[._-]+', actualFile)
	# dateFile = (dateName[0] + '-' +dateName[1] + '-' +dateName[2] + ' ' +dateName[3] + ':' +dateName[4] + ':' +dateName[5] )

	# # if a file is more recent than the last data => change the index to begin the file just before this one
	# if dateFile>lastData:
		# index = max(idx-1,0)
		# break
	
# for file in listFile[index:]:
	# # Open, save the content and close the file
	# f = open(file,'r')
	# lignes = f.readlines()[2:]
	# f.close()

	# # Find the highest ID in the Database to increment from it
	# cursor.execute('SELECT ISNULL(MAX(VALUE_ID),0) FROM dateaubase.dbo.value')
	# row = cursor.fetchone()
	# lastID = row[0]
	# i = 1

	# # For each line on the .par file, split the datas and get pertinent ones. If value = NaN : Value = 0
	# for ligne in lignes :
		# ligne = ligne.replace(",",".")
		# datas = ligne.split("\t")
		# # Split date and time
		# datetime = datas[0].split("  ")
		# # Format date to the DB format
		# Date = datetime[0].replace(".","-")
		# Time = datetime[1]
		# # If last data in the DB oldest than the the actual treated data : insert it
		# DateTime = Date + " " + Time
		# UTCTimestamp=time.mktime(time.strptime(DateTime,"%Y-%m-%d %H:%M:%S"))
		# if lastData<DateTime:
			# TSS = datas[2]
			# if TSS=="NaN":
				# TSS = 0
			# NO3N = datas[4]
			# if NO3N=="NaN":
				# NO3N = 0
			# COD = datas[6]
			# if COD=="NaN":
				# COD = 0
			# CODf = datas[8]
			# if CODf=="NaN":
				# CODf = 0
			# NH4N = datas[16]
			# if NH4N=="NaN":
				# NH4N = 0
			# K = datas[14]
			# if K=="NaN":
				# K = 0
			# pH = datas[12]
			# if pH=="NaN":
				# pH = 0
			# Temp = datas[10]
			# if Temp=="NaN":
				# Temp = 0
		
			# # Increment the ID
			# ID = lastID + i
	
			# # Insert the datas in the DB 
			
			# cursor.executemany(
				# "INSERT INTO dbo.value(Value_ID, Timestamp, Value, Number_of_experiment, Metadata_ID) VALUES (%d,%d,%d,%d,%d)",
				# [(ID, int(UTCTimestamp), float(TSS)/1000, 1,66),
				# (ID+1, int(UTCTimestamp), float(NO3N)/1000, 1,63),
				# (ID+2, int(UTCTimestamp), float(COD)/1000, 1,64),
				# (ID+3, int(UTCTimestamp), float(CODf)/1000, 1,65),
				# (ID+4, int(UTCTimestamp), float(NH4N), 1,80),
				# (ID+5, int(UTCTimestamp), float(K), 1,81),
				# (ID+6, int(UTCTimestamp), float(pH), 1,79),
				# (ID+7, int(UTCTimestamp), float(Temp), 1,82)])
			# conn.commit()
			# i=i+8
			# #debug
			# print (ID)	

# THIS IS THE END FOR ANAPRO IN GRANDPILES (Effluent)			
			
#--------------------------------------------------------------------------------------------------			
			
# Close the connexion to the DB
conn.close()
