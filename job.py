import mysql.connector
import csv
import sys
import boto3

session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',
                        endpoint_url='https://nyc3.digitaloceanspaces.com',
                        aws_access_key_id=sys.argv[5],
                        aws_secret_access_key=sys.argv[6])

spaceName = sys.argv[7]

def csvHeader(cursor):
	return [row[0] for row in cursor.description]
    
def csvRows(cursor):
	return cursor.fetchall()
    
def csvDoc(cursor):
	header = csvHeader(cursor)
	rows = csvRows(cursor)
    
	with open('output.csv', 'w') as csvfile:
		writer = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(header)
		for row in rows:
			writer.writerow(row)



inputDB = mysql.connector.connect(
  host=sys.argv[1],
  user=sys.argv[3],
  password=sys.argv[4]
)

outputDB = mysql.connector.connect(
  host=sys.argv[2],
  user=sys.argv[3],
  password=sys.argv[4]
)

inputCursor = inputDB.cursor()
outputCursor = outputDB.cursor()



# Define your queries
query = "SELECT COUNT(*) FROM Voter_Rolls.PAVoterRolls;"
query_2 = "SELECT * FROM time_series.all_state_time_series WHERE state = 'georgia' AND votes > 0 ORDER BY timestamp ASC;"

# Execute the query
inputCursor.execute(query)

#Walk over your first queries cursor
for (voter_id) in inputCursor:
  print(voter_id)

outputCursor.execute(query_2)

# Write the output cursor of your query to a CSV document
# This output csv will be archived as an artifact of this build.
csvDoc(outputCursor)


client.upload_file('output.csv',  # Path to local file
                   spaceName,  # Name of Space
                   'output.csv')  # Name for remote file

inputCursor.close()
outputCursor.close()
inputDB.close()
outputDB.close()
