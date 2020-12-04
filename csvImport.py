import mysql.connector
import csv
import sys
import boto3
import requests

import random
import string

from messytables import CSVTableSet, type_guess, \
  types_processor, headers_guess, headers_processor, \
  offset_processor, any_tableset

session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',
                        endpoint_url='https://nyc3.digitaloceanspaces.com',
                        aws_access_key_id=sys.argv[5],
                        aws_secret_access_key=sys.argv[6])

spaceName = sys.argv[7]
table_name = sys.argv[8]
database_name = sys.argv[9]
skip = int(sys.argv[11])

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    print("Random string of length", length, "is:", result_str)
    return result_str

def DownloadFile(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, stream = True)
    f = open(table_name + '.csv', 'wb')
    for chunk in r.iter_content(chunk_size=512 * 1024): 
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
    f.close()
    return 

# A table set is a collection of tables:
def csvParse(csv_file_path):
    fh = open(csv_file_path, 'rb')
    # Load a file object:
    table_set = CSVTableSet(fh)
    row_set = table_set.tables[0]
    # guess header names and the offset of the header:
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    # add one to begin with content, not the header:
    row_set.register_processor(offset_processor(offset + 1))
    # guess column types:
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types, strict=False))
    return row_set, headers, offset, types

def transformHeaderString(header_name, i):
    if len(header_name) == 0:
        return ("no_label_%s" % i)
    # return '"' + header_name.replace(" ", "_") + '"'
    sanitized = header_name.replace(" ", "_").replace(".", "_").replace("#", "_").replace(",", "_").replace(")", "_").replace("(", "_").replace("-", "_").replace("__", "_").replace("__", "_")
    if len(sanitized) > 50:
        return sanitized[:50] + sanitized[-3:] + ("_%s" % i)
    return sanitized + ("_%s" % i)

def transformHeaderType(header_type):
    if str(header_type) == 'String':
        return 'TEXT'
    elif str(header_type) == 'Integer':
        return 'TEXT'
    else:
        return 'TEXT'

def generateInsertSQL(table_name, headers, types):
    insert_sql = 'INSERT INTO ' + database_name + '.' + table_name + '('
    index = 0
    for col in headers:
        insert_sql = insert_sql + transformHeaderString(col, index) + ', '
        index = index + 1
    insert_sql = insert_sql[:len(insert_sql)-2] + ') VALUES ('
    for i in range(len(headers)):
        #insert_sql = insert_sql + ' %s::' + transformHeaderType(types[i]) + ', '
        insert_sql = insert_sql + ' "%s", '
    return insert_sql[:len(insert_sql)-2] + ')'

def generateCreateTableSQL(table_name, headers, types):
    create_table_sql = 'CREATE TABLE ' + database_name + '.' + table_name + '('
    for i in range(len(headers)):
        create_table_sql = create_table_sql + ('' + transformHeaderString(headers[i], i)) + ' ' + ('' + transformHeaderType(types[i])) + ', '
    return create_table_sql[:len(create_table_sql)-2] + ')'

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

def fetchRemoteCSV(url):
    DownloadFile(url)
    return table_name + '.csv'

row_set, headers, offset, types = csvParse(fetchRemoteCSV(sys.argv[10]))


create_table_sql = generateCreateTableSQL(table_name, headers, types);


inputDB = mysql.connector.connect(
  host=sys.argv[1],
  user=sys.argv[3],
  password=sys.argv[4]
)

print("Output Server %s" % (sys.argv[2]))

outputDB = mysql.connector.connect(
  host=sys.argv[2],
  user=sys.argv[3],
  password=sys.argv[4]
)

inputCursor = inputDB.cursor()



outputCursor = outputDB.cursor(prepared=True)
outputCursor.execute('DROP TABLE IF EXISTS ' + database_name + '.' + table_name)


print(create_table_sql)
outputCursor.execute(create_table_sql)

insert_sql = generateInsertSQL(table_name, headers, types)

limitCount = 1

for row in row_set:
    if limitCount <= skip:
        limitCount = limitCount + 1
        continue

    limitCount = limitCount + 1
    
    param_tuple = ()
    rowidx = 0
    for cell in row:
        if str(types[rowidx]) == 'Integer' and type(cell.value) != type(123):
            param_tuple = param_tuple + (0,)
        else:
            param_tuple = param_tuple + (str(cell.value).replace('\\', r'').replace('"', r'\"'),)
        rowidx = rowidx + 1
    print(insert_sql % param_tuple)
    outputCursor.execute(insert_sql % param_tuple)
    if limitCount % 10000:
        outputDB.commit()
    
    
outputDB.commit()

# client.upload_file('output.csv',  # Path to local file
#                    spaceName,  # Name of Space
#                    'output.csv')  # Name for remote file

inputCursor.close()
outputCursor.close()
inputDB.close()
outputDB.close()
