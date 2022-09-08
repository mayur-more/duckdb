import duckdb
import time
import psutil



# gives a single float value
print("cpu_percent at start: " , psutil.cpu_percent())

# gives an object with many fields
print("virtual_memory at start: " , psutil.virtual_memory())
available_s = psutil.virtual_memory().available
used_s = psutil.virtual_memory().used
free_s = psutil.virtual_memory().free


# get the start time
st = time.time()

con = duckdb.connect()
#count rows
query1 = "SELECT count(1) FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet')"
print("Total Count of records : " , con.execute(query1).fetchall())
print("\n")


#arrays of queries
queries = ["SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime DESC",
    "SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime ASC",
    "SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime DESC LIMIT 50",
    "SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime ASC LIMIT 50",
    "SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime DESC LIMIT 50",
    "SELECT startTime,error FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 50",
    "SELECT duration,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet')",
    "SELECT duration,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7'",
    "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime DESC LIMIT 500",
    "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') ORDER BY startTime ASC LIMIT 500",
    "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime DESC LIMIT 500",
    "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 500",
    "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 50",
    "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50",
    "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 500",
    "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 500",
    "SELECT startTime,error,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 50",
    "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50",
    "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*1",
    "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*2",
    "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*3",
    "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 500",
    "select spanId,traceId,startTime,error,operation,duration,resource.name,resource.namespace FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE resource.namespace = 'flipkart' LIMIT 50 ",
    "select s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE s3Tags ILIKE '%amazon%' LIMIT 50",
    "select startTime,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE s3Tags ILIKE '%amazon%' ORDER BY startTime DESC LIMIT 50",
    "select startTime,s3Tags FROM read_parquet('/Users/mayurmore/Documents/TODO/176/Raw_1.parquet') WHERE s3Tags ILIKE '%amazon%' ORDER BY startTime ASC LIMIT 50"]

for x in queries:
  print("\t Executing: - " + x)

  st1 = time.time()

  #Execute query
  con.execute(x)

  et1 = time.time()
  # get the execution time
  elapsed_time1 = et1 - st1
  print('\t Execution time:', elapsed_time1, 'seconds')

  #Print Result
  #print(con.fetchall())
  print("\n")



# get the end time
et = time.time()

# get the execution time
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')

# gives a single float value
print("cpu_percent at end: " , psutil.cpu_percent())

# gives an object with many fields
print("virtual_memory at end: " , psutil.virtual_memory())
available_e = psutil.virtual_memory().available
used_e = psutil.virtual_memory().used
free_e = psutil.virtual_memory().free

#print("available: " , (available_s - available_e) /1024 ** 2 , "mb")
print("used: " , (used_e - used_s) /1024 ** 2 , "mb")
print("free: " , (free_s - free_e) /1024 ** 2 , "mb")