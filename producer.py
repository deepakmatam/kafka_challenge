###################################### Import Section #############################################################
from kafka import KafkaProducer
import os
import gzip
import traceback

###################################### Initialistion Section ######################################################
FILE_PATH = "stream.jsonl.gz"
TOPIC_NAME = "user_frames"

####################################### Utility Section ###########################################################
# This function reads data from a file and connverts into list of JSON objects.
def read_data_from_file(file_path):
   try:
      with gzip.open(file_path, "rt", encoding="utf8") as f:
         print("Reading Data from {}. This will take few seconds......".format(file_path))
         data = f.read()
         data = data.replace(" ", "")
         data = " ".join(data.split())
         data = data.replace("} {", "}%^&{")
         data = data.split("%^&")
         print("Decoded data into list of JSON objects.")
         print("Total number of records in {} = {}".format(file_path, len(data)))
      return data
   except:
      print(traceback.format_exc())
      return None


############################################# Main Section #################################################################
data = read_data_from_file(FILE_PATH)

if not data:
   print("Something went wrong while reading the data from {} file. Please check the file path.".format(FILE_PATH))
else:
   try: 
      producer = KafkaProducer(security_protocol="PLAINTEXT",\
         bootstrap_servers = os.environ.get('KAFKA_HOST', 'localhost:9092'))
      print("Pushing Data.")
      for i in range(len(data)):
         producer.send(TOPIC_NAME, data[i].encode('utf-8'))
         producer.flush()
         print("Number of frames pushed ", i)
   except:
      print(traceback.format_exc())