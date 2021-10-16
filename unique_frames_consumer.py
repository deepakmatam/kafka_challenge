###################################### Import Section #############################################################
from kafka import KafkaConsumer
import json

###################################### Initialistion Section ######################################################
TOPIC_NAME_UNIQUE_USERS = "unique_users_summary"
consumer = KafkaConsumer(TOPIC_NAME_UNIQUE_USERS)

############################################# Main Section ########################################################
print("Waiting for the data...")
for msg in consumer:
    data = json.loads((msg.value).decode('utf-8'))
    print(data)
    average = int(int(data['frames_processed_in_minute'])/60) # average number of frames in a minute (not unique users)
    print("Total frames processed in a minute:", data['frames_processed_in_minute'])
    print("Total number of unique users in a minute", data['unique_users'])
    print("Average number of frames per second:", average)
    print("************************************************************************************")