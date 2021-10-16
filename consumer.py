###################################### Import Section #############################################################
from kafka import KafkaConsumer
from kafka import KafkaProducer
import os
import json
import traceback

###################################### Initialistion Section ######################################################
first_frame = True
count, i, processed_in_minute, users = 0, 0, 0, []
TOPIC_NAME_FRAMES = "user_frames"
TOPIC_NAME_UNIQUE_USERS = "unique_users_summary"

consumer = KafkaConsumer(TOPIC_NAME_FRAMES)
producer = KafkaProducer(security_protocol="PLAINTEXT",\
         bootstrap_servers = os.environ.get('KAFKA_HOST', 'localhost:9092'))

####################################### Utility Section ###########################################################
#This function pushes the JSON object which contains info about number of frames processed in a minute.
def push_data_to_topic(total_processed, processed_in_min, unique_count_in_minute):
    try:
        unique_users_in_minute = json.dumps({"total_frames_processed_so_far" : total_processed,\
            "frames_processed_in_minute" : processed_in_min,\
            "unique_users" : unique_count_in_minute})
        
        producer.send(TOPIC_NAME_UNIQUE_USERS, unique_users_in_minute.encode('utf-8'))
        producer.flush()
    except:
        print(traceback.format_exc())

############################################# Main Section ########################################################
print("Waiting for the data...")
try:
    for msg in consumer:
        i = i + 1
        frame_data = json.loads((msg.value).decode('utf-8')) #decodes the data into json object
        user_id = frame_data.get("uid") #get user id from the data object
        if first_frame:
            start_frame_time = frame_data.get("ts")  #Taking the first frame time stamp as start time
            first_frame = False  #once we get first frame time stamp we will set first frame flag as false.

        current_frame_time = frame_data.get("ts") #get time stamp of a frame.
        time_diff = current_frame_time - start_frame_time # time difference between the start time and time of current frame.
        print("Number of frames received:", i, "UserdID:",user_id, "Time stamp of frame:", current_frame_time)
        if time_diff >= 60:  #If the time difference is more than or equal to 60 seconds, i.e 1 minute, now we push the data of unique users to another topic.
            start_frame_time = current_frame_time #reset the start frame time stamp, now this will be our reference time
            push_data_to_topic(i,i-processed_in_minute, count) #push unique users data to topic.
            count, processed_in_minute, users = 0, i, [] #reset the variables with new reference.

        if user_id != None:
            if user_id in users: # check if user is unique or not?
                continue
            users.append(user_id)
            count = count + 1

        if i == 1000000:  # This is just for reference.Pushing at the end as we know our test data have 1000000 records.
            push_data_to_topic(i,i-processed_in_minute, count)
except:
    print(traceback.format_exc())
