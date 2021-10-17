# kafka_challenge

Download the sample data at  http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz
There are three python files, producer.py, consumer.py, unique_frames_consumer.py

producer.py:
------------
1) This program reads the sample data mentioned above.
2) Generates list of json objects. Each json object have frame information.
3) Pushes each json object to a topic.

consumer.py
-----------
1) Consumes each frame data pushed by producer.py
2) Uses list data structure to calculate unique users in a minute.
3) Pushes a json object to a new topic which contains information like number of unique users in a minute, total number of frames processed in a minute.

unique_frames_consumer.py
-------------------------
1) Consumes data pushed by consumer.py
2) Calcultes the average number of frames in a minute.
3) Prints the summary: No. of unique users, total number of frames in a minute, average number of frames per second.


Run Instructions:
-----------------
1) We need python3 to run the files, I choose python because I love to code in python, as we know, lines of code is too less compared to other programming languages.
2) Install Python client for the Apache Kafka. You can use this command to install. (pip install kafka-python)
3) Make sure to download sample data file from the link provided above.
4) Place sample data file in the folder where you place the above three python files.
5) Run the python files unique_frames_consumer.py, consumer.py, producer.py.
6) You can see a snippet of output in the ./output/ folder.

Outputs:
--------
1) The producer.py pushes each frame at a time and ouputs number of frames it pushed at some point of time. This is keep track of how many frame's data has been pushed to topic.
2) The consumer.py outputs number of frames it has recieved so far, user_id and time stamp of a record. This is to know how many frames it has received.
3) The unique_frames_consumer.py  outputs the summary of frames processed in a minute. It outputs number of unique users in a minute, average number of frammes in a second, number of frames in a minute. This gives performance information and number of unique users in a mminute.


Scaling
-------
1) Increasing kafka clusters can speed up the process, as we have lot of data to be processed.
