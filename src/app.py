__author__='bojohnson'

from pathlib import Path
import uuid
from models.user import User
from models.functions import csv_to_dict
import datetime
import csv
import json

'''
this is a small app the creates a user and then generates events for them
based on a csv file containing events, event meta data, a lookup table of user attributes
and use flows
'''
#TODO - need to figure out a way to make people churn, later project would be to get people to come back after churning
#TODO  - make it so that we handle all the shardkeys when ingested from csv file
##global values
output_data_path ='/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator/data_files/'
user_list_path = '/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator/user_id_list/'
user_id_list_file_name = 'user_id_list'
todays_date = datetime.datetime.now()
#TODO will need to write to some file and save
last_date_run = datetime.datetime.now() - datetime.timedelta(days=90)
number_of_users = 0


## check to see if we have a list of user ids, if not create one and save as a csv file
my_file = Path("{}{}".format(user_list_path,user_id_list_file_name))
if my_file.is_file():
    with open("{}{}".format(user_list_path,user_id_list_file_name),'r') as read_user_id_file:
        shard_key1_list = read_user_id_file.read()
        shard_key1_list = shard_key1_list.split(',')
    # add some new users to the file
    for new_user in range(20):
        shard_key1_list.append(uuid.uuid4())

else:
    shard_key1_list = list()
    for x in range(0, 500):
        shard_key1 = uuid.uuid4()
        shard_key1_list.append(shard_key1)
    with open("{}{}".format(user_list_path,user_id_list_file_name),'w') as write_user_id_file:
        for shard in shard_key1_list:
            write_user_id_file.write("{},".format(str(shard)))
    # set the "last run date" to be 3 months ago so we can back propogate some new data
    last_date_run = datetime.datetime.now() - datetime.timedelta(days=90)

if last_date_run < todays_date:
    ## ingest events and properties from csv
    event_file = "/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Events.csv"
    # open the file and turn the CSV file into a usable dictionary for later
    event_dict = csv_to_dict(event_file)
    #Loop through all user Ids
    for shard_key1 in range(10):
    #for id in shard_key1_list:
        # get the user flows file
        user_flows_file = '/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Flows.csv'
        # create the user
        user = User(userId=shard_key1, last_date_run=last_date_run, user_flows_file_location=user_flows_file, event_dict_file_location=event_file)
        # format the flows from the csv file into usable JSON
        user_flows = user.flow_dict
        # generate all user events and flows for the users
        data = user.generate_flows(user_flows)
        #make sure we returned some events in our data object
        if len(data) < 1:
            del data
        #check the date of the last event and if it isn't to the current run date re-run user.generate_flows to get more events
        dt = datetime.datetime.strptime(data[len(data)-1]['ts'], "%Y-%m-%d %H:%M:%S")
        while dt <= todays_date:
            more_data = user.generate_flows(user_flows)
            for line in more_data:
                data.append(line)
                # make sure to assign the most up to data
                dt =  datetime.datetime.strptime(line['ts'], "%Y-%m-%d %H:%M:%S")
        # put in json and write to file
        with open('{}user_{}'.format(output_data_path,shard_key1),'w') as f:
            f.write(json.dumps(data))

    ##TODO make it so that 'last_date_run equals today and write to a file




    ## check to see if lookup table is created, if not create it and write to file as JSON
