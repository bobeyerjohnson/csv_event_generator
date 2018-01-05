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
output_data_path ='/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github/csv_event_generator/data_files/user_flow_files/'
user_list_path = '/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github/csv_event_generator/data_files/user_id_list/'
user_id_list_file_name = 'user_id_list'
event_file = "/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Events.csv"
# get the user flows file
user_flows_file = '/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Flows.csv'
last_date_run_file ='/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github/csv_event_generator/data_files/last_date_run/last_date_script_was_run'
todays_date = datetime.datetime.now()
last_date_run = datetime.datetime.now()

# get the last time the script was run from the data file
with open(last_date_run_file,'r') as date_file:
    last_date_run = date_file.read()
    last_date_run =  datetime.datetime.strptime(last_date_run.strip(), "%Y-%m-%d %H:%M:%S")

# check to see if today's current time is earlier than the time right now. if it is run the script
if last_date_run < todays_date:

    # get the event dict
    event_dict = csv_to_dict(event_file)
    # get the shard keys to figure out what actors we have
    shard_keys = event_dict['shardkeys']
    #the first shard key in the csv file/list will be used as the primary shard key for the sake of data generation. lets save it for now
    primary_shark_key = shard_keys[0]

    ## check to see if we have a list of the primary shard key, if not create one and save as a csv file
    my_file = Path("{}{}".format(user_list_path,user_id_list_file_name))
    if my_file.is_file():
        with open("{}{}".format(user_list_path,user_id_list_file_name),'r') as read_user_id_file:
            primary_shard_key_list = read_user_id_file.read()
            primary_shard_key_list = primary_shard_key_list.split(',')
        # add some new users to the file - this is mostly so we can change metrics like retention and do "new user" analysis
        for new_user in range(20):
            primary_shard_key_list.append(uuid.uuid4())

    else:
        primary_shard_key_list = list()
        for x in range(0, 500):
            shard_key1 = uuid.uuid4()
            primary_shard_key_list.append(shard_key1)
        with open("{}{}".format(user_list_path,user_id_list_file_name),'w') as write_user_id_file:
            for shard in primary_shard_key_list:
                write_user_id_file.write("{},".format(str(shard)))
        # set the "last run date" to be 3 months ago so we can back propogate some new data
        last_date_run = datetime.datetime.now() - datetime.timedelta(days=90)
    #get the last date the script was run to see if

        #Loop through all user Ids
    for shard_key_value in range(10):
    #for id in primary_shard_key_list:
        # create the user
        user = User(primary_shard_key_value=shard_key_value, last_date_run=last_date_run, user_flows_file_location=user_flows_file, event_dict_file_location=event_file)
        # format the flows from the csv file into usable JSON
        user_flows = user.flow_dict
        # generate all user events and flows for the users
        data = user.generate_flows(user_flows)
        #make sure we returned some events in our data object
        if len(data) < 1:
            pass
        else:
            #check the date of the last event and if it isn't to the current run date re-run user.generate_flows to get more events
            dt = datetime.datetime.strptime(data[len(data)-1]['ts'], "%Y-%m-%d %H:%M:%S")
            while dt <= todays_date:
                more_data = user.generate_flows(user_flows)
                for line in more_data:
                    data.append(line)
                    # make sure to assign the most up to data
                    dt =  datetime.datetime.strptime(line['ts'], "%Y-%m-%d %H:%M:%S")
            # put in json and write to file
            with open('{}user_{}'.format(output_data_path, shard_key_value), 'w') as f:
                f.write(json.dumps(data))
    # once all events have been generated make it so the date file is updated to reflect the most recent date and hour the script was run
    with open(last_date_run_file,'w') as date_file:
        date_file.write(todays_date.strftime("%Y-%m-%d %H:%M:%S"))


    ## check to see if lookup table is created, if not create it and write to file as JSON
