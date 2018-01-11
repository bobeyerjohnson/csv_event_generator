__author__ = 'bojohnson'

from pathlib import Path
import uuid
import datetime
import json
import random
import gzip
from models.user import User
from models.functions import csv_to_dict, get_new_users
import os


'''
this is a small app the creates a user and then generates events for them
based various separate csv files containing events, event meta data, 
user flows, and a lookup table of user attributes
'''

#TODO data looks to uniform and "non human" need to make it look a little better visually
def generate_data(all_ids, last_run_date, initial_data_generation, today, event_folder_path, user_flows_file, event_file):
    #get the number of users to understand progress and print to console
    num_user = len(all_ids)
    progress_counter =0
    user_index = 0
    no_data_count =0
    primary_shard_key_dict =  all_ids
    # Loop through all user Id
    for shard_key, shard_key_values in primary_shard_key_dict.items():
        start_date = last_run_date
        # if this the first time we are generating user we need to make sure they aren't all created on the same day
        # we'll do a random roll to put the users creation date sometime between now and 90 days ago
        # if this isn't the first time we are generating data we will let "last_date_run" be equal to the saved time in the file
        if initial_data_generation == True:
            start_date = start_date + datetime.timedelta(days=(random.randrange(1,80)))
        # create the user
        user = User(primary_shard_key_value=shard_key,
                    last_date_run=start_date,
                    user_flows_file_location=user_flows_file,
                    event_dict_file_location=event_file,
                    user_probability=shard_key_values['user_probability'],
                    today=today)
        # format the flows from the csv file into usable JSON
        user_flows = user.flow_dict
        # generate all user events and flows for the users
        data, churn = user.generate_flows(user_flows)
        primary_shard_key_dict[shard_key]['churned'] = churn
        if data:
            #check if a folder today exists, if not create it
            event_folder_path_with_date = "{}{}/".format(event_folder_path, today_date.strftime("%Y-%m-%d"))
            event_directory = os.path.dirname(event_folder_path_with_date)
            if os.path.exists(event_directory):
                with gzip.open('{}/user_{}_{}.gz'.format(event_directory,
                                                        shard_key,
                                                        today_date.strftime("%Y-%m-%d")), 'w') as f:
                    f.write(json.dumps(data).encode(errors='ignore'))
            else:
                os.makedirs(event_directory)
                with gzip.open('{}/user_{}_{}.gz'.format(event_directory,
                                                          shard_key,
                                                          today_date.strftime("%Y-%m-%d")), 'w') as f:
                    f.write(json.dumps(data).encode(errors='ignore'))
        progress_counter = progress_counter + 1
        decimals = [x * 0.05 for x in range(0, 20)]
        for x in decimals:
            if round(x,2) == (round(progress_counter/num_user,2)):
                print("Progress - {}%".format(round((progress_counter/num_user)*100),4))
    # create a list of all ids that should now churn
    ids_to_remove = [ids for ids in primary_shard_key_dict if primary_shard_key_dict[ids]['churned'] == True]
    # remove the ids that should churn from our primary_shard_key_dict so they "churn" in the dataset
    for id in ids_to_remove:
        print("user churned! - {} ".format(id))
        del primary_shard_key_dict[id]
    # after all the ids have generated events write the remaining users to a file and save
    with open("{}{}.json".format(config_path, user_id_list_file_name), 'w') as write_user_id_file:
        write_user_id_file.write(json.dumps(primary_shard_key_dict))
    # once all events have been generated make it so the date file is updated
    # to reflect the most recent date and hour the script was run
    with open("{}{}".format(config_path, last_date_run_file), 'w') as date_file:
        date_file.write(today_date.strftime("%Y-%m-%d %H:%M:%S"))

def ensure_dir_exists(folder_path):
    directory = os.path.dirname(folder_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def ask_for_file_paths():
    event_file = input(
        "Please provide the Event csv file \n Ex:/mnt/tank/pool/filer2/demo_data/social_demo/events_csv.csv \n File: ")
    user_flows_file = input(
        "Please provide the User Flows csv file \n Ex:/mnt/tank/pool/filer2/demo_data/social_demo/user_flows_csv.csv \n File: ")
    return event_file, user_flows_file
def write_config_file(event_file, user_flows_file, config_path):
    config_data = dict()
    config_data['event_file_path'] = event_file
    config_data['flows_file_path'] = user_flows_file
    with open("{}config.json".format(config_path), 'w') as config_file_writer:
        config_file_writer.write(json.dumps(config_data))




#start of app
if __name__ == '__main__':
    ##global values
    user_id_list_file_name = 'user_id_list'
    last_date_run_file = 'last_date_script_was_run'
    today_date = datetime.datetime.now()
    new_user_to_generate_per_period = 20
    current_path = os.path.dirname(os.path.abspath(__file__))
    config_path = "{}/config/".format(current_path)
    config_directory = os.path.dirname(config_path)
    event_folder_path = "{}/events/".format(current_path)
    if not os.path.exists(config_directory):
        os.makedirs(config_directory)
        number_of_original_users =  int(input("How many users would you like to generate to start? "))
        event_file, user_flows_file = ask_for_file_paths()
        # write the config file which will just contain the file paths for the event and user flow files
        write_config_file(event_file, user_flows_file, config_path)
        #create the top level events folder
    else:
        # let's grab the event and flow files from the config file and generate some more data
        if os.path.getsize("{}config.json".format(config_path)) > 0:
            with open ("{}config.json".format(config_path), 'r') as config_file_reader:
                config_file_data = json.loads(config_file_reader.read())
                event_file = config_file_data['event_file_path']
                user_flows_file = config_file_data['flows_file_path']
        else:
            event_file, user_flows_file = ask_for_file_paths()
            write_config_file(event_file, user_flows_file, config_path)
    # get the last time the script was run from the data file
    date_file_check = Path("{}{}".format(config_path, last_date_run_file))
    if date_file_check.is_file():
        with open("{}{}".format(config_path, last_date_run_file), 'r') as date_file:
            last_date_run = date_file.read()
            last_date_run = datetime.datetime.strptime(last_date_run.strip(), "%Y-%m-%d %H:%M:%S")
    else:
        with open("{}{}".format(config_path, last_date_run_file), 'w') as date_file:
            last_date_run = datetime.datetime.now()-datetime.timedelta(days=90)
            date_file.write(last_date_run.strftime("%Y-%m-%d %H:%M:%S"))

    # check to see if today's current time is earlier than the time right now. if it is run the script
    if last_date_run < today_date:
        # get the event dict
        event_dict = csv_to_dict(event_file)
        ## check to see if we have a list of the primary shard key, if not create one and save as a csv file
        my_file = Path("{}{}.json".format(config_path, user_id_list_file_name))

        if my_file.is_file():
            with open("{}{}.json".format(config_path, user_id_list_file_name), 'r') as read_user_id_file:
                primary_shard_key_dict = json.loads(read_user_id_file.read())
                primary_shard_key_dict = primary_shard_key_dict
            # add some new users to the file - this is mostly so we can change metrics like retention and do "new user" analysis
            primary_shard_key_dict = get_new_users(primary_shard_key_dict=primary_shard_key_dict,
                                                   number_of_users=new_user_to_generate_per_period)
            generate_data(all_ids=primary_shard_key_dict,
                          last_run_date=last_date_run,
                          initial_data_generation=False,
                          today=today_date,
                          event_folder_path=event_folder_path,
                          user_flows_file=user_flows_file,
                          event_file=event_file)

        else:
            # create a dictionary of users where the key is the user id and store additional details about the user in another dict
            primary_shard_key_dict = dict()
            for x in range(number_of_original_users):
                primary_shard_key = str(uuid.uuid4())
                user_probability = random.uniform(0, 1)
                primary_shard_key_dict[primary_shard_key] = dict()
                primary_shard_key_dict[primary_shard_key]['user_probability'] = user_probability
                primary_shard_key_dict[primary_shard_key]['churned'] = 'False'
            # set the "last run date" to be 3 months ago so we can back propogate some new data
            last_date_run = datetime.datetime.now() - datetime.timedelta(days=90)
            generate_data(all_ids=primary_shard_key_dict,
                          last_run_date=last_date_run,
                          initial_data_generation=True,
                          today=today_date,
                          event_folder_path=event_folder_path,
                          user_flows_file=user_flows_file,
                          event_file=event_file)
    #TODO check to see if lookup table is created, if not create it and write to file as JSON
