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
from models.lookup_table import Lookup_Table_Generator
import csv
import time
import shutil

'''
this is a small app the creates a user and then generates events for them
based various separate csv files containing events, event meta data, 
user flows, and a lookup table of user attributes

'''

def file_is_locked(filepath):
    """
    Checks if a file is locked by opening it in append mode.
    If no exception thrown, then the file is not locked.
    """
    locked = None
    file_object = None
    if os.path.exists(filepath):
        try:
            buffer_size = 32
            # Opening file in append mode and read the first 8 characters.
            file_object = open(filepath, 'a', buffer_size)
            if file_object:
                locked = False
        except IOError:
            locked = True
        finally:
            if file_object:
                file_object.close()
    return locked

def write_lookup_to_csv_file(primary_shard_key_dict, lookup_table_file, events_folder_path):
    lookup_table_path = events_folder_path.replace('events/','lookup_table/')
    lookup_table_file_name = lookup_table_file.split("/")[-1]
    lookup_table_full_file_path = '{}{}'.format(lookup_table_path,lookup_table_file_name)

    # see if the look table file exists
    if os.path.isfile(lookup_table_full_file_path):
        # if it does make sure something else isn't writing to it
        response = file_is_locked(lookup_table_full_file_path)
        # if another program has the file open wait 60 seconds and try again
        if response:
            time.sleep(60)
            write_lookup_to_csv_file(primary_shard_key_dict, lookup_table_file, events_folder_path)
        else:
            shard_key_ids_from_dict = primary_shard_key_dict.keys()
            list_of_ids = list()
            with open("{}{}".format(lookup_table_path, lookup_table_file_name), 'r') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    list_of_ids.append(row['shard_key_id'])
            ids_to_add = list(set(shard_key_ids_from_dict) - set(list_of_ids))
            # create the lookup table if this is the first time we are creating data
            if len(ids_to_add) > 0:
                lookup_table = Lookup_Table_Generator(id_list=ids_to_add, lookup_file=lookup_table_file)
                lookup_table_list = lookup_table.create_table()
                with open("{}{}".format(lookup_table_path, lookup_table_file_name), 'a') as f:
                    # take lookup_table_list and write to to a csv file
                    fieldnames = lookup_table_list[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    for dict in lookup_table_list:
                        writer.writerow(dict)
    else:
        # create the lookup table directory
        lookup_table_directory = os.path.dirname(lookup_table_path)
        if not os.path.isdir(lookup_table_directory):
            os.makedirs(lookup_table_directory)
        # create the lookup table if this is the first time we are creating data
        lookup_table = Lookup_Table_Generator(id_list=primary_shard_key_dict, lookup_file=lookup_table_file)
        lookup_table_list = lookup_table.create_table()
        # take lookup_table_list and write to to a csv file
        with open("{}{}".format(lookup_table_path, lookup_table_file_name), 'w') as f:
            fieldnames = lookup_table_list[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for dicts in lookup_table_list:
                writer.writerow(dicts)

def check_if_file(user_input):
    file = user_input.strip()
    file_check = Path(file)
    if file_check.is_file():
        return True
    else:
        return False

def ask_for_event_file():
    event_file = input(
        "Please provide the Event csv file \n Ex:/mnt/tank/pool/filer2/demo_data/social_demo/events_csv.csv \n File: ")
    if not check_if_file(event_file):
        print("That doesn't seem to be a file. Please double check the file path and submit again")
        return ask_for_event_file()
    return event_file

def ask_for_flows_file():
    user_flows_file = input(
        "Please provide the User Flows csv file \n Ex:/mnt/tank/pool/filer2/demo_data/social_demo/user_flows_csv.csv \n File: ")
    if not check_if_file(user_flows_file):
        print("That doesn't seem to be a file. Please double check the file path and submit again")
        return ask_for_flows_file()
    return user_flows_file

def ask_for_lookup_table_file():
    lookup_table_file = input(
        "Please provide the Lookup Table Data csv file \n Ex:/mnt/tank/pool/filer2/demo_data/social_demo/user_lookup_table.csv \n File: ")
    if not check_if_file(lookup_table_file):
        print("That doesn't seem to be a file. Please double check the file path and submit again")
        return ask_for_lookup_table_file()
    return lookup_table_file

def ask_for_number_of_users():
    number_of_original_users = int(input("How many users would you like to generate to start? "))
    if type(number_of_original_users) != int or number_of_original_users <0:
        print("That was not a number. Please enter a number greater than 0")
        return ask_for_number_of_users()
    new_user_to_generate_per_period = ((number_of_original_users * .1) / 30)
    return number_of_original_users, new_user_to_generate_per_period

def write_config_file(event_file, user_flows_file, lookup_table_file, config_path, event_folder_path, new_user_to_generate_per_period):
    config_data = dict()
    config_data['event_file_path'] = event_file
    config_data['flows_file_path'] = user_flows_file
    config_data['lookup_table_path'] = lookup_table_file
    config_data['event_folder_path'] = event_folder_path
    config_data['new_user_to_generate_per_period'] = new_user_to_generate_per_period
    with open("{}config.json".format(config_path), 'w') as config_file_writer:
        config_file_writer.write(json.dumps(config_data))

def ask_to_specify_event_path(current_path):
    specify_path = input("Would you like to specify a specific location where Event files should be written to? \n if not and 'events' folder will be created here and all files will be placed in this foler \n yes or no? ")
    if specify_path.strip().lower() == 'yes':
        event_path = input("Please provide the path where you would like to place the files. \n Path:")
        event_path_check = os.path.dirname(event_path)
        if os.path.exists(event_path_check):
            return "{}events/".format(event_path)
        else:
            print("That directory location doesn't seem to exist. Please check the path and try again")
            return ask_to_specify_event_path()
    elif specify_path.strip().lower() == 'no':
        event_path = "{}/events/".format(current_path)
        return event_path
    else:
        print("That wasn't a valid answer. Please try again.")
        return ask_to_specify_event_path()

def gzip_existing_file(event_directory_path,output_file_name, json_file):
    # delete the trailing common from append_to_open_json_file()
    json_file.seek(-1, os.SEEK_END)
    json_file.truncate()
    #add closing bracket to make proper json
    json_file.write(']'.encode(errors='ignore'))
    # close the open file so we can swtich to read mode
    json_file.close()
    # read in json file and convert to gzip
    with open('{}/{}'.format(event_directory_path, output_file_name),'rb') as original_file, gzip.open('{}/{}.gz'.format(event_directory_path, output_file_name),'wb') as zipped_file:
        shutil.copyfileobj(original_file, zipped_file)
    # delete json file
    try:
        os.unlink('{}/{}'.format(event_directory_path, output_file_name))
    except Exception:
        pass

def generate_users(num_of_users):
    primary_shard_key_dict = dict()
    for x in range(num_of_users):
        primary_shard_key = str(uuid.uuid4())
        user_probability = random.uniform(0, 1)
        primary_shard_key_dict[primary_shard_key] = dict()
        primary_shard_key_dict[primary_shard_key]['user_probability'] = user_probability
        primary_shard_key_dict[primary_shard_key]['churned'] = 'False'
        primary_shard_key_dict[primary_shard_key]['skip_days_counter'] = 0
        primary_shard_key_dict[primary_shard_key]['churn_prob_reset_counter'] = 0
        primary_shard_key_dict[primary_shard_key]['last_event_time'] = datetime.datetime.strftime(last_date_run,
                                                                                                  "%Y-%m-%d %H:%M:%S")
    return primary_shard_key_dict


def generate_data(all_ids, last_run_date, initial_data_generation, today_date, event_folder_path, user_flows_file, event_file_path, lookup_table_file):
    event_file_name = datetime.datetime.now().strftime("%Y-%m-%d%H%M%S")
    primary_shard_key_dict = all_ids
    # get the number of users to understand progress and print to console, plus make sure we write final events to a file
    num_user = len(all_ids)
    progress_counter = 0
    ten_percent = round(num_user*.1)
    # Loop through all user Id
    for shard_key, shard_key_values in primary_shard_key_dict.items():
        progress_counter = progress_counter + 1
        # make sure that we write the last bits of data if combined_data is less than 1 GB
        if round(progress_counter % ten_percent) == 0:
            print("Event Writing Progress: {}%".format(round((progress_counter / num_user), 2) * 100))
        start_date = last_run_date
        # if this the first time we are generating user we need to make sure they aren't all created on the same day
        # we'll do a random roll to put the users creation date sometime between now and 90 days ago
        # if this isn't the first time we are generating data we will let "last_date_run" be equal to the saved time in the file
        if initial_data_generation == True:
            start_date = last_run_date + datetime.timedelta(days=(random.randrange(1,80)))
        # check to make sure we have a reset_churn_prob value, mostly a duck tape fix for backwards compatibility where data was already generated
        if not shard_key_values.get('skip_days_counter'):
            shard_key_values['skip_days_counter'] = 0
            shard_key_values['churn_prob_reset_counter'] = 0
        # check to see if the folder and file we want to write to exists or not
        event_dir_path_with_date = "{}{}/".format(event_folder_path, (today_date).strftime("%Y-%m-%d"))
        full_event_directory_path_obj = os.path.dirname(event_dir_path_with_date)
        # if the event folder does not exists let's create it
        if not os.path.exists(event_dir_path_with_date):
            # set permissions on folder
            os.umask(0)
            # create the directory
            os.makedirs(full_event_directory_path_obj)
            # make the first event file since we know it cannot exist as the dir does not exist yet
            events_file = open('{}/{}'.format(full_event_directory_path_obj, event_file_name), 'wb')
            # write the initial opening bracket for proper JSON formatting
            events_file.write('['.encode(errors='ignore'))
        #check if file exists, and if not open it. this is for when we run the script after the first run to make sure we have a file to write to when the dir exists from a previous run
        try:
            if events_file.closed:
                # open the file we want to write events to
                event_file_name = datetime.datetime.now().strftime("%Y-%m-%d%H%M%S")
                events_file = open('{}/{}'.format(full_event_directory_path_obj, event_file_name), 'wb')
                events_file.write('['.encode(errors='ignore'))
        except UnboundLocalError:
            events_file = open('{}/{}'.format(full_event_directory_path_obj, event_file_name), 'wb')
            events_file.write('['.encode(errors='ignore'))
        user = User(primary_shard_key_value=shard_key,
                    last_date_run=start_date,
                    user_flows_file_location=user_flows_file,
                    event_dict_file_location=event_file_path,
                    user_probability=shard_key_values['user_probability'],
                    today=today_date,
                    skip_days_counter=shard_key_values['skip_days_counter'],
                    churn_prob_reset_counter=shard_key_values['churn_prob_reset_counter'],
                    open_file=events_file)
        # format the flows from the csv file into usable JSON
        user_flows = user.flow_dict
        # generate all shard key events and flows for the users, return needed shard key states to be stored in shard key dict
        churn, shard_key_prob, shard_key_skip_days_counter, shard_key_churn_prob_reset_counter, last_event_time = user.generate_flows(user_flows)
        primary_shard_key_dict[shard_key]['churned'] = churn
        primary_shard_key_dict[shard_key]['user_probability'] = shard_key_prob
        primary_shard_key_dict[shard_key]['skip_days_counter'] = shard_key_skip_days_counter
        primary_shard_key_dict[shard_key]['churn_prob_reset_counter'] = shard_key_churn_prob_reset_counter
        primary_shard_key_dict[shard_key]['last_event_time'] = datetime.datetime.strftime(last_event_time,"%Y-%m-%d %H:%M:%S")
        # check the size of the currently open file to see if we need to close and zip it
        # open file so we can read in stats
        file_size = os.path.getsize('{}/{}'.format(full_event_directory_path_obj, event_file_name))
        # check to see if this is the last user, if so zip the currently open file
        if progress_counter == (num_user - 1):
            # zip the current json file for compression
            gzip_existing_file(event_directory_path=full_event_directory_path_obj, output_file_name=event_file_name,
                               json_file=events_file)
        elif file_size > 500000000:
            # zip the current json file for compression
            gzip_existing_file(event_directory_path=full_event_directory_path_obj, output_file_name=event_file_name,
                               json_file=events_file)
            # reset the file name so we can create a new file on the next pass
            event_file_name = datetime.datetime.now().strftime("%Y-%m-%d%H%M%S")
    # if first run create table, it not the first run read in csv file, check for new users, append new user data
    print('writing lookup file')
    write_lookup_to_csv_file(
                      lookup_table_file=lookup_table_file,
                      primary_shard_key_dict=primary_shard_key_dict,
                      events_folder_path=event_folder_path)
    print('removing churned shard keys')
    ids_to_remove = [ids for ids in primary_shard_key_dict if primary_shard_key_dict[ids]['churned'] == True]
    # remove the ids that should churn from our primary_shard_key_dict so they "churn" in the dataset
    for id in ids_to_remove:
        del primary_shard_key_dict[id]
    print('saving shard key list')
    # after all the ids have generated events write the remaining users to a file and save
    with open("{}{}.json".format(config_path, user_id_list_file_name), 'w') as write_user_id_file:
        write_user_id_file.write(json.dumps(primary_shard_key_dict))
    # once all events have been generated make it so the date file is updated
    # to reflect the most recent date and hour the script was run
    with open("{}{}".format(config_path, last_date_run_file), 'w') as date_file:
        date_file.write(today_date.strftime("%Y-%m-%d %H:%M:%S"))


#start of app
if __name__ == '__main__':
    #global values
    user_id_list_file_name = 'user_id_list'
    last_date_run_file = 'last_date_script_was_run'
    today_date = datetime.datetime.now()
    current_path = os.path.dirname(os.path.abspath(__file__))
    config_path = "{}/config/".format(current_path)
    config_directory = os.path.dirname(config_path)
    config_file_check = Path("{}config.json".format(config_path))
    if not config_file_check.is_file():
        number_of_original_users, new_user_to_generate_per_period = ask_for_number_of_users()
        event_file = ask_for_event_file()
        user_flows_file = ask_for_flows_file()
        lookup_table_file = ask_for_lookup_table_file()
        event_folder_path = ask_to_specify_event_path(current_path=current_path)
        # if config dir has been created on accident in the pass let this fail silently and continue
        try:
            os.makedirs(config_directory)
        except Exception:
            pass
    else:
        # let's grab the the configuration details we need
        with open ("{}config.json".format(config_path), 'r') as config_file_reader:
            config_file_data = json.loads(config_file_reader.read())
            event_file = config_file_data['event_file_path']
            user_flows_file = config_file_data['flows_file_path']
            lookup_table_file = config_file_data['lookup_table_path']
            event_folder_path = config_file_data['event_folder_path']
            new_user_to_generate_per_period = config_file_data['new_user_to_generate_per_period']
    # get the last time the script was run from the data file, if the file doesn't exist, write it
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
        # get the event dict and format into a python dict
        event_dict = csv_to_dict(event_file)
        # check to see if we have a list of the primary shard keys, if not create one and save as a file
        list_of_ids_file = Path("{}{}.json".format(config_path, user_id_list_file_name))
        if list_of_ids_file.is_file():
            with open("{}{}.json".format(config_path, user_id_list_file_name), 'r') as read_user_id_file:
                primary_shard_key_dict = json.loads(read_user_id_file.read())
                primary_shard_key_dict = primary_shard_key_dict
            # add some new users to the file - this is mostly so we can change metrics like retention and do "new user" analysis
            print('Generating New Shard Key Ids')
            primary_shard_key_dict = get_new_users(primary_shard_key_dict=primary_shard_key_dict,
                                                   number_of_users=new_user_to_generate_per_period,
                                                   event_start_time=last_date_run)
            print('Starting Event Generation')
            generate_data(all_ids=primary_shard_key_dict,
                          last_run_date=last_date_run,
                          initial_data_generation=False,
                          today_date=today_date,
                          event_folder_path=event_folder_path,
                          user_flows_file=user_flows_file,
                          event_file_path=event_file,
                          lookup_table_file= lookup_table_file)
            print('writing config files')
            # write the config files last
            write_config_file(event_file=event_file,
                              user_flows_file=user_flows_file,
                              lookup_table_file=lookup_table_file,
                              config_path=config_path,
                              event_folder_path=event_folder_path,
                              new_user_to_generate_per_period=new_user_to_generate_per_period)
            print('script finished running')
        else:
            # set the "last run date" to be 3 months ago so we can back propogate some new data
            last_date_run = datetime.datetime.now() - datetime.timedelta(days=90)
            # create a dictionary of users where the key is the first shard key listed in the event file and store additional details about the user in another dict
            print('Generating original list of shard keys')
            # generate our list of shard key ids and shard key properties
            primary_shard_key_dict = generate_users(number_of_original_users)
            print('Starting Event Generation')
            generate_data(all_ids=primary_shard_key_dict,
                          last_run_date=last_date_run,
                          initial_data_generation=True,
                          today_date=today_date,
                          event_folder_path=event_folder_path,
                          user_flows_file=user_flows_file,
                          event_file_path=event_file,
                          lookup_table_file=lookup_table_file)
            print('writing config files')
            # write the config files last
            write_config_file(event_file=event_file,
                              user_flows_file=user_flows_file,
                              lookup_table_file=lookup_table_file,
                              config_path=config_path,
                              event_folder_path=event_folder_path,
                              new_user_to_generate_per_period=new_user_to_generate_per_period)
            print('script finished running')

