__author__ = 'bojohnson'

from pathlib import Path
import uuid
import datetime
import json
import random
import gzip
from src.models.user import User
from src.models.functions import csv_to_dict, get_new_users


'''
this is a small app the creates a user and then generates events for them
based on a csv file containing events, event meta data, a lookup table of user attributes
and use flows
'''
##global values
output_data_path = '/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github2/csv_event_generator/src/data_files/user_flow_files/'
user_list_path = '/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github2/csv_event_generator/src/data_files/user_id_list/'
user_id_list_file_name = 'user_id_list'
event_file = "/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Events.csv"
user_flows_file = '/Users/bobeyer-johnson/Downloads/Dummy Data - SocialMessaging Data Set - Flows.csv'
last_date_run_file_location = '/Users/bobeyer-johnson/Documents/interana/Solutions/event_generator_github2/csv_event_generator/src/data_files/last_date_run/'
last_date_run_file = 'last_date_script_was_run'
today_date = datetime.datetime.now()
new_user_to_generate_per_period = 10
#TODO need to figure out if data is random enough, that there are peaks a volleys during the week, and that not all new users during the first data generation are created on the same day
def generate_data(all_ids, last_run_date, initial_data_generation, today):
    user_index = 0
    no_data_count =0
    primary_shard_key_dict =  all_ids
    # Loop through all user Ids
    for shard_key, shard_key_values in primary_shard_key_dict.items():
        print(shard_key)
        start_date = last_run_date
        # if this the first time we are generating user we need to make sure they aren't all created on the same day
        # we'll do a random roll to put the users creation date sometime between now and 90 days ago
        # if this isn't the first time we are generating data we will let "last_date_run" be equal to the saved time in the file
        if initial_data_generation == True:
            start_date = start_date + datetime.timedelta(days=(random.randrange(1,30)))
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
            with gzip.open('{}user_{}_{}.gz'.format(output_data_path, shard_key, today_date.strftime("%Y-%m-%d")), 'w') as f:
                f.write(json.dumps(data).encode(errors='ignore'))
            user_index = user_index + 1
            print("count of users saved: {}".format(user_index))
            print("count of users with no data {}".format(no_data_count))
    # create a list of all ids that should now churn
    ids_to_remove = [ids for ids in primary_shard_key_dict if primary_shard_key_dict[ids]['churned'] == True]
    # remove the ids that should churn from our primary_shard_key_dict so they "churn" in the dataset
    for id in ids_to_remove:
        print("user churned! - {} ".format(id))
        del primary_shard_key_dict[id]
    # after all the ids have generated events write the remaining users to a file and save
    with open("{}{}.json".format(user_list_path, user_id_list_file_name), 'w') as write_user_id_file:
        write_user_id_file.write(json.dumps(primary_shard_key_dict))
    # once all events have been generated make it so the date file is updated
    # to reflect the most recent date and hour the script was run
    with open("{}{}".format(last_date_run_file_location, last_date_run_file), 'w') as date_file:
        date_file.write(today_date.strftime("%Y-%m-%d %H:%M:%S"))


#start of app
if __name__ == '__main__':
    # get the last time the script was run from the data file
    date_file_check = Path("{}{}".format(last_date_run_file_location,last_date_run_file))
    if date_file_check.is_file():
        with open("{}{}".format(last_date_run_file_location,last_date_run_file), 'r') as date_file:
            last_date_run = date_file.read()
            last_date_run = datetime.datetime.strptime(last_date_run.strip(), "%Y-%m-%d %H:%M:%S")
    else:
        with open("{}{}".format(last_date_run_file_location,last_date_run_file),'w') as date_file:
            last_date_run = datetime.datetime.now()-datetime.timedelta(days=90)
            date_file.write(last_date_run.strftime("%Y-%m-%d %H:%M:%S"))

    # check to see if today's current time is earlier than the time right now. if it is run the script
    if last_date_run < today_date:
        # get the event dict
        event_dict = csv_to_dict(event_file)
        ## check to see if we have a list of the primary shard key, if not create one and save as a csv file
        my_file = Path("{}{}.json".format(user_list_path, user_id_list_file_name))

        if my_file.is_file():
            with open("{}{}.json".format(user_list_path, user_id_list_file_name), 'r') as read_user_id_file:
                primary_shard_key_dict = json.loads(read_user_id_file.read())
                primary_shard_key_dict = primary_shard_key_dict
            # add some new users to the file - this is mostly so we can change metrics like retention and do "new user" analysis
            primary_shard_key_dict = get_new_users(primary_shard_key_dict=primary_shard_key_dict,
                                                   number_of_users=new_user_to_generate_per_period)
            generate_data(all_ids=primary_shard_key_dict,
                          last_run_date=last_date_run,
                          initial_data_generation=False,
                          today=today_date)

        else:
            # create a dictionary of users where the key is the user id and store additional details about the user in another dict
            primary_shard_key_dict = dict()
            for x in range(100):
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
                          today=today_date)
    #TODO check to see if lookup table is created, if not create it and write to file as JSON
