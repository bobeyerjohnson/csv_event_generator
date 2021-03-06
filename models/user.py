import csv
import json
import random
import uuid
from models.event import Event
from models.functions import csv_to_dict, increase_time_event_time
import datetime



'''
User class to handle the creation of a user and hold specific
pieces of information constant such as user properties -- this might happend via a lookup table
so this might not happen
'''


class User(object):

    def __init__(self, primary_shard_key_value, last_date_run, user_flows_file_location, event_dict_file_location, user_probability, today, skip_days_counter, churn_prob_reset_counter, open_file):
        self.primary_shard_key_value = primary_shard_key_value
        self.user_flows_file_location = user_flows_file_location
        self.probability = user_probability
        self.event_dict = csv_to_dict(event_dict_file_location)
        self.time = last_date_run
        self.today = today
        self.churn = False
        self.session_id = uuid.uuid4()
        self.open_file = open_file
        self.additional_shard_keys_list = self.event_dict['shardkeys']
        self.primary_shard_key_name = self.additional_shard_keys_list[0]
        #remove session_id from shard key list if it is present so we can have more control of session_id creation and deletion within user flows
        #remove the primary_shard_key_value to better handle remaining secondary shard keys in the event class
        self.additional_shard_keys_list = [x for x in self.additional_shard_keys_list if x.lower().strip() not in ['session_id', 'sessionid', self.additional_shard_keys_list[0].lower()]]
        self.shard_key_dict = dict()
        for single_additional_shard_key in self.additional_shard_keys_list:
            self.shard_key_dict[single_additional_shard_key] = uuid.uuid4()
        # delete the shard keys from the event and property dic so we we can handle separately from events/flow creation later on in the even class
        del self.event_dict['shardkeys']
        self.churn_threshold = .7
        self.skip_days_counter = skip_days_counter
        self.churn_prob_reset_counter = churn_prob_reset_counter
        self.skip_days_counter_reset_value = 4

    def write_to_open_file(self,open_file,event):
        open_file.write(json.dumps(event).encode())
        open_file.write(','.encode())

    def random_events(self, number_of_events, event_instance, open_file,event_dict, ts):
        for x in range(number_of_events):
            if (self.time + datetime.timedelta(hours=1)) < self.today:
                random_event_name = event_dict[round(random.uniform(0, 1) * (len(event_dict) - 1))]
                single_event = event_instance.generate_event(event_name=random_event_name, ts=ts)
                self.write_to_open_file(open_file,single_event)
                ## increase the time stamp so we move forward in time
                self.time, self.probability, self.skip_days_counter, self.churn_prob_reset_counter = increase_time_event_time(
                    time=self.time,
                    probability=self.probability,
                    skip_days_counter=self.skip_days_counter,
                    churn_prob_reset_counter=self.churn_prob_reset_counter,
                    skip_days_counter_reset_value=self.skip_days_counter_reset_value
                )
            else:
                break

    def generate_events(self, flows_obj, start_time):
        self.time = start_time
        event_list = list()
        while True:
            if (self.time + datetime.timedelta(hours=1)) > self.today:
                break
        # loop through all of the user flows
            for keys, values in flows_obj.items():
                # make sure we have not passed today's date
                if (self.time + datetime.timedelta(hours=1)) < self.today:
                    event_description = Event(primary_shard_key_name=self.primary_shard_key_name,
                                    primary_shard_key_value=self.primary_shard_key_value,
                                    event_properties=self.event_dict,
                                    session_id=self.session_id,
                                    shard_key_dict=self.shard_key_dict
                                  )
                    for event_in_flow in values:
                        # make sure we have not passed today's date
                        if (self.time + datetime.timedelta(hours=1)) < self.today:
                            # random check to see if we change the the other non-primary shard_key ids
                            if (random.uniform(0, 1)) >= .8:
                                self.session_id = uuid.uuid4()
                                for key, value in self.shard_key_dict.items():
                                    self.shard_key_dict[key] = uuid.uuid4()
                            # random check to see if they do the event, self.probability is per user so that "good" user succeed more and "bad" user fail
                            if (self.probability * random.uniform(.9, 1.1)) >= .25:
                                # add the name of the event
                                single_event = event_description.generate_event(event_name=event_in_flow,ts=self.time)
                                self.write_to_open_file(open_file=self.open_file, event=single_event)
                                ## increase the time stamp so we move forward in time
                                self.time, self.probability, self.skip_days_counter, self.churn_prob_reset_counter = increase_time_event_time(
                                    time=self.time,
                                    probability=self.probability,
                                    skip_days_counter=self.skip_days_counter,
                                    churn_prob_reset_counter=self.churn_prob_reset_counter,
                                    skip_days_counter_reset_value=self.skip_days_counter_reset_value)
                                # random roll to see if they do more events between events in a flow
                                # this will be totally random as to whether "good" or "bad" user do more events or not
                                if random.uniform(0, 1) >= .5:
                                    self.random_events(number_of_events=6,
                                                       event_instance=event_description,
                                                       open_file=self.open_file,
                                                       event_dict=self.event_dict['event'],
                                                       ts=self.time
                                                       )
                            # if they do not pass the funnel flow, make sure they break out of that flow
                            # and generate 5 random events
                            else:
                                self.random_events(number_of_events=5,
                                                   event_instance=event_description,
                                                   open_file=self.open_file,
                                                   event_dict=self.event_dict['event'],
                                                   ts=self.time
                                                   )
                                break
                        else:
                            break
                else:
                    break
            # make it so that bad user can churn here otherwise we have 100% retention when we first generate new users
            if self.probability <= self.churn_threshold or self.probability == True:
                break
            # after we have run through a defined flow of events let's do a check to see if other random events are done. Good users should do this more often
            if (self.probability * random.uniform(.9, 1.1)) >= .20 and (self.time + datetime.timedelta(hours=1)) < self.today:
                self.random_events(number_of_events=10,
                                   event_instance=event_description,
                                   open_file=self.open_file,
                                   event_dict=self.event_dict['event'],
                                   ts=self.time
                                   )

    def generate_flows(self,flows_obj):
        self.generate_events(flows_obj=flows_obj, start_time=self.time)
        # do a check to see if we make the user churn or not. very bad users will churn every time
        if self.probability <= self.churn_threshold:
            # give user below the churn threshold a chance to stick around
            if self.probability <= random.uniform(0.2,0.9):
                self.churn = True
        return self.churn, self.probability, self.skip_days_counter, self.churn_prob_reset_counter, self.time

    # property to properly format the csv file of the event flows
    @property
    def flow_dict(self):
        flows = dict()
        with open(self.user_flows_file_location, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) # skip the header of the file
            for row in reader:
                flows[row[0]] = row[1:]
        for keys, pair in flows.items():
            flows[keys] = list(filter(None,flows[keys]))
        return flows




