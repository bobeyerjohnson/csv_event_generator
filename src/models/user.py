import csv
import random
import uuid
from src.models.event import Event
from src.models.functions import csv_to_dict, increase_time


'''
User class to handle the creation of a user and hold specific
pieces of information constant such as user properties -- this might happend via a lookup table
so this might not happen
'''


class User(object):

    def __init__(self, primary_shard_key_value, last_date_run, user_flows_file_location, event_dict_file_location, user_probability):
        self.primary_shard_key_value = primary_shard_key_value
        self.user_flows_file_location = user_flows_file_location
        self.probability = user_probability
        self.event_dict = csv_to_dict(event_dict_file_location)
        self.time = last_date_run
        self.churn = False
        self.shard_keys = self.event_dict['shardkeys']
        self.primary_shard_key_name = self.shard_keys[0]
        #remove session_id from shard key list if it is present so we can have more control of session_id creation and deletion within user flows
        #remove the primary_shard_key_value to better handle remaining secondary shard keys in the event class
        self.shard_keys = [x for x in self.shard_keys if x.lower().strip() not in ['session_id','sessionid', self.shard_keys[0].lower()]]

        # delete the shard keys from the event and property dic so we we can handle separately from events/flow creation later on in the even class
        del self.event_dict['shardkeys']

    def generate_flows(self, flows_obj):
        event_list = list()
        #generate a session_id -- could move this into the for .items loop to make sure session is changed each time we do a new flow. not needed at the moment
        session_id = uuid.uuid4()
        #TODO need to handle the additional shard keys in a smarter way
        shard_key_dict = dict()
        for shard_key in self.shard_keys:
            shard_key_dict[shard_key] = uuid.uuid4()
        #loop through all of the user flows
        #TODO need to create more variablilty between good and bad users. total number of events created is too similar
        for keys, values in flows_obj.items():
            for event_in_flow in values:
                # random check to see if we change the session_id as well as the other non-primary ids
                if (random.uniform(0, 1)) >= .8:
                    session_id = uuid.uuid4()
                    for key, value in shard_key_dict.items():
                        shard_key_dict[key] = uuid.uuid4()
                # random check to see if they do the event, self.probability is per user so that "good" user succeed more and "bad" user fail
                if self.probability >= .6:
                    single_event = Event(event_name=event_in_flow,
                                         primary_shard_key_name=self.primary_shard_key_name,
                                         primary_shard_key_value=self.primary_shard_key_value,
                                         event_dict=self.event_dict,
                                         ts=self.time,
                                         session_id=session_id,
                                         shard_key_dict=shard_key_dict)
                    event = single_event.generate_event()
                    event_list.append(event)
                    # random roll to see if they do more events between events in a flow
                    # this will to totally random as to whether "good" or "bad" user do more events or not
                    if random.uniform(0,1) >= .5:
                        for x in range(6):
                            random_event = self.event_dict['event'][round(random.uniform(0, 1) * (len(self.event_dict['event']) - 1))]
                            single_event = Event(event_name=random_event,
                                                 primary_shard_key_name=self.primary_shard_key_name,
                                                 primary_shard_key_value=self.primary_shard_key_value,
                                                 event_dict=self.event_dict, ts=self.time, session_id=session_id,
                                                 shard_key_dict=shard_key_dict)
                            event = single_event.generate_event()
                            event_list.append(event)
                            ## increase the time stamp so we move forward in time
                            self.time = increase_time(time=self.time,
                                              probability=self.probability)
                # if they do not pass the funnel flow, make sure they break out of that flow
                # and generate 5random events and add to their event list only if they are
                # "good" user. probability of  great than 70
                else:
                    for x in range(5):
                        random_event = self.event_dict['event'][round(random.uniform(0,1)*(len(self.event_dict['event'])-1))]
                        single_event = Event(event_name=random_event, primary_shard_key_name=self.primary_shard_key_name, primary_shard_key_value=self.primary_shard_key_value, event_dict=self.event_dict, ts=self.time, session_id=session_id, shard_key_dict=shard_key_dict)
                        event = single_event.generate_event()
                        event_list.append(event)
                        self.time = increase_time(time=self.time,
                                                  probability=self.probability)
                    break
            #after we have run through a defined flow fo events let's do a check to see if other random events are done. Good users should do this more often
            if self.probability >= .6:
                for x in range(10):
                    random_event = self.event_dict['event'][round(random.uniform(0,1)*(len(self.event_dict['event'])-1))]
                    single_event = Event(event_name=random_event, primary_shard_key_name=self.primary_shard_key_name, primary_shard_key_value=self.primary_shard_key_value, event_dict=self.event_dict, ts=self.time, session_id=session_id, shard_key_dict=shard_key_dict)
                    event = single_event.generate_event()
                    event_list.append(event)
                    ## increase the time stamp so we move forward in time
                    self.time = increase_time(time=self.time,
                                              probability=self.probability)
            # random check about the user's probability to see if they continue doing flows of if they stop
            # random check will be low because if they do not pass this test we will also make them churn
            if self.probability <= .2:
                self.churn = True
                break
        return event_list, self.churn
    # property to properly format the csv file of the event flows
    @property
    def flow_dict(self):
        flows = dict()
        with open(self.user_flows_file_location, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                flows[row[0]] = row
                flows[row[0]].remove(row[0])
        del flows['Flow name']
        for keys, pair in flows.items():
            flows[keys] = list(filter(None,flows[keys]))
        return flows






