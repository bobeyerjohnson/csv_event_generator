import csv
import random
import uuid
from models.event import Event
from models.functions import csv_to_dict, increase_time
import datetime


'''
User class to handle the creation of a user and hold specific
pieces of information constant such as user properties -- this might happend via a lookup table
so this might not happen
'''


class User(object):

    def __init__(self, primary_shard_key_value, last_date_run, user_flows_file_location, event_dict_file_location, user_probability, today):
        self.primary_shard_key_value = primary_shard_key_value
        self.user_flows_file_location = user_flows_file_location
        self.probability = user_probability
        self.event_dict = csv_to_dict(event_dict_file_location)
        self.time = last_date_run
        self.today = today
        self.churn = False
        self. session_id = uuid.uuid4()
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
        self.churn_threshold = .6

    def generate_events(self, flows_obj, start_time):
        self.time = start_time
        event_list = list()
        while True:
            if (self.time + datetime.timedelta(days=.5)) > self.today:
                break
        # loop through all of the user flows
            for keys, values in flows_obj.items():
                # make sure we have not passed today's date
                if (self.time + datetime.timedelta(days=.5)) < self.today:
                    for event_in_flow in values:
                        # make sure we have not passed today's date
                        if (self.time + datetime.timedelta(days=.5)) < self.today:
                            #     break
                            # random check to see if we change the session_id as well as the other non-primary ids
                            if (random.uniform(0, 1)) >= .8:
                                self.session_id = uuid.uuid4()
                                for key, value in self.shard_key_dict.items():
                                    self.shard_key_dict[key] = uuid.uuid4()
                            # random check to see if they do the event, self.probability is per user so that "good" user succeed more and "bad" user fail
                            if (self.probability * random.uniform(.9, 1.1)) >= .25:
                                single_event = Event(event_name=event_in_flow,
                                                     primary_shard_key_name=self.primary_shard_key_name,
                                                     primary_shard_key_value=self.primary_shard_key_value,
                                                     event_dict=self.event_dict,
                                                     ts=self.time,
                                                     session_id=self.session_id,
                                                     shard_key_dict=self.shard_key_dict)
                                event = single_event.generate_event()
                                event_list.append(event)
                                # random roll to see if they do more events between events in a flow
                                # this will be totally random as to whether "good" or "bad" user do more events or not
                                if random.uniform(0, 1) >= .5:
                                    for x in range(6):
                                        if (self.time + datetime.timedelta(days=.5)) < self.today:
                                            random_event = self.event_dict['event'][round(random.uniform(0, 1) * (len(self.event_dict['event']) - 1))]
                                            single_event = Event(event_name=random_event,
                                                                 primary_shard_key_name=self.primary_shard_key_name,
                                                                 primary_shard_key_value=self.primary_shard_key_value,
                                                                 event_dict=self.event_dict, ts=self.time,
                                                                 session_id=self.session_id,
                                                                 shard_key_dict=self.shard_key_dict)
                                            event = single_event.generate_event()
                                            event_list.append(event)
                                            ## increase the time stamp so we move forward in time
                                            self.time = increase_time(time=self.time,
                                                                      probability=self.probability)
                                        else:
                                            break
                            # if they do not pass the funnel flow, make sure they break out of that flow
                            # and generate 5random events and add to their event list only if they are
                            # "good" user
                            else:
                                for variable in range(5):
                                    if (self.time + datetime.timedelta(days=.5)) < self.today:
                                        random_event = self.event_dict['event'][round(random.uniform(0, 1) * (len(self.event_dict['event']) - 1))]
                                        single_event = Event(event_name=random_event,
                                                             primary_shard_key_name=self.primary_shard_key_name,
                                                             primary_shard_key_value=self.primary_shard_key_value,
                                                             event_dict=self.event_dict,
                                                             ts=self.time,
                                                             session_id=self.session_id,
                                                             shard_key_dict=self.shard_key_dict)
                                        event = single_event.generate_event()
                                        event_list.append(event)
                                        self.time = increase_time(time=self.time,
                                                                  probability=self.probability)
                                    else:
                                        break
                                break
                        else:
                            break
                else:
                    break
            # make it so that bad user can churn here otherwise we have 100% retention when we first generate new users
            if self.probability <= self.churn_threshold:
                break
            # after we have run through a defined flow of events let's do a check to see if other random events are done. Good users should do this more often
            if self.probability >= .6 and (self.time + datetime.timedelta(days=.5)) < self.today:
                for counter in range(10):
                    if (self.time + datetime.timedelta(days=.5)) < self.today:
                        random_event = self.event_dict['event'][round(random.uniform(0, 1) * (len(self.event_dict['event']) - 1))]
                        single_event = Event(event_name=random_event,
                                             primary_shard_key_name=self.primary_shard_key_name,
                                             primary_shard_key_value=self.primary_shard_key_value,
                                             event_dict=self.event_dict,
                                             ts=self.time,
                                             session_id=self.session_id,
                                             shard_key_dict=self.shard_key_dict)
                        event = single_event.generate_event()
                        event_list.append(event)
                        ## increase the time stamp so we move forward in time
                        self.time = increase_time(time=self.time,
                                                  probability=self.probability)

        if len(event_list) == 0:
            return None
        else:
            return event_list

    def generate_flows(self, flows_obj):
        final_list_of_events = self.generate_events(flows_obj=flows_obj, start_time=self.time)
        # if we return none for the user list set make it so no_events_in_list is True
        if final_list_of_events:
            no_events_in_list = False
        else:
            no_events_in_list = True

        # random check about the user's probability to see if they continue doing flows or if they stop
        # random check will be low because if they do not pass this test we will also make them churn
        # We also need to check to see if the user has any events to reutrn and handle all possibilities of no events and churn
        # return should be the list of event (or None) and churn as true or false
        if self.probability <= self.churn_threshold:
            self.churn = True
            if no_events_in_list:
                return None, self.churn
            else:
                return final_list_of_events, self.churn
        elif no_events_in_list:
            return None, self.churn
        elif not no_events_in_list:
            return final_list_of_events, self.churn

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




