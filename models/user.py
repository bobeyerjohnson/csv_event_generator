import csv
import random
from models.event import Event
from models.functions import csv_to_dict, increase_time
import datetime
import uuid

'''
User class to handle the creation of a user and hold specific
pieces of information constant such as user properties -- this might happend via a lookup table
so this might not happen
'''


class User(object):

    def __init__(self, userId, last_date_run, user_flows_file_location, event_dict_file_location):
        self.userId = userId
        self.user_flows_file_location = user_flows_file_location
        self.probability = random.uniform(0,1)
        self.event_dict = csv_to_dict(event_dict_file_location)
        self.time = last_date_run
        #TODO clean up the shard key column so we don't assume 3 shard keys, need to determine amount and produce accordinly 
        self.shardKey2 = self.event_dict['shardkeys'][1]
        self.shardKey3 = self.event_dict['shardkeys'][2]

    def generate_flows(self, flows_obj):
        event_list = list()
        #generate a session_id -- could move this into the for .items loop to make sure session is changed each time we do a new flow. not needed at the moment
        session_id = uuid.uuid4()
        #loop through all of the user flows
        for keys, values in flows_obj.items():

            #loop through all the events in each flow one by one
            if 'session_id' in [self.shardKey2.lower(), self.shardKey3.lower()]:
                for event_in_flow in values:

                    #random check to see if we change the session_id for the user
                    if (random.uniform(0, 1)) > .9:
                        session_id = uuid.uuid4()

                    #random check to see if they do the event
                    if (random.uniform(0,1)) <= self.probability: ## do a probability check -  if number is less than or equal to the probability of user value let them continue
                        single_event = Event(event_name=event_in_flow, user_id=self.userId, event_dict=self.event_dict, ts=self.time, session_id=session_id)
                        event = single_event.generate_event()
                        event_list.append(event)

                        #TODO random roll to see if they do more events between events in a flow

                        ## increase the time stamp so we move forward in time
                        self.time = increase_time(self.time)

                    # if they do not pass the funnel flow, make sure they break out of that flow
                    # and generate 5 random events and add to their event list only if they are
                    # "good" user. probability of  great than 70
                    else:
                        self.time = increase_time(self.time)
                        break
                    if self.probability > .7:
                        for x in range(5):
                            random_event = self.event_dict['event'][round(random.uniform(0,1)*(len(self.event_dict['event'])-1))]
                            single_event = Event(event_name=random_event, user_id=self.userId, event_dict=self.event_dict, ts=self.time, session_id=session_id)
                            event = single_event.generate_event()
                            event_list.append(event)

                            ## increase the time stamp so we move forward in time
                            self.time = increase_time(self.time)
            else:
                for event_in_flow in values:

                    # random check to see if we change the session_id for the user
                    if (random.uniform(0, 1)) > .9:
                        session_id = uuid.uuid4()

                    # random check to see if they do the event
                    if random.uniform(0,1) <= self.probability: ## do a probability check -  if number is less than or equal to the probability of user value let them continue
                        single_event = Event(event_name=event_in_flow, user_id=self.userId, event_dict=self.event_dict, ts=self.time, session_id=session_id)
                        event = single_event.generate_event()
                        event_list.append(event)

                        #TODO random roll to see if they do more events between events in a flow

                        ## increase the time stamp so we move forward in time
                        self.time = increase_time(self.time)

                    # if they do not pass the funnel flow, make sure they break out of that flow
                    # and generate 5 random events and add to their event list only if they are
                    # "good" user. probability of  great than 70
                    else:
                        self.time = increase_time(self.time)
                        break
                    if self.probability > .7:
                        for x in range(5):
                            random_event = self.event_dict['event'][round(random.uniform(0,1)*(len(self.event_dict['event'])-1))]
                            single_event = Event(event_name=random_event, user_id=self.userId, event_dict=self.event_dict, ts=self.time)
                            event = single_event.generate_event()
                            event_list.append(event)

                            ## increase the time stamp so we move forward in time
                            self.time = increase_time(self.time)
        return event_list
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

    # calculate chance user has to get through a flow. range is 1% to 100%
    def probability_to_complete(self, flow):
        flow_length =len(flow)
        probability = random.randrange(.01,1,.01)
        return probability
        pass





