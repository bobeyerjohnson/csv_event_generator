import random

'''
Event class to handle the creation of a event and associated meta data
'''


class Event(object):

    def __init__(self,primary_shard_key_name, primary_shard_key_value, event_name, event_dict, ts, session_id, shard_key_dict):
        self.primary_shard_key_name = primary_shard_key_name
        self.primary_shard_key_value = primary_shard_key_value
        self.event_name = event_name
        self.event_dict = event_dict
        self.ts = ts
        self.session_id = session_id
        self.shard_key_dict = shard_key_dict

    def generate_event(self):
        event = dict()
        event[self.primary_shard_key_name] = str(self.primary_shard_key_value)
        event['event'] = self.event_name
        event['session_id'] = str(self.session_id)
        for key, values in self.shard_key_dict.items():
            event[key] = str(values)
        for property_name, values in self.event_dict.items():
            #get the number of property values for each property
            values_length = len(values)
            event[property_name] = self.event_dict[property_name][(round(values_length * (random.uniform(0,1)))-1)]
            event['ts'] = self.ts.strftime("%Y-%m-%d %H:%M:%S")
        return event


