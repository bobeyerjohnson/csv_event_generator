import random

'''
Event class to handle the creation of a event and associated meta data
'''


class Event(object):

    def __init__(self, user_id, event_name, event_dict, ts, session_id):
        self.user_id = user_id
        self.shard_key1 = event_dict['shardkeys'][0]
        self.shard_key2 = event_dict['shardkeys'][1]
        self.shard_key3 = event_dict['shardkeys'][2]
        self.event_name = event_name
        self.event_dict = event_dict
        self.ts = ts
        self.session_id = session_id

    def generate_event(self):
        event = dict()
        event[self.shard_key1] = str(self.user_id)
        event['event'] = self.event_name
        event['session_id'] = str(self.session_id)
        for property_name, values in self.event_dict.items():
            # check to make sure we do not treat the event and shardkeys columns as properties
            if property_name not in ['event', 'shardkeys']:
                #get the number of property values for each property
                values_length = len(values)
                event[property_name] = self.event_dict[property_name][(round(values_length * (random.uniform(0,1)))-1)]
                event['ts'] = self.ts.strftime("%Y-%m-%d %H:%M:%S")
        return event


