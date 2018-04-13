import random
import math


'''
Event class to handle the creation of a event and associated meta data
'''


class Event(object):

    def __init__(self,primary_shard_key_name, primary_shard_key_value, event_properties, session_id, shard_key_dict):
        self.event = dict()
        self.event[primary_shard_key_name] = str(primary_shard_key_value)
        self.event['session_id'] = str(session_id)
        for key, shard_values in shard_key_dict.items():
            self.event[key] = str(shard_values)
        for property_name, values in event_properties.items():
            # be sure to skip the event dict
            if not property_name == 'event':
                # get the number of property values for each property
                values_index = (len(values) - 1)
                random_roll = random.uniform(0, 1)
                if values_index == 1:
                    if random_roll > .4:
                        property_index = 0
                    else:
                        property_index = 1
                elif values_index == 2:
                    if random_roll > .5:
                        property_index = 0
                    elif random_roll <= .5 and random_roll >= .2:
                        property_index = 1
                    else:
                        property_index = 2
                elif values_index > 2:
                    intervals = math.floor(values_index / 4)
                    index_remainder = values_index % 4
                    if random_roll > .6:
                        property_index = random.randrange(0, (intervals + index_remainder))
                    elif random_roll <= .6 and random_roll >= .30:
                        property_index = random.randrange(intervals, (intervals * 2) + index_remainder)
                    elif random_roll < .30 and random_roll > .05:
                        property_index = random.randrange((intervals * 2), (intervals * 3) + index_remainder)
                    else:
                        property_index = random.randrange((intervals * 3), ((intervals * 4) + index_remainder))
                else:
                    property_index = 0
                self.event[property_name] = event_properties[property_name][property_index]


    def generate_event(self, event_name, ts):
        self.event['event'] = event_name
        self.event['ts'] = ts.strftime("%Y-%m-%d %H:%M:%S")
        return self.event



