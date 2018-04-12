'''
Class to handle the creation of event properties
'''

import random
import math

class Event_Properties(object):
    def __init__(self,event_dict):
        self.event_dict = event_dict

    def get_event_properties(self):
        event_properties = dict()
        for property_name, values in self.event_dict.items():
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
                    event_properties[property_name] = self.event_dict[property_name][property_index]
        return event_properties
