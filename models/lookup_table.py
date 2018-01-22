'''
Lookup class to turn a csv table into json for each user to create more permenate user values
this will do a better job of creating "winners" and "losers" on conversion like event after data
is generated

'''
import csv
from models.functions import csv_to_dict
import random
import math


class Lookup_table(object):

    def __init__(self, lookup_table_csv, user_list):
        self.csv_file = lookup_table_csv
        self.lookup_dict = csv_to_dict(lookup_table_csv)
        self.user_list = user_list

    def generate_lookup_table(self):
        user_ids_list = list(self.user_list.keys())
        user_properties = dict()
        for user_id in user_ids_list:
            user_properties['id'] = str(user_id)
            for property_name, property_values in self.lookup_dict.items():
            #get the number of property values for each property
                values_length = (len(property_values)-1)
                random_roll = random.uniform(0, 1)
                if values_length == 1:
                    if random_roll > .4:
                        property_index = 0
                    else:
                        property_index = 1
                elif values_length == 2:
                    if random_roll > .5:
                        property_index = 0
                    elif random_roll <= .5 and random_roll >= .2:
                        property_index = 1
                    else:
                        property_index = 2
                elif values_length > 2:
                    intervals = math.floor(values_length / 4)
                    if random_roll > .6:
                        property_index = random.randrange(0,intervals+1)
                    elif random_roll <= .6 and random_roll >= .30:
                        property_index = random.randrange(intervals,(intervals*2)+1)
                    elif random_roll < .30 and random_roll > .05:
                        property_index = random.randrange((intervals*2), (intervals * 3)+1)
                    else:
                        property_index = random.randrange((intervals * 3), (intervals * 4)+1)
                else:
                    property_index = 0
                user_properties[property_name] = self.event_dict[property_name][property_index]
        return user_properties











