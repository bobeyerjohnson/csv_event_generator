import random
import csv
import math


class Lookup_Table_Generator(object):
    def __init__(self, id_list,lookup_file):
        self.id_list = id_list
        self.lookup_dict = self.csv_to_dict(lookup_file=lookup_file)

    def csv_to_dict(self, lookup_file):
        data = dict()
        with open(lookup_file, newline='', encoding='utf-8', errors='ignore') as csv_events:
            reader = csv.DictReader(csv_events)
            for header in reader.fieldnames:
                # do not include blanks in dict
                if header != '':
                    data[header] = list()
            for row in reader:
                for header in reader.fieldnames:
                    # do not include blanks in dict
                    if row[header] != '':
                        data[header].append(row[header])
        return data

    def create_table(self):
        lookup_table_data = list()
        x = 0
        for id in self.id_list:
            lookup_table_data.append({"shard_key_id":id})
            for property_name,property_value in self.lookup_dict.items():
                if property_name != 'shardkeys':
                    values_index = (len(property_value) - 1)
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
                            property_index = random.randrange(0, (intervals+index_remainder))
                        elif random_roll <= .6 and random_roll >= .30:
                            property_index = random.randrange(intervals, (intervals * 2) + index_remainder)
                        elif random_roll < .30 and random_roll > .05:
                            property_index = random.randrange((intervals * 2), (intervals * 3) + index_remainder)
                        else:
                            property_index = random.randrange((intervals * 3), ((intervals * 4) + index_remainder))
                    else:
                        property_index = 0
                    lookup_table_data[x][property_name] = self.lookup_dict[property_name][property_index]
            x = x + 1
        return lookup_table_data