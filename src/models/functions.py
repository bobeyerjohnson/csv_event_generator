import csv
import datetime
import random
import uuid


def csv_to_dict(file):
    data = dict()
    with open(file, newline='', encoding='utf-8',errors='ignore') as csv_events:
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

# increase the time stamp so we move forward in time
def increase_time(time, probability):
    if probability >= .6:
        time_increase = random.randrange(1, (60 * 60 * 6), 10)
        time = time + datetime.timedelta(seconds=time_increase)
    else:
        time_increase = random.randrange(1, (60 * 60 * 12), 10)
        time = time + datetime.timedelta(seconds=time_increase)
    return time

def get_new_users(primary_shard_key_dict, number_of_users):
    for new_user in range(number_of_users):
        shard_key1 = str(uuid.uuid4())
        user_probability = random.uniform(0, 1)
        primary_shard_key_dict[shard_key1] = dict()
        primary_shard_key_dict[shard_key1]['user_probability'] = user_probability
    return primary_shard_key_dict


