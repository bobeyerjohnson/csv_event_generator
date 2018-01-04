import csv
import datetime
import random

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

def increase_time(time):
    ## increase the time stamp so we move forward in time
    time_increase = random.randrange(1, (60 * 20), 1)
    time = time + datetime.timedelta(seconds=time_increase)
    return time
