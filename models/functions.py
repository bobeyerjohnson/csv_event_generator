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

def increase_time_in_seconds(event_time, start_sec, stop_sec, interval_sec):
    time_increase = round(random.randrange(start_sec, stop_sec, interval_sec))
    new_time = event_time + datetime.timedelta(seconds=time_increase)
    return new_time
def increase_time_in_days(event_time, start_days, stop_days, interval):
    days_increase = random.randrange(start_days, stop_days,interval)
    new_time = event_time + datetime.timedelta(days=days_increase)
    return new_time

# increase the time stamp so we move forward in time
def increase_time_event_time(time, probability, skip_days_counter_reset_value, skip_days_counter, churn_prob_reset_counter):
    new_prob = probability
    new_skip_days_counter = skip_days_counter
    new_churn_prob_reset_counter = churn_prob_reset_counter
    day_of_week = time.weekday()
    hour_of_day = time.hour
    # make it so that monday through friday and 9 am to 10 pm have more events by assigning a variable that makes it so
    # random range interval is smaller and thus will product more events in theory
    if day_of_week in [0,1,2,3,4] and hour_of_day in [9,10,11,12,13,14,15,16,17,18,19]:
        interval = 5
        if probability >= .6:
            new_time = increase_time_in_seconds(event_time=time, start_sec=1, stop_sec=(60 * 60),
                                                interval_sec=interval)
        else:
            new_time = increase_time_in_seconds(event_time=time, start_sec=(60*5), stop_sec=(60 * 60 *3),
                                                interval_sec=(interval * 6))
    else:
        interval = (60*30)
        if probability >= .6:
            new_time = increase_time_in_seconds(event_time=time, start_sec=60, stop_sec=(60 * 60 * 12),
                                                interval_sec=interval)
        else:
            new_time = increase_time_in_seconds(event_time=time, start_sec=(60 * 5), stop_sec=(60 * 60 * 12),
                                            interval_sec=(interval * 4))
    # check to see if it a shard key has been producing events over a five days span. If it has been 5 days or greater reset the counter
    if skip_days_counter >= skip_days_counter_reset_value:
        # depending on the shard keys churn probability make the number of days shard keys don't perform events different
        if probability >= .9:
            # random roll to see how many days the shard key goes dormant/does not produce events
            new_time = increase_time_in_days(event_time=new_time, start_days=1, stop_days=3, interval=1)
        elif probability < .9 and probability >= .8:
            # random roll to see how many days the shard key goes dormant/does not produce events
            new_time = increase_time_in_days(event_time=new_time, start_days=1, stop_days=5, interval=1)
        elif probability < .8 and probability >= .7:
            new_time = increase_time_in_days(event_time=new_time, start_days=1, stop_days=10, interval=2)
        elif probability < .7 and probability >= .6:
            new_time = increase_time_in_days(event_time=new_time, start_days=2, stop_days=14, interval=2)
        elif probability < .6 and probability >= .4:
            new_time = increase_time_in_days(event_time=new_time, start_days=3, stop_days=21, interval=3)
        elif probability < .4 and probability >= .2:
            new_time = increase_time_in_days(event_time=new_time, start_days=4, stop_days=24, interval=4)
        else:
            new_time = increase_time_in_days(event_time=new_time, start_days=5, stop_days=30, interval=5)
        #reset the counter
        new_skip_days_counter = 0
        #increment up the number of times we have reset the churn_prob_reset_counter counter. the idea being that after a certian
        #number of times we want to re-evaluate the shard keys probability so we do not have users the exist forever
        new_churn_prob_reset_counter = churn_prob_reset_counter + 1
        # see how many times we have reset skip_days_counter to zero, if it's been more than 8 times, assign a new churn probability
        # this is so that "good" user don't last forever
        if new_churn_prob_reset_counter >= 5:
            new_churn_prob_reset_counter = 0
            #reset the shard key probability
            new_prob = random.uniform(0,1)
    else:
        # check to see if the time of our new event is in the next calendar day compared to old event time
        # if so increase the skip_days_counter for the shard keys so that after a certain amount of days
        # we can make them stop producing events
        new_time_str = new_time.strftime("%Y-%m-%d")
        old_time_str = time.strftime("%Y-%m-%d")
        if new_time_str > old_time_str:
            days_difference = new_time.day - time.day
            new_skip_days_counter = skip_days_counter + days_difference
    return new_time, new_prob, new_skip_days_counter, new_churn_prob_reset_counter

def get_new_users(primary_shard_key_dict, number_of_users,event_start_time):
    for new_user in range(round(number_of_users)):
        shard_key1 = str(uuid.uuid4())
        user_probability = random.uniform(0, 1)
        primary_shard_key_dict[shard_key1] = dict()
        primary_shard_key_dict[shard_key1]['user_probability'] = user_probability
        primary_shard_key_dict[shard_key1]['churned'] = 'False'
        primary_shard_key_dict[shard_key1]['skip_days_counter'] = 0
        primary_shard_key_dict[shard_key1]['churn_prob_reset_counter'] = 0
        primary_shard_key_dict[shard_key1]['last_event_time'] = datetime.datetime.strftime(event_start_time,"%Y-%m-%d %H:%M:%S")

    return primary_shard_key_dict


