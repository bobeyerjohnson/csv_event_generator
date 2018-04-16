# Dummy Event Generation Script
This is a simple python script designed to generate events based on a formatted csv file with dummy data. Dummy data is outputted in JSON
### How to Run
Within the root folder run:
```linux
$ python3 event_generator.py
``` 
Then follow the prompts to provide the needed files and dir locations.

### Requirements
   3 csv files:
1) Events File - This contains all the events and possible event properties that will be used for data generation
2) Flows File - "Flows" referrers to the "flows" or "path" that events will occur. This gives structure to the order in which events will be performed and is fundamental to creating trends and behaviors in the data
3) Lookup Table File - csv file that takes in values and assigns a single value for each property per Id

   Example template for all files can be found [here](https://docs.google.com/spreadsheets/d/1b0LrIq6rxfB5uJaZGDOg9VxvtkabRAym2mgwMC9T0ME/edit?usp=sharing)

 
## Script Logic
### Basic Logic
Once provided a number of shard keys to generate, the Event File, the Flows File, the Lookup Table File and a file output location the script will generate events for each primary shard key one shard key at a time. For each primary shard key, we will take in all flows provided in the "Flows file" and attempt to run the shard key through each flow. At each event we see if the shard key "succeeds" and thus performs the event, or if they fail and break out of the flow. If at any point a shard key breaks out of a single flow they will then move onto the next flow and attempt each of those events in order. This process will continue until the shard key "churns" (more info below) or until they have performed enough events to move them to the day the script was run. I.e. the present time.
### Primary Shard Keys Churning
As this script is designed to try and mimic real-life behavior of users of a product or an IoT device, there is logic for shard keys to stop generating events so this "churn" behavior can be analyzed. As of now churn is rudimentary and each user is assigned a "churn probability" upon their creation. In theory each user has a 70% of churning at some point after being created, but this logic is loose, and the end result of the data set could be more or less than 70% of the original shard keys churn.

### Session Ids
Session Ids are created automatically for the dataset. If a value of "session id" is passed as a desired shard key in the Events File, logic ensures that two separate session id properties are not created for each shard key. 

### Secondary Shard Keys
For all non-primary shark keys, i.e. those included in the Events File that are not the first value in the "shard keys" column, these are created and changed all at the same time and with the same logic. The logic is basic and completely random. Every time a shard key enters a new "flow" as defined in the csv Flow File we do a random roll between 0 and 1, if the value is greater than .8 all secondary shard key values are changed before the next flow is started.

### User Behavior "mimicking"
In addition to the primary shard key churning over time there are two other mechanics in place to attempt and spread out the frequency of events generated per shard key. Both can be considered properties of each individual shard key and can be viewed in the `user_id_list.json` file under the generated config/ folder. Specifically, each shard key has a "skip_days_counter" and a "churn_prob_reset_counter." The "skip_days_counter" property increments by one for each day a user performs events. After they have performed events over a 5 day time period we reset the counter and based on their "user_probabilty" property we then increase their "last_event_time" property by a semi-random amount of time so they stop producing events for a set amount of time. The amount of time shard key will stop producing events varies from 1-30 days with "good shard keys" time being closer to 1-3 days and "bad shard keys" time being closer to 21-30 days. The idea being that those who like the product more will be active more days each month than those who do not.

The second factor in producing events in a more life like manner is the "churn_prob_reset_counter" property for each shard key. Every time we reset the "skip_days_counter" shard key property we increment up the "churn_prob_reset_counter." Once we have done this 5 times we then randomly reset the shard key's "user_probability" value. The reason for this is that without resetting a shard key's "user_probability" "good users" almost never churn and after time the data becomes skewed to show patters for only "good shard keys." Thus, by periodically reassigning the shard key's "user_probability" we can better insure a normal distribution of events between good and bad users like you would see from most products in the wild. 

### Influencing frequency of events
If you wish for a single event, or a set of events to occur more often than others simply increase the frequency of said event(s) in the csv Flows File. In addition, having events occur at the beginning of a flow, or by including events in flows that are ranked higher in the csv Flow File (i.e. row 2 is ranked higher than row 10), you will increase the frequency at which these events are performed.

### Influencing frequency of properties
Event property value frequency is determined by their position within their respective property column. In general, the lowering the row index the more likely those property values are to be selected when events are build. Below is the general logic
* 2 Property values:
  - The first one listed will be added to a higher percentage of events
* 3 Property values 
  - The first property value listed will appear the most followed by the second and then third property value
* 4 or More Property Values:
  - Whenever 4 or more property values are included the script will divide the list into quartiles and values that fall into the top quartile (first x number of rows in the csv file) will appear the most, with frequency of property values decreasing as we move into the second of the fourth quartiles and so on

