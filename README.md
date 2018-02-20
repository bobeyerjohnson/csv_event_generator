# Dummy Event Generation Script
This is a simple python script designed to generate events based on a formatted csv file with dummy data. Dummy data is outputed in JSON
### How to Run
Within the root folder run --
```linux
$ python3 event_generator.py
``` 
Then follow the promts to provide the needed files and dir locations.

### Requirements
   2 csv files:
1) Events File - This contains all the events and possible event properites that will be used for data generation
2) Flows File - "Flows" referers to the "flows" or "path" that events will occur. This gives structure to the order in which events will be performed and is fundemental to creating trends and behaviors in the data
3) Lookup Table File - csv file that takes in values and assigns a single value for each property per Id

   Example template for both of thes files can be found [here](https://docs.google.com/spreadsheets/d/1b0LrIq6rxfB5uJaZGDOg9VxvtkabRAym2mgwMC9T0ME/edit?usp=sharing)
   
## Script Logic
### Basic Logic
Once provided a number of shardkeys to generate, the Event File, Flows File, and a file output location the script will generate events for each primary shardkey one shardkey at a time. For each primary shardkey we will take in all flows provided in the "Flows file" and attempt to run the shardkey through each flow. At each event we see if the shardkey "succeeds" and thus performs the event, or if they fail and break out of the flow. If at any point a shardkey breaks out of a single flow they will then move onto the next flow and attempt each of those events in order. This process will continue until the shardkey "churns" (more info below) or until they have performed enough events to move them to the day the script was run. I.e. the present time.
### ShardKeys Churing
As this script is designed to try and mimic real life behavior of users of a product or a IoT device, there is logic for shardkeys to stop generating events so this "churn" behavior can be analyzed. As of now churn is rudimentry and each user is assigned a "churn probability" upon their creation. In theory each user has a 60% of churning at some point after being created, but this logic is loose and will be updated through later updates. 
### Infulencing frequency of evnets
If you wish for a single event, or a set of events to occur more often then other simple include the frequency said event(s) appear in the "flows." In addition, having these events occur at the beginning of a flow will increase the frequency at which they are performed
### Influenceing frequency of properties
Event property value frequency is determined by their position within their respecitve property column. In general the lowering the row index the more likely those property values are to be selected when events are build. Below is the general logic
* 2 Property values:
  - The first one listed will be added to a higher percentage of events
* 3 Property values 
  - The first property value listed will appear the most followed by the second and then third property value
* 4 or More Property Values:
  - Whenever 4 or more property values are included the script will divide the list into quartiles and values that fall into the top quartile (first x number of rows in the csv file) will appear the most, with fequency of property values decreasing as we move into the second of the fourth quartiles and so on

