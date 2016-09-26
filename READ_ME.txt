This folder contains all my projects

check out my homepage here: http://104.197.20.219/ahmed/divvy/home.html
 
see my html and perl scripts on the webserver:/var/www/html/ahmed/divvy/ /var/www/cgi-bin/ahmed/divvy/

MY PROJECT: 

DATA: I took a couple years of divy trip data, divy station data, and weather data cleaned this data and processed it with pig scripts to join all of it. I split the data based on geography so I could compare northside vs. southside. Ultimately I took averages of information, made counts, and used weather. 
 
WEBSITE: My website allows you to view the data in the hbase, as well as add a new tuple to a kafka queue. Note this is data for a single drip, whereas my HBASE contains data aggregated over all days for a single trip. This is reasonable because I have counts for how many trips account for the aggregated data, and I can use this to recalculate a new average, when adding on the new tuples. 
 
SPEED LAYER: If you run the storm topology I created, data will be pulled from the queue and added to a different hbase table.  I checked the hbase to make sure the storm topology accurately updates the hbase table with any values you insert on my website. The details are in my weather topology. 

#### WEATHER TOPOLOGY


#### 
