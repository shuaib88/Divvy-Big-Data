--load the trip data

TRIP_WITH_STATIONS = LOAD '/mnt/scratch/ahmed/trip_with_stations' USING PigStorage(',') 
AS (station_id:int,station_name:chararray,latitude:double, longitude:double, dpcapacity:int, landmark:int, trip_id:int, starttime:chararray, stoptime:chararray, bikeid:int, trip_duration:int, from_station_id:int, from_station_name:chararray, to_station_id:int, to_station_name:chararray, usertype:chararray, gender:chararray, birthyear:int);


--Group the data by day and trip, average the information, also determine the count

TRIP_WITH_STATIONS_PRUNED = FOREACH TRIP_WITH_STATIONS GENERATE FLATTEN(STRSPLIT(starttime, ' ')) as (date:chararray, starttime:chararray), latitude, longitude, trip_id, trip_duration;

TRIP_PRUNED = FOREACH TRIP_WITH_STATIONS_PRUNED GENERATE date, latitude, longitude, trip_id, trip_duration;


--NorthSide Trip Data (this lattitude is Chicago zero coordinate)

TEMP_TRIP_NORTH = FILTER TRIP_PRUNED BY latitude >= 41.881979;

TEMP_TRIP_NORTH_GROUPED = GROUP TEMP_TRIP_NORTH BY date;

TRIP_NORTH = FOREACH TEMP_TRIP_NORTH_GROUPED GENERATE group AS date, COUNT($1.trip_id)as trip_count, AVG($1.trip_duration) as avg_trip_length;

--SouthSide Trip Data (this lattitude is Chicago zero coordinate)

TEMP_TRIP_SOUTH = FILTER TRIP_PRUNED BY latitude < 41.881979;

TEMP_TRIP_SOUTH_GROUPED = GROUP TEMP_TRIP_SOUTH BY date;

TRIP_SOUTH = FOREACH TEMP_TRIP_SOUTH_GROUPED GENERATE group AS date, COUNT($1.trip_id)as trip_count, AVG($1.trip_duration) as avg_trip_length;

-- Reformat weather data to have correct date

WEATHER = LOAD '/mnt/scratch/ahmed/weather' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
AS 
(station, station_name, w_date:chararray, prcp, snwd, snow, tmax, tmin, awnd, wdf2, wdf5, wsf2, wsf5, pgtm, wt09, wt01, wt06, wt05, wt02, wt04, wt08, wt03);

TEMP_WEATHER_PRUNED = FOREACH WEATHER GENERATE SUBSTRING(w_date,0,4) as year, SUBSTRING(w_date,4,6) as month, SUBSTRING(w_date,6,8) as day, tmax, tmin, awnd, prcp, snow;

TEMP_WEATHER_DATE_REFORMAT = FOREACH TEMP_WEATHER_PRUNED GENERATE REGEX_EXTRACT(month,'0*(\\d+)?', 1) as mm, REGEX_EXTRACT(day,'0*(\\d+)?', 1) as dd, year, tmax, tmin, awnd, prcp, snow;

WEATHER_CLEAN = FOREACH TEMP_WEATHER_DATE_REFORMAT GENERATE CONCAT(mm, '/', dd, '/', year) as w_date, tmax, tmin, awnd, prcp, snow;


-- JOIN NorthSide data with weather
TEMP_NORTH_TRIP_AND_WEATHER = JOIN TRIP_NORTH BY date, WEATHER_CLEAN BY w_date;

NORTH_TRIP_AND_WEATHER = FOREACH TEMP_NORTH_TRIP_AND_WEATHER GENERATE date,trip_count, avg_trip_length, tmax, tmin, awnd, prcp, snow;

-- JOIN SouthSide data with weather
TEMP_SOUTH_TRIP_AND_WEATHER = JOIN TRIP_SOUTH BY date, WEATHER_CLEAN BY w_date;

SOUTH_TRIP_AND_WEATHER = FOREACH TEMP_SOUTH_TRIP_AND_WEATHER GENERATE date,trip_count, avg_trip_length, tmax, tmin, awnd, prcp, snow;

-- store in northside data HBase
STORE NORTH_TRIP_AND_WEATHER INTO 'hbase://ahmed_northside_trip_and_weather'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
   'column:trip_count, column:avg_trip_length, column:tmax, column:tmin, column:awnd, column:prcp, column:snow;');

-- store in southside data HBase
STORE SOUTH_TRIP_AND_WEATHER INTO 'hbase://ahmed_southside_trip_and_weather'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
   'column:trip_count, column:avg_trip_length, column:tmax, column:tmin, column:awnd, column:prcp, column:snow;');
