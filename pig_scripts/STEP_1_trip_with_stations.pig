--concatenate STATIONS with TRIP data and create a new file


STATIONS = LOAD '/mnt/scratch/ahmed/stations' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
AS (station_id:int, station_name:chararray, latitude:double,longitude:double,dpcapacity:int,landmark:int);

TRIP = LOAD '/mnt/scratch/ahmed/trip' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
AS (trip_id:int, starttime:chararray, stoptime, bikeid:int, trip_duration:int, from_station_id:int, from_station_name, to_station_id:int, to_station_name, usertype, gender, birthyear:int);

--JOIN the station and trip data
TRIP_WITH_STATIONS = JOIN STATIONS by station_id, TRIP by from_station_id;

-- Store the resulting data into HDFS as a CSV file
STORE TRIP_WITH_STATIONS INTO '/mnt/scratch/ahmed/trip_with_stations' USING PigStorage(',');