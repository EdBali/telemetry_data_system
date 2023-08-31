# DROP TABLES
raw_sensor_data_drop = "DROP TABLE IF EXISTS  raw_sensor_data;"
location_based_data_drop = "DROP TABLE IF EXISTS  location_based_data;"
time_of_day_based_data_drop = "DROP TABLE IF EXISTS  time_of_day_based_data;"
data_based_on_location_and_time_of_day_data_drop = "DROP TABLE  data_based_on_location_and_time_of_day_data;"



# create tables
raw_sensor_data = ("""CREATE TABLE IF NOT EXISTS raw_sensor_data (
    ts timestamp,
    device text,
    co double,
    humidity double,
    light boolean,
    lpg double,
    motion boolean,
    smoke double,
    temp double,
    PRIMARY KEY (device, ts)
)
                             ;""")

location_based_data = ("""CREATE TABLE IF NOT EXISTS location_based_data (
    location text,
    ts timestamp,
    co double,
    humidity double,
    light boolean,
    lpg double,
    motion boolean,
    smoke double,
    temp double,
    PRIMARY KEY (location, ts)
)
                             ;""")

time_of_day_based_data = ("""CREATE TABLE IF NOT EXISTS time_of_day_based_data (
    time_of_day text,
    ts timestamp,
    location text,
    co double,
    humidity double,
    light boolean,
    lpg double,
    motion boolean,
    smoke double,
    temp double,
    PRIMARY KEY (time_of_day, ts, location)
)
                           ;""")


data_based_on_location_and_time_of_day_data = ("""CREATE TABLE IF NOT EXISTS data_based_on_location_and_time_of_day_data (
    location text,
    time_of_day text,
    ts timestamp,
    co double,
    humidity double,
    light boolean,
    lpg double,
    motion boolean,
    smoke double,
    temp double,
    PRIMARY KEY ((location, time_of_day), ts)
                          );""")


# INSERT RECORDS

raw_sensor_data_insert = ("""
    INSERT INTO raw_sensor_data (ts, device, co, humidity, light, lpg, motion, smoke, temp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ;""")

location_based_data_insert = ("""
    INSERT INTO location_based_data (location, ts, co, humidity, light, lpg, motion, smoke, temp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ;""")

time_of_day_based_data_insert = ("""
    INSERT INTO time_of_day_based_data (time_of_day, ts, location, co, humidity, light, lpg, motion, smoke, temp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ;""")

data_based_on_location_and_time_of_day_data_insert = ("""
    INSERT INTO data_based_on_location_and_time_of_day_data (location, time_of_day, ts, co, humidity, light, lpg, motion, smoke, temp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ;""")

# load raw_sensor_data without null values in device and ts columns
raw_sensor_data_select = ("""
    SELECT * FROM raw_sensor_data
    WHERE device IS NOT NULL AND ts IS NOT NULL
    ;""") 

# QUERY LISTS

create_table_queries = [raw_sensor_data, location_based_data, time_of_day_based_data, data_based_on_location_and_time_of_day_data]
drop_table_queries = [raw_sensor_data_drop, location_based_data_drop, time_of_day_based_data_drop, data_based_on_location_and_time_of_day_data_drop]