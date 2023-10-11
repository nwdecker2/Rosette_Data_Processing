#readbtl_2022.R
#Code to process James Bay 2022 bottle files

library(tidyverse)
library(here)
library(fs)
################################################################################
############################### Part 1 #########################################
################################################################################
time_data <- tibble(files = fs::dir_ls(here("2022 data")))  %>% #created a list of files to be imported
  mutate(data = pmap(list(files),
                     ~ read_tsv(..1, col_names = FALSE))) %>% #imported the files
  mutate(flag_for_rows_containing_sdev = pmap(list(files, data),
                                              ~ grep("sdev",readLines(..1)))) %>% #flagged which lines contain the word "sdev" because these lines will contain the timestamp 
  mutate(time_data = pmap(list(files, data, flag_for_rows_containing_sdev),
                          ~ slice(..2,..3))) %>% #exported rows containing the word "sdev" to a separate set of tables called time_data
  select(time_data) %>% #removed everything except time_data
  map_df(bind_rows) %>% #joined all tables in time_data into one
  separate(X1, c("time_utc","a","b","c","d","e"), sep = " +") %>% #split one column (character string) at whitespace into multiple columns and assigned column names
  select(time_utc) %>% #deleted all columns except time_utc
  rowid_to_column() #added a column containing row ID
################################################################################
## Part 1 works as intended

################################################################################
############################### Part 2 #########################################
################################################################################
bottle_data <- tibble(files = fs::dir_ls(here("2022 data")))  %>% #created a list of files to be imported
  mutate(data = pmap(list(files),
                     ~ read_tsv(..1, col_names = FALSE))) %>% #imported the files
  mutate(data = pmap(list(files, data), 
                     ~ mutate(..2, source_file = as.character(..1)))) %>% #added file name to each row in each file, will be used later to obtain cast number  
  mutate(flag_for_rows_containing_avg = pmap(list(files, data),
                                             ~ grep("avg",readLines(..1)))) %>% #flagged which lines contain the word "avg" 
  mutate(filtered_data = pmap(list(files, data, flag_for_rows_containing_avg),
                              ~ slice(..2,..3))) %>% #exported rows containing the word "avg" to a separate set of tables called filtered_data
  select(filtered_data) %>% #removed everything except filtered_data
  map_df(bind_rows) %>% #joined all tables in filtered_data into one
  mutate(cast = stringr::str_extract(source_file, "(?<=WK22_Ros_)[:digit:]+")) %>% #created a column containing cast number for each row, derived from the file name 
  separate(X1, c("bottle_position","month","day","year", "pressure_db", "depth_m",
                 "temperature_deg_c","conductivity_ms_cm", "salinity_psu",
                 "density_kg_m3", "specific_volume_(sva)", "oxygen_raw_V", "oxygen_umol_kg",
                 "oxygen_saturation_%", "spar", "type_of_measurement"), sep = " +") %>% #split one column (character string) at whitespace into multiple columns and assigned column names
  select(-source_file,-type_of_measurement) %>% #deleted columns containing file names and type of measurement (it was "avg" for each row)
  mutate(month = match(month,month.abb)) %>% #changed month from MMM (e.g., AUG) to M (e.g., 8)
  mutate(bottle_position = as.numeric(bottle_position), month = as.numeric(month), 
         day = as.numeric(day), year = as.numeric(year), 
         temperature_deg_c = as.numeric(temperature_deg_c), salinity_psu = as.numeric(salinity_psu), 
         conductivity_ms_cm = as.numeric(conductivity_ms_cm), 
         pressure_db = as.numeric(pressure_db), cast = as.numeric(cast)) %>% #converted variables from character to numeric 
  rowid_to_column() #added a column containing row ID
################################################################################

################################################################################
############################### Part 3 #########################################
################################################################################
#! MAKE SURE BOTTLE_DATA AND TIME_DATA HAVE THE SAME NUMBER OF ROWS BEFORE PROCEEDING 
#! IF THE NUMBER OF ROWS IS NOT THE SAME - THERE IS AN ISSUE.
bottle_data_with_time <- left_join(bottle_data, time_data, by = "rowid") %>% #joined time data to the rest of bottle data by row ID
  select(-rowid) #removed row ID column

write.csv(bottle_data_with_time, here("wk22_ros_bottle_data.csv"), row.names=FALSE) #saved the extracted bottle data as a .CSV file

sample_info <- read_csv(here("WK2022_LOGBOOK final.csv")) %>% #read in the file containing sample IDs, cast number, and bottle number
  rename(cast = cast_nu, bottle_position = niskin_bottle_nu) #renamed the columns in the sample_info table to match column names in bottle_data_with_time table
## for some reason the data types are different for cast between the dfs.
sample_info <- sample_info %>%
  mutate(cast = as.double(cast))

sample_info_with_bottle_data <- left_join(sample_info, bottle_data_with_time, by = c('cast','bottle_position')) #joined sample_info with bottle_data_with_time tables whenever there was a match for both cast and bottle position values
sample_info_with_bottle_data$yyyy_m_d <- paste(sample_info_with_bottle_data$year.x, sample_info_with_bottle_data$month.x, sample_info_with_bottle_data$day.x, sep="/")  #created a column containing year, month, day
sample_info_with_bottle_data$timestamp_utc <- paste(sample_info_with_bottle_data$yyyy_m_d, sample_info_with_bottle_data$time_utc, sep="T") #created a column containing year, month, day, and timestamp
sample_info_with_bottle_data <- sample_info_with_bottle_data %>% 
  select(-cast, -bottle_position, -year.x, -month.x, -day.x, -time_utc, -yyyy_m_d) #removed unnecessary columns


write.csv(sample_info_with_bottle_data, here("sample_id_with_bottle_data.csv"), row.names=FALSE) 