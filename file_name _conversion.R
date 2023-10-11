library(tidyverse)
library(here)
library(fs)
library(stringr)

##############
# Need to convert the file names from 2022 to a more usable state
# Specify the source and destination folder paths
source_folder <- here("2022 data")  

# List all files in the source folder
file_list <- list.files(source_folder)

# Define a regular expression pattern to match the original file names
pattern <- "WK22_Ros_\\2.btl"

# Define the replacement pattern for renaming
replacement <- "WK22_ROS_\\2.btl"

# Rename files matching the pattern
for (file_name in file_list) {
  if (grepl(pattern, file_name)) {
    new_file_name <- gsub(pattern, replacement, file_name)
    old_file_path <- file.path(source_folder, file_name)
    new_file_path <- file.path(source_folder, new_file_name)
    file.rename(old_file_path, new_file_path)
    cat("Renamed:", old_file_path, "to", new_file_path, "\n")
  }
}