#' Author: Stephen Bradshaw
#' Written and mainted by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 27 May 2024
#' Title: CMIP6 climate data
#' Details:
#'     - Outline: 
#'     - (A) CMIP6 projections for wind-wave data read by lng, lat, depth and days --> list of 12 months, each saved by year
#'     - (B) Assign each row in data a long, lat and time index from netcdf files
#'     - (C) Extract all hs, uwnd and vwnd values
#'     - Code Here:
#'     - (D) Generated data with time, date and lat, lon
#'     - (E) There is a requirement to assign the .rds (netcdf) file indices to raw data to enable extraction of features
#'     - (E) Script aggregates and assigns CMIP6 features to flat data
#'     - (F) Parallelise over yyyymm --> outputs into netCDFrds_CMIP6_output
#' RESOURCES:
#'     - https://data.csiro.au/collection/csiro:60106
#'     

#### LIBRARIES ####
rm(list=ls())

#' Packages (not in environment) -------------------------------------------
list.of.packages <- c(
  "magrittr", "tidyr"
  , "plyr", "dplyr"
  , "ggplot2"
  ,"stringr", "purrr", "rebus"
  , "foreach", "doParallel"
  ,"lubridate"
  , "scales", "pheatmap"
  , "foreach", "doParallel"
  , "ncdf4"
)


new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#' Libraries ---------------------------------------------------------------
req_packages <- list.of.packages 
sapply(req_packages,require,character.only = TRUE, quietly=TRUE)
rm(list = setdiff(ls(), "req_packages"))
#####

#### Housekeeping ####
#--> Set TZ ####
Sys.setenv(TZ = "Australia/Perth")


#--> Set Parent Directory ####
tmp_openFile <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_split("/") %>% unlist() %>% tail(1)
dirParent <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_remove(tmp_openFile)
rm(tmp_openFile)

#--> Set Source (Functions) ####
source(paste0(dirParent, "/00_src/functions.R"))

#--> [USER IMPUT] Set Other Directories ####
dirScripts  <- paste0(dirParent, "01_scripts")
# dirRdsCMIP6 <- "M:/Fisheries Research/ASA_ClimateData/03_netCDFrds_CMIP6"
dirRdsCMIP6 <- "03_netCDFrds_CMIP6"
dirRdsCMIP6_output <- "netCDFrds_CMIP6_output"

##Above directories will get created in current working directory
func_checkCreateDirectory(dirRdsCMIP6_output)
setwd(dirParent)

#--> Misc Options ####
options(stringsAsFactors = FALSE)
options(scipen = 999)
#####

#################################################################
################### (A) EXAMPLE GENERATED DATA ##################
#################################################################
#' Data requires Latitude / Longitude / DateTime
#' Below generated data is created

#--> [DEFINE EXTENTS] Encompass Western Australia (WA) ####
# lng.west <- 110
# lng.east <- 130
# lat.north <- -12
# lat.south <- -37

#--> [USER DEFINED LL and DATES] Generated Data ####
#' Raw data:
#'  - Assume 5 sites (hard coded lat long based on extents observed from plotted heatmap)
#'  - Assign random date-times (based on from-to) thresholds 
## [HERE] Create some generated data
sample_lat <- c(-35.05, -15.81, -24.79, -20.00, -31.75)
sample_lon <- c(110.89, 121.62, 113.84, 115.75,  114.00)

## [HERE] Generate a sequence of dates from 201401 to 201608
# start_date <- as.Date("2014-01-01")
# end_date <- as.Date("2016-08-31")
start_date <- as.Date("2023-01-01")
end_date <- as.Date("2023-12-31")
all_dates <- seq.Date(start_date, end_date, by = "day")

## Function to randomly sample 30 datetimes for a given site
sample_datetimes_for_site <- function(lat, lon, num_samples = 30) {
  sampled_dates <- sample(all_dates, num_samples, replace = TRUE)
  sampled_times <- format(
    ISOdatetime(year = year(sampled_dates),
                month = month(sampled_dates),
                day = day(sampled_dates),
                hour = sample(0:23, num_samples, replace = TRUE),
                min = sample(0:59, num_samples, replace = TRUE),
                sec = sample(0:59, num_samples, replace = TRUE)),
    "%Y-%m-%d %H:%M:%S"
  )
  data.frame(
    Latitude = lat,
    Longitude = lon,
    DateTime = as.POSIXct(sampled_times, format = "%Y-%m-%d %H:%M:%S")
  )
}

## Create a dataframe by sampling 30 datetimes for each site
df_list <- lapply(seq_along(sample_lat), function(i) {
  sample_datetimes_for_site(sample_lat[i], sample_lon[i])
})

## Combine all dataframes into one
df <- bind_rows(df_list)
rm(df_list, sample_datetimes_for_site )

#--> [USER DEFINED PARAMETERS] To run / to delete ####
runAddCMIP6toRaw <- TRUE
deleteIndivFile <- TRUE
#####

#################################################################
################### (B) PREP // ASSIGNMENT ######################
#################################################################
#' Assign indices to .rds (netcdf) matrices based on lat, long and year/month/time
#' Data is then separated into yyyymm objects (dataframes) for parallelisation

#--> Clean / Prep Data --> TIME ####
#' Wrangled:
#'  - Separate dates
#'  - Define clock hour
#'  - Define clock hour + 1 (as 0 index not suitable for .rds files)
#'  - Obtain time index based on 3h bins for wind, wave datasets

df <- df %>% mutate(
  year = lubridate::year(DateTime)
  , month = lubridate::month(DateTime)
  , day_calendar = lubridate::day(DateTime)
  , clock_hour_1 = DateTime %>% format("%H") %>% as.numeric() %>% "+"(1)
  , index_time = get_netcdf_time3h_index(year, month, day_calendar, clock_hour_1)
)

##Obtain grouping variable (for parallel processing)
df$yearMonth <- paste0(df$year, sprintf("%02d", df$month))


#--> Clean / Prep Data --> LL ####
#' Wrangled:
#'  - Round LL to nearest 0.1
#'  - Create sequence based on extents of parent data
#'  - Assign sequence
df <- df %>% mutate( Lat_rd = Latitude %>% round_any(0.5)
                     , Long_rd = Longitude %>% round_any(0.5)
)

## Assuming you have a function that can list files in the subdirectories and extract ranges from the file names.
get_ranges_from_files <- function(folder_path) {
  
  files <- list.files(folder_path, pattern = "\\.rds$", full.names = TRUE)[1]
  tmp_extents <- files %>% str_replace_all(DOT %R% "rds", "") %>% str_split("_") %>% unlist() %>% tail(1) %>% str_split(ALPHA) %>% unlist() %>% tail(-1)
  lat_north <- tmp_extents[1] %>% as.numeric()
  lat_south <- tmp_extents[2] %>% as.numeric()
  long_east <- tmp_extents[3] %>% as.numeric()
  long_west <- tmp_extents[4] %>% as.numeric()
  
  return(list(lat_north = lat_north, lat_south = lat_south, long_east = long_east, long_west = long_west))
}

## Extracting extents for each region
north_extents <- get_ranges_from_files(paste0(dirRdsCMIP6,"/NORTH"))
south_extents <- get_ranges_from_files(paste0(dirRdsCMIP6,"/SOUTH"))
west_extents <- get_ranges_from_files(paste0(dirRdsCMIP6,"/WEST"))

## Define regions based on latitude and longitude bounds
df <- df %>%
  mutate(
    Region = case_when(
      Lat_rd >= north_extents$lat_south & Lat_rd <= north_extents$lat_north &
        Long_rd >= north_extents$long_west & Long_rd <= north_extents$long_east ~ "NORTH",
      Lat_rd >= south_extents$lat_south & Lat_rd <= south_extents$lat_north &
        Long_rd >= south_extents$long_west & Long_rd <= south_extents$long_east ~ "SOUTH",
      Lat_rd >= west_extents$lat_south & Lat_rd <= west_extents$lat_north &
        Long_rd >= west_extents$long_west & Long_rd <= west_extents$long_east ~ "WEST",
      TRUE ~ NA_character_
    ),
    extLatMin = case_when(
      Region == "NORTH" ~ north_extents$lat_south,
      Region == "SOUTH" ~ south_extents$lat_south,
      Region == "WEST" ~ west_extents$lat_south,
      TRUE ~ NA_real_
    ),
    extLatMax = case_when(
      Region == "NORTH" ~ north_extents$lat_north,
      Region == "SOUTH" ~ south_extents$lat_north,
      Region == "WEST" ~ west_extents$lat_north,
      TRUE ~ NA_real_
    ),
    extLonMin = case_when(
      Region == "NORTH" ~ north_extents$long_west,
      Region == "SOUTH" ~ south_extents$long_west,
      Region == "WEST" ~ west_extents$long_west,
      TRUE ~ NA_real_
    ),
    extLonMax = case_when(
      Region == "NORTH" ~ north_extents$long_east,
      Region == "SOUTH" ~ south_extents$long_east,
      Region == "WEST" ~ west_extents$long_east,
      TRUE ~ NA_real_
    )
  )

df <- df %>%
  rowwise() %>%
  mutate(
    lat_seq = list(seq(from = extLatMin, to = extLatMax, by = 0.5)),
    long_seq = list(seq(from = extLonMin, to = extLonMax, by = 0.5)),
    index_lat = as.integer(cut(Lat_rd, breaks = unlist(lat_seq), labels = FALSE)),
    index_long = as.integer(cut(Long_rd, breaks = unlist(long_seq), labels = FALSE))
  ) %>%
  ungroup() %>% 
  select(-c(Lat_rd, Long_rd, extLatMin, extLatMax, extLonMin, extLonMax, lat_seq, long_seq))  # Remove temp columns if not needed

rm(north_extents, south_extents, west_extents, all_dates, start_date, end_date, sample_lat, sample_lon)

#--> Remove unwanted columns ####
df <- df %>% dplyr::select(-c(year, month, day_calendar, clock_hour_1))

#####

#--> Dataset --> list by yyyymm ####
#' process permits // running for yyyymm groupings
#' Here we have limited amounts of data, but for extensive time series // is advised

##Look at dataset (here gen)
head(df)
df$yearMonth %>% table()


## Split the data frame into a list based on 'year_month'
dfl <- split(df, list(df$yearMonth, df$Region))

#--> Create list for // ==> cdfl ####
cdfl <- list()
for (n in 1:length(dfl)){
  cdfl[[n]] <- list()
  cdfl[[n]][[1]] <- dfl[[n]]
  cdfl[[n]][[2]] <- names(dfl)[n]
}

#####

#################################################################
################### (C) PARALLELIZE TO RAW ######################
#################################################################

#--> parallelise reading of netcdf files to assign hs, uwnd & vwnd ####
if (runAddCMIP6toRaw == TRUE){

  ## Define the number of cores to use
  num_cores <- max(detectCores()-4, 3)
  
  ## Register a parallel backend
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  
  ## Run the function in parallel using foreach
  result_list <- foreach(df = cdfl, .combine = "c") %dopar% {
    func_assignNetCDF_CMIP6_parallel(df, required.packages = req_packages
                               , input_filePath = paste0(dirRdsCMIP6, "/", df[[2]] %>% str_split(DOT) %>% unlist() %>% pluck(2))
                               , output_filePath = paste0(dirRdsCMIP6_output, "/"))
  }
  
  # Stop the parallel backend
  stopCluster(cl)


}
#####

#--> Write combined data-frame file ####
## Get the list of all .rds files in the directory
file_paths <- dir(dirRdsCMIP6_output, full.names = TRUE, pattern = "\\.rds$")

## Read each .rds file and combine them into a single dataframe
combined_df <- bind_rows(lapply(file_paths, readRDS))

## Print the resulting dataframe
print(combined_df)

## Save and optional delete of files
saveRDS(combined_df, paste0(dirRdsCMIP6_output, "/", Sys.Date() %>% str_remove_all("-"), "_CMIP6_combinedTest.rds"))

if (deleteIndivFile == TRUE){
  unlink(file_paths)
}


#####

#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################