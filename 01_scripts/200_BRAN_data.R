#' Author: Stephen Bradshaw
#' Written and maintained by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 11 Jun 2024
#' Title: BRAN2020 data
#' Details:
#'     - Outline
#'     - (A) BRAN2020 provides features for temporal scales of daily, monthly and annual records
#'     - (B) The data is sourced by lng, lat, depth (and quantity of Days) --> resolved at 0.05 --> reconciled at 0.1 deg
#'     - (C) Some metrics do not have depth (such as mixing layer, as it is a single measurement) 
#'     - (D) Dimensions are: 201 x 251 x 51 (lon x lat x depth) for annual files ["Data_BRAN_ocean_temp_ann_2020.rds"]
#'           Dimensions are: 201 x 251 x 51 (lon x lat x depth) for monthly files ["Data_BRAN_ocean_temp_mth_2020_06.rds"]
#'           Dimensions are: 201 x 251 x 51 x 30 (lon x lat x depth x QtyDays) for daily files ["Data_BRAN_ocean_temp_2020_06.rds"]
#'     - Code Here:
#'     - (D) Generated data with time, date and lat, lon
#'     - (E) This version of the code assigns yearly, monthly and/or daily feature records to a given date record
#'     - (F) There is a requirement to assign the .rds (netcdf) file indices to raw data to enable extraction of features
#'     - (G) Parallelise over yyyymm --> outputs into netCDFrds_BRAN_output
#' RESOURCES:
#'     - https://research.csiro.au/bluelink/bran2020-data-released/
#'
#' VERSION:
#'   - 1.0.0: Initial version applied to a generated dataset with a Lat/Long/Time step (not applicable to life history unless temp aggregated and/or lagged)


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
# dirRdsBRAN  <- "M:/Fisheries Research/ASA_ClimateData/02_netCDFrds_BRAN"
dirRdsBRAN  <- "02_netCDFrds_BRAN"
dirRdsBRAN_output <- "netCDFrds_BRAN_output"

##Above directories will get created in current working directory
# func_checkCreateDirectory(dirRdsBRAN)
func_checkCreateDirectory(dirRdsBRAN_output)
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

#--> [BLOCKED] Observe raster-type images ####
# ##Observe what the data looks like
# # tmp <- readRDS(paste0(dirRdsBRAN,"/", dir(dirRdsBRAN)[1]))
# tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_ann_2020.rds")) 
# tmp[,,1] %>% hmap_matrix()
# 
# tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_mth_2020_06.rds"))
# tmp[,,1] %>% hmap_matrix()
# 
# tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_2020_06.rds")) 
# tmp[,,1,25] %>% hmap_matrix()
# 
# rm(tmp)


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
runAddBRANtoRaw <- TRUE
deleteIndivFile <- TRUE
#####

#################################################################
################### (B) PREP // ASSIGNMENT ######################
#################################################################
#' Assign indices to .rds (netcdf) matrices based on lat, long and year/month/time
#' Data is then separated into yyyymm objects (dataframes) for parallelisation

#--> Clean / Prep Data --> TIME ####
#' Wrangled:
#'  - Separate dates (year, month, day)

df <- df %>% mutate(
  year = lubridate::year(DateTime)
  , month = lubridate::month(DateTime)
  , day = lubridate::day(DateTime)
)


##Obtain grouping variable (for parallel processing)
df$yearMonth <- paste0(df$year, sprintf("%02d", df$month))


#--> Clean / Prep Data --> LL ####
#' Wrangled:
#'  - Round LL to nearest 0.1
#'  - Create sequence based on extents of parent data
#'  - Assign sequence
df <- df %>% mutate( Lat_rd = Latitude %>% round_any(0.1)
                     , Long_rd = Longitude %>% round_any(0.1)
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
north_extents <- get_ranges_from_files(paste0(dirRdsBRAN,"/NORTH"))
south_extents <- get_ranges_from_files(paste0(dirRdsBRAN,"/SOUTH"))
west_extents <- get_ranges_from_files(paste0(dirRdsBRAN,"/WEST"))

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
    lat_seq = list(seq(from = extLatMin, to = extLatMax, by = 0.1)),
    long_seq = list(seq(from = extLonMin, to = extLonMax, by = 0.1)),
    index_lat = as.integer(cut(Lat_rd, breaks = unlist(lat_seq), labels = FALSE)),
    index_long = as.integer(cut(Long_rd, breaks = unlist(long_seq), labels = FALSE))
  ) %>%
  ungroup() %>% 
  select(-c(Lat_rd, Long_rd, extLatMin, extLatMax, extLonMin, extLonMax,lat_seq, long_seq))  # Remove temp columns if not needed

#--> Remove unwanted columns ####
rm(north_extents, south_extents, west_extents, all_dates, start_date, end_date, sample_lat, sample_lon)

#####

#--> Dataset --> list by yyyymm ####
#' process permits // running for yyyymm groupings
#' Here we have limited amounts of data, but for extensive time series // is advised

## Split the data frame into a list based on 'year_month'
rownames(df) <- c()
df$id <- rownames(df)

dfl <- split(df, df$id)

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

#--> Parallelise reading of netcdf files to assign hs, uwnd & vwnd ####
if (runAddBRANtoRaw == TRUE){

  ## Define the number of cores to use
  num_cores <- max(detectCores()-4, 3)
  
  ## Register a parallel backend
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  ## Run the function in parallel using foreach
  result_list <- foreach(df = cdfl, .combine = "c") %dopar% {
    func_assignNetCDF_BRAN_parallel(df, required.packages = req_packages
                               , input_filePath = paste0(dirRdsBRAN, "/", df[[1]]$Region)
                               , output_filePath = paste0(dirRdsBRAN_output)
                               , features = c("temp_", "eta_t_")
                               # , features = c("eta_t_", "force_", "mld_", "salt_", "temp_", "tx_trans_int_z_", "ty_trans_int_z_", "u_", "v_", "w_")
                               , addYearly = TRUE
                               , addMonthly = TRUE
                               , addDaily = TRUE
                               , atDepth_str = "100")
  }
  
  ## Stop the parallel backend
  stopCluster(cl)


}
#####

#--> Write combined data-frame file ####
## Get the list of all .rds files in the directory
file_paths <- dir(dirRdsBRAN_output, full.names = TRUE, pattern = "\\.rds$")

## Read each .rds file and combine them into a single dataframe
combined_df <- bind_rows(lapply(file_paths, readRDS))
combined_df <- combined_df %>% arrange(as.numeric(id))

## Save and optional delete of files
saveRDS(combined_df, paste0(dirRdsBRAN_output, "/", Sys.Date() %>% str_remove_all("-"), "_BRAN_combinedTest.rds"))

if (deleteIndivFile == TRUE){
  unlink(file_paths)
}


#####

#####################################################################
#####################################################################
#####################################################################
