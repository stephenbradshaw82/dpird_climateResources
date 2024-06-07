#' Author: Stephen Bradshaw
#' Written and mainted by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 04 Jun 2024
#' Title: BRAN2020 data
#' Details:
#'     - Legacy files: 
#'     - (A) BRAN2020 provides features for temporal scales of daily, monthly and annual records
#'     - (B) The data is sourced by lng, lat, depth (and quantity of Days) --> resolved at 0.05 --> provided at 0.1deg?
#'     - (C) Some metrics do not have depth (such as mixing layer, as it is a single measurement) 
#'     - (D) Dimensions are: 201 x 251 x 51 (lon x lat x depth) for annual files ["Data_BRAN_ocean_temp_ann_2020.rds"]
#'           Dimensions are: 201 x 251 x 51 (lon x lat x depth) for monthly files ["Data_BRAN_ocean_temp_mth_2020_06.rds"]
#'           Dimensions are: 201 x 251 x 51 x 30 (lon x lat x depth x QtyDays) for daily files ["Data_BRAN_ocean_temp_2020_06.rds"]
#'     - Code Here:
#'     - (D) Generated data with time, date and lat, lon
#'     - (E) There is a requirement to assign the .rds (netcdf) file indices to raw data to enable extraction of features
#'     - (E) Script aggregates and assigns CMIP6 features to flat data
#'     - (F) Parallelise over yyyymm --> outputs into 04_CMIP6combinedData
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
  # , "gam", 
  ,"stringr", "purrr", "rebus"
  # ,"ggpubr"
  , "foreach", "doParallel"
  # , "geosphere"
  # , "leaflet"
  ,"lubridate"
  , "scales", "pheatmap"
  # , "mapview", "dbscan", "FNN", "sf", "mgcv"
  # , "gridExtra"
  # , "ozmaps"
  # , "bayesplot"
  # , "gghighlight"
  # , "V8"
  # , "bayestestR"
  # , 'posterior'
  # , "foreach", "doParallel"
  , "ncdf4"
  # , "ggmap"
  # , "data.table"
                      # , "grid"
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

#--> [DEFINE EXTENTS] Encompass Western Australia (WA) ####
lng.west <- 110
lng.east <- 130
lat.north <- -12
lat.south <- -37

#--> [USER DEFINED LL and DATES] Generated Data ####
#' Raw data:
#'  - Assume 5 sites (hard coded lat long based on extents observed from plotted heatmap)
#'  - Assign random date-times (based on from-to) thresholds 

##Observe what the data looks like
# tmp <- readRDS(paste0(dirRdsBRAN,"/", dir(dirRdsBRAN)[1]))
tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_ann_2020.rds")) 
tmp[,,1] %>% hmap_matrix()

tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_mth_2020_06.rds"))
tmp[,,1] %>% hmap_matrix()

tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_2020_06.rds")) 
tmp[,,1,25] %>% hmap_matrix()

rm(tmp)


## [HERE] Create some generated data
sample_lat <- c(-33.05, -34.81, -24.79, -19.87, -14.45)
sample_lon <- c(126.89, 114.62, 110.84, 118.37,  128.32)

## [HERE] Generate a sequence of dates from 201401 to 201608
start_date <- as.Date("2014-01-01")
end_date <- as.Date("2016-08-31")
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

#####


###################################

# tmp <- readRDS(paste0(dirRdsBRAN,"/", dir(dirRdsBRAN)[1]))
tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_ann_2020.rds")) 
tmp[,,1] %>% hmap_matrix()

tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_mth_2020_06.rds"))
tmp[,,1] %>% hmap_matrix()

tmp <- readRDS(paste0(dirRdsBRAN,"/", "Data_BRAN_ocean_temp_2020_06.rds")) 
tmp[,,1,25] %>% hmap_matrix()

rm(tmp)

###################################


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

## Create sequence bins as per netCDF
lat_seq <- seq(from = lat.south, to = lat.north, by = 0.1)
long_seq <- seq(from = lng.west, to = lng.east, by = 0.1)

## Bin the latitude and longitude values
df$index_lat <- as.integer(cut(df$Lat_rd, breaks = lat_seq, labels = FALSE))
df$index_long <- as.integer(cut(df$Long_rd, breaks = long_seq, labels = FALSE))


#--> Remove unwanted columns ####
df <- df %>% dplyr::select(-c(Lat_rd, Long_rd))

#####

#--> Dataset --> list by yyyymm ####
#' process permits // running for yyyymm groupings
#' Here we have limited amounts of data, but for extensive time series // is advised

##Look at dataset (here gen)
head(df)
df$yearMonth %>% table()


## Split the data frame into a list based on 'year_month'

rownames(df) <- c()
df$id <- rownames(df)

# dfl <- split(df, df$yearMonth)
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

#--> parallelise reading of netcdf files to assign hs, uwnd & vwnd ####
if ("require" == "runAddBRANtoRaw"){

  ## Define the number of cores to use
  num_cores <- max(detectCores()-4, 3)
  
  ## Register a parallel backend
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  
  ## Run the function in parallel using foreach
  result_list <- foreach(df = cdfl, .combine = "c") %dopar% {
    func_assignNetCDF_BRAN_parallel(df, required.packages = req_packages
                               , input_filePath = dirRdsBRAN
                               , output_filePath = paste0(dirRdsBRAN_output, "/")
                               , features = c("temp_", "eta_t_")
                               # , features = c("eta_t_", "force_", "mld_", "salt_", "temp_", "tx_trans_int_z_", "ty_trans_int_z_", "u_", "v_", "w_")
                               , addYearly = TRUE
                               , addMonthly = FALSE
                               , addDaily = FALSE
                               , atDepth = "SST")
  }
  
  # Stop the parallel backend
  stopCluster(cl)


}
#####

#--> Write combined data-frame file ####
## Get the list of all .rds files in the directory
file_paths <- dir(dirRdsBRAN_output, full.names = TRUE, pattern = "\\.rds$")

## Read each .rds file and combine them into a single dataframe
combined_df <- bind_rows(lapply(file_paths, readRDS))

## Print the resulting dataframe
print(combined_df)

combined_df <- combined_df %>% arrange(as.numeric(id))

## Save and optional delete of files
saveRDS(combined_df, paste0(dirRdsBRAN_output, "/", Sys.Date() %>% str_remove_all("-"), "_BRAN_combinedTest.rds"))

if ("deleteIndivFile" == TRUE){
  unlink(file_paths)
}


#####


#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################





