#' Author: Stephen Bradshaw
#' Written and mainted by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 17 May 2024
#' Title: CMIP6 climate data
#' Details:
#'     - AIM: 
#'     - (A) CMIP6 projections for wind-wave data read by lng, lat, depth and days --> list of 12 months, each saved by year
#'     - (B) Assign each row in data a long, lat and time index from netcdf files
#'     - (C) Extract all hs, uwnd and vwnd values
#'     - (D) Parallelise over yyyymm --> outputs into 04_netCDFcombinedWithRaw
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
  # , "foreach", "doParallel", "geosphere"
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


#--> Set Source (Functions) ####
source(paste0(dirParent, "/00_src/functions.R"))
#--> Set Directories ####
tmp_openFile <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_split("/") %>% unlist() %>% tail(1)
dirParent <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_remove(tmp_openFile)
rm(tmp_openFile)

dirScripts  <- paste0(dirParent, "01_scripts")
dirRdsBRAN  <- "02_netCDFrds_BRAN"
dirRdsCMIP6 <- "03_netCDFrds_CMPI6"

func_checkCreateDirectory(dirRdsBRAN)
func_checkCreateDirectory(dirRdsCMIP6)


# dirOutputs <- paste0(dirParent, "03_outputs")
# dirModelOutputs <- paste0(dirParent, "04_modelOutputs")
# dirNETCDFOutputs <- paste0(dirParent, "04_netCDFOutputs")

setwd(dirParent)

#--> Misc Options ####
options(stringsAsFactors = FALSE)
options(scipen = 999)
#####



#################################################################
################### (B) PREP // ASSIGNMENT ######################
#################################################################

#--> Read raw data counts ####
dirOutputs <- paste0(dirParent, "03_outputs")

##read raw data
dfr <- readRDS(file=dir(paste0(dirOutputs, "/dump200/"), full.names=TRUE)[dir(paste0(dirOutputs, "/dump200/")) %>% str_detect("_rampDump.rds")] %>% tail(1))

#--> Add additional columns ####
# dfr %>% str()
dfr$clock_hour_1 <- dfr$clock_hour + 1
dfr$day_calendar <- lubridate::day(dfr$date)

#--> Assign index values of time ####
dfr <- dfr %>% mutate(index_time = get_netcdf_time3h_index(year, month, day_calendar, clock_hour_1))

#--> Assign index for Lat and Long [Could result in NA on land --> fixed in extraction] ####
## Read in rampsLL file and add more columns
rampsLL <- readxl::read_excel(paste0(dirData, "/Ramp_LatLong.xlsx" ))
rampsLL <- rampsLL %>% mutate( Lat_rd = Lat %>% round_any(0.5),
                               Long_rd = Long %>% round_any(0.5),
                               # Lat_id = NA,
                               # Long_id = NA
                               )

## Create sequence bins as per netCDF
lat_seq <- seq(from = -36, to = -13, by = 0.5)
long_seq <- seq(from = 112, to = 129, by = 0.5)

## Bin the latitude and longitude values
rampsLL$index_lat <- as.integer(cut(rampsLL$Lat, breaks = lat_seq, labels = FALSE))
rampsLL$index_long <- as.integer(cut(rampsLL$Long, breaks = long_seq, labels = FALSE))

## Assign LL indices to dfr data
dfr <- dfr %>% left_join(rampsLL, by = c("site" = "Ramp"))


rm(lat_seq, long_seq, rampsLL)

#--> split dfr --> list of files by yyyymm ####
## Convert 'year' and 'month' to character to create a grouping variable
dfr$yearMonth <- paste0(dfr$year, sprintf("%02d", dfr$month))

## Split the data frame into a list based on 'year_month'
dfl <- split(dfr, dfr$yearMonth)

# # Optionally, you can remove the 'year_month' column from each element of the list
# dfl <- lapply(dfl, function(x) {x$year_month <- NULL; return(x)})

names(dfl)
length(dfl)


#--> Create list for // ==> cdfl ####
cdfl <- list()
for (n in 1:length(dfl)){
  cdfl[[n]] <- list()
  cdfl[[n]][[1]] <- dfl[[n]]
  cdfl[[n]][[2]] <- names(dfl)[n]
}


dfl %>% names() %>% pluck(1)
#####


#################################################################
################### (C) PARALLELIZE TO RAW ######################
#################################################################

#--> parallelise reading of netcdf files to assign hs, uwnd & vwnd ####
if ("require" == "runAddCMIP6toRaw"){

  # Define the number of cores to use
  num_cores <- 3#detectCores()
  
  # Register a parallel backend
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  # Run the function in parallel using foreach
  result_list <- foreach(df = cdfl, .combine = "c") %dopar% {
    func_assignNetCDF_parallel(df, required.packages = req_packages, output_filePath = "04_netCDFcombinedWithRaw/")
  }
  
  # Stop the parallel backend
  stopCluster(cl)


}
#####





#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################

#####

# tmp[[1]][,,1] %>% hmap_matrix(rotateCCW=FALSE, title="Test")
# tmp[[1]][,,1] %>% dim()






