#' Author: Stephen Bradshaw
#' Written and maintained by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 17 May 2024
#' Title: CMIP6 climate data and BRAN2020 Oceanographic data
#' Details:
#'     - AIM: 
#'     - (A) Capture and store climate data from netcdf for spatial extents and time periods of interest
#'     - (B) Write out files in .rds for smaller memory footprint
#' Pending:
#'     - Script Executable to look at directory diff and download missing files
#'     - Addition of all metrics taken from CMIP6 (wind and signficiant wave height)
#'     - Additional of all metric taken from BRAN
#'      
#' RESOURCES:
#'     - https://data.csiro.au/collection/csiro:60106
#'     - https://research.csiro.au/bluelink/bran2020-data-released/
#'     

#### Packages / Libraries ####
rm(list=ls())


#' Packages (not in environment) -------------------------------------------
list.of.packages <- c(
  "magrittr", "tidyr"
  , "plyr", "dplyr"
  , "ggplot2"
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


#--> Set Parent Directory ####
tmp_openFile <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_split("/") %>% unlist() %>% tail(1)
dirParent <- dirname(rstudioapi::getActiveDocumentContext()$path) %>% str_remove(tmp_openFile)
rm(tmp_openFile)

#--> Set Source (Functions) ####
source(paste0(dirParent, "/00_src/functions.R"))

#--> [USER IMPUT] Set Other Directories ####
dirScripts  <- paste0(dirParent, "01_scripts")
# dirRdsCMIP6 <- "M:/Fisheries Research/ASA_ClimateData/03_netCDFrds_CMIP6"
# dirRdsBRAN  <- "M:/Fisheries Research/ASA_ClimateData/02_netCDFrds_BRAN"
dirRdsCMIP6 <- "03_netCDFrds_CMIP6"
dirRdsBRAN  <- "02_netCDFrds_BRAN"


##Above directories will get created in current working directory
func_checkCreateDirectory(dirRdsBRAN)
func_checkCreateDirectory(dirRdsCMIP6)

setwd(dirParent)

#--> Misc Options ####
options(stringsAsFactors = FALSE)
options(scipen = 999)
#####

#### DPIRD Specific Data features ####
#--> [USER INPUT] Time Range ####
##Years
#' Need to consider dataset
#' start.year will be adjusted in BRAN code to have a minimum value of 1993
#' end.year will be adjusted in BRAN code to have a minimum value of 1993

start.year <- 1990 
end.year   <- Sys.Date() %>% year() %>% "+"(2)

#--> [USER INPUT] Spatial Extents ####
##Encompass Western Australia (WA)
lng.west <- 110
lng.east <- 130
lat.north <- -12
lat.south <- -37
#####


########################################################################
#################### (A) EXTRACT FROM NET to RDS #######################
########################################################################

if ("require" == "CMIP6data"){
  
  #### Extract 3hrly wind and significant wave height data from CMIP6 ####
  # https://nci.org.au/our-services/data-services
  # https://nci-data-training.readthedocs.io/en/latest/_notebook/tds/tds.html
  # Gets each grid bound for WA and metric for 3h window by year_month
  # double[35 x 47 x 31] 
  #       lng x lat x qty_3h_periods in month
  
  #---> [NOTES] Branch (web) ####
  # ##>= 2015
  # input_branch <- "https://data-cbr.csiro.au/thredds/dodsC/catch_all/oa-cmip6-wave/UniMelb-CSIRO_CMIP6_projections/ssp585/ACCESS-CM2/CDFAC1/ww3_ounf_glout/"
  # 
  # ##<= 2014
  # input_branch <- "https://data-cbr.csiro.au/thredds/catalog/catch_all/oa-cmip6-wave/UniMelb-CSIRO_CMIP6_projections/historical/ACCESS-CM2/CDFAC1/ww3_ounf_glout/"
  
  #---> Stem (parameters extracted) ####
  input_head <- "ww3."
  input_tail1 <- "_wnd.nc"    #10-m wind speed Eastward and Northward components (m/s)
  input_tail2 <- "_hs.nc"     #Significant wave height, Hs (m)
  
  #---> Leaf (year and month) ####
  yvec <- seq(start.year, end.year,1)
  mvec <- seq(1,12,1) %>% str_pad(2,pad="0")
  
  
  #---> Define Chores ####
  # Define the input variables
  input_head <- "ww3."
  branch <- NA
  
  ## Create a dataframe
  chores <- expand.grid(yvec = yvec, mvec = mvec) %>% arrange(yvec)
  
  # Add the columns 'input_head' and 'branch'
  chores$input_head <- input_head
  chores$branch <- branch
  
  # Add the 'branch' column based on the value of 'yvec'
  chores$branch <- ifelse(chores$yvec >= 2015,
                      "https://data-cbr.csiro.au/thredds/dodsC/catch_all/oa-cmip6-wave/UniMelb-CSIRO_CMIP6_projections/ssp585/ACCESS-CM2/CDFAC1/ww3_ounf_glout/",
                      "https://data-cbr.csiro.au/thredds/dodsC/catch_all/oa-cmip6-wave/UniMelb-CSIRO_CMIP6_projections/historical/ACCESS-CM2/CDFAC1/ww3_ounf_glout/")
  
  # Reorder the columns to match the desired structure
  chores <- chores[, c("branch", "input_head", "yvec", "mvec")]
  
  chores <- chores %>%
    mutate(input = paste0(branch, input_head, yvec, mvec))
  
  
  yyyymm <- expand.grid(yvec = yvec, mvec = mvec) %>% arrange(yvec, mvec)
  yyyymm <- paste0(yyyymm$yvec, yyyymm$mvec)

  
  #---> Ignore files that have been downloaded, extracted and saved ####
  chores_already_processed <- c()
  for (i in 1:length(yyyymm)){

    netcdf_partname <- yyyymm[i]
    
    if (sum(str_detect(dir(dirRdsCMIP6),netcdf_partname)) == 1){
      chores_already_processed[i] <- TRUE
    } else {
      chores_already_processed[i] <- FALSE
    }
  }
  
  chores <- chores[!chores_already_processed,]
  
  
  #---> Extract and Save ####
  # for (m in 1:length(yvec)){
  for (m in 1:dim(chores)[1]){
    
    ##wnd
    inputfile1 <- paste0(chores$input[m], input_tail1)
    
    ##hs
    inputfile2 <- paste0(chores$input[m], input_tail2)
    
    #---> Open NC for specific VARIABLE and YEAR/MONTH ####
    nc1 <- nc_open(inputfile1)
    nc2 <- nc_open(inputfile2)
      
    ##Notes (commended syntax to check variables)
    {
      # ##Trying to get spatial grid details
      # nc %>% str()
      # nc$var$MAPSTA$size
      # nc$var$MAPSTA$dim
      # nc$var$uwnd
      # # 3 hrly * 31 days
      # # 24/3 * 31
      # nc$var$uwnd$units
      # nc$var$uwnd %>% str() 
    }
      
    ##Get vectors in nc data
    lng <- ncvar_get(nc1, "longitude") %>% as.vector()
    lat <- ncvar_get(nc1, "latitude") %>% as.vector()
      
    ##Encompass Western Australia (WA)
    lng.start <- which.min(abs( lng - lng.west) )
    lng.end <- which.min(abs( lng - lng.east) )
    lat.start <- which.min(abs( lat - lat.south) )
    lat.end <- which.min(abs( lat - lat.north) )
      
    #--> Extract (test) significant wave height ####
    l.hs <- ncvar_get(nc2, "hs", start=c(lng.start,lat.start,1)
                      , count=c(length(lng[lng.start:lng.end])     ##lng width of WA
                                ,length(lat[lat.start:lat.end])    ##lat height of WA
                                ,nc2$var$hs$varsize[3]             ##all time
                      )
    ) 
    
    #--> Extract data for uwnd ####
    l.uwnd <- ncvar_get(nc1, "uwnd", start=c(lng.start,lat.start,1)
                        , count=c(length(lng[lng.start:lng.end])
                                  ,length(lat[lat.start:lat.end])
                                  ,nc1$var$uwnd$varsize[3]
                        )
    ) 
      
    #--> Extract data for vwnd ####
    l.vwnd <- ncvar_get(nc1, "vwnd", start=c(lng.start,lat.start,1)
                        , count=c(length(lng[lng.start:lng.end])
                                  ,length(lat[lat.start:lat.end])
                                  ,nc1$var$vwnd$varsize[3]
                        )
    )
      
    ##Notes (commended plotting syntax to check spatial extents and product)
    {
      # l.hs[,,1] %>% hmap_matrix(title="Test: WA: significant wave height", rotateCCW = FALSE)
      # l.hs[,,1] %>% dim()
    }
      
      
    ##Group environmental metrics into single list for year_month record
    myl <- list(l.hs,
                l.uwnd,
                l.vwnd
    )
      
    
    ##Save output
    saveRDS(myl, paste0(dirRdsCMIP6, "/", paste0("Data_cmip6_", paste0(chores$yvec[m], chores$mvec[m]),".rds")))
      
    rm(myl)
    gc()
    Sys.sleep(1)
      
  } #end of m
  #####
}


if ("require" == "BRANdata"){
  
  #### Extract ocean metrics from BRAN2020 ####
  # https://research.csiro.au/bluelink/bran2020-data-released/
  # https://nci.org.au/our-services/data-services
  # https://nci-data-training.readthedocs.io/en/latest/_notebook/tds/tds.html
  # Gets each grid within spatial range and gets sbt for each year and month
  # double[xx x yy x all x 31]
  #       lng x lat x depth x days
  
  ##Examples of file structures
  # /thredds/ncss/grid/gb6/BRAN/BRAN2020/daily/atm_flux_diag_1993_03.nc
  # /thredds/ncss/grid/gb6/BRAN/BRAN2020/annual/atm_flux_diag_ann_1993.nc
  # /thredds/ncss/grid/gb6/BRAN/BRAN2020/month/atm_flux_diag_mth_1993_03.nc
  

  #### Establish chores for downloading data ####
  #--> Set base of file names to download ####
  input_trunk <- "https://dapds00.nci.org.au/thredds/dodsC/gb6/BRAN/BRAN2020/"
  input_tempscale <- c("daily", "month", "annual")
  
  ##Note the ocean branch and subsequent stems are listed here. If ice and other features are required either modify the code (or ask)
  input_branch <- "/ocean_"
  input_stem <- c("eta_t_", "force_", "mld_", "salt_", "temp_", "tx_trans_int_z_", "ty_trans_int_z_", "u_", "v_", "w_")
  # input_stem <- c("eta_t_", "temp_")
  
  ## Initialize an empty data frame to store permutations
  chores <- data.frame(treeNoLeaves = character(), stringsAsFactors = FALSE)
  
  ## Nested loops to iterate over input_tempscale, input_branch, and input_stem
  for (tempscale in input_tempscale) {
    for (branch in input_branch) {
      for (stem in input_stem) {
        # Create a permutation by concatenating input_trunc, tempscale, branch, and stem
        permutation <- paste0(input_trunk, tempscale, branch, stem)
        
        # Conditionally add input_poststem based on tempscale
        if (tempscale == "daily") {
          permutation <- paste0(permutation, "")
        } else if (tempscale == "month") {
          permutation <- paste0(permutation, "mth_")
        } else if (tempscale == "annual") {
          permutation <- paste0(permutation, "ann_")
        }
        
        # Add the permutation to the data frame
        chores <- rbind(chores, data.frame(treeNoLeaves = permutation, stringsAsFactors = FALSE))
      }
    }
  }
  #---> Leaf (year) ####
  yvec <- seq(max(start.year, 1993), min(end.year, year(Sys.Date())-1),1)
  chores <- expand_grid(treeNoLeaves = chores$treeNoLeaves, year = yvec) %>%
    mutate(treeNoLeaves = paste(treeNoLeaves, year, sep = "")) %>% select(-year)  # Remove the 'year' column if not needed
  

  #---> Leaf (month) ####
  mvec <- seq(1,12,1) %>% str_pad(2,pad="0")

  ##Expand grid for annual URLs
  chores_annual <- chores[chores$treeNoLeaves %>% str_detect("/annual/"),]
  
  ##Filter URLs containing "/daily/" or "/monthly/"
  daily_monthly_urls <- filter(chores, str_detect(treeNoLeaves, "/daily/|/month/"))#
  
  ##Expand grid for monthly URLs
  expanded_monthly <- expand_grid(treeNoLeaves = daily_monthly_urls$treeNoLeaves, month = mvec) %>%
    mutate(treeNoLeaves = paste(treeNoLeaves, month, sep = "_")) %>% select(-month)
  
  ##Combine expanded data frames
  chores_expanded <- bind_rows(chores_annual, expanded_monthly)
  rm(chores_annual, expanded_monthly)
  
  ##Assuming your tibble is named chores_expanded
  chores_expanded$treefull <- paste0(chores_expanded$treeNoLeaves, ".nc") 
  chores_expanded <- chores_expanded %>% select(-treeNoLeaves)
  
  #---> Ignore files that been downloaded, extracted and saved ####
  chores_already_processed <- c()
  for (i in 1:length(chores_expanded$treefull)){
    
    netcdf_partname <- chores_expanded$treefull[i] %>% str_split("/") %>% lapply(tail,1) %>% unlist() %>% str_remove(".nc")
    
    if (sum(str_detect(dir(dirRdsBRAN),netcdf_partname)) == 1){
      chores_already_processed[i] <- TRUE
      } else {
        chores_already_processed[i] <- FALSE
      }
  }
  
  chores_expanded <- chores_expanded[!chores_already_processed,]

  
  #---> Extract and Save ####
  for (m in 1:dim(chores_expanded)[1]){
  # for (m in c(1, 100, 6000, 7750)){
    # m<-226
    # m<-7000 #no xt
    
    inputfile <- chores_expanded$treefull[m]
    nc <- nc_open(inputfile)
    varname <- nc$var %>% names() %>% tail(1)
    
    ##Get vectors in nc data
    lng <- ncvar_get(nc, ifelse("xt_ocean" %in% names(nc$dim), "xt_ocean", "xu_ocean")) %>% as.vector()
    lat <- ncvar_get(nc, ifelse("yt_ocean" %in% names(nc$dim), "yt_ocean", "yu_ocean")) %>% as.vector()

    ##Encompass Western Australia (WA)
    lng.start <- which.min(abs( lng - lng.west) )
    lng.end <- which.min(abs( lng - lng.east) )
    lat.start <- which.min(abs( lat - lat.south) )
    lat.end <- which.min(abs( lat - lat.north) )
    
    #--> Some metrics have depth, others a single level ####
    ##If metric has no depth component
    if (length(nc$var[[varname]]$varsize) == 3){
      
      mymetric <- ncvar_get(nc, varname, start=c(lng.start,lat.start,1)
                            , count=c(length(lng[lng.start:lng.end])          #lng width of WA
                                      ,length(lat[lat.start:lat.end])         #lat width of WA
                                      ,nc$var[[varname]]$varsize %>% tail(1)  #single record
                            ))
      
    } else{
      mymetric <- ncvar_get(nc, varname, start=c(lng.start,lat.start,1,1)
                            , count=c(length(lng[lng.start:lng.end])          #lng width of WA
                                      ,length(lat[lat.start:lat.end])         #lat width of WA
                                      ,-1                                     #all depths    
                                      ,nc$var[[varname]]$varsize %>% tail(1)  #single record
                            ))

    }
    
    # ##Notes (commended plotting syntax to check spatial extents and product)
    # {
    #   tmp[,,1] %>% hmap_matrix(rotateCCW = TRUE, title = paste0(varname, " - Depth 0"))
    #   tmp[,,]  %>% dim()
    # }
    
    ##Save output
    saveRDS(mymetric, paste0(dirRdsBRAN, "/", paste0("Data_BRAN_", inputfile %>% str_split("/") %>% unlist() %>% tail(1) %>% str_remove(".nc"),".rds")))
    
    # rm(myl)
    gc()
    Sys.sleep(2)
    
  } #end of m
    
  #####
}

#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################
#####################################################################

