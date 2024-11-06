#' -------------------------------------------------------------------------
#' Project:     netCDF functions & climate data for DPIRD
#'
#' Created by:  Stephen Bradshaw
#' Modified:    27/05/2024 (change history must be captured in code)
#' Version:     1.01 - Initial Release
#'              
#' Purpose:     Functions sourced to (potentially) be used in netcdf extraction and wrangling
#' 
#' -------------------------------------------------------------------------

###############################################################
########################## FUNCTIONS ##########################
###############################################################

#' Modification to the standard "in" function
#' Use: x %!in% vector etc
`%!in%` = Negate(`%in%`)


#' Checks if directory present, creates if not
#' @param directory_name directory name as a string
#' @returns print statement of outcome
func_checkCreateDirectory <- function(directory_name) {
  if (!dir.exists(directory_name)) {
    dir.create(directory_name)
    print("Directory created successfully.")
  } else {
    print("Directory already exists.")
  }
}


#' Function implements plotting of raster
#' Tests AIC and updates knots for set smoothing terms
#' @x matrix (can handle NA values which are common with netcdf environmental data)
#' @rotateCCW boolean to rotate for viewing
#' @title figure title
#' @returns plot image
hmap_matrix <- function(x, rotateCCW=TRUE, title="Test"){
  
  if (rotateCCW == TRUE){
    m <- apply(t(x),2,rev)
  } else {
    m <- x
  }
  
  pheatmap(m, cluster_rows = FALSE, cluster_cols = FALSE, main = title)
  
} 


#' Function assigns index of year-month-3h from year-month-(clock_hour+1) records
#' @year int year 
#' @month int calendar month
#' @day int day number from month
#' @clock_hour int clock hour (adjusted to be from 1 to 24)
#' @returns index that observation/record occurs within the month
get_netcdf_time3h_index <- function(year, month, day, clock_hour) {
  # Calculate the number of days in the month
  days_in_month <- as.numeric(format(as.Date(paste(year, month, "01", sep = "-")), "%d"))
  
  # Calculate the total number of 3-hour time blocks in the month
  total_blocks <- days_in_month * 8
  
  # Calculate the index
  index <- ((day - 1) * 8) + ((clock_hour - 1) %/% 3) + 1
  
  return(index)
}


#' Function takes df of yyyymm raw data and assigns hs, uwnd and vwnd records from CMIP6
#' @list_df_name list df (raw file) and name (yyyydd) for saving
#' @required.packages vector of packages used in R
#' @input_filePath file path to netcdf files
#' @output_filePath file path for temporary outputs
#' @returns NULL writes out files on the fly into output_filePath
func_assignNetCDF_CMIP6_parallel <- function(list_df_name, required.packages = req_packages
                                       , input_filePath
                                       , output_filePath) {
  
  # #--> Test ####
  # list_df_name <- cdfl[[10]]
  # required.packages <- req_packages
  # input_filePath = paste0(dirRdsCMIP6, "/", list_df_name[[2]] %>% str_split(DOT) %>% unlist() %>% pluck(2))
  # output_filePath <- dirRdsCMIP6_output#"netCDFrds_CMIP6_output"
  # #####
  
  #--> Install packages ####
  sapply(req_packages,require,character.only = TRUE, quietly=TRUE)
  
  #--> isolate objects ####
  tmp_df <- list_df_name[[1]]
  tmp_name <- list_df_name[[2]] %>% str_split(DOT) %>% unlist() %>% pluck(1)
  
  #--> read in netcdf (3: hs, uwnd and vwnd) #####
  tmp_ncdf <- readRDS(dir(input_filePath, full.names=TRUE)[dir(input_filePath) %>% str_detect(tmp_name)])
  
  #                         lon x lat x time
  # hmap_matrix(tmp_ncdf[[1]][6,,1], rotateCCW=FALSE, title="Test")
  
  #--> Extract & Assign from netcdf layer based on indices ####
  ## Function
  # extract_values <- function(input_matrix, index_long, index_lat, index_time) {
  #   indices <- cbind(index_long, index_lat, index_time)
  #   values <- input_matrix[indices]
  #   return(values)
  # }
 
  ##Function now includes NA fix (BROOME ETC)
  extract_values <- function(input_matrix, index_long, index_lat, index_time) {
    values <- numeric(length(index_long))  # Initialize vector to store values
    
    for (i in 1:length(index_long)) {
      long <- index_long[i]
      lat <- index_lat[i]
      time <- index_time[i]
      
      # Check if the value at the current index is NA
      if (is.na(input_matrix[long, lat, time])) {
        # Extract the 3x3 grid of values centered around the NA value
        grid_values <- input_matrix[max(1, long - 1):min(nrow(input_matrix), long + 1),
                                    max(1, lat - 1):min(ncol(input_matrix), lat + 1),
                                    time]
        
        # Calculate the average of non-NA grid values
        avg_value <- mean(grid_values, na.rm = TRUE)
        
        # Replace NA value with the average
        values[i] <- avg_value
      } else {
        # If the value is not NA, simply assign it to the result
        values[i] <- input_matrix[long, lat, time]
      }
    }
    
    return(values)
  }

  
  ## Extract & Assign values
  tmp_df <- cbind(tmp_df
                  , hs = extract_values(tmp_ncdf[[1]], tmp_df$index_long, tmp_df$index_lat, tmp_df$index_time)
                  , uwnd = extract_values(tmp_ncdf[[2]], tmp_df$index_long, tmp_df$index_lat, tmp_df$index_time)
                  , vwnd = extract_values(tmp_ncdf[[3]], tmp_df$index_long, tmp_df$index_lat, tmp_df$index_time)
  )

  
  #--> Save output ####
  saveRDS(tmp_df, paste0(output_filePath, "/",tmp_name,"_netcdfCMIP6withRaw.rds") )
  
  gc()
  
  #--> Return ####
  return()
  #####
}


#' Function takes df of yyyymm raw data and assigns vectors for DAILY, MONTHLY, YEARLY for chosen metrics
#' @list_df_name list df (raw file) and name (yyyydd) for saving
#' @required.packages vector of packages used in R
#' @input_filePath file path to netcdf files
#' @output_filePath file path for temporary outputs
#' @features vector of strings, example: c("temp_", "eta_t_")
#' @addYearly boolean TRUE | FALSE indicating whether to assign features to data
#' @addMonthly boolean TRUE | FALSE indicating whether to assign features to data
#' @addDaily boolean TRUE | FALSE indicating whether to assign features to data
#' @atDepth_str Depth of desired feature as a string. Inf == "SBT", 0m == "SST", 100m == "100"
#' @returns NULL writes out files on the fly into output_filePath
func_assignNetCDF_BRAN_parallel <- function(list_df_name, required.packages = req_packages
                                            , input_filePath
                                            , output_filePath
                                            , features
                                            , addYearly
                                            , addMonthly
                                            , addDaily
                                            , atDepth_str) {
  
  # #--> Test ####
  # list_df_name<- cdfl[[1]]
  # required.packages <- req_packages
  # input_filePath <- paste0(dirRdsBRAN, "/", tmp_df[[1]]$Region)#dirRdsBRAN#"02_netCDFrds_BRAN"
  # output_filePath <- dirRdsBRAN_output#"netCDFrds_BRAN_output"
  # features = c("temp_", "eta_t_")
  # addYearly = TRUE
  # addMonthly = TRUE
  # addDaily = TRUE
  # atDepth_str = "100"#"SBT"
  # #####
  
  #--> Install packages ####
  sapply(req_packages,require,character.only = TRUE, quietly=TRUE)
  
  #--> isolate objects ####
  tmp_df <- list_df_name[[1]]
  tmp_name <- list_df_name[[2]]
  
  ##If appropriate spatial extent files not present, remove data
  if (is.na(tmp_df) %>% sum() %>% ">"(0)){

    tmp_df <- tmp_df %>% na.omit()
    
    print(paste0("DATA FOR year=", tmp_df$year[1], " & month=", tmp_df$month[1], " CONTAINS NA VALUES"))
    print("THIS DATA HAS BEEN REMOVED")
    print("YOU WILL NEED TO SOURCE THE APPROPRIATE NETCDF FILES FOR THIS SPATIAL EXTENT")
  }
  
  #--> Identify netcdf(rds) files param ####
  #' Need to detect files based on yyyymm AND features
  #' Is this going to crash if hitting the same rds at the same time??
  
  #--> Depending on triggering codebase (100 or 110) ####
  if ("yearMonth" %in% c(names(tmp_df))){
    ## 100 code base
    yearMonth <- tmp_df$yearMonth[1]
    pattern_year <- paste0(substr(yearMonth, 1, 4))
    pattern_month <- paste0("_", substr(yearMonth, 5, 6))

  } else {
    
    ## 110 code base
    if ("month" %in% c(names(tmp_df))){
      ## monthly data
      pattern_year <- tmp_df$year[1]
      pattern_month <- paste0("_", sprintf("%02d", tmp_df$month))[1]
      
    } else {
      ## yearly only
      pattern_year <- tmp_df$year[1]
      
    }
  }
  
  #--> Applying ####
  for (n in features){
    # n <- "temp_" ##with depth
    # n <- "eta_t_" ##single level

    func_length_to_index <- function(x) {
      valid_lengths <- c(seq(0, 40, by=5), seq(50, 200, by=10))
      nearest_length <- valid_lengths[which.min(abs(valid_lengths - x))]
      which(valid_lengths == nearest_length)
    }
    
    func_get_last_valid <- function(x, y, depth, z) {
      
      if(length(dim(tmp_ncdf)) == 3){
        vals <- tmp_ncdf[x, y, ]
        
        if (!is.na(vals[func_length_to_index(as.numeric(depth))])){
          
          return(vals[func_length_to_index(as.numeric(depth))])
          
        } else if (all(is.na(vals))) {
          
          return(NA)
          
        } else {
          
          return(vals[max(which(!is.na(vals)))])
          
        }
        
      ### IF WE HAVE DAILY DATA WITH A 4th DIMENSION  
      } else {
        vals <- tmp_ncdf[x, y, ,z]
        
        if (!is.na(vals[func_length_to_index(as.numeric(depth))])){
          
          return(vals[func_length_to_index(as.numeric(depth))])
          
        } else if (all(is.na(vals))) {
          
          return(NA)
          
        } else {
          
          return(vals[max(which(!is.na(vals)))])
          
        }

      }
 
    }
    
    #--> Annual ####
    if (addYearly){
      
      ##Get new column name
      newColName <- paste0(n, "ann")
      
      ##Find feature file
      tmp_file <- dir(input_filePath, full.names=TRUE)[( str_detect(dir(input_filePath),n) ) &
                                                         ( str_detect(dir(input_filePath), pattern_year) ) &
                                                         ( str_detect(dir(input_filePath), "_ann"))
                                                       ]
      ##Open feature file
      tmp_ncdf <- readRDS(tmp_file)
      
      ##if feature has no depth
      if(length(dim(tmp_ncdf))==2){

        tmp_df[[newColName]] <- tmp_ncdf[tmp_df$index_long, tmp_df$index_lat]

      ##else Consider depth  
      } else {
        
        if (atDepth_str == "SST"){
          
          tmp_df[[newColName]] <- tmp_ncdf[tmp_df$index_long, tmp_df$index_lat,1]
          
        } else if (atDepth_str == "SBT"){
          
          tmp_df[[newColName]] <- ifelse(all(is.na(tmp_ncdf[tmp_df$index_long, tmp_df$index_lat,]))
                                         , yes=NA
                                         , no=tail(na.omit(tmp_ncdf[tmp_df$index_long, tmp_df$index_lat,]), 1))

        } else {
          
          result <- mapply(func_get_last_valid, 
                           x = tmp_df$index_long, 
                           y = tmp_df$index_lat, 
                           MoreArgs = list(depth = atDepth_str))
          tmp_df[[newColName]] <- result

        }

      }

    } #end addYearly
    
    #--> Monthly ####
    if (addMonthly){
      
      ##Get new column name
      newColName <- paste0(n, "mth")
      
      ##Find feature file
      tmp_file <- dir(input_filePath, full.names=TRUE)[  ( str_detect(dir(input_filePath),n) ) &
                                                         ( str_detect(dir(input_filePath), as.character(pattern_year)) ) &
                                                         ( str_detect(dir(input_filePath),  paste0(pattern_month,"_N")) ) &
                                                         ( str_detect(dir(input_filePath), "_mth"))
                                                       ]

      ##Open feature file
      tmp_ncdf <- readRDS(tmp_file)

      ##if feature has no depth
      if(length(dim(tmp_ncdf))==2){
        
        tmp_df[[newColName]] <- tmp_ncdf[tmp_df$index_long, tmp_df$index_lat]
        
        ##else Consider depth  
      } else {
        
        if (atDepth_str == "SST"){
          
          result <- rep(NA, length(tmp_df$index_long))
          result <- tmp_ncdf[cbind(tmp_df$index_long, tmp_df$index_lat, rep(1, length(tmp_df$index_long)))]
          tmp_df[[newColName]] <- result
          rm(result)
          
        } else if (atDepth_str == "SBT"){
          
          idx_matrix <- cbind(tmp_df$index_long, tmp_df$index_lat)
          result <- apply(idx_matrix, 1, function(idx) {
            vals <- tmp_ncdf[idx[1], idx[2], ]
            if(all(is.na(vals))) {
              return(NA)
            } else {
              return(vals[max(which(!is.na(vals)))])
            }
          })
          tmp_df[[newColName]] <- result
          rm(result)
          
        } else {
        
          result <- mapply(func_get_last_valid, 
                           x = tmp_df$index_long, 
                           y = tmp_df$index_lat, 
                           MoreArgs = list(depth = atDepth_str))
          tmp_df[[newColName]] <- result
          rm(result)
          
        }
        
      }
      
    }#end addMonthly
    
    #--> Daily ####
    #' Currently memory inefficient
    if (addDaily){
      
      ##Get new column name
      newColName <- paste0(n, "day")
      
      ##Find feature file
      tmp_file <- dir(input_filePath, full.names=TRUE)[( str_detect(dir(input_filePath),n) ) &
                                                         ( str_detect(dir(input_filePath), pattern_year) ) &
                                                         ( str_detect(dir(input_filePath),  paste0(pattern_month,"_N")) ) &
                                                         ( !str_detect(dir(input_filePath), "_mth"))
      ]
      
      ##Open feature file
      tmp_ncdf <- readRDS(tmp_file)

      ##if feature has no depth (changed here as Daily has up to 4 dimensions if feature has depth)
      if(length(dim(tmp_ncdf))==3){
        
        tmp_df[[newColName]] <- tmp_ncdf[tmp_df$index_long, tmp_df$index_lat, tmp_df$day]
        
        ##else Consider depth  
      } else {
        
        if (atDepth_str == "SST"){
          
          tmp_df[[newColName]] <- tmp_ncdf[tmp_df$index_long, tmp_df$index_lat, 1, tmp_df$day]
          
        } else if (atDepth_str == "SBT"){
          
          tmp_df[[newColName]] <- ifelse(all(is.na(tmp_ncdf[tmp_df$index_long, tmp_df$index_lat,,tmp_df$day]))
                                         , yes=NA
                                         , no=tail(na.omit(tmp_ncdf[tmp_df$index_long, tmp_df$index_lat,,tmp_df$day]), 1))
          
        } else {
          
          result <- mapply(func_get_last_valid, 
                           x = tmp_df$index_long, 
                           y = tmp_df$index_lat, 
                           MoreArgs = list(depth = atDepth_str),
                           z = tmp_df$day)
          tmp_df[[newColName]] <- result
          rm(result)
          
          
        }
        
      }
      
    }#end addDaily
    #####
  }

  #--> Save output ####
  saveRDS(tmp_df, paste0(output_filePath, "/",tmp_name,"_netcdfBRANwithRaw.rds") )
  
  gc()
  
  #--> Return ####
  return()
  #####
}



###############################################################
###############################################################
###############################################################