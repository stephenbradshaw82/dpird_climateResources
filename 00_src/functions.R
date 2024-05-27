#' -------------------------------------------------------------------------
#' Project:     netCDF functions
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
#' @output_filePath file path for temporary outputs
#' @returns NULL writes out files on the fly into output_filePath
func_assignNetCDF_parallel <- function(list_df_name, required.packages = req_packages, output_filePath = "04_netCDFcombinedWithRaw/") {
  
  #--> Test ####
  # # tmp_df <- cdfl[[10]][[1]]
  # # tmp_name <- cdfl[[10]][[2]]
  # tmp_df <- cdfl[[22]][[1]] #%>% filter(sitef=="WOODMAN POINT")
  # tmp_name <- cdfl[[22]][[2]] #%>% filter(sitef=="WOODMAN POINT")
  # netcdf.path = dirNETCDFOutputs
  # required.packages <- req_packages
  # output_filePath = "04_netCDFcombinedWithRaw/"
  #####
  
  #--> Install packages ####
  sapply(req_packages,require,character.only = TRUE, quietly=TRUE)
  
  #--> isolate objects ####
  tmp_df <- list_df_name[[1]]
  tmp_name <- list_df_name[[2]]
  
  #--> read in netcdf (3: hs, uwnd and vwnd) #####
  tmp_ncdf <- readRDS(dir(dirNETCDFOutputs, full.names=TRUE)[dir(dirNETCDFOutputs) %>% str_detect(tmp_name)])
  
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
  saveRDS(tmp_df, paste0(output_filePath,tmp_name,"_netCDFwithRaw.rds") )
  
  gc()
  
  #--> Return ####
  return()
  #####
}


###############################################################
###############################################################
###############################################################

