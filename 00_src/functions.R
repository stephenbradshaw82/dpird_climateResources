#' -------------------------------------------------------------------------
#' Project:     Camera Data Analysis Functions
#'
#' Created by:  Stephen Bradshaw
#' Modified:    17/01/2024 (change history must be captured in code)
#' Version:     1.01 - Initial Release
#'              1.02 - Created toption for adding in survey columns using DataTable 
#'              
#' Purpose:     This project aims to produce an "effort" metric from video ramp data 
#'              Scripts call functions listed below
#' -------------------------------------------------------------------------

###############################################################
########################## FUNCTIONS ##########################
###############################################################

#' Modification to the standard "in" function
#' Use: x %!in% vector etc
`%!in%` = Negate(`%in%`)


#' Function to connect to FileMaker server and download Camera data
#' @param today date used for labelling files. Use format(Sys.Date(), format="%Y%m%d")
#' @param vec_selectSites vector of site names (need to be as per Database, or use "allSites")
#' @param start_date string of start date as "%d/%m/%Y"
#' @param end_date string of end date as "%d/%m/%Y"
#' @param onMacNetwork Boolean (TRUE) Currently needs to stay as TRUE as sourcing data from the Mac Server
#' @info Connects to FileMaker database and downloads (raw data) used to get launch and Retreival data from Ramp cameras
#' @returns two dataframes: counts (sourced from the Counts table) and missdata (sourced from MissingData tables)
func_extractCameraDataFileMaker <- function(today
                                            , vec_selectSites = "allSites"
                                            , start_date = "01/01/1900"
                                            , end_date = Sys.Date() %>% format("%d/%m/%Y")
                                            , outputFolder = "01_data/000_RawData/"
                                            , returnIndivOutputs = FALSE
                                            , db_macnetwork_password = Sys.getenv("DB_MACNETWORK_PASSWORD")
                                            , onMacNetwork = TRUE) {
  
  # # ##### TEST #####
  # onMacNetwork <- TRUE
  # today <- format(Sys.Date(), format="%Y%m%d")
  # # vec_selectSites = "allSites"
  # vec_selectSites = "Hillarys"
  # start_date <- Sys.Date()  %>% "-"(90) %>% format("%d/%m/%Y")
  # end_date <- Sys.Date() %>% format("%d/%m/%Y")
  # outputFolder = "01_data/000_RawData/"
  # # db_macnetwork_password = Sys.getenv("DB_MACNETWORK_PASSWORD")
  # # #####
  
  if (onMacNetwork){
    #--> Connect to the database using the DSN ####
    #' Had to do:
    #' Download and install FileMaker drivers
    #' https://support.claris.com/s/article/Software-Update-FileMaker-xDBC-client-drivers-for-FileMaker-1503692806454?language=en_US
    #' Note: downloaded older version 19.6.3 (seen being used on Kim's mac downstairs --> FilemakerServerMac) --> redownloaded 20.3.2 
    #' Download and install ODBC manager
    #' https://odbcmanager.net/
    #' Configure the connection
    #'  Name: atHillarysMastercounts
    #'  Host: 192.168.82.7
    #'  Database: MasterCounts
    
    #--> Legacy Note: ####
    # Connection string used at Hillarys
    # 192.168.82.7  # (EDIT: CD 1/9/17)
    # Connection string used off site
    # 180.216.72.167
    #####
    
    #--> Establish channel ####
    ## Check the dns names that are known by the system
    # odbcDataSources(type="all")
    dsn <- "atHillarysMastercounts"
    uid <- "Data Extraction"
    pwd <- db_macnetwork_password
    
    channel <- RODBC::odbcConnect(dsn=dsn, uid=uid, pwd=pwd)
    

    if (returnIndivOutputs){
      ## Check which tables are available in the "MasterCounts" database
      Available.Tables <- RODBC::sqlTables(channel)
      # head(Available.Tables)
      
      ## Get details of table structure
      CountsColumns <- RODBC::sqlColumns(channel, "Counts")
      # head(CountsColumns)
      
      filename <- paste(outputFolder, today, "CountsColumns.RData", sep="")
      save(CountsColumns, file=filename)
      # load("CountsColumns.RData")
      
      MissingDataColumns <- RODBC::sqlColumns(channel, "MissingData")
      head(MissingDataColumns)
      
      filename <- paste(outputFolder, today, "MissingDataColumns.RData", sep="")
      save(MissingDataColumns, file=filename)
    }
    
    #####
    
    ##### [BLOCKED] #####
    # counts <- sqlQuery(channel,"select * from Counts")
    # head(counts)
    # test<- sqlQuery(channel,"select \"record number\" from MissingData")
    # test
    #####
    
    ##### [BLOCKED] #####
    # The commented code was used for an earlier run.
    # Set up a query to read only the Ocean Reef data
    # oceanreefqry<-paste("select \"Time Launched\", ActivityDate, \"Time Retrieved\",",
    #           " SiteDescription, VehicleTypeDescription, \"Record Number\",",
    #           " \"Time Zone\", \"total launchs\", \"Total Retreivals\",",
    #           " LaunchTimeMod, RetrieveTimeMod, \"time entered\",",
    #           " ActivityStartTime, ActivityEndTime, ActivityTypeCode,",
    #           " ActivityUnit, Reader, PrivateRamp, \"modification date\",",
    #           " \"modification user\" from Counts where SiteDescription='Ocean Reef'")
    
    # Read the Ocean Reef data into a data frame
    # oceanreef<-sqlQuery(channel,oceanreefqry,rows_at_time=1)
    
    # The field names were later modified by using the code below. Again, this has been
    # superseded, but has been left in the script as commented code to provide a further example of
    # the use of the functions within the RODBC package.
    # Rename the fields that contained spaces
    # oceanreef <- rename(oceanreef, c("Time Launched"="TimeLaunched"))
    # oceanreef <- rename(oceanreef, c("Time Retrieved"="TimeRetrieved"))
    # oceanreef <- rename(oceanreef, c("Record Number"="RecordNumber"))
    # oceanreef <- rename(oceanreef, c("Time Zone"="TimeZone"))
    # oceanreef <- rename(oceanreef, c("total launchs"="TotalLaunchs"))
    # oceanreef <- rename(oceanreef, c("Total Retreivals"="TotalRetreivals"))
    # oceanreef <- rename(oceanreef, c("time entered"="TimeEntered"))
    # oceanreef <- rename(oceanreef, c("modification date"="ModificationDate"))
    # oceanreef <- rename(oceanreef, c("modification user"="ModificationUser"))
    
    # Display the field names
    # names(oceanreef)
    
    ## Set up a query to read all camera data
    # getcounts <- paste("select \"Time Launched\", ActivityDate, \"Time Retrieved\",",
    #                    " SiteDescription, VehicleTypeDescription, \"Record Number\",",
    #                    " \"Time Zone\", \"total launchs\", \"Total Retreivals\",",
    #                    " LaunchTimeMod, RetrieveTimeMod, \"time entered\",",
    #                    " ActivityStartTime, ActivityEndTime, ActivityTypeCode,",
    #                    " ActivityUnit, Reader, PrivateRamp, \"modification date\",",
    #                    " \"modification user\" from Counts")
    #####
    
    #### [BLOCKED] ####
    # # Set up a query to read only camera data for a specific time period
    # getcounts <- paste("select \"Time Launched\", ActivityDate, \"Time Retrieved\",",
    #                    " SiteDescription, VehicleTypeDescription, \"Record Number\",",
    #                    " \"Time Zone\", \"total launchs\", \"Total Retreivals\",",
    #                    " LaunchTimeMod, RetrieveTimeMod, \"time entered\",",
    #                    " ActivityStartTime, ActivityEndTime, ActivityTypeCode,",
    #                    " ActivityUnit, Reader, PrivateRamp, \"modification date\",",
    #                    " \"modification user\" from Counts where ActivityDate>='01/11/2017' and ActivityDate<='20/11/2017'")
    # 
    # #  # Set up a query to read only data for one ramp
    # getcounts<-paste("select \"Time Launched\", ActivityDate, \"Time Retrieved\",",
    #           " SiteDescription, VehicleTypeDescription, \"Record Number\",",
    #           " \"Time Zone\", \"total launchs\", \"Total Retreivals\",",
    #           " LaunchTimeMod, RetrieveTimeMod, \"time entered\",",
    #           " ActivityStartTime, ActivityEndTime, ActivityTypeCode,",
    #           " ActivityUnit, Reader, PrivateRamp, \"modification date\",",
    #           " \"modification user\" from Counts where SiteDescription='Canal Rocks'")
    #####
    
    #--> Construct SQL Query for COUNTS ####
    ## Define sites as strings
    vec_selectSites_quoted <- sapply(vec_selectSites, function(site) paste0("'", site, "'"))
    
    ## Construct WHERE clause with both site and date conditions
    where_clause <- ifelse(all(vec_selectSites != "allSites"), sprintf("SiteDescription IN (%s) AND ", paste(vec_selectSites_quoted, collapse = ", ")), "")
    where_clause <- paste("WHERE", where_clause, "ActivityDate >= '", start_date, "' AND ActivityDate <= '", end_date, "'")
    
    ## Construct SQL query
    sql_query <- paste("
    SELECT
        \"Time Launched\",
        ActivityDate,
        \"Time Retrieved\",
        SiteDescription,
        VehicleTypeDescription,
        \"Record Number\",
        \"Time Zone\",
        \"total launchs\",
        \"Total Retreivals\",
        LaunchTimeMod,
        RetrieveTimeMod,
        \"time entered\",
        ActivityStartTime,
        ActivityEndTime,
        ActivityTypeCode,
        ActivityUnit,
        Reader,
        PrivateRamp,
        \"modification date\",
        \"modification user\"
    FROM Counts", where_clause)
    
    ## Execute the SQL query
    counts <- RODBC::sqlQuery(channel, sql_query)
    
    ## Save the raw counts
    if(returnIndivOutputs){
      filename <- paste(outputFolder, today, "RawCounts.RData", sep="")
      save(counts, file=filename)
    }
    
    #--> Construct SQL Query for MISSING ####
    ## Define sites as strings
    vec_selectSites_quoted <- sapply(vec_selectSites, function(site) paste0("'", site, "'"))
    
    ## Construct WHERE clause with both site and date conditions
    where_clause <- ifelse(all(vec_selectSites != "allSites"), sprintf("camera IN (%s) AND ", paste(vec_selectSites_quoted, collapse = ", ")), "")
    where_clause <- paste("WHERE", where_clause,
                          "((\"date missing start\" >= '", start_date, "' AND \"date missing end\" <= '", end_date, "')",
                          " OR (\"date missing start\" <= '", start_date, "' AND \"date missing end\" >= '", end_date, "'))")
    
    ## Construct SQL query
    sql_query <- paste("
    SELECT
        *
    FROM MissingData", where_clause)
    
    ## Execute the SQL query
    missdata <- RODBC::sqlQuery(channel, sql_query)
    
    ## Save the raw missing data
    if(returnIndivOutputs){
      filename <- paste(outputFolder, today, "RawMissingData.RData", sep="")
      save(missdata, file=filename)
    }
    
    #--> Return object ####
    ## Close the RODBC connection when done
    RODBC::odbcClose(channel)
    
    ## Set up the list that will be returned from this function
    out_list <- list(counts=counts, missdata=missdata)
    filename <- paste(outputFolder, today, "_List_counts_missing.rds", sep="")
    saveRDS(out_list, file=filename)
    
    
    return(print("List object saved"))
    
  } else {
    
    return(print("Not set up for external...yet"))
  }
  
}


#' Clean Site (Ramp) names
#' @param input_namevector column as vector of SiteDescriptions from data
#' @param input_validSites hardcoded variable (vector) of siteDescriptions
#' @param error_str label afforded to erroneous record
#' @returns dataframe with SiteDescription_mod and ErrSite
func_fixRampNames <- function(input_namevector, input_validSites=siteNames, error_str="siteNames") {
  
  #### TEST ####
  # input_namevector <- df_counts$SiteDescription
  # error_str="siteNames"
  # input_validSites <- siteNames
  # input_namevector <- df_cam_read %>% select(site) %>% pluck(1)
  # input_namevector <- rl %>% names()
  ##########
  
  ##Cosmetics
  vSiteRaw <- input_namevector
  vSite <- vSiteRaw %>% str_to_upper() %>% trimws()
  
  ##Consistency Changes
  vSite[vSite %>% str_detect("WOODMAN")] <- "WOODMAN POINT"
  vSite[vSite %>% str_detect("BATAVIA MARINA")] <- "BATAVIA"
  vSite[vSite %>% str_detect("EMU POINT")] <- "ALBANY"
  vSite[vSite %>% str_detect("BANDY CREEK")] <- "ESPERANCE"
  
  ##Check Errors
  vSiteErr <- rep("", length=length(vSite))
  vSiteErr[which(vSite %!in% input_validSites)] <- "siteError"
  
  ##Combine and return
  out <- cbind(vSite, vSiteErr) %>% as.data.frame()
  names(out) <- c("SiteDescription_mod", "ErrSite")
  
  return(out) 
}


#' Clean Vehicle Type names
#' @param input_namevector column as vector of VehicleTypeDescription from data
#' @param input_validSites hardcoded variable (vector) of VehicleTypeDescription
#' @param error_str label afforded to erroneous record
#' @returns dataframe with VehicleTypeDescription_mod and ErrVeh
func_fixVehNames <- function(input_namevector, input_validVeh=vehicleNames, error_str="vehNames") {
  
  #### TEST ####
  # input_namevector <- dfc$VehicleTypeDescription
  # input_validVeh <- vehicleNames
  # error_str="vehNames"
  ##########

  ##Cosmetics
  vVehRaw <- input_namevector
  vVeh <- vVehRaw %>% str_to_upper() %>% trimws()
  

  ##Check Errors
  vVehErr <- rep("", length=length(vVeh))
  vVehErr[which(vVeh %!in% input_validVeh)] <- "vehError"
  
  ##Combine and return
  out <- cbind(vVeh, vVehErr) %>% as.data.frame()
  names(out) <- c("VehicleTypeDescription_mod", "ErrVeh")
  
  return(out) 
}


#' Read excel sheets, listify, save as rds
#' @param input_filename excel read files as string
#' @returns list object of excels (ignoring iSurvey)
func_readExcelAllsheets <- function(input_filename) {

  #### TEST ####
  # input_filename <- fileCameraRead
  ##############
  
  sheets <- readxl::excel_sheets(input_filename)
  sheets <- sheets[sheets %>% str_detect("iSurvey|\\(PHE", negate=TRUE)]
  
  rsl <- list()
  for (n in 1:length(as.vector(sheets))){
    rsl[[n]] <-  readxl::read_excel(input_filename, sheet = sheets[n], col_types = "text")
  }
  
  names(rsl) <- sheets #%>% str_to_upper() %>% trimws()
  return(rsl)
}
#####


#' Function to clean 'Read Records'
#' @param input_list read excel file, now a list object
#' @info The returns for AUGUSTA, HAMELIN BAY, CANAL ROCKS, PORT GEOGRAPHE are erroneous...
#' @info ...--> need to include ALL (census) within 100_execution.R code for these
#' @returns 
func_cleanReadRecordsData <- function(input_list, func=func_fixRampNames, reqNames = siteNames) {
  
  # #--> Test ####
  # input_list <- rl
  # func <- func_fixRampNames
  # reqNames <- siteNames 
  # #####
  
  #### CAMERA READ RECORDS ####
  #--> Read Data ####
  l_cam <- input_list
  
  ##Requirement for other other function
  input_validSites <- reqNames
  
  #--> Fix dates (excel) and format columns ####
  ldf <- list()
  for (n in 1:length(l_cam)){
    # n<-2
    ##Rbind date columns
    tdf <- rbind(l_cam[[n]] %>% select(Site, Date, Read1) %>% 
                   dplyr::rename(site = Site, olddate = Date, isread = Read1) %>%
                   filter(!is.na(olddate))
                 , l_cam[[n]] %>% select(Site, `Replacement date`, Read2) %>% dplyr::rename(site = Site, olddate = `Replacement date`, isread = Read2) %>% filter(!is.na(olddate))
    ) %>% 
      mutate(isread = isread %>% str_to_upper() %>% trimws()) %>%
      filter(isread == "Y")
    
    tdf$isNumDate <- ifelse(tdf$olddate %>% str_detect(START %R% one_or_more(DGT) %R% END), yes=1, no=0)
    
    ##Fix date formats
    tdf$date <- as.Date(1) ##place holder
    tdf$date[tdf$isNumDate==1] <- tdf$olddate[tdf$isNumDate==1] %>% as.numeric() %>% as.Date(origin = "1899/12/30") ##origin seems to work for our data
    tdf$date[tdf$isNumDate==0] <- tdf$date[tdf$isNumDate==0] %>% as.Date(format="%d/%m/%Y")
    tdf <- tdf %>% select(-c(olddate, isNumDate))
  
    ldf[[n]] <- tdf
    rm(tdf)
    
  }
  
  #-->Return the read files as a df ####
  ##Collapse list into df
  df_cam_read <- do.call("rbind", ldf)
  
  dfcr <- cbind(df_cam_read
                , func(input_namevector=df_cam_read$site, input_validSites=siteNames, error_str="siteNames")) %>%
    select(SiteDescription_mod, date, isread, ErrSite) #%>% rename(site = SiteDescription_mod)
  
  
  ##Save .rds
  # saveRDS(df_cam_read, paste(dirOut, input_list[[1]]$fileCameraRead %>% str_replace("Read", "Clean") %>% str_replace(".csv", ".rds"), sep=""))

  return(dfcr)
  #####
}


#' Using an API. Data ongoing update from 2014 onwards
#' https://data.gov.au/data/dataset/australian-holidays-machine-readable-dataset
#' @returns df_hols dataframe of holidays for National and WA
func_getPublicHols <- function(yrs_available = c("2021plus", "2020", "2019", "201718", "201617", "201516", "201415")){
  #####TEST#####
  
  ##############
  #--> API to get data ####
  ## Define the URL and the query parameters
  url <- "https://data.gov.au/data/api/3/action/datastore_search"
  
  data_hols <- yrs_available
  data_resource_id <- c("33673aca-0857-42e5-b8f0-9981b4755686",
                        "c4163dc4-4f5a-4cae-b787-43ef0fcf8d8b",
                        "bda4d4f2-7fde-4bfc-8a23-a6eefc8cef80",
                        "253d63c0-af1f-4f4c-b8d5-eb9d9b1d46ab",
                        "a24ecaf2-044a-4e66-989c-eacc81ded62f",
                        "13ca6df3-f6c9-42a1-bb20-6e2c12fe9d94",
                        "56a5ee91-8e94-416e-81f7-3fe626958f7e"
  )
  
  ##Extract yearly data with appropriate column formatting
  hols_list <- list()
  
  for (n in 1:length(data_hols)){
    
    tmp_response <- GET(url, query = list(resource_id = data_resource_id[n], limit = 10000))
    tmp_json <- fromJSON(rawToChar(tmp_response$content), flatten = TRUE)
    
    if("Jurisdiction" %in% names(tmp_json$result$records)){
      tmp_hols <- tmp_json$result$records %>% filter(Jurisdiction == "wa") %>%
        select(c(`Date`, `Holiday Name`))
    } else {
      tmp_hols <- tmp_json$result$records %>% filter(str_to_upper(`Applicable To`) %>% str_detect("WA|NAT")) %>%
        select(c(`Date`, `Holiday Name`))
    }
    
    hols_list[[n]] <- tmp_hols
    
    rm(tmp_response, tmp_json, tmp_hols)  
    
  }
  
  #--> Format output ####
  ##Combine list elements
  df_hols <- do.call("rbind", hols_list) %>% 
    dplyr::rename(date = Date
           ,holidayName = `Holiday Name`
    )
  
  ##Sort date field
  df_hols$date <- df_hols$date %>% lubridate::ymd()
  
  ##Check Day
  df_hols$day <- df_hols$date %>% lubridate::wday(label = T, week_start = getOption("lubridate.week.start", 1))
  
  ##Remove national-WA duplicates
  df_hols <- df_hols[!duplicated(df_hols),]
  
  rm(hols_list)
  
  # #--> [BLOCKED] Legacy method ####
  # ## IMPORT DATA
  # ## public holidays - updated up until the end of 2025
  # pub.hols <- read.csv(paste0(dirdata, "Public Holidays.csv"), stringsAsFactors = F)
  # head(pub.hols)
  # 
  # ## Restructuring data
  # ## public holidays
  # str(pub.hols)
  # pub.hols$Date <- dmy(pub.hols$Date, tz="Australia/Perth")
  # pub.hols$year <- as.factor(pub.hols$year)
  # pub.hols$month <- lubridate::month(pub.hols$month, label = T)
  #####
  return(df_hols)
}


##check wrong time
#' Function to to check hms validity for strings
#' @param input_hmsStr "HH:MM:SS"
#' Checks if each is appropriate for non-na records
#' @returns string stating error "time format" on true
func_chkTimeStr <- function(input_hmsStr, error_str="timeFormat") {
  
  #### test ####
  # input_hmsStr <- "23:01:01"
  # input_hmsStr <- "21:00:99"
  # input_hmsStr <- "24:00:00"
  # input_hmsStr <- NA      #Convert to "" prior (20240312)
  # input_hmsStr <- "24:00"
  # input_hmsStr <- "12:00" #should be good
  # input_hmsStr <- "12:012:00"
  # error_str="timeFormat"
  # input_hmsStr <- ""      #should be good
  # input_hmsStr <- "12:00"
  # input_hmsStr <- "40:52:00"
  # lapply(df$TimeLaunched, func_chkTimeStr, "timeLaunched") %>% unlist()
  # df$TimeRetrieved %>% head()
  ##########
  
  out <- ""
  
  if(is.na(input_hmsStr)){
    input_hmsStr <- ""
  }
  
  if( (input_hmsStr != "") ){
    
    ##split time into vector
    tmp <- input_hmsStr %>% str_split(":") %>% unlist()# %>% as.numeric()
    
    ##CHECK STRINGS
    if (tmp %>% str_split(":") %>% lapply(nchar) %>% unlist() %>% ">"(2) %>% sum() != 0){
      
      out <- error_str
      
    } else {
      
      ##CHECK NUMERIC VALUES  
      tmp <- tmp %>% as.numeric()
      
      ##check validity for hms separately
      if (length(tmp) == 2){
        if (sum(c(tmp[1] %>% between(0,23)
                  , tmp[2] %>% between(0,59)) %>% not()) > 0){
          out <- error_str
        }
        
      } else if (length(tmp) == 3){
        if (sum(c(tmp[1] %>% between(0,23)
                  , tmp[2:3] %>% between(0,59)) %>% not()) > 0){
          out <- error_str
        }
        
      } else {
        out <- error_str
      }
      
    }
    
    
  }
  
  out
  return(out) 
}


#' Takes a vector that are meant to be in HH:MM:SS and cleans
#' @param time_vector 
#' @returns HH:MM:SS as string
func_splitTimeString <- function(time_vector) {
  
  ####test####
  # time_vector <- head(df_miss$timemissingend)
  # time_vector <- c("40:52:00")
  ############
  
  
  # Initialize empty vectors for hours, minutes, and seconds
  hours <- minutes <- seconds <- numeric(length(time_vector))
  
  # Loop through each element in the input vector
  for (i in seq_along(time_vector)) {
    # Use a tryCatch block to handle errors gracefully
    tryCatch({
      # Split the H:M:S string into hours, minutes, and seconds
      time_parts <- strsplit(time_vector[i], ":")[[1]]
      
      # Convert hours, minutes, and seconds to numeric values
      hours[i] <- as.numeric(time_parts[1])
      minutes[i] <- as.numeric(time_parts[2])
      seconds[i] <- as.numeric(time_parts[3])
    }, error = function(e) {
      # Handle errors by setting the corresponding values to NA
      cat("Error in processing element", i, ": ", e$message, "\n")
      hours[i] <- minutes[i] <- seconds[i] <- NA
    })
  }
  
  # Create a data frame with the results
  result_df <- data.frame(Hours = hours, Minutes = minutes, Seconds = seconds) %>%
    replace(is.na(.), 0) %>% mutate_all(~if(is.numeric(.)) sprintf("%02d", .) else .)
  
  return_time_str <- unite(result_df, collapsed_col, 1:3, sep = ":") %>% pluck(1)
  
  return(return_time_str)
}


#' Function to clean COUNTING data
#' @param df count data
#' @info Susceptible to different formats when downloaded. Ensure output colnames are valid
#' @returns Error file for counts, updated df for counts
func_cleanCounts <- function(df) {
  
  #--> Test ####
  # df <- dfc_tmp
  #####
  
  #--> Establish output list for clean function #####
  #' Counts file
  #' Errors
  out_counts <- list()
  
  #--> Filter columns of interest ####
  df <-  df %>%  select(RecordNumber
                        , SiteDescription_mod
                        , ActivityDate
                        , TimeLaunched
                        , TimeRetrieved
                        , ActivityTypeCode
                        , VehicleTypeDescription_mod
  ) 
  
  
  #--> Establish erroneous data ####
  
  ##If Duplicated add error
  df$err0 <- ""
  df$err0[which(duplicated(df))] <- "errDup"
  
  ##Both records of entry and exist for same Record Number / row
  df$err1 <- ""
  df$err1[which( ((df$TimeLaunched=="") & (df$TimeRetrieved==""))
                |
                 ((df$TimeLaunched!="") & (df$TimeRetrieved!=""))
                )] <- "qtyEntries"
  
  
  ##Time Launch Formatting
  df$err2 <- lapply(df$TimeLaunched, func_chkTimeStr, "timeLaunched") %>% unlist()
  
  ##Time Retrieved Formatting
  df$err3 <- lapply(df$TimeRetrieved, func_chkTimeStr, "timeRetrieved") %>% unlist()
  
  ##Activity Date (NA or __ or 0202 type date which becomes a nonsense year)
  df$err4 <- ifelse(df$ActivityDate %>% is.na(), "dateIssue", "")
  df[which(df$ActivityDate == ""),"err4"] <- "dateIssue"
  df[which(df$ActivityDate %>% year() %>% as.character() %>% nchar() != 4),"err4"] <- "dateIssue"
  df[which(df$ActivityDate %>% year() < 1990),"err4"] <- "dateIssue"
  
  df$Error <- apply(df[,c("err0", "err1", "err2", "err3", "err4")], 1, function(row) paste(row, collapse = ";")) %>% 
    str_remove_all(START %R% one_or_more(";")) %>%
    str_replace_all(one_or_more(";"), ";") %>%
    str_remove_all(one_or_more(";") %R% END)
  
  
  out_counts[[2]] <- df %>% filter(Error != "") %>% select(-c(err0, err1, err2, err3, err4))
  
  #--> Clean valid data #####
  #' ## DTG data
  #' #' Add ":00" to times without seconds (HH:MM)
  #' #' Essentially find those that have 1 or 2 digits and added the extra :00
  #' df_counts_filt$activityTime_mod <- sub("^([0-9]{1,2}:[0-9]{1,2})$", "\\1:00", df_counts_filt$activityTime %>% as.character())
  #' ... later on...
  #' format(strptime(df_cleant$activityTime_mod, format = "%H:%M:%S"), format = "%H:%M:%S") %>% as.character()
  
  ##Clean data
  df_clean <- df %>% filter(Error == "") %>% select(-c(err0, err1, err2, err3, err4)) %>%
    mutate(TimeLaunched = na_if(TimeLaunched, "")
           , TimeRetrieved = na_if(TimeRetrieved, "")) %>%
    
    mutate(ActivityTime = coalesce(TimeLaunched, TimeRetrieved)
           # , ActivityDate = dmy(ActivityDate) ##[20240312]
           , DTG_str = paste(ActivityDate %>% as.character()
                             , ActivityTime %>% as.character()
                             , "AWST",sep=" ")
           , DTG = as.POSIXct(DTG_str, format = "%Y-%m-%d %H:%M", tz="")
           , VehicleTypeDescription = VehicleTypeDescription_mod %>% as.factor()
           , ActivityTypeCode[ActivityTypeCode != "L"] <- "R" 
    ) %>% 
    select(-c(TimeLaunched, TimeRetrieved, Error)) %>%
    dplyr::rename(date = ActivityDate
           , site = SiteDescription_mod
           , vesselType = VehicleTypeDescription_mod
           , activity = ActivityTypeCode
           , activityTime = ActivityTime) %>%
    select(site, date, vesselType, activity , activityTime) %>%
    filter(vesselType=="POWERBOAT")
  
  out_counts[[1]] <- df_clean
  
  #--> VALID DATA Checks ####
  ##Check na values (don't care about recordNum & "untrustworthy" isPrivateRamp columns in check)
  # sapply(df_clean, function(x) sum(is.na(x)))
  # df_clean <- df_counts_clean[apply(is.na(df_counts_clean), 1, sum) == 0,]

  #--> Return ####
  return(out_counts)
  #####
}


#' Function to clean OUTAGES/MISSING data
#' @param df outages/missing data
#' @info Susceptible to different formats when downloaded. Ensure output colnames are valid
#' @returns Error file for outages/missing, updated df for outages/missing
func_cleanMissing <- function(df) {
  
  #--> Test ####
  # df <- dfm_tmp %>% arrange(datemissingstart)
  # df$datemissingend[2] <- df$datemissingend[2] +1 ##add in clash
  #####
  
  #--> Establish output list for clean function #####
  #' Counts file
  #' Errors
  out_miss <- list()
  
  #--> Filter columns of interest ####
  df <-  df %>%  dplyr::rename(site = SiteDescription_mod
                        , startDate = datemissingstart
                        , endDate = datemissingend
                        , startTime = timemissingstart
                        , endTime = timemissingend
                        , recordNum = recordnumber
                        , reader = RecordedBy) %>%
    mutate(reader = reader %>% str_to_upper() %>% trimws()
           , startTime = func_splitTimeString(startTime)
           , endTime = func_splitTimeString(endTime)
           , recordNum = as.integer(recordNum))
  
  ##Modify startTime and endTime
  df$startTime_mod <- as.POSIXlt(df$startTime, format="%H:%M:%S") %>% format("%H:%M:%S") %>% as.character() %>% lapply(function(x) ifelse(is.null(x), NA, x)) %>% unlist()
  df$endTime_mod <- as.POSIXlt(df$endTime, format="%H:%M:%S") %>% format("%H:%M:%S") %>% as.character() %>% lapply(function(x) ifelse(is.null(x), NA, x)) %>% unlist()

  df <- df %>% mutate(outageStart = ymd_hms(with(., paste(startDate, startTime_mod, sep = " "))) %>% floor_date(unit = "minute")
                      , outageEnd = ymd_hms(with(., paste(endDate, endTime_mod, sep = " "))) %>% floor_date(unit = "minute")
                      , durationSecs = as.numeric(outageEnd - outageStart)
                      , durationMins = duration(durationSecs, units = "seconds"))
  
  df <- df %>% select(recordNum, site, reader, startDate, startTime, startTime_mod
                      , endDate, endTime, endTime_mod, outageStart, outageEnd
                      , durationSecs, durationMins)
  

  #--> Establish Errors to write out ####
    #--> extract df_err0 (ie no or incomplete outage data) ####
  df_err0 <- df %>% filter(is.na(outageStart) | is.na(outageEnd))
  df <- df %>% filter(!is.na(outageStart) & !is.na(outageEnd))

    #--> Establish major user-entered, erroneous data ####
  ## Check if outageStart to outageEnd intersects with any other outageStart-outageEnd

  # a <- Sys.time()
  
  tmp <- df %>% select(recordNum, outageStart, outageEnd)
  
  ## Set DT
  setDT(tmp)
  
  ## Create an empty column to store clashing record numbers
  tmp[, recordNumNew := ""]
  
    #--> [20240321 LOOP WORKING] ####
  # ## Function to check for overlap between two intervals
  # check_overlap <- function(start1, end1, start2, end2) {
  #   return(start1 <= end2 & end1 >= start2)
  # }
  # ## Loop through each row and compare with all others
  # for (i in 1:nrow(tmp)) {
  #   clashing_records <- numeric(0)  # Initialize vector to store clashing records
  #   for (j in 1:nrow(tmp)) {
  #     if (i != j) {  # Avoid self-comparison
  #       if (check_overlap(tmp$outageStart[i], tmp$outageEnd[i], tmp$outageStart[j], tmp$outageEnd[j])) {
  #         clashing_records <- c(clashing_records, tmp$recordNum[j])
  #       }
  #     }
  #   }
  #   
  #   ## Append original and clashing record numbers to recordNumNew column
  #   clashing_records <- sort(unique(c(clashing_records, tmp$recordNum[i])))
  #   tmp[i, recordNumNew := paste(clashing_records, collapse = ";")]
  # }
  
    #--> [20240322 PARTIAL VECTORIZING] ####
  # ## Initialize a matrix to store overlapping indices
  # overlap_matrix <- matrix(FALSE, nrow = nrow(tmp), ncol = nrow(tmp))
  # 
  # # Loop through each row and compare with all others
  # for (i in 1:(nrow(tmp)-1)) {
  #   for (j in (i+1):nrow(tmp)) {
  #     if (check_overlap(tmp$outageStart[i], tmp$outageEnd[i], tmp$outageStart[j], tmp$outageEnd[j])) {
  #       overlap_matrix[i, j] <- TRUE
  #       overlap_matrix[j, i] <- TRUE
  #     }
  #   }
  # }
  # 
  # # Extract clashing records and update recordNumNew column
  # for (i in 1:nrow(tmp)) {
  #   clashing_records <- c(tmp$recordNum[i], tmp$recordNum[overlap_matrix[i,]])
  #   clashing_records <- sort(unique(clashing_records))
  #   tmp[i, recordNumNew := paste(clashing_records, collapse = ";")]
  # }
    #--> [20240322 VECTORIZING] ####
  # Create an empty column to store clashing record numbers
  tmp[, recordNumNew := as.character(recordNum)]
  
  # Function to check for overlaps between all pairs of intervals
  check_overlap_matrix <- function(start, end) {
    start_mat <- matrix(rep(start, length(start)), nrow = length(start), byrow = TRUE)
    end_mat <- matrix(rep(end, length(end)), nrow = length(end), byrow = TRUE)
    overlaps <- start_mat <= t(end_mat) & end_mat >= t(start_mat)
    overlaps | t(overlaps)
  }
  
  # Compute overlap matrix
  overlap_matrix <- check_overlap_matrix(tmp$outageStart, tmp$outageEnd)
  
  # Extract clashing records and update recordNumNew column
  for (i in 1:nrow(tmp)) {
    overlapping_indices <- which(overlap_matrix[i, ])
    if (length(overlapping_indices) > 0) {
      clashing_records <- c(tmp$recordNum[i], tmp$recordNum[overlapping_indices])
      clashing_records <- sort(unique(clashing_records))
      tmp[i, recordNumNew := paste(clashing_records, collapse = ";")]
    }
  }
  
  
  # Sys.time() - a
  
  ##DONGARA EXAMPLE
  # recordNum         outageStart           outageEnd            recordNumNew
  # 1:   9040493 2016-12-01 06:54:00 2016-12-01 06:54:00                 9040493
  # 2:   9052529 2017-10-29 13:04:00 2017-10-30 13:05:00                 9052529
  # 3:   9052530 2017-11-04 15:56:00 2017-11-04 16:29:00                 9052530
  # 4:   9052531 2017-11-04 16:30:00 2017-11-04 18:17:00                 9052531
  # 5:   9052532 2017-11-04 18:18:00 2017-11-04 19:06:00                 9052532
  # ---                                                                          
  #   308:   9059249 2023-01-22 00:23:00 2023-01-22 00:25:00         9058838;9059249
  # 309:   9059251 2023-04-04 14:14:00 2023-04-04 14:45:00 9058838;9059251;9059252
  # 310:   9059252 2023-04-04 14:45:00 2023-04-04 14:48:00 9058838;9059251;9059252
  # 311:   9059253 2023-09-17 08:31:00 2023-09-17 09:29:00         9058838;9059253
  # 312:   9058848 2023-11-19 06:31:00 2022-11-19 06:39:00                 9058848
  
  # df %>% dplyr::filter(recordNum %in% c("9058838","9059251","9059252"))

  ##DONGARA DEEP DIVE
  # Issue persists for tmp[146,] as this should have enveloped nearly all others...
  # tmp[146,]
  
  df <- df %>% merge(tmp)
  rm(tmp)
  
  # # Concatenate two numerical columns by ordering and separating with ;
  # df$clash_outages <- apply(df %>% select(recordNum, intersecting_records), 1, function(row) {
  #   toString(sort(row)) %>% str_replace(",", ";") %>% 
  #     str_replace(START %R% ";", "") %>% 
  #     str_replace("; ", ";") %>%
  #     trimws()
  # })
  
  ##Holding error for below
  df$err0 <- ""
  
  ##If Clash / overlap between outages
  df$err1 <- ""
  df$err1[which(str_detect(df$recordNumNew, ";"))] <- "errClash"
  
  ##Negative durations
  df$err2 <- ""
  df$err2[which(df$durationSecs <= 0)] <- "durationIssue"
  
  ##Outage error
  df$err3 <- ""
  df$err3[which(is.na(df$outageStart) | is.na(df$outageEnd))] <- "outageError"
  
  ##Add initial raw DT error
  # Convert endTime_mod to a character vector in df_err0
  df_err0$endTime_mod <- as.character(df_err0$endTime_mod)
  
  if (dim(df_err0)[1] > 0){
    df_err0$err0 <- "rawDateTime"
  }

  ## Combine the data frames
  combined_df <- bind_rows(df, df_err0) %>%
    mutate_at(vars(err0, err1, err2, err3), ~ifelse(is.na(.), "", .))
  
  combined_df$Error <- apply(combined_df[,c("err0", "err1", "err2", "err3")], 1, function(row) paste(row, collapse = ";")) %>% 
    str_remove_all(START %R% one_or_more(";")) %>%
    str_replace_all(one_or_more(";"), ";") %>%
    str_remove_all(one_or_more(";") %R% END) 
  
  ##Write errors
  # out_miss[[2]] <- combined_df %>% filter((Error != "") | (str_detect(clash_outages,";")==TRUE)) %>% 
  #   select(-c(err0, err1, err2, err3)) %>% 
  #   mutate(clash_outages = ifelse(intersecting_records=="", "", clash_outages))
  out_miss[[2]] <- combined_df %>% arrange(desc(abs(durationMins))) %>% filter((Error != "")) %>% 
    select(-c(err0, err1, err2, err3)) 
  
  #--> Establish a working outage flat file ####
  #' Keep clashes in here as they "may" be valid and we need script to combine / aggregate
  
  ##Notes about errors
  # out_miss[[2]] %>% filter(recordNum %in% c("9001901","9057307"))#head(10)
  # ##If Clash / overlap between outages
  # df$err1 <- "errClash"
  # 
  # ##Negative durations
  # df$err2 <- "durationIssue"
  # 
  # ##Outage error
  # df$err3 <- "outageError"
  # 
  # ##Raw time Issue
  # df_err0$err0 <- "rawDateTime"

    #--> Clean valid data (except clashes) #####
  # tmp <- combined_df %>% filter(Error == "") %>% select(-c(err0, err1, err2, err3, Error, durationSecs, durationMins))
  
  tmp <- combined_df %>% filter(Error == "" | Error == "errClash") %>% select(-c(err0, err1, err2, err3, Error, durationSecs, durationMins))

  
    # #--> [LOOP Method] Need to collapse outages that encompass others to get less rows ####
  # ##Loop Method
  # # Function to check if an outage falls within the time bounds of another outage
  # check_within_interval <- function(start_i, end_i, start_j, end_j) {
  #   start_i >= start_j & end_i <= end_j
  # }
  # 
  # # Initialize logical vector to track rows to remove
  # rows_to_remove <- logical(nrow(tmp))
  # 
  # # Loop through each row
  # for (i in 1:nrow(tmp)) {
  #   start_i <- tmp$outageStart[i]
  #   end_i <- tmp$outageEnd[i]
  # 
  #   # Check if both outageStart and outageEnd are not missing
  #   if (!is.na(start_i) & !is.na(end_i)) {
  #     # Loop through all other rows to check for overlaps
  #     for (j in 1:nrow(tmp)) {
  #       if (i != j) {
  #         start_j <- tmp$outageStart[j]
  #         end_j <- tmp$outageEnd[j]
  # 
  #         # Check if both outageStart and outageEnd of row j are not missing
  #         if (!is.na(start_j) & !is.na(end_j)) {
  #           # If the outage period of row i falls within the time bounds of row j, mark row i for removal
  #           if (check_within_interval(start_i, end_i, start_j, end_j)) {
  #             rows_to_remove[i] <- TRUE
  #             break  # No need to check further, move to the next row
  #           }
  #         }
  #       }
  #     }
  #   }
  # }
  # 
  # # Remove rows marked for removal
  # tmp <- tmp[!rows_to_remove,]
    # #--> [VECTOR Method] Need to collapse outages that encompass others to get less rows ####
    ##Vector method
  # Function to check if an outage entirely encompasses the time bounds of another outage
  check_encompassed_interval <- function(start_i, end_i, start_j, end_j) {
    start_i <= start_j & end_i >= end_j
  }
  
  # Create matrix of start and end times
  start_times <- matrix(tmp$outageStart, nrow = nrow(tmp), ncol = 1)
  end_times <- matrix(tmp$outageEnd, nrow = nrow(tmp), ncol = 1)
  
  # Initialize logical vector to track rows to remove
  rows_to_remove <- logical(nrow(tmp))
  
  # Perform vectorized comparison to identify rows to remove
  for (i in 1:nrow(tmp)) {
    encompassed <- check_encompassed_interval(start_times, end_times, tmp$outageStart[i], tmp$outageEnd[i])
    rows_to_remove[i] <- any(encompassed & seq_along(encompassed) != i)
  }
  
  # Remove rows marked for removal
  tmp <- tmp[!rows_to_remove, ]
  # tmp[!rows_to_remove, ] %>% arrange(outageStart) %>% View()

  
    #--> Removing ajoining outages --> getting unique id ####
  ## Function to aggregate overlapping outages
  func_aggregateOverlapping <- function(df) {
    df %>%
      arrange(outageStart) %>%
      group_by(grp = cumsum(outageStart > lag(outageEnd + minutes(0), default = first(outageEnd)))) %>%
      summarise(outageStart = min(outageStart),
                outageEnd = max(outageEnd),
                recordNumNew = toString(recordNumNew)) %>%
      ungroup() %>%
      select(-grp)
  }
  

  ## Apply the function to aggregate overlapping outages
  df_aggregated <- func_aggregateOverlapping(tmp) %>% 
    mutate(recordNumNew2 = str_replace_all(recordNumNew, ", ", ";"))
  
  
  func_sort_numbers <- function(input_string) {
    ## Split the input string by ";"
    numbers <- unlist(strsplit(input_string, ";"))
    ## Convert to numeric and remove any non-numeric elements
    numbers <- as.numeric(numbers[!is.na(as.numeric(numbers))])
    ## Sort the numbers
    sorted_numbers <- sort(numbers)
    ## Collapse the sorted numbers into a string with ";" separator
    sorted_string <- paste(sorted_numbers, collapse = ";")
    return(sorted_string)
  }
  
  ##Fix order of numbers
  df_aggregated$recordNumNew2 <- sapply(df_aggregated$recordNumNew2, func_sort_numbers)
  
  df_aggregated <- df_aggregated %>% select(-recordNumNew) %>% 
    mutate(site = df$site[1]) %>% 
    rename(recordsCombined = recordNumNew2) %>%
    select(site, outageStart, outageEnd, recordsCombined)
  
  out_miss[[1]] <- df_aggregated
  
  #--> Return ####
  return(out_miss)
  #####
}


#' Function to imputations
#' @param input_list rds file names for count and missing data
#' @returns null csv moved to 'movedCsv', output saved as .rds
func_combineData <- function(input_list) {
  
  # #--> Test ####
  # input_list <- mergelist
  ####
  
  #--> [BLOCKED] required packages ####
  # sapply(req_packages, require, character.only = TRUE, quietly=TRUE)
  
  #--> Establish output list for combineData function #####
  #' merged data
  #' Errors
  out_merge <- list()
  
  #--> Read Data ####
  df_counts <- input_list[[1]]
  df_miss <- input_list[[2]]
  df_camera <- input_list[[3]]
  
  ##Get data from function (API)
  dfh <- func_getPublicHols() %>% 
    mutate(date = date %>% lubridate::ymd()
           , day = date %>% lubridate::wday(label = T, week_start = getOption("lubridate.week.start", 1)))
  
  
  #--> Manipulate Counts & Missing Data ####
  ##Flag South West
  dfc <- df_counts %>% 
    mutate(isSW = ifelse(site %in% c("PORT GEOGRAPHE", "CANAL ROCKS", "HAMELIN BAY", "AUGUSTA"), yes=1, no=0))
  
  rm(df_counts)
  
  ##Filter for regions and Launches or Retrievals/ Manipulate Counts
  dfc <- dfc %>% filter(case_when(isSW==1 ~ activity=="L", isSW==0 ~ activity=="R"))
  
  # ##Get date range
  # fromDate <- dfc$date %>% min()
  # toDate <- dfc$date %>% max()
  
  ##Get hour window of retrieval
  dfc$clock_hour <- dfc$activityTime %>% substr(1,2) %>% as.numeric() #%>% "+"(1)
  dfc <- dfc %>% group_by(site, date, clock_hour) %>% summarise(counts = n()) %>% ungroup()
  
  # myPalette <- colorRampPalette(rev(brewer.pal(11, "Spectral")))
  # sc <- scale_colour_gradientn(colours = myPalette(100), limits=c(1, 20))
  # ggplot(dfc %>% head(100), aes(x = date, y = clock_hour, colour = counts)) +
  #   geom_point(size = 10) +
  #   sc
  
  ## Manipulate Missing Data
  dfm <- df_miss
  # rm(df_miss)
  
  #####
  
  #### COMBINING ####
  #--> Execute datatable process for missing by hour and proportion of hour ####
  if(100==100){
    # dfm <- dfm %>% arrange(recordNum) %>% as.data.table()
    dfm <- dfm %>% arrange(outageStart) %>% as.data.table() ##HERE
    
    dfm[, startDate := as.Date(outageStart)]
    dfm[, endDate := as.Date(outageEnd)]
    # dfm[, startTime :=  format(outageStart, "%H:%M:%S")]
    # dfm[, endTime := format(outageEnd, "%H:%M:%S")]
    dfm[, outageStart := as.POSIXct(outageStart, format = "%Y-%m-%d %H:%M:%S")]
    dfm[, outageEnd := as.POSIXct(outageEnd, format = "%Y-%m-%d %H:%M:%S")]
    
    ##outage --> lists
    #' split those outages into 0--23 clock_hours for the days it encompasses
    # l1 <- split(dfm, dfm$recordNum)
    l1 <- split(dfm, dfm$recordsCombined)
    l2 <- lapply(l1, function(x) with(x, expand.grid(date = c(startDate:endDate) %>% as.Date(origin=lubridate::origin), clock_hour = 0:23))) ##Note: order gets adjusted
    
    # lapply(l2, dim) #--> good
    
    ##Need to add list name 
    df2 <- imap_dfr(l2, function(df, name) {
      result <- df
      result$recordsCombined <- name
      result
    }) %>% arrange(recordsCombined, date, clock_hour)
    
    
    ##Merge padded hour records with outages
    joined_dt <- merge(df2, dfm, all=TRUE) %>% select(-c(endDate))
    # merge(df2, dfm) %>% select(-c(endDate, reader)) %>% dim()
    # merge(df2, dfm, all=TRUE) %>% select(-c(endDate, reader)) %>% dim()
    

    ##Blow away clock_hours which don't intersect outage
    # ... done below using intersect ...
    # ...
    
    ##Calc sequential hour count for each outage 
    # joined_dt <- joined_dt %>% group_by(recordNum) %>% mutate(hr_window= seq_len(n())-1, year = year(date)) %>% ungroup()
    joined_dt <- joined_dt %>% group_by(recordsCombined) %>% mutate(hr_window= seq_len(n())-1, year = year(date)) %>% ungroup()
    joined_dt$floor_dtg <- joined_dt$outageStart %>% "+"(lubridate::hours(joined_dt$hr_window)) %>% floor_date("hour")
    joined_dt$ceiling_dtg <- joined_dt$floor_dtg %>% "+"(lubridate::hours(1))
    
    ##Filter cases where records exist for the intersection of the "hour-bin" and the "outage"
    #' This results in non-continuity between hours
    intersected_dt <- joined_dt %>% filter( !is.na(lubridate::intersect( interval(floor_dtg, ceiling_dtg), interval(outageStart, outageEnd))) 
                                            # , between(floor_dtg, outageStart, outageEnd)
                                            , (outageEnd != floor_dtg)
    )


    ##Update clock time
    intersected_dt$clock_hour <- intersected_dt$floor_dtg %>% hour()
  
  
    ##Get proportion of hourly outage
    dfmmod <- intersected_dt %>%
      mutate(pct_out = lubridate::intersect(interval(outageStart, outageEnd)
                                            ,interval(floor_dtg, ceiling_dtg)) %>%
               lubridate::as.duration() %>% as.numeric() %>% "/"(60*60) %>% round(4)
      ) %>%
      mutate(datefloor = as.Date(floor_dtg))
  
  } #end trigger_if
  
  rm(df2, l1, l2, joined_dt, intersected_dt, dfm)
  
  #--> Grouping separate outages within same hour --> sum(pct_out) ####
  # dfmmod_gpd <- dfmmod %>% group_by(site, datefloor, clock_hour, ceiling_dtg) %>% 
  #   summarise(pct_out = sum(pct_out), recordNum = paste(recordNum, collapse=";")) %>% 
  #   ungroup() %>% 
  #   arrange(site, datefloor, clock_hour)
  dfmmod_gpd <- dfmmod %>% group_by(site, datefloor, clock_hour, ceiling_dtg) %>% 
    summarise(pct_out = sum(pct_out), recordsCombined = paste(recordsCombined, collapse=";")) %>% 
    ungroup() %>% 
    arrange(site, datefloor, clock_hour)
  
  rm(dfmmod)
  
  # dfmmod_gpd[dfmmod_gpd$pct_out > 1,]
  # dfmmod_gpd$pct_out %>% hist()
  
  #--> [MERGE] Combine Count with missing data records ####
  #' merge permits outage data to create new rows of records
  
  ##Merge outages and counts
  #' we need to keep the outages present prior to adding counts
  dfout <- merge(x = dfc, y = dfmmod_gpd %>% select(site, datefloor, clock_hour, pct_out, recordsCombined)
                 , by.x = c("site", "date", "clock_hour")
                 , by.y = c("site", "datefloor", "clock_hour")
                 , all.x=TRUE, all.y=TRUE)# %>%
  
  df_inputs <- merge(x = dfout, y = df_camera, by = c("site", "date"), all.x=TRUE, all.y=TRUE ) %>%
    arrange(site, date, clock_hour) %>% filter(complete.cases(clock_hour))
  ##[STEVE] 20240212 revisit why these cases are not complete...
  
  rm(dfc, dfmmod_gpd, df_camera)

  
  #--> Report on Errors in Data ####
  ## Assign errors to output list
  #' Counts file
  #' Errors
  out_merge[[2]] <- dfout %>% filter(!is.na(counts), pct_out==1) %>% select(site, date, recordsCombined) %>% 
    mutate(year = year(date)
           ,month = month(date)
           ,Error = "OutagewithCounts") %>%
    select(-date) %>% distinct()
  
  
  #--> Merged data pad out with hours and dates from min to max ####
  #' (i)   Pad out merged data from earliest day to latest day
  #' (ii) Remove unnecessary columns
  
  dtRange <- seq(from = min(df_inputs$date), to = max(df_inputs$date), by='days')
  hrRange <- 0:23
  
  ##Establish hour-day df
  datetime <- merge(dtRange, chron(time = paste(hrRange, ':', 0, ':', 0)))
  colnames(datetime) <- c('date', 'time')
  datetime$clock_hour <- datetime$time %>% substr(1,2) %>% as.numeric()
  rm(dtRange, hrRange)

  ##Merge outputs with date-times
  df_inputs_dt_merge <- merge(datetime, df_inputs, by = c("date", "clock_hour"), all.x=TRUE, all.y=TRUE) %>% arrange(date, clock_hour)
  
  # ## TESTING DIFFERENT DF SIZES
  # # Find rows in df_inputs_dt_merge that are not in datetime
  # not_common_df1 <- df_inputs_dt_merge %>% select(date, time, clock_hour) %>%
  #   anti_join(datetime, by = c("date", "time", "clock_hour"))
  # 
  # # Find rows in datetime that are not in df_inputs_dt_merge
  # not_common_df2 <- datetime %>%
  #   anti_join(df_inputs_dt_merge %>% select(date, time, clock_hour) , by = c("date", "time", "clock_hour"))
  # 
  # # Combine the results
  # bind_rows(not_common_df1, not_common_df2)
  
  ##Add in day, dayType and site
  df_inputs_dt_merge$day <- df_inputs_dt_merge$date %>% lubridate::wday(label=TRUE)
  df_inputs_dt_merge$dayType <- as.factor(ifelse(df_inputs_dt_merge$date %in% dfh$date, "PubHol",
                                       ifelse(df_inputs_dt_merge$day == "Sat" | df_inputs_dt_merge$day == "Sun", "WEnd", "WDay")))

  ##Select columns of interest  
  df_processed <- df_inputs_dt_merge %>% dplyr::select(date, clock_hour, site, day, dayType, counts, pct_out) %>% drop_na(site)
  #[STEVE] 20240212 This output has NA day-hour records to completeness. These can be removed by later users

  #--> Clean up ####
  rm(dfout)
  gc()
  
  #--> Send out ####
  #' df_inputs: site x date x recorded clock hours of counts AND percentages of hourly outage
  #' store: list of site elements, Hourly record from start dtg to end dtg to assign counts to 
  out_merge[[1]] <- df_processed
  
  return(out_merge)
  #####
  
}


#' Function to take site data (COUNT, OUTAGES/MISSING and READ)
#' @param input_list list of dataframes, source and functions
#' @info 
#' @returns Pending...
func_wrangling_parallel <- function(input_list) {
  
  # #--> Test ####
  # input_list <- par.list[[1]] #AUGUSTA
  # input_list <- par.list[[7]] #DENHAM
  # input_list <- par.list[[8]] #DONGARA
  # input_list <- par.list[[23]] #WOODMAN
  # input_list %>% str()
  # #####
  
  #### SET UP ####
  #--> Assign data ####
  dfc_tmp                <- input_list[[1]]
  dfm_tmp                <- input_list[[2]]
  dfcr_tmp               <- input_list[[3]]
  req_packages           <- input_list[[4]]
  req_functions          <- input_list[[5]]

  #--> required packages ####
  sapply(req_packages, require, character.only = TRUE, quietly=TRUE)
  
  #--> required functions ####
  source(req_functions)
  #####
  
  #### COUNTS ####
  lc <- dfc_tmp %>% func_cleanCounts()
  # lc[[2]] #--> errors for count
  # lc[[1]] #--> cleaned count data

  #### OUTAGES / MISSING ####
  lout <- dfm_tmp %>% func_cleanMissing()
  # lout[[2]] #--> errors for missing/outages
  # lout[[1]] #--> cleaned missing/outage data

  #### CAMERAS READ ####
  dfcr_tmp <- dfcr_tmp %>% dplyr::rename(site=SiteDescription_mod) %>% select(-ErrSite)
  
  #### MERGE ####
  mergelist <- list(lc[[1]], lout[[1]], dfcr_tmp)
  rm(dfc_tmp, dfm_tmp, dfcr_tmp)
  
  lmerge <- mergelist %>% func_combineData()
  rm(mergelist)
  
  ##Test
  # lmerge[[1]]$pct_out %>% range(na.rm = TRUE)
  
  ##### RETURN / SAVE ####
  #--> Write errors in sites for Wrangling process ####
  # Create a new Excel workbook
  wb <- createWorkbook()
  
  # Add the first dataframe to the workbook
  addWorksheet(wb, sheetName = "wrangled_Counts")
  writeData(wb, sheet = "wrangled_Counts", x = lc[[2]])
  
  # Add the second dataframe to the workbook
  addWorksheet(wb, sheetName = "wrangled_Outages")
  writeData(wb, sheet = "wrangled_Outages", x = lout[[2]])
  
  # Add the second dataframe to the workbook
  addWorksheet(wb, sheetName = "wrangled_Merged")
  writeData(wb, sheet = "wrangled_Merged", x = lmerge[[2]])
  
  # Specify the Excel file path
  excel_file <- paste0(input_list[[6]],"/", Sys.Date() %>% str_remove_all("-"),"_",input_list[[1]]$SiteDescription_mod[1], "_wrangled_errors.xlsx")

  # Save the workbook to an Excel file
  saveWorkbook(wb, file = excel_file, overwrite = TRUE)
  
  # Print a message indicating successful write
  cat("Wrangled Errors for Counts, Outages and Merging written to Excel file:", excel_file, "\n")
  
  
  #--> Save output data for site ####
  saveRDS(lmerge[[1]], file = paste0(input_list[[6]],"/tmpWrangled/", input_list[[1]]$SiteDescription_mod[1], "_wrangled.rds", sep=""))
  

  #####
  
  return(paste("Done:", input_list[[1]]$SiteDescription_mod[1]))
  #####
  
} ##end // function


#' Function to clean COUNTING data
#' @param row_vector with a parameter labelled 'date'
#' @info looks at legacy closure dates (hard coded within) and assigns fields
#' @returns completed row_vector with rpt.6, season.fish, rpt.period = NA, date.from, date.to
func_applyLegacySeasons <- function(row_vector){
  
  #### TEST ####
  # row_vector <-  df[1,]
  # row_vector <- tmp[n, ]
  # row_vector 
  # row_vector <-  df[4000,] %>% as.vector()
  # row_vector %>% as.vector()
  
  # row_vector <- df[3211,]
  
  ##############
  
  # row_vector <- row_vector %>% as.vector()
  
  # Convert date string to Date object
  row_vector$date <- as.Date(row_vector$date)
  
  # # Define date ranges (without specific years) and corresponding rpt.6 values
  # date_ranges <- list(
  #   c(parse_date("10-15"), parse_date("12-15")),  # 15 Oct to 15 Dec --> 5 (closed)
  #   c(parse_date("12-16"), parse_date("01-31")),  # 16 Dec to 31 Jan --> 6 (open)
  #   c(parse_date("02-01"), parse_date("03-31")),  # 01 Feb to 31 Mar --> 1 (open)
  #   c(parse_date("04-01"), parse_date("07-31")),  # 1 Apr to 31 Jul  --> 2 (open)
  #   c(parse_date("08-01"), parse_date("09-22")),  # 1 Aug to 22 Sep  --> 3 (open)
  #   c(parse_date("09-23"), parse_date("10-14"))   # 23 Sep to 14 Oct --> 4 (open)
  # )

  
  ##Assign start year (based on data value)
  if ( ( (month(row_vector$date) == 10) && (day(row_vector$date) >= 15)) | (month(row_vector$date) > 10)   ){
    use_year <- row_vector$date %>% year() 
  } else {
    use_year <- row_vector$date %>% year() %>% "-"(1)
  }


  row_vector$date

  
  ##Function to parse dates with new year
  parse_date <- function(date_str) {
    as.Date(date_str, format="%d-%m-%Y")
  }
  
  ##Year based legacy windows
  date_ranges <- data.frame(
    date.from   = parse_date(c("15-10", "16-12", "01-02", "01-04", "01-08", "23-09") %>% paste0(paste0("-", rep(use_year, 6) + c(0,0,1,1,1,1)))),
    date.to     = parse_date(c("15-12", "31-01", "31-03", "31-07", "22-09", "14-10") %>% paste0(paste0("-", rep(use_year, 6) + c(0,1,1,1,1,1)))),
    rpt.6 = c(5, 6, 1, 2, 3, 4),
    season.fish = c("closed", "open", "open", "open", "open", "open")
  ) 
  
  
  ## Initialize rpt.6, season.fish, date.from, and date.to columns
  row_vector$rpt.6 <- NA
  row_vector$season.fish <- NA
  row_vector$date.from <- as.Date(NA)
  row_vector$date.to <- as.Date(NA)
  
  ##Assign unknown <-- legacy values
  ##return vector
  # row_vector[c("date.from", "date.to", "rpt.6", "season.fish")] <- date_ranges[(row_vector$date >= date_ranges$date.from) & (row_vector$date <= date_ranges$date.to),]
  
  ##return dataframe single row
  row_vector[1, c("date.from", "date.to", "rpt.6", "season.fish")] <- date_ranges[(row_vector$date >= date_ranges$date.from) & (row_vector$date <= date_ranges$date.to),]
  
  
  return(row_vector)
}


#' Function to assign groupings v1
#' @param df dataframe with temporal data labelled 'date'
#' @param dfs_path full file path to WCDFisgingSeasons.csv
#' @info dfs .csv expected to have columns: start(date), end(date), reporting period(int), reporting quarter(int), season(chr)
#' @returns parent df with columns:
func_assignWCDSeason <- function(df, dfs_path, incl.dates=TRUE) {
  
  # #--> Test ####
  # df <- ldf[[4]]
  # df <- dfr
  # dfs_path <- dir("01_data", full.names = TRUE)[dir("01_data/") %>% str_detect("FishingSeasons")]
  # incl.dates <- TRUE
  # #####
  
  #--> read in csv #####
  ## Read data from Sheet1
  dfs_oc <- read_excel(dfs_path, sheet = "OpenClosed") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  
  ## Read data from Sheet2
  dfs_survey <- read_excel(dfs_path, sheet = "Surveys") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  
  
  #--> Initialise (for Open Closed) and assign date metrics #####
  df <- df %>% mutate(
    year = date %>% lubridate::year(),
    month = date %>% lubridate::month(),
    year.fin = case_when(
      month > 6 ~ paste0(str_sub(as.character(year),3,4), str_sub(as.character(year+1),3,4)),
      month <= 6 ~ paste0(str_sub(as.character(year-1),3,4), str_sub(as.character(year),3,4))
    ),
    season.climate = case_when(
      month %in% c(12,1,2) ~ "SUMMER",
      month %in% c(3,4,5) ~ "AUTUMN",
      month %in% c(6,7,8) ~ "WINTER",
      month %in% c(9,10,11) ~ "SPRING"
    ),
    season.fish = NA,
    rpt.6 = NA,
    rpt.period = NA
  )
  
  
  ## Create Date From/To if necessary
  if (incl.dates){
    df$date.from <- as.Date(NA)
    df$date.to <- as.Date(NA)
  }
  
  #--> Assign Data from Table (Uncommon Seasons) ####
  for (i in 1:nrow(dfs_oc)) {
    ## Identify rows in df that match the condition
    matching_rows <- df$date >= dfs_oc$start_date[i] & df$date <= dfs_oc$end_date[i]
    
    ## Assign the corresponding season to matching rows in df
    df$season.fish[matching_rows] <- dfs_oc$fish_season[i]
    df$rpt.6[matching_rows] <- dfs_oc$rpt_6[i]
    df$rpt.period[matching_rows] <- dfs_oc$rpt_period[i]
    
    if(incl.dates){
      df$date.from[matching_rows] <- dfs_oc$start_date[i] %>% as.Date()
      df$date.to[matching_rows] <- dfs_oc$end_date[i] %>% as.Date()
    }
    
  }
  
  #--> Assign Data from Table (Legacy Seasons) ####
  ## Find rows with NA values in rpt.6 column
  legacy_rows <- is.na(df$rpt.6) %>% which()
  
  for (n in legacy_rows){
    df[n,] <- df[n, ] %>% func_applyLegacySeasons()
  }
  
  #--> Add ISurvey Year ####
  df$survey <- NA
  df <- df %>%
    mutate(survey = sapply(df$date, function(x) {
      survey_rows <- dfs_survey %>%
        filter(start_date <= x & x <= end_date)
      
      if (nrow(survey_rows) > 0) {
        unlist(map(survey_rows$survey, as.character)) %>% 
          paste(collapse = ';')
      } else {
        NA_character_
      }
    }))
  
  #--> Return ####
  return(df)
  #####
}


#' Function to assign groupings v1 --> PARALLEL
#' @param df dataframe with temporal data labelled 'date'
#' @param dfs.path full file path to WCDFisgingSeasons.csv
#' @param incl.dates creates fields for date.from and date.to and initialises as NA 
#' @param run.parallel bypasses the loop process to apply the function across multiple cores
#' @param required.packages A character vector of library/packages 
#' @info dfs .csv expected to have columns: start(date), end(date), reporting period(int), reporting quarter(int), season(chr)
#' @returns parent df with columns:
func_assignWCDSeason_parallel <- function(df, dfs.path, incl.dates=TRUE, run.parallel = TRUE, required.packages = req_packages) {
  
  # # #--> Test ####
  # # # df <- ldf[[4]]
  # df <- df %>% filter(site=="HILLARYS")
  # dfs.path <- dir("01_data/", full.names = TRUE)[dir("01_data/") %>% str_detect("FishingSeasons")]
  # incl.dates <- TRUE
  # run.parallel = TRUE
  # required.packages <- req_packages
  # # #####
  
  #--> read in csv #####
  
  ## Read data from Sheet1
  dfs_oc <- read_excel(dfs.path, sheet = "OpenClosed") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  
  ## Read data from Sheet2
  dfs_survey <- read_excel(dfs.path, sheet = "Surveys") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  
  #--> Initialise (for Open Closed) and assign date metrics #####
  df <- df %>% mutate(
    year = date %>% lubridate::year(),
    month = date %>% lubridate::month(),
    year.fin = case_when(
      month > 6 ~ paste0(str_sub(as.character(year),3,4), str_sub(as.character(year+1),3,4)),
      month <= 6 ~ paste0(str_sub(as.character(year-1),3,4), str_sub(as.character(year),3,4))
    ),
    season.climate = case_when(
      month %in% c(12,1,2) ~ "SUMMER",
      month %in% c(3,4,5) ~ "AUTUMN",
      month %in% c(6,7,8) ~ "WINTER",
      month %in% c(9,10,11) ~ "SPRING"
    ),
    season.fish = NA,
    rpt.6 = NA,
    rpt.period = NA
  )
  
  
  ## Create Date From/To if necessary
  if (incl.dates){
    df$date.from <- as.Date(NA)
    df$date.to <- as.Date(NA)
  }
  
  #--> Assign Data from Table (Uncommon Seasons) ####
  for (i in 1:nrow(dfs_oc)) {
    ## Identify rows in df that match the condition
    matching_rows <- df$date >= dfs_oc$start_date[i] & df$date <= dfs_oc$end_date[i]
    
    ## Assign the corresponding season to matching rows in df
    df$season.fish[matching_rows] <- dfs_oc$fish_season[i]
    df$rpt.6[matching_rows] <- dfs_oc$rpt_6[i]
    df$rpt.period[matching_rows] <- dfs_oc$rpt_period[i]
    
    if(incl.dates){
      df$date.from[matching_rows] <- dfs_oc$start_date[i] %>% as.Date()
      df$date.to[matching_rows] <- dfs_oc$end_date[i] %>% as.Date()
    }
    
  }
  
  #--> Assign Data from Table (Legacy Seasons) ####
  #' This is for the older historical datasets which need to be labelled iaw new groupings
  ## Find rows with NA values in rpt.6 column
  legacy_rows <- which(is.na(df$rpt.6))
  
  if (run.parallel){
    
    ## Extract elements from ldf based on legacy_rows indices
    ldf <- split(df, seq(nrow(df)))
    legacy_ldf <- ldf[legacy_rows]
    
    ## Create a cluster with specified number of cores
    cl <- makeCluster(detectCores()-2)
    # clusterExport(cl, c("required.packages", "func_applyLegacySeasons"))
    clusterExport(cl, c("func_applyLegacySeasons"), envir = .GlobalEnv)
    
    apply_legacy_seasons <- function(ldf_element, required.packages) {
      ## Load required packages
      sapply(required.packages, require, character.only = TRUE)
      
      ## Apply func_applyLegacySeasons
      func_applyLegacySeasons(ldf_element)
    }
    
    
    ## Apply apply_legacy_seasons to each element of legacy_ldf in parallel
    legacy_results <- parLapply(cl, legacy_ldf, apply_legacy_seasons, required.packages)
    
    ## Stop the cluster
    stopCluster(cl)
    rm(cl)
    
    ## Convert the list of data frames into a single data.table
    combined_legacy_results <- data.table::rbindlist(legacy_results, fill = TRUE)
    
    ## Assign the combined results back to the corresponding rows in the original dataframe df
    df[legacy_rows, ] <- combined_legacy_results
    
    ## Clean
    rm(combined_legacy_results, legacy_results, legacy_ldf, ldf)
    
    gc()
    
  } else {
    ##[PRE20240319 Solution]
    for (n in legacy_rows){
      df[n,] <- df[n, ] %>% func_applyLegacySeasons()
    }
    
  }
  
  gc()
  
  #--> Add ISurvey Year ####
  df$survey <- NA
  df <- df %>%
    mutate(survey = sapply(df$date, function(x) {
      survey_rows <- dfs_survey %>%
        filter(start_date <= x & x <= end_date)
      
      if (nrow(survey_rows) > 0) {
        unlist(map(survey_rows$survey, as.character)) %>% 
          paste(collapse = ';')
      } else {
        NA_character_
      }
    }))
  
  #--> Return ####
  return(df)
  #####
}


#' Function to assign groupings v2 --> DATA TABLE
#' @param df dataframe with temporal data labelled 'date'
#' @param dfs_path full file path to WCDFisgingSeasons.csv
#' @param isRamps boolean TRUE = inclde all ramps around state, FALSE = West Coast
#' @info dfs .csv expected to have columns: start(date), end(date), reporting period(int), reporting quarter(int), season(chr)
#' @returns parent df with columns:
func_assignWCDSeason_dt <- function(df, dfs.path, isRamps, incl.dates=TRUE) {
  
  ####TEST####
  # dfs.path <- dir("01_data/", full.names = TRUE)[dir("01_data/") %>% str_detect("FishingSeasons")]
  # df <- df
  # isRamps <- TRUE
  # incl.dates <- TRUE
  # <-"Survey_encompassed_for_ramp"
  # use_sheet <- "Surveys"
  ############

  #--> Read in necessary files ####
  setDT(df)
  
  # # Read data from Excel Sheets
  # dfs_oc <- fread(dfs.path, select = c("start_date", "end_date", "fish_season", "rpt_6", "rpt_period"), key="start_date")
  # dfs_survey <- fread(dfs.path, sheet = "Surveys", select = c("start_date", "end_date", "survey"), key="start_date")
  
  ## Read data from Sheet1
  dfs_oc <- read_excel(dfs.path, sheet = "OpenClosed") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  
  ## Read data from Sheet2
  if (isRamps){
    # dfs_survey <- read_excel(dfs.path, sheet = "Survey_encompassed_for_ramp") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
    dfs_survey <- read_excel(dfs.path, sheet = "Surveys") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  } else {
    dfs_survey <- read_excel(dfs.path, sheet = "Surveys") %>% mutate(start_date = as.Date(start_date, format="%Y-%m-%d"), end_date = as.Date(end_date, format="%Y-%m-%d"))
  }

  ## Convert dfs_oc and dfs_survey to data.table
  setDT(dfs_oc)
  setDT(dfs_survey)
  
  #--> Sort RECENT data ####
  ## Convert to Date format
  dfs_oc[, c("start_date", "end_date") := lapply(.SD, as.Date, format="%Y-%m-%d"), .SDcols = c("start_date", "end_date")]
  dfs_survey[, c("start_date", "end_date") := lapply(.SD, as.Date, format="%Y-%m-%d"), .SDcols = c("start_date", "end_date")]
  
  ## Initialize columns
  df[, c("year", "month", "year.fin", "season.climate", "season.fish", "rpt.6", "rpt.period") := .(lubridate::year(date), lubridate::month(date), ifelse(lubridate::month(date) > 6, paste0(substr(lubridate::year(date), 3, 4), substr(lubridate::year(date) + 1, 3, 4)), paste0(substr(lubridate::year(date) - 1, 3, 4), substr(lubridate::year(date), 3, 4))), 
                                                                                                   case_when(lubridate::month(date) %in% c(12,1,2) ~ "SUMMER",
                                                                                                             lubridate::month(date) %in% c(3,4,5) ~ "AUTUMN",
                                                                                                             lubridate::month(date) %in% c(6,7,8) ~ "WINTER",
                                                                                                             lubridate::month(date) %in% c(9,10,11) ~ "SPRING"),
                                                                                                   NA, NA, NA)]
  ## Create Date From/To if necessary
  if (incl.dates) {
    df[, c("date.from", "date.to") := .(as.Date(NA), as.Date(NA))]
  }
  
  ## Mis match between df and dfs_oc
  # Convert "season.fish" to character in df
  df[, season.fish := as.character(season.fish)]
  
  ## Convert "rpt.6" and "rpt.period" to numeric in df
  df[, c("rpt.6", "rpt.period") := .(
    as.numeric(as.character(rpt.6)),
    as.numeric(as.character(rpt.period))
  )]
  
  ## Assign Data from Table (Uncommon Seasons)
  df[dfs_oc, c("season.fish", "rpt.6", "rpt.period") := .(as.character(i.fish_season), as.numeric(i.rpt_6), as.numeric(i.rpt_period)), on = .(date >= start_date, date <= end_date)]
  if (incl.dates) {
    df[dfs_oc, c("date.from", "date.to") := .(i.start_date, i.end_date), on = .(date >= start_date, date <= end_date)]
  }
  
  ## Assign Data from Table (Legacy Seasons)
  rownames(df) <- c()
  legacy_rows <- df$rpt.6 %>% is.na() %>% which()
  legacy_df <- df[legacy_rows]
  # recent_df <- df[!legacy_rows]
  
  ##Check dimensions
  # dim(df)[1] == (dim(legacy_df)[1] + dim(recent_df)[1])
  
  #--> LEGACY records ####
  
  ## Define date ranges (with specific years) and corresponding rpt.6 values
  date_ranges <- data.table(
    date.from = as.Date(c("10-15", "12-16", "02-01", "04-01", "08-01", "09-23"), format = "%m-%d"),
    date.to = as.Date(c("12-15", "01-31", "03-31", "07-31", "09-22", "10-14"), format = "%m-%d"),
    year.from.mod = c(1, 1, 0, 0, 0, 0),
    year.to.mod = c(1, 0, 0, 0, 0, 0),
    rpt.6 = c(5, 6, 1, 2, 3, 4),
    season.fish = c("closed", "open", "open", "open", "open", "open")
  )
  
  # ##We're trying to create a summy year variable (too difficult to solely compare month and day ranges)
  date_ranges$date.from <- date_ranges$date.from - lubridate::years(date_ranges$year.from.mod)
  date_ranges$date.to <- date_ranges$date.to - lubridate::years(date_ranges$year.to.mod)
  
  ## Copy the date column to tmp_date
  legacy_df[, tmp_date := date]
  
  ## Change the year to 2024 in tmp_date
  legacy_df[, tmp_date := as.Date(format(tmp_date, "%m-%d-2024"), format = "%m-%d-%Y")]
  
  ## Check if tmp_date is greater than or equal to October 15th
  legacy_df[, tmp_date := as.Date(ifelse(as.Date(tmp_date) >= as.Date(paste0(date_ranges$date.to %>% tail(1) %>% year(), "-10-15")), 
                                         format(tmp_date, paste0("%m-%d-",date_ranges$date.to %>% tail(1) %>% year() %>% "-"(1))), 
                                         format(tmp_date, paste0("%m-%d-",date_ranges$date.to %>% tail(1) %>% year()))), 
                                  format = "%m-%d-%Y")]
  
  ## Joining based on tmp_date falling between date.from and date.to
  legacy_df[date_ranges, 
            `:=`(rpt.6 = i.rpt.6, 
                 season.fish = i.season.fish), 
            on = .(tmp_date >= date.from, tmp_date <= date.to)]
  
  ## Remove the tmp_date column
  legacy_df[, tmp_date := NULL]
  #--> Combined RECENT with LEGACY ####
  ## Merge df and legacy_df
  combined_df <- rbind(df[!legacy_rows], legacy_df)
  
  # combined_df$rpt.6 %>% table(useNA = "always")
  
  #--> Add ISurvey Year ####
  combined_df[, survey := NA_character_]
  
  ## [SUPER FAST] Set keys for non-equi join
  #' but doesn't permit case if multiple surveys are overlapping
  setkey(dfs_survey, start_date, end_date)
  combined_df[, survey := dfs_survey[.SD, on = .(start_date <= date, end_date >= date), mult = "first", survey]]
  
  ## [SLOWER] Permits combined surveys though
  # combined_df[, survey := lapply(date, function(x) {
  #   survey_rows <- dfs_survey[start_date <= x & x <= end_date]
  #   if (nrow(survey_rows) > 0) {
  #     paste(survey_rows$survey, collapse = ';')
  #     } else {
  #     NA_character_
  #     }
  #   })]
  
  #--> Fix rpt.6.name for SADA conventions ####
  #' is missing date.from and date.to
  #' Make this standard categorical groupings aligned with rpt.6  
  # xtabs(~ rpt.6 + date.from, data = combined_df) %>% View()
  
  ## Initialize rpt.6.name column
  combined_df[, rpt.6.name := NA_character_]
  
  ## Assign values based on rpt.6 values
  combined_df[rpt.6 == 1, rpt.6.name := "1 Feb-31 Mar"]
  combined_df[rpt.6 == 2, rpt.6.name := "1 Apr-31 Jul"]
  combined_df[rpt.6 == 3, rpt.6.name := "1 Aug-20 Sep"]
  combined_df[rpt.6 == 4, rpt.6.name := "21 Sep-6 Oct"]
  combined_df[rpt.6 == 5, rpt.6.name := "7 Oct-15 Dec"]
  combined_df[rpt.6 == 6, rpt.6.name := "16 Dec-31 Jan"]
  
  ##Order factore
  combined_df$rpt.6.name <- combined_df$rpt.6.name %>% factor(levels=c("1 Feb-31 Mar", "1 Apr-31 Jul"
                                                                       , "1 Aug-20 Sep", "21 Sep-6 Oct"
                                                                       , "7 Oct-15 Dec", "16 Dec-31 Jan"))
  
  #--> Drop multiple columns ####
  combined_df[, c("date.from", "date.to", "rpt.period") := NULL]
  
  return(combined_df)
  
}


#' Function for implementing GAMS for models
#' Tests AIC and updates knots for set smoothing terms
#' @param par_data_list data frame with temporal data labelled 'date'
#' @info input list has model.name, model.terms, dataframe and seed 
#' @returns list with models and scores
GAM_k_cycle <- function(par_data_list, lhs_param = "counts_soh"){
  
  
  #### TEST ####
  # ii<-3
  # par_data_list <- par.list[[ii]]
  ##############
  
  
  ## ASSIGN FUNCTION INPUTS TO VARIABLES
  model.name <- par_data_list[[1]]
  elemsModel <- par_data_list[[2]]
  data.used <- par_data_list[[3]]
  chooseSeed <- par_data_list[[4]]
  
  
  ###STEVE mod on 20230604
  model.terms <- elemsModel
  
  ## INITIAL MODEL SETUP
  kValTooLow <- TRUE
  k_AIC_store <- list()
  k_counter <- 0
  Base_mm_store <- list() #what is this?
  
  ## REPEATING LOOP TO ENSURE K IS SATISFIED
  while(kValTooLow == TRUE){
    k_counter <- k_counter + 1
    #--> Run model ####
    
    print(paste0(lhs_param, " ~ ", paste(elemsModel, collapse= " + "),sep=""))
    
    ##SB TODO Needs a catch block with knots sometimes being too low? Model 4 Depth
    
    ##redundant upper check
    if (str_detect(model.terms, "bs='cc'") %>% sum() > 0){
      set.seed(chooseSeed)
      mod <- mgcv::gam(as.formula(paste(paste0(lhs_param, " ~ "), paste(elemsModel, collapse = " + ")))
                       # mod <- mgcv::gam(as.formula(baseModel)
                       , data = data.used, family=poisson()
                       , select=TRUE
                       , method = "REML"
                       , control = gam.control(nthreads=6)
                       , knots = list(month = c(1,12), clock_hour = c(1,23))
      )
    } else { #no cc knots require specification
      set.seed(chooseSeed)
      mod <- mgcv::gam(as.formula(paste(paste0(lhs_param, " ~ "), paste(elemsModel, collapse = " + ")))
                       # mod <- mgcv::gam(as.formula(baseModel)
                       , data = data.used, family=poisson()
                       , select=TRUE
                       , method = "REML"
                       , control = gam.control(nthreads=6)
      )
    }
    
    print(paste("k_counter: ", k_counter, sep=""))
    
    
    # ####### TEST MOD #######
    # mod$coefficients
    # mod %>% summary()
    # 
    # # yr_boolean <- out[[length(out)]][[1]]$coefficients %>% names() %>% str_detect("Year")
    # # yr_boolean
    # # est <- summary(out[[length(out)]][[1]])[1] %>% pluck(1)
    # # est <- est[yr_boolean]
    # # est
    # # se <- summary(out[[length(out)]][[1]])[2] %>% pluck(1)
    # # se <- se[yr_boolean]
    # # se
    # ##### END TEST ####
    
    ##If there are NO k terms at all (2022-10-13)
    if (str_detect(model.terms, "k=") %>% sum() > 0){
      
      ##Repeat k.check process 10 times to have multiple samples.
      ##Take params if they fail 7+ times and improve their k
      tempstore <-list()
      for (ii in 1:10){
        #--> Identify k values which need to be increased ####
        f <- function(b, k.sample = 5000, k.rep = 800) {
          capture.output(printCoefmat(mgcv:::k.check(b, subsample = k.sample, n.rep = k.rep), digits = 3))
        }
        basis <- f(mod)
        tempstore[[ii]] <- basis
        print(paste("ii: ", ii))
      }
      
      #Checking specific Base model and updating Model
      kToCorrect_qty <- list()
      for (pp in 1:length(tempstore)){
        temp <- tempstore[[pp]][str_detect(tempstore[[pp]], pattern = START %R% c("s|ti|te") %R% "\\(")]
        temp <- sapply(temp, str_split, pattern=" ")
        storename <- c()
        storescore <- c()
        
        for (xx in 1:length(temp)){
          storename[xx] <- temp[[xx]][1] %>% str_remove(pattern = c("s\\(|ti\\(|te\\(") ) %>% str_remove(pattern = "\\)")
          storescore[xx] <- as.numeric(str_remove(temp[[xx]][str_length(temp[[xx]]) > 0][5],"<")) < 0.05
        }
        if (length(storename[storescore])>0){
          kToCorrect_qty[[pp]] <- storename[storescore]
        } else {
          kToCorrect_qty[[pp]] <- ""
        }
        print(paste("pp: ", pp))
      }
      kToCorrect <- plyr::count(unlist(kToCorrect_qty))$x[plyr::count(unlist(kToCorrect_qty))$freq >= 7]
      kToCorrect <- kToCorrect[str_length(kToCorrect) != 0]
      
    } else {
      kToCorrect <- NULL 
    }#NEW END
    
    
    ##stored minor info for k and AIC
    k_AIC_store[[k_counter]] <- c(elemsModel, AIC(mod))
    
    ##Stored info
    results <- list()
    results[[1]] <- mod         #Model just executed (will be overwritten in list)
    results[[2]] <- elemsModel  #Model just executed (will be overwritten in list)
    results[[3]] <- AIC(mod)    #Final AIC
    results[[4]] <- k_AIC_store #Model used and AIC
    
    print(elemsModel)
    print(AIC(mod))
    # print("###################")
    
    if (length(kToCorrect) > 0){
      for (yy in 1:length(kToCorrect)){
        smoothToCorrect <- elemsModel[which(str_detect(elemsModel, kToCorrect[yy]))]
        keqVal <- smoothToCorrect %>% str_split(",") %>% unlist() %>% tail(1) %>% str_remove(pattern = "\\)")
        knum <- keqVal %>% str_remove(pattern = "k=")
        keqValNEW <- paste0("k=", ceiling(1.5*as.numeric(knum)),"") #floor(2*as.numeric(knum)),"")
        elemsModel[which(str_detect(elemsModel, kToCorrect[yy]))] <- str_replace(elemsModel[which(str_detect(elemsModel, kToCorrect[yy]))], keqVal, keqValNEW)
      }
    } else {
      kValTooLow <- FALSE
    }
    
    print(kValTooLow)
    print("###################")
    
    
    ##Master store
    Base_mm_store[[k_counter]] <- results
    
  } #end of while look to satisfy k
  
  # saveRDS(Base_mm_store, file=paste("_outputs/out012_", model.name, "_spatialModRuns.rds", sep="") )
  return(Base_mm_store) #note: model would return last calculated value if not specified
  
} #END OF FUNCTION


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

