#' Author: Stephen Bradshaw
#' Written and maintained by: Stephen Bradshaw
#' Contact: stephen.bradshaw@dpird.wa.gov.au
#'          stephen.bradshaw@utas.edu.au
#'          https://www.linkedin.com/in/stephenbradshaw82/
#' Date: 06 Nov 2024
#' Title: WA spatial extents to refine data downloading for BRAN2020 and CMIP6
#' Details:
#'     - AIM: 
#'     - (A) Separate coastal area into 3 sections which includes offshore waters
#'     - (B) Display these graphically for the user refine the appropriate spatial extents
#'     - (C) Write out figures. Note the extents set here are NOT automatically carried over to data sourcing 000

#### Packages / Libraries ####
rm(list=ls())

#' Packages (not in environment) -------------------------------------------
list.of.packages <- c(
  "magrittr"
  , "tidyr"
  , "dplyr"
  , "ggplot2"
  ,"stringr"
  , "sf"
  , "ggridges"
  , "viridis"
  , "maps"
  # , "ggspatial"
  # , "gginsetmap"
  # , "ggrepel"
  , "grid"
  , "mapdata"
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
dirRdsBRAN  <- "02_netCDFrds_BRAN"
dirRdsCMIP6 <- "03_netCDFrds_CMIP6"
dirRdsVis <- "04_visualisations"
setwd(dirParent)

#--> Misc Options ####
options(stringsAsFactors = FALSE)
options(scipen = 999)
#####

#### DPIRD Specific Data features ####


#--> [USER INPUT] Spatial Extents ####
#' Set for Western Australia Nothern, Western and Southern
#' Encompass Western Australia (WA)
#' Points provided from NE in a CW direction
#' 5th point closes the loop of the quandrilateral polygon

ldf <- list()
ldf[["NORTH"]] <- data.frame(
  Long = c(129.5, 129.5, 115.5, 115.5, 129.5),
  Lat = c(-11.5, -21.5, -21.5, -11.5, -11.5)
)

ldf[["WEST"]] <- data.frame(
  Long = c(116, 116, 108, 108, 116),
  Lat = c(-11.5, -32, -32, -11.5, -11.5)
)

ldf[["SOUTH"]] <- data.frame(
  Long = c(129.5, 129.5, 108.0, 108.0, 129.5),
  Lat = c(-31.5, -39.0, -39.0, -31.5, -31.5)
)

#--> Convert list to spatial object ####
## Convert each region to an sf object
ldf_sf <- lapply(ldf, function(df) st_polygon(list(as.matrix(df))) %>% st_sfc(crs = 4326) %>% st_sf())

## Combine into a single sf object
regions_sf <- do.call(rbind, ldf_sf)
regions_sf$Region <- names(ldf_sf)

## Define the main plot with transparent polygons and colored borders
p_main <- ggplot() +
  geom_sf(data = regions_sf, aes(fill = Region, color = Region), alpha = 0.5, size = 1) +  # Solid colored border with fill transparency
  borders("world", regions = "Australia", fill = "grey90", color = "black") +
  theme_minimal() +
  theme(legend.position = "none") +  # Remove legend
  coord_sf(xlim = c(105, 132.5), ylim = c(-40, -10)) +  # Wider extents
  labs(x = "Longitude", y = "Latitude") +
  # Annotate regions directly on the map
  geom_text(data = regions_sf, aes(label = Region, geometry = geometry), stat = "sf_coordinates", color = "black", size = 4)

## Define the inset map of Australia with highlighted region and black border
p_inset <- ggplot() +
  borders("world", regions = "Australia", fill = "grey90", color = "black") +
  geom_rect(aes(xmin = 108, xmax = 130, ymin = -40, ymax = -10), color = "red", fill = NA, size = 0.5) +
  coord_fixed(ratio = 1) +
  theme_void() 

## Arrange main plot and inset map, positioned in the top-right
library(cowplot)
p_combined <- ggdraw() +
  draw_plot(p_main) +
  draw_plot(p_inset, x = 0.75, y = 0.1, width = 0.2, height = 0.2)  # Adjust position to top-right

## Display the final combined plot
p_combined

#--> Save ####
func_checkCreateDirectory(dirRdsVis)
plot_aspect_ratio <- 1.3
ggsave(
  filename = paste0(dirRdsVis, "/", "image_spatialExtents.png"),
  plot = p_combined,
  width = 10,  # Fixed width in inches
  height = 10/plot_aspect_ratio,  # Height determined by aspect ratio
  dpi = 300
)

#####################################################################
#####################################################################







