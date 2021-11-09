library(tidyverse)
library(raster)

setwd('c:/Users/joe.vanbuskirk/cloudstor/Bushfire_Smoke_for_CAR_Project/Predictors/')

grid <- readRDS('data_derived_for_predicting/AUS_points_5km.rds') %>% 
  sf::st_as_sf(coords = c('X', 'Y'), crs = 3577) %>% 
  sf::st_transform(4283)

windu10 <- dir(file.path(
  'c:/Users/joe.vanbuskirk/cloudstor/Environment_General',
  'Copernicus_Climate_Change_Service/ERA5_avg_daily_wind_1950_2020',
  'data_provided/u10/'), 
  full.names = TRUE, include.dirs = TRUE)

windv10 <- dir(file.path(
  'c:/Users/joe.vanbuskirk/cloudstor/Environment_General',
  'Copernicus_Climate_Change_Service/ERA5_avg_daily_wind_1950_2020',
  'data_provided/v10/'), 
  full.names = TRUE, include.dirs = TRUE)

windu100 <- dir(file.path(
  'c:/Users/joe.vanbuskirk/cloudstor/Environment_General',
  'Copernicus_Climate_Change_Service/ERA5_avg_daily_wind_1950_2020',
  'data_provided/u100/'), 
  full.names = TRUE, include.dirs = TRUE)

windv100 <- dir(file.path(
  'c:/Users/joe.vanbuskirk/cloudstor/Environment_General',
  'Copernicus_Climate_Change_Service/ERA5_avg_daily_wind_1950_2020',
  'data_provided/v100/'), 
  full.names = TRUE, include.dirs = TRUE)

## Filter for years after 2000
windu10 <- windu10[as.numeric(gsub(".*(\\d{4})$", "\\1", windu10)) >= 2000]
windv10 <- windv10[as.numeric(gsub(".*(\\d{4})$", "\\1", windv10)) >= 2000]
windu100 <- windu100[as.numeric(gsub(".*(\\d{4})$", "\\1", windu100)) >= 2000]
windv100 <- windv100[as.numeric(gsub(".*(\\d{4})$", "\\1", windv100)) >= 2000]


library(progress)
library(doSNOW)

## Assign multiple cores
cl <- makeCluster(16)
registerDoSNOW(cl)

## Define a function to loop over
## As the file structures for the inputs are identicaly, only one of the above 
## 'dir' commands are actually used. The rest I just substitute with the relevant
## u10/v10/u100/v100 subfolder.
windextract <- function(u){
  u10 <- u
  v10 <- gsub("u10", "v10", u)
  u100 <- gsub("u10", "u100", u)
  v100 <- gsub("u10", "v100", u)
  date <- gsub(".*(\\d{8}).*", "\\1", u)
  
  u10rast <- raster::raster(u10)
  v10rast <- raster::raster(v10)
  
  u100rast <- raster::raster(u100)
  v100rast <- raster::raster(v100)
  
  wind10 <- sqrt(u10rast^2 + v10rast^2)
  wind100 <- sqrt(u100rast^2 + v100rast^2)
  
  ex10 <- velox::velox(wind10)$extract_points(grid)
  ex100 <- velox::velox(wind100)$extract_points(grid)
  
  data.frame(FID = grid$FID,
             date = date,
             wind10 = ex10,
             wind100 = ex100,
             row.names = NULL)}


for(y in 1:length(windu10)){
  yr <- gsub(".*(\\d{4})$", "\\1", windu10[y])
  print(yr)
  u10 <- dir(windu10[y], full.names = TRUE, recursive = TRUE)

  pb <- txtProgressBar(max = length(u10), style = 3)  
  progress <- function(n) setTxtProgressBar(pb, n)
  ex.out <- foreach(
    u = u10,
    .packages = c('raster', 'velox', 'tidyverse'),
    .options.snow = list(progress = progress),
    .combine = rbind) %dopar% 
    
    
    windextract(u)

  saveRDS(ex.out,
          paste0('data_derived_for_predicting/Spatiotemporal/',
                 'Wind/data_derived/Wind_',
                 yr,
                 ".rds"))
}


## Not sure why 2005 doesn't work with the above... do it manually below


yr <- gsub(".*(\\d{4})$", "\\1", windu10[6])

print(yr)
u10 <- dir(windu10[y], full.names = TRUE, recursive = TRUE)
pb <- progress_bar$new(total = length(u10))  

ex.out <- purrr::map_df(u10,
                        function(u){
                          pb$tick()
                          windextract(u)
                          }
                        )

saveRDS(ex.out,
        glue::glue('data_derived_for_predicting/Spatiotemporal/',
                   "Wind/data_derived/Wind_{yr}.rds"))





