
setwd('c:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/Spatiotemporal/ActiveFire/Codes/')

purrr::map(2000:2020,
           function(yr){
t <- glue::glue(
"if (!require('sf')) install.packages('sf')
if (!require('tidyverse')) install.packages('tidyverse')
if (!require('glue')) install.packages('glue')

library(glue)
library(sf)
library(tidyverse)

#### Grid locations ####
grid <- readRDS('/home/jvan8679/ActiveFire/AUS_points_5km.rds') %>%
  sf::st_as_sf(coords = c('X', 'Y'),
               crs = 3577)

activefires <- read.csv(file.path('/home/jvan8679/ActiveFire/data_provided/',
                                   'modis_<<yr>>_Australia.csv'),
                        stringsAsFactors = FALSE) %>%
  dplyr::filter(confidence >= 30) %>%
  sf::st_as_sf(coords = c('longitude','latitude'), crs = 4283) %>%
  sf::st_transform(3577)

grid.buff500 <- sf::st_buffer(grid, dist = 500e3)

test.out <- purrr::map_df(
  unique(activefires$acq_date),
  function(dt){
    fire.df <- activefires[activefires$acq_date == dt,]
    afires500 <- rowSums(sf::st_contains(grid.buff500,
                                         fire.df,
                                         sparse = FALSE))
                                         
    out <- data.frame(FID = grid$FID,
                      date = dt,
                      Fires500 = afires500)
    return(out)
    
  })


saveRDS(test.out, 
        '/project/RDS-FMH-env_health-RW/working_joe/ActiveFires/data_derived/ActiveFire_500_<<yr>>.rds')", 
.open = "<<", 
.close = ">>")

writeLines(t, glue::glue('ActiveFires{yr}.R'))

output <- file(glue::glue('ActiveFires{yr}.pbs'), 'wb')
pbs <- glue::glue(
  "#!/bin/bash
  #PBS -P env_health                          
  #PBS -l select=1:ncpus=8:mem=32GB    
  #PBS -l walltime=24:00:00
  
  cd /home/jvan8679/ActiveFire/Codes/
  module load R/3.6.0
  Rscript ActiveFires{yr}.R")

write(pbs, output)
close(output)

})



