# Authors
# José Domínguez Pérez
# Ismael Muñoz Aztout
# Jonatan Ruedas Mora

# Libraries

## If a package is installed, it will be loaded. If any
## are not, the missing package(s) will be installed
## from CRAN and then loaded.

## First specify the packages of interest
packages = c("tidyverse")

## Now load or install & load all
package.check <- lapply(
  packages,
  FUN = function(x) {
    if (!require(x, character.only = TRUE)) {
      install.packages(x, dependencies = TRUE)
      library(x, character.only = TRUE)
    }
  }
)

# Load the dataset information
flightsData <- read_csv(file = '../../dataverse_files/1987.csv')

# Remove forbidden variables
forbidden <- c("ArrTime",
               "ActualElapsedTime",
               "AirTime",
               "TaxiIn",
               "Diverted",
               "CarrierDelay",
               "WeatherDelay",
               "NASDelay",
               "SecurityDelay",
               "LateAircraftDelay")

flightsData <- flightsData[, !(names(flightsData) %in% forbidden)]

# Observe fist values
head(flightsData)
