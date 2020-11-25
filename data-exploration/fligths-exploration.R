# Authors
# José Domínguez Pérez
# Ismael Muñoz Aztout
# Jonatan Ruedas Mora

# Libraries

## If a package is installed, it will be loaded. If any
## are not, the missing package(s) will be installed
## from CRAN and then loaded.

## First specify the packages of interest
packages = c("tidyverse","corrgram","corrplot","ggplot2")

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
flightsData <- read_csv(file = '../../dataverse_files/2008.csv')

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

cleanFlightsData <- flightsData[, !(names(flightsData) %in% forbidden)]

# Remove non numerical variables
categorical <- c("UniqueCarrier",
                 "TailNum",
                 "Origin",
                 "Dest",
                 "CancellationCode")

cleanFlightsData <- (cleanFlightsData[, !(names(cleanFlightsData) %in% categorical)])

# Remove useless variables
useless <- c("Cancelled")

cleanFlightsData <- (cleanFlightsData[, !(names(cleanFlightsData) %in% useless)])

# Clean columns full of NA values
cleanFlightsData<-na.omit(cleanFlightsData)
cleanFlightsData<-cleanFlightsData[colSums(!is.na(cleanFlightsData)) > 0]

# Correlation matrix
corMatrix <- cor(cleanFlightsData)

# Correlation plot
corrplot(corMatrix, method = "number", type = "upper", tl.col = "black", tl.srt = 45, bg = "grey55")

# Scatterplot of ArrDelay and DepDelay
ggplot(cleanFlightsData, aes(x=ArrDelay, y=DepDelay)) + geom_point()
