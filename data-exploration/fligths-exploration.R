# Authors
# José Domínguez Pérez
# Ismael Muñoz Aztout
# Jonatan Ruedas Mora

# Libraries

## If a package is installed, it will be loaded. If any
## are not, the missing package(s) will be installed
## from CRAN and then loaded.

## First specify the packages of interest
packages = c("tidyverse","corrgram","corrplot")

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

flightsData <- flightsData[, !(names(flightsData) %in% forbidden)]

# Observe fist values
head(flightsData)

# Remove non numerical variables
categorical <- c("UniqueCarrier",
                 "TailNum",
                 "Origin",
                 "Dest",
                 "CancellationCode")

cleanFlightsData <- (flightsData[, !(names(flightsData) %in% categorical)])

# Clean columns full of NA values
cleanFlightsData<-cleanFlightsData[colSums(!is.na(cleanFlightsData)) > 0]
cleanFlightsData<-na.omit(cleanFlightsData)

# Plot histogram of variables vs ArrDelay
# TODO: Arreglar ggplot(data = cleanflightsData, aes(x=DayofMonth, y=ArrDelay)) + labs(x="Mes", y="Retraso")

# Correlation matrix
corMatrix <- cor(cleanFlightsData)

# Correlation plot
corrplot(corMatrix, method = "number", type = "upper", tl.col = "black", tl.srt = 45, bg = "grey55")
