# Authors
# José Domínguez Pérez
# Ismael Muñoz Aztout
# Jonatan Ruedas Mora

# Libraries

## If a package is installed, it will be loaded. If any
## are not, the missing package(s) will be installed
## from CRAN and then loaded.

## First specify the packages of interest
packages = c("tidyverse","corrgram","corrplot","ggplot2", "ppcor")

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

# Defines the type of the columns
types = cols(.default = col_integer(),
                 UniqueCarrier = col_character(),
                 TailNum = col_character(),
                 Origin = col_character(),
                 Dest = col_character(),
                 CancellationCode = col_character())

# Load the dataset information
path <- readline("Introduce the path to the dataset file: ")
flightsData <- read_csv(file = path, col_types = types)

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

# Remove useless variables
useless <- c("Cancelled", "CancellationCode")

cleanFlightsData <- (cleanFlightsData[, !(names(cleanFlightsData) %in% useless)])

# Create new date column
cleanFlightsData$Date <- as.Date(with(cleanFlightsData, paste(DayofMonth, Month, Year,sep="-")), "%d-%m-%Y")

# Parse non numerical variables to factors
cleanFlightsData$UniqueCarrier <- as.numeric(factor(cleanFlightsData$UniqueCarrier))
cleanFlightsData$TailNum <- as.numeric(factor(cleanFlightsData$TailNum))
cleanFlightsData$Origin <- as.numeric(factor(cleanFlightsData$Origin))
cleanFlightsData$Dest <- as.numeric(factor(cleanFlightsData$Dest))
cleanFlightsData$Date <- as.numeric(factor(cleanFlightsData$Date))

# Replace NA with 0
cleanFlightsData[is.na(cleanFlightsData)] <- 0

# Correlation matrix
corMatrix <- cor(cleanFlightsData)

# Correlation plot
corrplot(corMatrix, method = "number", type = "upper", tl.col = "black", tl.srt = 45, bg = "grey55")

# Scatterplot of ArrDelay and DepDelay
ggplot(cleanFlightsData, aes(x=DepDelay, y=ArrDelay)) + 
  geom_point(colour="blue", alpha=.2) + 
  geom_abline(colour="red") + theme_light()

# Scatterplot of ArrDelay and TaxiOut
ggplot(cleanFlightsData, aes(x=TaxiOut, y=ArrDelay)) + 
  geom_point(colour="blue", alpha=.2) + 
  geom_abline(colour="red") + theme_light()

