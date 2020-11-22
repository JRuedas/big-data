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
# TODO: Fix those charts to show distribution of delays related to a categorical variable
daysAsFactor <- as.factor(cleanFlightsData$DayOfWeek) # Parses numerical to categorical 

ggplot(data = cleanFlightsData, aes(x=Month, y=ArrDelay)) + labs(x="Month", y="Delay")
+ geom_line() + ggtitle("Evolution of delays during the year")

ggplot(data = cleanFlightsData, aes(x=DayofMonth, y=ArrDelay)) + labs(x="Day of month", y="Delay")
+ geom_line() + ggtitle("Evolution of delays during the month")

ggplot(data = cleanFlightsData, aes(x=daysAsFactor, y=ArrDelay)) + labs(x="Day of wek", y="Delay")
+ geom_bar(aes(fill=ArrDelay)) + ggtitle("Evolution of delays during the week")

# Correlation matrix
corMatrix <- cor(cleanFlightsData)

# Correlation plot
corrplot(corMatrix, method = "number", type = "upper", tl.col = "black", tl.srt = 45, bg = "grey55")
