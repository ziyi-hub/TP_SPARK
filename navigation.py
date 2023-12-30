import sys
from pyspark import SparkContext
import time

# Finds out the index of "name" in the array firstLine 
# returns -1 if it cannot find it
def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[2]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./data/CLIWOC15.csv")

# The first line of the file defines the name of each column in the cvs file
# We store it as an array in the driver program
firstLine = wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"','').split(',')

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not ("RecID" in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

##### Create an RDD that contains all nationalities observed in the
##### different entries

# Information about the nationality is provided in the column named
# "Nationality"

# First find the index of the column corresponding to the "Nationality"
column_index=findCol(firstLine, "Nationality")
print("{} corresponds to column {}".format("Nationality", column_index))

# Q1
# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates
print("1. Consider these two entries as the same one")
nationalities = entries.map(lambda x: x[column_index]).map(lambda x: x.replace(" ", "")).distinct()

# Display the 5 first nationalities
print("A few examples of nationalities:")
for elem in nationalities.sortBy(lambda x: x).take(5):
	print(elem)

# Q2
print("2. Count the total number of observations included in the dataset (that are not NA)")
observationIndex = findCol(firstLine, "OtherRem")
observations = entries.filter(lambda x: x[observationIndex] != "NA")
print(observations.count())

# Q3
print("3. Count the number of years over which observations have been made")
yearIndex = findCol(firstLine, "Year")
years = observations.map(lambda x: x[yearIndex]).distinct()
print("Number of years {}".format(years.count()))

#Q4
print("4. Display the oldest and the newest year of observation")
oldestYear = years.min()
newestYear = years.max()
print("Oldest year: {}".format(oldestYear))
print("Newest year: {}".format(newestYear))

# Q5
print("5. Display the years with the minumum and the maximum number of observations (with the corresponding number)")
yearsObservations = observations.map(lambda x: (x[yearIndex] , 1)).reduceByKey(lambda x , y : x+y).collect()
yearsObservations.sort(key = lambda x: x[1])
print("Years with minimum occurance: {}".format(yearsObservations[0]))
print("Years with maximum occurance: {}".format(yearsObservations[-1]))

# Q6
print("6. Count the distinct departure places using two methods and compare the execution time.")
departuresIndex = findCol(firstLine, "VoyageFrom")

startDistinct = time.time()
departuresDistinct = entries.map(lambda x: x[departuresIndex]).distinct()
endDistinct = time.time()
print("Count (with distinct()): {}".format(departuresDistinct.count()))
print("Execution time (with distinct()): {}".format(endDistinct - startDistinct))

startReduceByKey = time.time()
departuresReduceByKey = entries.map(lambda x: (x[departuresIndex] , 1)).reduceByKey(lambda x , y : x+y).collect()
endReduceByKey = time.time()
print("Count (with ReduceByKey()): {}".format(len(departuresReduceByKey)))
print("Execution time (with ReduceByKey()): {}".format(endReduceByKey  - startReduceByKey ))
print("using the distint() function takes less execution time than the reduceByKey() function")

# Q7
print("7. Display the 10 most popular departures")
departuresReduceByKey.sort(key = lambda x: -x[1])
print(departuresReduceByKey[0:9])

# Q8
print("8. Display the 10 roads the most of ten taken")
print("Version 1:")
fromIndex = findCol(firstLine, "VoyageFrom")
toIndex = findCol(firstLine, "VoyageTo")

# Utiliser l'opération 'map' pour créer un RDD contenant des paires ("VoyageFrom", "VoyageTo")
departures = entries.filter(lambda x: x[fromIndex] != "NA" and x[toIndex] != "NA")
routesV1 = departures.map(lambda x: ((x[fromIndex].replace('"',''), x[toIndex].replace('"','')), 1))

# Utiliser l'opération 'groupByKey' pour regrouper les données par paires de routes
groupedRoutes = routesV1.groupByKey()

# Calculer le nombre d'occurrences de chaque paire de routes
countRoutes = groupedRoutes.map(lambda x: ((x[0][0], x[0][1]), len(x[1]))).collect()

countRoutes.sort(key = lambda x: -x[1])
print(countRoutes[0:9])

print("Version 2:")
routesV2 = departures.map(lambda x: (tuple(sorted((x[fromIndex].replace('"',''), x[toIndex].replace('"','')))), 1)).reduceByKey(lambda x, y: x+y).collect()
routesV2.sort(key = lambda x: -x[1])
print(routesV2[0:9])

# Q9
print("9. Compute the hottest month on average over the years considering all temperatures reported in the dataset")
temperatureIndex = findCol(firstLine, "ProbTair")
monthIndex = findCol(firstLine, "Month")
temperature = entries.filter(lambda x: x[temperatureIndex] != 'NA')
temperatureByMonth = temperature.map(lambda x: (int(x[monthIndex]), float(x[temperatureIndex]))).distinct()

# Utiliser l'opération 'groupByKey' pour regrouper les données par mois
regroupedByMonth = temperatureByMonth.groupByKey()

# Calculer la température moyenne pour chaque mois = la somme de toutes les valeurs de température pour le mois donné divise par le nombre de valeurs de température
temperatureMoyenneParMois = regroupedByMonth.map(lambda x: (x[0], sum(x[1]) / len(x[1])))

# Trouver le mois le plus chaud basé sur la température moyenne
moisLePlusChaud = temperatureMoyenneParMois.max(lambda x: x[1])

# Afficher le résultat
print("Le mois le plus chaud est le mois {} avec une température moyenne de {:.2f} degrés Celsius".format(moisLePlusChaud[0], moisLePlusChaud[1]))
