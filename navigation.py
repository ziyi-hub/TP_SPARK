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
sc = SparkContext("local[1]")
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

# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates
print("1. Consider these two entries as the same one")
nationalities = entries.map(lambda x: x[column_index]).map(lambda x: x.replace(" ", "")).distinct()
# nationalities = entries.map(lambda x: x[column_index].replace(" ", "")).distinct()

# Display the 5 first nationalities
print("A few examples of nationalities:")
for elem in nationalities.sortBy(lambda x: x).take(5):
	print(elem)

# Count the total number of observations
print("2. Count the total number of observations included in the dataset")
total_observations = entries.count()

# Display the result
print("Total number of observations: {}".format(total_observations))

#Count the number of years over which observations have been made
year_index = findCol(firstLine, "Year")
print("{} corresponds to column {}".format("Year", year_index))

observations = entries.filter(lambda x: findCol(firstLine, "Distance") != "NA")
observations.cache()
years = observations.map(lambda x: x[year_index]).distinct()

print("3. Count the number of years over which observations have been made")
print(f"Number of years {years.count()}")

print("4. Display the oldest and the newest year of observation")
oldest_year = years.min()
newest_year = years.max()
print("Oldest year of observation {}".format(oldest_year))
print("Newest year of observation {}".format(newest_year))


# Q5


print("8. Display the 10 roads the most of ten taken")
from_index = findCol(firstLine, "VoyageFrom")
to_index = findCol(firstLine, "VoyageTo")

# Utiliser l'opération 'map' pour créer un RDD contenant des paires ("VoyageFrom", "VoyageTo")
departures = entries.filter(lambda x: x[from_index] != "NA" and x[to_index] != "NA")
routes = departures.map(lambda x: ((x[from_index].replace('"',''), x[to_index].replace('"','')), 1))

# Utiliser l'opération 'groupByKey' pour regrouper les données par paires de routes
grouped_routes = routes.groupByKey()

# Calculer le nombre d'occurrences de chaque paire de routes
count_routes = grouped_routes.map(lambda x: ((x[0][0], x[0][1]), len(x[1])))

# Trouver les 10 routes les plus empruntées
top_10_routes = count_routes.takeOrdered(10, key=lambda x: -x[1])

# Display the result
print("The 10 most often taken roads:")
for road, count in top_10_routes:
	print("{}-{}: {} times".format(road[0], road[1], count))

# Code de l'ami
# departures = entries.filter(lambda x: x[from_index] != "NA" and x[to_index] != "NA").map(lambda x: (tuple(sorted((x[from_index], x[to_index]))), 1))
# result = departures.reduceByKey(lambda x, y: x + y).takeOrdered(10, key=lambda x: -x[1])
# print("Top 10 roads taken often")
# print(result)


print("9. Compute the hottest month on average over the years consid- ering all temperatures")
temperature_index = findCol(firstLine, "ProbTair")
month_index = findCol(firstLine, "Month")
temperature = entries.filter(lambda x: x[temperature_index] != 'NA')
temperature_by_month = temperature.map(lambda x: (int(x[month_index]), float(x[temperature_index]))).distinct()

# Utiliser l'opération 'groupByKey' pour regrouper les données par mois
regrouped_by_month = temperature_by_month.groupByKey()

# Calculer la température moyenne pour chaque mois = la somme de toutes les valeurs de température pour le mois donné divise par le nombre de valeurs de température
temperature_moyenne_par_mois = regrouped_by_month.map(lambda x: (x[0], sum(x[1]) / len(x[1])))

#print(temperature_moyenne_par_mois.collect())

# Trouver le mois le plus chaud basé sur la température moyenne
mois_le_plus_chaud = temperature_moyenne_par_mois.max(lambda x: x[1])

# Afficher le résultat
print("Le mois le plus chaud est le mois {} avec une température moyenne de {:.2f} degrés Celsius".format(mois_le_plus_chaud[0], mois_le_plus_chaud[1]))
