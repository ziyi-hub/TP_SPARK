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