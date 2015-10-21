#
# This is a tutorial on using sparkR
# Reference: http://ampcamp.berkeley.edu/5/exercises/sparkr.html
#
# Before starting, make sure to install "SparkR" in advance.
# >library(devtools)
# >install_github("amplab-extras/SparkR-pkg", subdir="pkg")
#
# Data can be downloaded from here:
#   http://ampcamp.berkeley.edu/5/exercises/getting-started.html

# Load the SparkR library
library(SparkR)

# creating a SparkContext
sc <- sparkR.init(master="local[*]")

# load data
data <- textFile(sc, "data/tsv_wiki")
take(data, 2)
count(data)

# parse our data and create an RDD containing these fields in a list
parseFields <- function(record) {
  Sys.setlocale("LC_ALL", "C") # necessary for strsplit() to work correctly
  parts <- strsplit(record, "\t")[[1]]
  list(id=parts[1], title=parts[2], modified=parts[3], text=parts[4], username=parts[5])
}
parsedRDD <- lapply(data, parseFields)
#cache(parsedRDD)
count(parsedRDD)

# Generate a histogram of the number of contributions by each user
usernames <- lapply(parsedRDD, function(x) { x$username })
nonEmptyUsernames <- Filter(function(x) { !is.na(x) }, usernames)
userContributions <- lapply(nonEmptyUsernames, function(x) { list(x, 1) })
userCounts <- collect(reduceByKey(userContributions, "+", 8L))
top10users <- tail(userCounts[order(unlist(sapply(userCounts, `[`, 2)))], 10)
counts <- unlist(sapply(userCounts, `[`, 2))
plot(sort(counts), log="y", type="h", lwd=10, lend=2)

# How many articles contain the word “California”?
calArticles <- Filter(function(item) {
  Sys.setlocale("LC_ALL", "C")
  grepl("California", item$text)
}, parsedRDD)
count(calArticles)
