"""Big Data Project - Job 1 - Spark"""

from pyspark import SparkContext, SparkConf
import operator
import time

# I need to measure the time of execution, this is the start
startTime = time.time()

conf = SparkConf().setAppName("Big Data Project - Job 1 - Spark")
sc = SparkContext(conf=conf)

# Take input file from hdfs
# TODO: Update this path in your local execution
contentRDD = sc.textFile("/path_to_input_directory_in_hdfs/input.csv")

# Skip empty rows
nonEmptyLines = contentRDD.filter(lambda x: len(x) > 0)

# Skip first row and the one who's out of the years range
# At the end, fixedInput=(ticker, open, close, adj_close, lowThe, highThe, volume, date)
fixedInput = nonEmptyLines.map(lambda x: x.split(','))\
    .filter(lambda x: x[0] != "ticker")\
    .filter(lambda x: 1998 <= int(x[7][0:4]) <= 2018)\

# Lowest price (lowThe column) for every ticker
# At the end, lowestPrice=(ticker, lowest_price)
lowestPrice = fixedInput.map(lambda x: (x[0], x[4]))\
    .reduceByKey(lambda x, y: min(float(x), float(y)))

# Highest price (highThe column) for every ticker
# At the end, highestPrice=(ticker, highest_price)
highestPrice = fixedInput.map(lambda x: (x[0], x[5]))\
    .reduceByKey(lambda x, y: max(float(x), float(y)))

# Mean value of the volume (volume column) for every ticker
# At the end, meanVolume=(ticker, mean_volume)
meanVolume = fixedInput.map(lambda x: (x[0], (1, int(x[6]))))\
    .reduceByKey(lambda x, y: tuple(map(operator.add, x, y)))\
    .map(lambda x: (x[0], x[1][1]/x[1][0]))

# Lowest close price (close column) for every ticker in 1998
# At the end, lowestClosePriceYear1998=(ticker, lowest_price)
lowestClosePriceYear1998 = fixedInput.filter(lambda x: int(x[7][0:4]) == 1998)\
    .map(lambda x: (x[0], float(x[2])))\
    .reduceByKey(lambda x, y: min(x, y))\

# Highest close price (close column) for every ticker in 2018
# At the end, highestClosePriceYear2018=(ticker, highest_price)
highestClosePriceYear2018 = fixedInput.filter(lambda x: int(x[7][0:4]) == 2018)\
    .map(lambda x: (x[0], float(x[2])))\
    .reduceByKey(lambda x, y: max(x, y))\

# Join between lowestPrice and highestPrice
# At the end, lowestHighestPricesJoined=(ticker, (lowest_price, highest_price))
lowestHighestPricesJoined = lowestClosePriceYear1998.join(highestClosePriceYear2018)

# Percentage increase of close prices for every ticker
# Return only the highest 10 elements, ordered by percentage increase
# At the end, percentageIncreaseOfClosePrices=(ticker, percentage_increase)
percentageIncreaseOfClosePrices = \
    lowestHighestPricesJoined.map(lambda x: (x[0], (100 * (float(x[1][1]) - float(x[1][0]))) / float(x[1][0])))\
    .sortBy(lambda x: x[1], ascending=False)\
    .take(10)

# Set the output, joining percentageIncrease, lowestPrice, highestPrice, meanVolume and sorting by percentage increase
# At the end, output=(ticker, percentage_increase, lowest_price, highest_price, mean_volume)
output = sc.parallelize(percentageIncreaseOfClosePrices).join(lowestPrice)\
    .map(lambda x: (x[0], (x[1][0], x[1][1])))\
    .join(highestPrice)\
    .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])))\
    .join(meanVolume)\
    .map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][1]))\
    .sortBy(lambda x: x[1], ascending=False)

# And this is the end of the execution
endTime = time.time()

print("This execution took " + str(endTime - startTime) + " seconds or, if you prefer, "
      + str((endTime - startTime) / 60) + " minutes")
# It took 353.07688546180725 seconds, 5.884614757696787 minutes, on my 10 y/o PC with ~21M rows in input

# Send the output to hdfs
# TODO: Update this path in your local execution
output.coalesce(1).saveAsTextFile("/path_to_output_directory_in_hdfs/output_dir")
