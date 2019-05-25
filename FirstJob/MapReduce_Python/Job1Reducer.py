#!/usr/bin/env python3
"""Big Data Project - Job 1 - MapReduce - Reducer"""

import sys
# import time
from collections import defaultdict

# startTime = time.time()

# Dictionary with String Ticker key and List Daily Share value
tickerShareDict = defaultdict(list)

# Dictionary with String Ticker key and Float Price value
tickerPriceDict = defaultdict(float)

# Variable to get the name of the last ticker managed
lastTicker = None

# Read the rows from the stdin coming from the shuffle and sort
# Be careful, in Python they don't came in form (k1, [v1,v2]),(k2, [v3,v4]) but (k1, v1),(k1,v2),(k2,v3),(k2,v4)
# So remember to manage it ;)
for line in sys.stdin:

    # Remove useless whitespaces at the beginning and at the end of the line
    line = line.strip()

    # Cut empty line
    if line == "":
        continue

    # Divide the Ticker from the rest of the row, splitting this
    tabIndex = line.index('\t')
    currentTicker = line[0:tabIndex]
    dailyShare = line[tabIndex + 1:]
    ticker, openPrice, closePrice, adj_close, lowestPrice, highestPrice, volume, currentDate = dailyShare.split(",")
    openPrice = float(openPrice)
    closePrice = float(closePrice)
    lowestPrice = float(lowestPrice)
    highestPrice = float(highestPrice)
    volume = int(volume)
    currentYear = int(currentDate[0:4])

    # Enter in the following if statement if this isn't the first iteration
    # and he finish all the rows with the same ticker
    if lastTicker != currentTicker:
        # Enter in the following if statement if this isn't the first iteration
        # and he get at least one row in 1998 and 2018
        if lastTicker and lowestClosePrice1998 != sys.float_info.max and highestClosePrice2018 != sys.float_info.min:
            meanVolume = sumVolume / count
            percentageIncrease = ((highestClosePrice2018 - lowestClosePrice1998) / lowestClosePrice1998) * 100
            if percentageIncrease > 0.0:
                tickerShareDict[lastTicker].append((lastTicker, percentageIncrease, lowestPriceTicker,
                                                    highestPriceTicker, meanVolume))
                tickerPriceDict[lastTicker] = percentageIncrease
        # Reset all the variables, in case this is the first iteration or he change ticker
        lastTicker = currentTicker
        count = 0
        sumVolume = 0
        lowestPriceTicker = lowestPrice
        highestPriceTicker = highestPrice
        lowestClosePrice1998 = sys.float_info.max
        highestClosePrice2018 = sys.float_info.min
    # With the same ticker, he sum the volume value, increment his count and get the min close price in 1998,
    # the max in 2018 and the lowest and higher in the range 1998-2018
    sumVolume += volume
    count += 1
    highestPriceTicker = max(highestPriceTicker, highestPrice)
    lowestPriceTicker = min(lowestPriceTicker, lowestPrice)
    if currentYear == 1998:
        lowestClosePrice1998 = min(lowestClosePrice1998, closePrice)
    if currentYear == 2018:
        highestClosePrice2018 = max(highestClosePrice2018, closePrice)

# Sort the dictionary
sortedDictionary = sorted(tickerPriceDict.items(), key=lambda elem: elem[1], reverse=True)

# Send to hdfs the first 10 element of the list, that are the best
for i in sortedDictionary[0:10]:
    print(tickerShareDict[i[0]][0])

# endTime = time.time()
# Uncomment to view these times in the output file
# print()
# print("This execution of the reducer took " + str(endTime - startTime) + " seconds or, if you prefer, "
#       + str((endTime - startTime) / 60) + " minutes")
