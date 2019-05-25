#!/usr/bin/env python3
"""Big Data Project - Job 1 - MapReduce - Mapper"""

import sys

# Need to skip first line of the csv file, that's the job of this variable
firstLine = True

# Read the csv from the stdin
# Be careful, from this you'll get some white rows
csvIn = sys.stdin
for line in csvIn:

    # Remove useless whitespaces at the beginning and at the end of the line
    line = line.strip()

    # Cut empty lines
    if line == "":
        continue

    # Skip only the first line, where are located the name of the columns
    if firstLine:
        firstLine = False
        continue

    splittedLine = line.split(',')
    ticker = splittedLine[0]
    currentDate = splittedLine[7]

    # Send to the Shuffle and Sort the following string
    # "ticker"\t"ticker,open,close,adj_close,lowThe,highThe,volume,date"
    if 1998 <= int(currentDate[0:4]) <= 2018:
        print(ticker + "\t" + line)
