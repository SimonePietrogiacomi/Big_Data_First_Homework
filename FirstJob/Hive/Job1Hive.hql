-- Remove the comment marks to delete views and table
-- DROP VIEW sharePrices19982018;
-- DROP VIEW minimumClosePrices1998;
-- DROP VIEW maximumClosePrices2018;
-- DROP VIEW minimumLowPrices;
-- DROP VIEW maximumHighPrices;
-- DROP VIEW percentageIncrease;
-- DROP VIEW meanVolume;
-- DROP TABLE sharePricesTable;
-- 

CREATE TABLE IF NOT EXISTS sharePricesTable (
	ticker STRING,
	open DOUBLE,
	close DOUBLE,
	adj_close STRING,
	low DOUBLE,
	high DOUBLE,
	volume INT,
	currentDate STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- TODO: Update this path in your local execution
LOAD DATA INPATH '/path_to_input_directory_in_hdfs/input.csv' INTO TABLE sharePricesTable;

CREATE VIEW IF NOT EXISTS sharePrices19982018 AS SELECT * FROM sharePricesTable WHERE YEAR(currentDate) >= 1998 AND YEAR(currentDate) <= 2018;

CREATE VIEW IF NOT EXISTS minimumClosePrices1998 AS SELECT ticker, MIN(close) AS min FROM sharePrices19982018 WHERE YEAR(currentDate) = 1998 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS maximumClosePrices2018 AS SELECT ticker, MAX(close) AS max FROM sharePrices19982018 WHERE YEAR(currentDate) = 2018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS minimumLowPrices AS SELECT ticker, MIN(low) AS min FROM sharePrices19982018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS maximumHighPrices AS SELECT ticker, MAX(high) AS max FROM sharePrices19982018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS meanVolume AS SELECT ticker, AVG(volume) AS mean FROM sharePrices19982018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS percentageIncrease AS SELECT m2018.ticker, 100.0*((m2018.max - m1998.min) / m1998.min) AS percentIncr FROM minimumClosePrices1998 AS m1998 JOIN maximumClosePrices2018 AS m2018 ON m1998.ticker = m2018.ticker;

-- TODO: Update this path in your local execution
INSERT OVERWRITE DIRECTORY '/path_to_output_directory_in_hdfs/output_dir' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT p.ticker, p.percentIncr, m1.min, m2.max, mean.mean FROM percentageIncrease AS p JOIN minimumLowPrices AS m1 ON p.ticker = m1.ticker JOIN maximumHighPrices AS m2 ON m1.ticker=m2.ticker JOIN meanVolume AS mean ON m2.ticker=mean.ticker ORDER BY p.percentIncr DESC LIMIT 10;
