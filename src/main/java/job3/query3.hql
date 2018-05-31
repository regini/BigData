CREATE TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"", "escapeChar"    = "\0")
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/Users/regini/Desktop/CSV/Cinque/cinque.csv"
OVERWRITE INTO TABLE amazonfoodreviews;

INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/job3Hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT
t1.productId AS item1, t2.productId AS item2, COUNT(1) AS cnt
FROM
(SELECT DISTINCT productId, userId FROM amazonfoodreviews) t1
JOIN
(SELECT DISTINCT productId, userId FROM amazonfoodreviews) t2
ON (t1.userId = t2.userId) 
GROUP BY t1.productId, t2.productId
HAVING t1.productId != t2.productId 
ORDER BY item1 ASC;
