CREATE TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"", "escapeChar"    = "\0")
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/Users/regini/Desktop/CSV/Uno/uno.csv"
OVERWRITE INTO TABLE amazonfoodreviews;

add jar /Users/regini/Desktop/CSV/UnixYear-1.jar;

CREATE TEMPORARY FUNCTION unix_year AS 'unix2year.Unix2Year'; 

INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/job2Hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT tmp.productId, collect_list(tmp.year2score) FROM
	(SELECT productId, CONCAT(CONCAT("(", CONCAT_WS(", ", unix_year(time), cast(avg(score) as STRING))), ")") AS year2score
	FROM amazonfoodreviews
	WHERE unix_year(time)>2002 AND unix_year(time)<2013
	GROUP BY productId, unix_year(time)
	ORDER BY productId, year2score) tmp
GROUP BY productId
ORDER BY productId;

