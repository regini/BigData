CREATE TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"", "escapeChar"    = "\0")
stored as textfile
LOCATION '${INPUT}';

SELECT tmp.productId, collect_list(tmp.year2score) FROM
	(SELECT productId, CONCAT(CONCAT("(", CONCAT_WS(", ", cast(year(from_unixtime(CAST(time AS BIGINT))) as STRING))), cast(avg(score) as STRING))), ")") AS year2score
	FROM amazonfoodreviews
	WHERE year(from_unixtime(CAST(time AS BIGINT)))>2002 AND year(from_unixtime(CAST(time AS BIGINT)))<2013
	GROUP BY productId, year(from_unixtime(CAST(time AS BIGINT)))
	ORDER BY productId, year2score) tmp
GROUP BY productId
ORDER BY productId;

