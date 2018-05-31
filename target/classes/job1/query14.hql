CREATE EXTERNAL TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"", "escapeChar"    = "\0")
stored as textfile
LOCATION '${INPUT}';

CREATE OR REPLACE VIEW words AS
SELECT year(from_unixtime(CAST(time AS BIGINT))) as year, exp.word
FROM amazonfoodreviews
LATERAL VIEW explode(split(lower(summary), ' ')) exp AS word;

SELECT year, collect_set(CONCAT(CONCAT("(", CONCAT_WS(", ", word, CAST(cnt AS STRING))), ")")) FROM
	(SELECT year, word, COUNT(1) AS cnt, rank() over ( partition by year order by COUNT(1) desc) as rank
	FROM words
	GROUP BY year, word
	ORDER BY year, cnt DESC) tmp
WHERE rank < 11
GROUP BY year;


