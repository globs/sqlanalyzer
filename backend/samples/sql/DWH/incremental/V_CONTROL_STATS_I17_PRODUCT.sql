CREATE OR REPLACE
VIEW V_CONTROL_STATS_I17_PRODUCT (SSD_CF,
ESB_CF,
STAT_NAME,
STAT_VALUE,
BALSHRMTH_NF,
BALSHEY_NF,
TRNCOD_CF_CHAR ) AS (
SELECT
	SSD_CF,
	ESB_CF,
	'I17_PRODUCT_ROW' AS STAT_NAME,
	COUNT(1) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	SSD_CF,
	ESB_CF,
	'I17_PRODUCT_ROW_ERROR' AS STAT_NAME,
	COUNT(1) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	NEWCOLS3_NF IS NULL
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	SSD_CF,
	ESB_CF,
	'I17_PRODUCT_CTR' AS STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	CTR_NF IS NOT NULL
	AND CTR_NF != ''
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	SSD_CF,
	ESB_CF,
	'I17_PRODUCT_CTR_ERROR' AS STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	CTR_NF IS NOT NULL
	AND CTR_NF != ''
	AND NEWCOLS3_NF IS NULL
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	SSD_CF,
	ESB_CF,
	'I17_PRODUCT_CTR_CORRECT' AS STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	CTR_NF IS NOT NULL
	AND CTR_NF != ''
	AND NEWCOLS3_NF IS NOT NULL
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	DISTINCT SSD_CF,
	ESB_CF,
	'I17_PRODUCT_RETCTR' AS STAT_NAME,
	COUNT(DISTINCT RETCTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	(CTR_NF IS NULL
	OR CTR_NF = '')
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	DISTINCT SSD_CF,
	ESB_CF,
	'I17_PRODUCT_RETCTR_ERROR' AS STAT_NAME,
	COUNT(DISTINCT RETCTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	NEWCOLS3_NF IS NULL
	AND (CTR_NF IS NULL
	OR CTR_NF = '')
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1)
UNION
SELECT
	DISTINCT SSD_CF,
	ESB_CF,
	'I17_PRODUCT_RETCTR_CORRECT' AS STAT_NAME,
	COUNT(DISTINCT RETCTR_NF) AS STAT_VALUE,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1) AS TRNCOD_CF_CHAR
FROM
	BI_<env>.TFULLGLT
WHERE
	(CTR_NF IS NULL
	OR CTR_NF = '')
GROUP BY
	SSD_CF,
	ESB_CF,
	BALSHRMTH_NF,
	BALSHEY_NF,
	SUBSTRING(TRNCOD_CF, 8, 1));
