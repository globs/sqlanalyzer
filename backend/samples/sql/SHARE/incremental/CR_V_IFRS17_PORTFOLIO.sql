SET SCHEMA SHARE_<env>;

CREATE OR REPLACE VIEW V_IFRS17_PORTFOLIO AS
SELECT 
CAST("IFRS17_Portfolio" AS CHAR(10)) AS "IFRS17_Portfolio",
CAST("IFRS17_Portfolio_LAB" AS VARCHAR(64)) AS "IFRS17_Portfolio_LAB"
FROM
(SELECT 
GRPIFRSSEG_CT AS "IFRS17_Portfolio",
GRPIFRSSEG_LL AS "IFRS17_Portfolio_LAB"
FROM BI_<env>.TSECIFRS A
GROUP BY 
GRPIFRSSEG_CT,
GRPIFRSSEG_LL);