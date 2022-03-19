SET SCHEMA SHARE_<env>;

CREATE OR REPLACE VIEW V_INITIAL_PROFITABILITY AS 
SELECT 
CAST("Initial_Profitability" AS CHAR(5)) AS "Initial_Profitability",
CAST("Initial_Profitability_LAB" AS VARCHAR(16)) AS "Initial_Profitability_LAB",
CAST("Initial_Profitability_Description" AS VARCHAR(32)) AS "Initial_Profitability_Description"
FROM
(SELECT 
COLVAL_CT AS "Initial_Profitability",
COLVAL_LS AS "Initial_Profitability_LAB",
COLVAL_LM AS "Initial_Profitability_Description"
FROM BI_<env>.TBANALL
WHERE COL_LS = 'IFRSPRO_CF'
AND LAG_CF = 'E');