SET SCHEMA SHARE_<env>;

CREATE OR REPLACE VIEW V_TRANSITION_MODE AS
SELECT 
CAST("Transition_Mode" AS CHAR(5)) AS "Transition_Mode",
CAST("Transition_Mode_LAB" AS VARCHAR(16)) AS "Transition_Mode_LAB",
CAST("Transition_Mode_Description" AS VARCHAR(32)) AS "Transition_Mode_Description"
FROM
(SELECT 
COLVAL_CT AS "Transition_Mode",
COLVAL_LS AS "Transition_Mode_LAB",
COLVAL_LM AS "Transition_Mode_Description"
FROM BI_<env>.TBANALL
WHERE COL_LS = 'IFRSTRA_CF'
AND LAG_CF = 'E');
