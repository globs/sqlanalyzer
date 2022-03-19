SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_DURATION_TYPE AS
SELECT
    CAST(COLVAL_CT AS CHAR(5)) AS "Duration_Type",
    CAST(COLVAL_LS AS VARCHAR(16)) AS "Duration_Type_Description"
FROM
    BI_<env>.TBANTECL
WHERE
    COL_LS = 'FA004'
    AND LAG_CF = 'E'
GROUP BY
    COLVAL_CT,
    COLVAL_LS;