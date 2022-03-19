SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_IFRS_FLAG AS
SELECT
    CAST(IFRS AS CHAR(5)) AS IFRS,
    CAST("IFRS_Description" AS VARCHAR(16)) AS "IFRS_Description"
FROM
    (
    SELECT
        TBA.COLVAL_CT AS IFRS,
        TBA.COLVAL_LS AS "IFRS_Description"
    FROM
        BI_<env>.TBANTECL TBA
    WHERE
        TBA.COL_LS = 'ASSFINANCE_CT'
        AND TBA.LAG_CF = 'E'
    GROUP BY
        TBA.COLVAL_CT,
        TBA.COLVAL_LS);