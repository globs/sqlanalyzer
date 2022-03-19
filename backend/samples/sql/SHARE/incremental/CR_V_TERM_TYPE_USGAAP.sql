SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_TERM_TYPE_USGAAP AS
SELECT
    CAST("Term_Type" AS CHAR(5)) AS "Term_Type",
    CAST("Term_Type_Description" AS VARCHAR(16)) AS "Term_Type_Description"
FROM
    (
    SELECT
        TBA.COLVAL_CT AS "Term_Type",
        TBA.COLVAL_LS AS "Term_Type_Description"
    FROM
        BI_<env>.TBANTECL TBA
    WHERE
        TBA.COL_LS = 'USGAAP_CT'
        AND TBA.LAG_CF = 'E'
    GROUP BY
        TBA.COLVAL_CT,
        TBA.COLVAL_LS);