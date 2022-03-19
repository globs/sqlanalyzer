SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_SECTION_STATUS AS
SELECT
    CAST("Section_Status" AS CHAR(5)) AS "Section_Status",
    CAST("Section_Status_Description" AS VARCHAR(16)) AS "Section_Status_Description"
FROM
    (
    SELECT
        TBA.COLVAL_CT AS "Section_Status",
        TBA.COLVAL_LS AS "Section_Status_Description"
    FROM
        BI_<env>.TBANTECL TBA
    WHERE
        TBA.COL_LS = 'CTRSTS_CT'
        AND TBA.LAG_CF = 'E'
    GROUP BY
        TBA.COLVAL_CT,
        TBA.COLVAL_LS);