SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_SOB AS
SELECT
    CAST(SOB AS CHAR(2)) AS SOB,
    CAST("SOB_Description" AS VARCHAR(16)) AS "SOB_Description"
FROM
    (
    SELECT
        SOB_CF AS SOB,
        SOB_GS AS "SOB_Description"
    FROM
        BI_<env>.TSOBL
    WHERE
        LAG_CF = 'E'
    GROUP BY
        SOB_CF,
        SOB_GS);