SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_TOP AS
SELECT
    CAST("Top" AS CHAR(3)) AS "Top",
    CAST("Top_Description" AS VARCHAR(16)) AS "Top_Description"
FROM
    (
    SELECT
        TTO.TOP_CF AS "Top",
        TTO.TOP_GS AS "Top_Description"
    FROM
        BI_<env>.TTOPL TTO
    WHERE
        TTO.LAG_CF = 'E'
    GROUP BY
        TTO.TOP_CF,
        TTO.TOP_GS);