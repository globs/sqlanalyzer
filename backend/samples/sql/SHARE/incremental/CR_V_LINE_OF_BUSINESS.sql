SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_LINE_OF_BUSINESS AS
SELECT
    CAST("Lob" AS CHAR(2)) AS "Lob",
    CAST("Lob_Description" AS VARCHAR(16)) AS "Lob_Description"
FROM
    (
    SELECT
        TLO.LOB_CF AS "Lob",
        TLO.LOB_GS AS "Lob_Description"
    FROM
        BI_<env>.TLOBL TLO
    WHERE
        TLO.LAG_CF = 'E'
    GROUP BY
        TLO.LOB_CF,
        TLO.LOB_GS);