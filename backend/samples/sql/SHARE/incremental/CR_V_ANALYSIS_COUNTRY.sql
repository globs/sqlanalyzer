SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_ANALYSIS_COUNTRY AS
SELECT
    CAST("Analysis_Country" AS CHAR(3)) AS "Analysis_Country",
    CAST("Analysis_Country_Description" AS VARCHAR(16)) AS "Analysis_Country_Description"
FROM
    (
    SELECT
        TCT.CTY_CF AS "Analysis_Country",
        TCT.USLCTY_LS AS "Analysis_Country_Description"
    FROM
        BI_<env>.TCTYL TCT
    WHERE
        TCT.LAG_CF = 'E'
    GROUP BY
        TCT.CTY_CF,
        TCT.USLCTY_LS);