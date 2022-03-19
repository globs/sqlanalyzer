SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_CEDENT_GROUP AS
SELECT
    "Cedent-retrocessionnaire_Group",
    CAST("Cedent-retrocessionnaire_Group_Description" AS VARCHAR(32)) AS "Cedent-retrocessionnaire_Group_Description"
FROM
    (
    SELECT
        TUW.HLD_NF AS "Cedent-retrocessionnaire_Group",
        TUW.HLDABBR_LD AS "Cedent-retrocessionnaire_Group_Description"
    FROM
        BI_<env>.TUWCLI TUW
    WHERE
        TUW.HLD_NF IS NOT NULL
        AND TUW.HLDABBR_LD IS NOT NULL
    GROUP BY
        TUW.HLD_NF,
        TUW.HLDABBR_LD);