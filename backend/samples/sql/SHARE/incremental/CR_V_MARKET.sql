SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_MARKET AS
SELECT
    "Market",
    CAST("Market_Description" AS VARCHAR(16)) AS "Market_Description",
    "Market_Business_Unit",
    CAST("Market_Business_Description" AS VARCHAR(32)) AS "Market_Business_Description",
    "Macro_Market",
    CAST("Macro_Market_Description" AS VARCHAR(16)) AS "Macro_Market_Description"
FROM
    (
    SELECT
        TUW.SUBMRK_NT AS "Market",
        TUW.SUBMRK_LS AS "Market_Description",
        TUW.BUSUNIT_CF AS "Market_Business_Unit",
        TUW.BUSUNIT_LM AS "Market_Business_Description",
        TUW.MRKUNT_NT AS "Macro_Market",
        TUW.MRKUNT_LS AS "Macro_Market_Description"
    FROM
        BI_<env>.TUWSEC TUW
    WHERE
        TUW.SUBLEDGER_CF NOT IN ('05-01',
        '05-10',
        '06-01',
        '07-01',
        '10-01',
        '16-01',
        '18-16',
        '18-17',
        '20-01',
        '20-02',
        '26-01',
        '26-02',
        '26-03',
        '26-05',
        '26-06',
        '26-07',
        '26-08',
        '26-09',
        '26-10')
        AND TUW.SSD_CF NOT IN (14,
        25)
        AND TUW.ACTIV2_CF = '400  '
        AND TUW.MRKUNT_NT IS NOT NULL
        AND TUW.BUSUNIT_CF IS NOT NULL
        AND TUW.END_D = '9999-12-31'
        AND TUW.SUPP_D = '9999-12-31'
    GROUP BY
        TUW.SUBMRK_NT,
        TUW.SUBMRK_LS,
        TUW.BUSUNIT_CF,
        TUW.BUSUNIT_LM,
        TUW.MRKUNT_NT,
        TUW.MRKUNT_LS);