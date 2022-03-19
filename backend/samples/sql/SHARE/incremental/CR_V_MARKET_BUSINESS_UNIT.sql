SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_MARKET_BUSINESS_UNIT AS
SELECT
    "Market_Business_Unit",
    CAST("Market_Business_Unit_Description" AS VARCHAR(32)) AS "Market_Business_Unit_Description"
FROM
    (
    SELECT
        TUW.BUSUNIT_CF AS "Market_Business_Unit",
        TUW.BUSUNIT_LM AS "Market_Business_Unit_Description"
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
        AND TUW.SECSTS_CT <> 25
        AND TUW.SSD_CF NOT IN (14,
        25)
        AND TUW.ACTIV2_CF = '400'
        AND TUW.MRKUNT_NT IS NOT NULL
        AND TUW.BUSUNIT_CF IS NOT NULL
        AND TUW.END_D = '9999-12-31'
        AND TUW.SUPP_D = '9999-12-31'
    GROUP BY
        TUW.BUSUNIT_CF,
        TUW.BUSUNIT_LM);