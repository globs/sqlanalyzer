SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_CEDENT AS
SELECT
    "Cedent-Retrocessionnaire",
    CAST("Cedent-Retrocessionnaire_Description" AS VARCHAR(25)) AS "Cedent-Retrocessionnaire_Description",
    "First_Underwriting_Year",
    "Min_Creation_Date",
    CAST("Cedent_Retrocessionnaire_Country" AS CHAR(3)) AS "Cedent_Retrocessionnaire_Country",
    "Cedent-Retrocessionnaire_Group",
    "Ultimate_Cedent-Retrocesionnaire"
FROM
    (
    SELECT
        TCO.CED_NF AS "Cedent-Retrocessionnaire",
        TUWC.CLISHONAM_LD AS "Cedent-Retrocessionnaire_Description",
        MIN(TCO.FRSUWY_NF) AS "First_Underwriting_Year",
        MIN(DATE_PART('YEAR', TCO.CRE_D)) AS "Min_Creation_Date",
        TUWC.CLICTY_CF AS "Cedent_Retrocessionnaire_Country",
        TUWC.HLD_NF AS "Cedent-Retrocessionnaire_Group",
        TUWC.ULT_NF AS "Ultimate_Cedent-Retrocesionnaire"
    FROM
        BI_<env>.TCONTR TCO
    INNER JOIN BI_<env>.TUWCLI TUWC ON
        (TUWC.CLI_NF = TCO.CED_NF
        AND TCO.CTRTYP_CT = 1
        AND TCO.SUPP_D = '9999-12-31')
    INNER JOIN BI_<env>.TUWSEC TUW ON
        TUW.CTR_NF = TCO.CTR_NF
        AND TUW.UWY_NF = TCO.UWY_NF
        AND TUW.UW_NT = TCO.UW_NT
        AND TUW.END_NT = TCO.END_NT
        AND TUW.SSD_CF = TCO.SSD_CF
    WHERE
        TCO.CTRTYP_CT = 1
        AND TUW.SUBLEDGER_CF NOT IN ('05-01',
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
        AND TUW.ACTIV2_CF IN ('400')
        AND TO_CHAR(TUW.END_D,
        'yyyy-mm-dd') = '9999-12-31'
        AND TUW.CED_NF <> 0
    GROUP BY
        TCO.CED_NF,
        TUWC.CLISHONAM_LD,
        TUWC.ULT_NF,
        TUWC.HLD_NF,
        TUWC.CLICTY_CF
UNION
    SELECT
        TPL.RTO_NF AS "Cedent-Retrocessionnaire",
        TUWC.CLISHONAM_LD AS "Cedent-Retrocessionnaire_Description",
        MIN(TRETS.RTY_NF) AS "First_Underwriting_Year",
        MIN(DATE_PART('YEAR', TRETC.CRE_D)) AS "Min_Creation_Date",
        TUWC.CLICTY_CF AS "Cedent_Retrocessionnaire_Country",
        TUWC.HLD_NF AS "Cedent-Retrocessionnaire_Group",
        TUWC.ULT_NF AS "Ultimate_Cedent-Retrocesionnaire"
    FROM
        BI_<env>.TRETCTR TRETC
    INNER JOIN BI_<env>.TRETSEC TRETS ON
        TRETC.RTY_NF = TRETS.RTY_NF
        AND TRETC.RETCTR_NF = TRETS.RETCTR_NF
        AND TRETC.SSD_CF = TRETS.SSD_CF
    INNER JOIN BI_<env>.TPLACEMT TPL ON
        TRETC.RTY_NF = TPL.RTY_NF
        AND TRETC.RETCTR_NF = TPL.RETCTR_NF
        AND TRETC.SSD_CF = TPL.SSD_CF
    INNER JOIN BI_<env>.TUWCLI TUWC ON
        TPL.RTO_NF = TUWC.CLI_NF
    INNER JOIN BI_<env>.TUWRETSEC TUWR ON
        TUWR.RETCTR_NF = TRETS.RETCTR_NF
        AND TUWR.RTY_NF = TRETS.RTY_NF
        AND TUWR.RETSEC_NF = TRETS.RETSEC_NF
        AND TUWR.SSD_CF = TRETS.SSD_CF
    WHERE
        TUWR.ACTIV2_CF IN ('400')
        AND SUBSTR(CAST((100 +(TRETC.SSD_CF)) AS CHAR(3)),
        2,
        2)|| '-' || SUBSTR(CAST((100 +(TRETC.ESB_CF)) AS CHAR(3)),
        2,
        2) NOT IN ('05-01',
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
        AND TRETC.SSD_CF NOT IN (14,
        25)
        AND TO_CHAR(TUWR.END_D,
        'yyyy-mm-dd') = '9999-12-31'
    GROUP BY
        TPL.RTO_NF,
        TUWC.CLISHONAM_LD,
        TUWC.ULT_NF,
        TUWC.HLD_NF,
        TUWC.CLICTY_CF);