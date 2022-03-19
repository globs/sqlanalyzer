SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_CONTRACT AS (
SELECT
    'A' AS Assumed_Retro_FLAG,
    TUWSEC.CTR_NF AS Contract,
    TUWSEC.UWY_NF,
    TUWSEC.UW_NT,
    TUWSEC.END_NT,
    TUWSEC.SSD_CF,
    TUWSEC.SEC_NF,
    TUWSEC.START_D,
    TUWSEC.END_D,
    FIRST_INCEPTION_DATE,
    MIN_FIRST_UW_YEAR,
    MIN_CREATION_DATE,
    MAX_SECTION
FROM
    BI_<env>.TUWSEC TUWSEC
LEFT OUTER JOIN (
    SELECT
        MIN(TCONTR.FRSUWY_NF) AS MIN_FIRST_UW_YEAR,
        MIN(DATE_PART('YEAR', TCONTR.CRE_D)) AS MIN_CREATION_DATE,
        CTR_NF,
        CTRTYP_CT
    FROM
        BI_<env>.TCONTR TCONTR
    WHERE
        supp_d = '9999-12-31'
    GROUP BY
        CTR_NF,
        CTRTYP_CT) TCONTR ON
    TUWSEC.CTR_NF = TCONTR.CTR_NF
LEFT OUTER JOIN (
    SELECT
        to_char(MIN(TUWCTR.FRSINC_D),
        'yyyymmdd') AS FIRST_INCEPTION_DATE,
        ctr_nf
    FROM
        BI_<env>.TUWCTR TUWCTR
    WHERE
        TUWCTR.END_D = '9999-12-31'
    GROUP BY
        CTR_NF) TUWCTR ON
    TUWSEC.CTR_NF = TUWCTR.CTR_NF
LEFT OUTER JOIN (
    SELECT
        CTR_NF,
        MAX(TSECTION.SEC_NF) AS MAX_SECTION
    FROM
        BI_<env>.TSECTION TSECTION
    WHERE
        supp_d = '9999-12-31'
    GROUP BY
        CTR_NF) TSECTION ON
    TUWSEC.CTR_NF = TSECTION.CTR_NF
WHERE
    TCONTR.CTRTYP_CT = 1
    AND TUWSEC.SUBLEDGER_CF NOT IN ('05-01',
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
    AND TUWSEC.ACTIV2_CF = '400'
UNION
SELECT
    'R' AS Assumed_Retro_FLAG,
    TUWRETSEC.RETCTR_NF,
    TUWRETSEC.RTY_NF,
    '1' AS UW_NT,
    '0' AS END_NT,
    TUWRETSEC.SSD_CF,
    TUWRETSEC.RETSEC_NF AS SECTION,
    START_D,
    END_D,
    FIRST_INCEPTION_DATE,
    FIRST_UNDERWRITING_YEAR,
    MIN_CREATION_DATE,
    MAX_SECTION
FROM
    BI_<env>.TUWRETSEC TUWRETSEC
LEFT OUTER JOIN (
    SELECT
        MIN(TRETCTR.RTY_NF) AS FIRST_UNDERWRITING_YEAR,
        TO_CHAR(MIN(TRETCTR.CTRINC_D),
        'YYYYMMDD') AS FIRST_INCEPTION_DATE,
        MIN(DATE_PART('YEAR', TRETCTR.CRE_D)) AS MIN_CREATION_DATE,
        retctr_nf
    FROM
        BI_<env>.TRETCTR TRETCTR
    GROUP BY
        RETCTR_NF) TRETCTR ON
    TUWRETSEC.RETCTR_NF = TRETCTR.RETCTR_NF
LEFT OUTER JOIN (
    SELECT
        MAX(TRETSEC.RETSEC_NF) AS MAX_SECTION,
        retctr_nf
    FROM
        BI_<env>.TRETSEC TRETSEC
    GROUP BY
        retctr_nf ) TRETSEC ON
    TUWRETSEC.RETCTR_NF = TRETSEC.RETCTR_NF
WHERE
    TUWRETSEC.ACTIV2_CF = '400'
    AND SUBSTR(CAST((100 +(TUWRETSEC.SSD_CF)) AS CHAR(3)),
    2,
    2)|| '-' || SUBSTR(CAST((100 +(TUWRETSEC.ESB_CF)) AS CHAR(3)),
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
    '26-10') );