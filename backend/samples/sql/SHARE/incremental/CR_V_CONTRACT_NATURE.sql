SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_CONTRACT_NATURE AS
SELECT
    CAST(CTRNAT_CT AS CHAR(1)) AS "Contract_Nature",
    CAST(CTRNAT_LS AS VARCHAR(16)) AS "Contract_Nature_Description"
FROM
    BI_<env>.TUWSEC
WHERE
    ACTIV2_CF = '400'
    AND SUBLEDGER_CF NOT IN ('05-01',
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
    AND supp_d = '9999-12-31'
    AND END_D = '9999-12-31'
GROUP BY
    CTRNAT_CT,
    CTRNAT_LS;