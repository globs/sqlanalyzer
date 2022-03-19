SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_CLIENT AS
SELECT
    "Client_Number",
    "Client_Name",
    "Company_Name"
FROM
    (
    SELECT
        CLI_NF AS "Client_Number",
        CLISHONAM_LD AS "Client_Name",
        CASE
            WHEN CLILGST1_LA IS NOT NULL
            AND CLILGST2_LA IS NULL THEN CLILGST1_LA
            ELSE CLILGST1_LA || ' ' || CLILGST2_LA
        END AS "Company_Name"
    FROM
        BI_<env>.TCLIENT)
GROUP BY
    "Client_Number",
    "Client_Name",
    "Company_Name";