DROP TABLE DWH_USAGE_TYPE IF EXISTS;

CREATE TABLE DWH_USAGE_TYPE (
   ID           SMALLINT    NOT NULL,
   CODE         VARCHAR(32) NOT NULL,
   NAME         VARCHAR(64) NOT NULL,
   CREATED_DATE TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
   CONSTRAINT PK_DWH_USAGE_TYPE PRIMARY KEY (ID)  
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;

COMMENT ON TABLE DWH_USAGE_TYPE IS
'Usage Type (see domain)';

COMMENT ON COLUMN DWH_USAGE_TYPE.CREATED_DATE IS
'System generated date';