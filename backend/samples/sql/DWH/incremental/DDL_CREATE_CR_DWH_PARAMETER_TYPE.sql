DROP TABLE DWH_PARAMETER_TYPE IF EXISTS;

CREATE TABLE DWH_PARAMETER_TYPE (
   ID                   SMALLINT              
      NOT NULL,
   CODE                 VARCHAR(32)           
      NOT NULL,
   NAME                 VARCHAR(64)           
      NOT NULL,
   CREATED_DATE         TIMESTAMP              DEFAULT CURRENT_TIMESTAMP
,
   GROUP_CODE           VARCHAR(32),
   CONSTRAINT PK_DWH_PARAMETER_TYPE PRIMARY KEY (ID)  
)
 ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;

COMMENT ON TABLE DWH_PARAMETER_TYPE IS
'Type of the IFRS 17 Parameter';

COMMENT ON COLUMN DWH_PARAMETER_TYPE.CREATED_DATE IS
'System generated date';

COMMENT ON COLUMN DWH_PARAMETER_TYPE.GROUP_CODE IS
'Possibility to group the different parameter types';
