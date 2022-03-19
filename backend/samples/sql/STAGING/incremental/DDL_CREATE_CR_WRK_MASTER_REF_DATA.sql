SET SCHEMA STAGING_<env>;

DROP TABLE WRK_MASTER_REF_DATA IF EXISTS;

CREATE TABLE WRK_MASTER_REF_DATA (
	"Reference_Data" VARCHAR(26) NOT NULL,
	"Code" VARCHAR(129) NOT NULL,
	"Name" VARCHAR(100)
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;

