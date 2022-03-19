SET SCHEMA STAGING_<env>;

DROP TABLE  WRK_TCSM_MACRO_AOC_TO_OMEGA IF EXISTS;

CREATE TABLE "WRK_TCSM_MACRO_AOC_TO_OMEGA"  
(
	"SSD_CF" INTEGER,
	"ESB_CF" INTEGER,
	"NUMLINE_NT" INTEGER,
	"BALSHEY_NF" INTEGER,
	"BALSHRMTH_NF" INTEGER,
	"BALSHRDAY_NF" INTEGER,
	"VALPERY_NF" INTEGER,
	"VALPERMTH_NF" VARCHAR(10),
	"TRNCOD_CF" VARCHAR(8),
	"RETAUTGEN_B" SMALLINT,
	"CTR_NF" VARCHAR(9),
	"END_NT" INTEGER,
	"SEC_NF" INTEGER,
	"UWY_NF" INTEGER,
	"UW_NT" INTEGER,
	"OCCYEA_NF" INTEGER,
	"ACY_NF" INTEGER,
	"SCOSTRMTH_NF" INTEGER,
	"SCOENDMTH_NF" INTEGER,
	"CLM_NF" INTEGER,
	"CUR_CF" VARCHAR(3),
	"AMT_M" DECFLOAT(34),
	"RETCTR_NF" VARCHAR(9),
	"RETEND_NT" INTEGER,
	"RETSEC_NF" INTEGER,
	"RTY_NF" INTEGER,
	"RETUW_NT" INTEGER,
	"PLC_NT" INTEGER,
	"RETOCCYEA_NF" INTEGER,
	"RETACY_NF" INTEGER,
	"RETSCOSTRMTH_NF" INTEGER,
	"RETSCOENDMTH_NF" INTEGER,
	"RCL_NF" INTEGER,
	"RETCUR_CF" VARCHAR(3),
	"RETAMT_M" DECFLOAT(34),
	"COMMAC_LL" VARCHAR(64),
	"SPEENTTYP_CF" INTEGER,
	"SPEENTNAT_CT" INTEGER,
	"EVT_NT" VARCHAR(10),
	"REVT_NT" VARCHAR(1),
	"REQUEST_ID" VARCHAR(50),
	"COMPOSITE_KEY"   VARBINARY(64),
	"FILE_NAME"  VARCHAR(25)
	)   
ORGANIZE BY COLUMN IN TBS_<env> 
DISTRIBUTE ON RANDOM;