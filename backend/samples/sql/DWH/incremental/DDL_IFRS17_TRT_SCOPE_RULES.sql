DROP TABLE IFRS17_TRT_SCOPE_RULES IF EXISTS;

CREATE TABLE IFRS17_TRT_SCOPE_RULES (
	ID 					INT 			NOT NULL,
	O2_SUBLEDGER		VARCHAR(32)
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;