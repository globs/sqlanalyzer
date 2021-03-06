DROP TABLE ERROR_MESSAGE IF EXISTS;

CREATE TABLE ERROR_MESSAGE
(
    ERROR_MESSAGE_ID        BIGINT       NOT NULL,
    REJECTION_TYPE          VARCHAR(64),
    THRESHOLD               INTEGER,
    ACTION_TYPE_CODE        VARCHAR(16),
    ACTION_TYPE_LABEL       VARCHAR(64),
    ERROR_MESSAGE_LABEL     VARCHAR(255),
    MESSAGE_TYPE_FLAG       VARCHAR(1),
    CREATION_DT             DATE,
    LAST_UPDATE_DT          DATE
)  
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;