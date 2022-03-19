DROP TABLE stats IF EXISTS;

CREATE TABLE stats
(
    moment            TIMESTAMP(6),
    pid               VARCHAR(20),
    father_pid        VARCHAR(20),
    root_pid          VARCHAR(20),
    system_pid        BIGINT,
    project           VARCHAR(50),
    job               VARCHAR(255),
    job_repository_id VARCHAR(255),
    job_version       VARCHAR(255),
    context           VARCHAR(50),
    origin            VARCHAR(255),
    message_type      VARCHAR(255),
    message           VARCHAR(255),
    duration          BIGINT
)
     ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;