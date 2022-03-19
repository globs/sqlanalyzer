DROP TABLE logs IF EXISTS;

CREATE TABLE logs
(
    moment     TIMESTAMP(6),
    pid        VARCHAR(20),
    root_pid   VARCHAR(20),
    father_pid VARCHAR(20),
    project    VARCHAR(50),
    job        VARCHAR(255),
    context    VARCHAR(50),
    priority   INTEGER,
    type       VARCHAR(255),
    origin     VARCHAR(255),
    message    VARCHAR(255),
    code       INTEGER
)    
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;