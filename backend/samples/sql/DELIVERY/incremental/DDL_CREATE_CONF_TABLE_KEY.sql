DROP TABLE CONF_TABLE_KEY IF EXISTS;

CREATE TABLE CONF_TABLE_KEY
(
    TABLE_NAME   VARCHAR(100) NOT NULL,
    KEY_NAME     VARCHAR(50)  NOT NULL,
    COLUMN_NAME  VARCHAR(100) NOT NULL,
    COLUMN_ORDER SMALLINT     NOT NULL,
    CONSTRAINT PK_CONF_TABLE_KEY PRIMARY KEY (TABLE_NAME,KEY_NAME,COLUMN_NAME,COLUMN_ORDER)
)
    ORGANIZE BY COLUMN IN TBS_<env>
    DISTRIBUTE BY HASH (TABLE_NAME, KEY_NAME, COLUMN_NAME, COLUMN_ORDER);