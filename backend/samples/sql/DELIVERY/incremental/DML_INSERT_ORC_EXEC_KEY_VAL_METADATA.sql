DELETE FROM EXECUTION_KEY_VALUE_METADATA WHERE GROUP='FILE_WATCHER';
INSERT INTO EXECUTION_KEY_VALUE_METADATA ("GROUP", "KEY", VALUE)
	VALUES('FILE_WATCHER', 'LAST_FILE_WATCHED_TIMESTAMP', '2021-01-01 01:00:00.000000');