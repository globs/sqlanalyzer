UPDATE ORCHESTRATION_PROCESS_EXECUTION SET STATUS='AWAITING' WHERE STATUS='PROCESSING';
UPDATE ORCHESTRATION_PROCESS_STEP_EXECUTION SET STATUS='FAILED' WHERE STATUS='ERROR';
UPDATE BATCH_EXECUTION SET STATUS='FAILED' WHERE STATUS='ERROR';
UPDATE EVENT_INSTANCE SET STATUS='FAILED' WHERE STATUS='ERROR';
UPDATE BUSINESS_CALENDAR_EXECUTION SET STATUS='FAILED' WHERE STATUS='ERROR';
COMMIT;