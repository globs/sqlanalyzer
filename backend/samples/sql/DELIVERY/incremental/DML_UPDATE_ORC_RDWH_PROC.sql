UPDATE BATCH SET POST_BATCH_PROCESSING='[POST-COMPLETION]api-dip:checkIpdsAutoTrigger'
WHERE BATCH_ID IN (1, 11, 21, 31, 41, 51);
COMMIT;