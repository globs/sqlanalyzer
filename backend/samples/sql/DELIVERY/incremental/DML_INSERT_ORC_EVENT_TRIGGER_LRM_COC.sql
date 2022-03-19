DELETE FROM EVENT_TRIGGER WHERE EVENT_CODE IN ('EVT_LRM_TO_DIP_COC_VALIDATED');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_LRM_TO_DIP_COC_VALIDATED', '{
"dipRunId" : null,
"lrmMode": "INCR",
"region": "LRM",
"eventCode" : "EVT_LRM_TO_DIP_COC_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/ilias/in",
"dataFileList" : []
}');
