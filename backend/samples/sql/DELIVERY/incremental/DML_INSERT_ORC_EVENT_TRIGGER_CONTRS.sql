DELETE FROM EVENT_TRIGGER WHERE EVENT_CODE IN ('EVT_I17_ATTRIBUTES_CONTROL','EVT_I17_PRODUCT_CONTROL'
,'EVT_CODES_CONTROL','EVT_OMEGA_AE_CONTROL');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_I17_ATTRIBUTES_CONTROL', '{
    "eventCode": "EVT_I17_ATTRIBUTES_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_I17_PRODUCT_CONTROL', '{
    "eventCode": "EVT_I17_PRODUCT_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_CODES_CONTROL', '{
    "eventCode": "EVT_CODES_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_OMEGA_AE_CONTROL', '{
    "eventCode": "EVT_OMEGA_AE_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');