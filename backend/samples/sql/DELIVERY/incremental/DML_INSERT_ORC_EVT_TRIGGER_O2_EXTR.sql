DELETE FROM EVENT_TRIGGER WHERE EVENT_CODE IN ('EVT_DIP_TO_OMEGA_FOR_DSC', 'EVT_DIP_TO_OMEGA_FOR_LKR'
, 'EVT_DIP_TO_OMEGA_FOR_FWD', 'EVT_DIP_TO_OMEGA_FOR_RA', 'EVT_DIP_TO_OMEGA_FOR_RATIO');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_DSC', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_DSC",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_LKR', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_LKR",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_FWD', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_FWD",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_RA', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_RA",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_RATIO', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_RATIO",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');
