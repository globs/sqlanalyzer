CALL DELIVERY_<env>.SP_ORCHESTRATION_BATCH_METADATA_FOR_MU_REPADJ();
CALL DELIVERY_<env>.SP_BATCH_METADATA_FOR_BODS_BPE();
CALL DELIVERY_<env>.SP_ORC_BATCH_METADATA_FOR_FFX_EC();
CALL DELIVERY_<env>.SP_BATCH_GROUPING_REORG();
CALL DELIVERY_<env>.SP_BATCH_METADATA_FOR_RDWH_DTM();
COMMIT;