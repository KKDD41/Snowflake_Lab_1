CREATE OR REPLACE TASK reload_data_from_stage_to_core_dwh
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0,30 * * * * UTC' -- Runs at the start and 30th minute of every hour
AS
  CALL reload_data_from_stage_to_core_dwh_procedure();

CREATE OR REPLACE TASK reload_data_from_core_dwh_to_data_mart
  WAREHOUSE = COMPUTE_WH
  AFTER = reload_data_from_stage_to_core_dwh
AS
  CALL reload_data_from_core_dwh_to_data_mart_procedure();

ALTER TASK EPAM_LAB.CORE_DWH.reload_data_from_core_dwh_to_data_mart RESUME;
ALTER TASK EPAM_LAB.CORE_DWH.reload_data_from_stage_to_core_dwh RESUME;

SHOW TASKS;

