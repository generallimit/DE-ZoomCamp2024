-- Docs: https://docs.mage.ai/guides/sql-blocks

-- DROP TABLE ny_taxi.green_cab_data

SELECT count(*)
FROM ny_taxi.green_cab_data
WHERE trip_distance > 0
AND passenger_count > 0;

SELECT DISTINCT vendor_id
FROM ny_taxi.green_cab_data;

--astute-impulse-412600.terra_dataset.test_connection;