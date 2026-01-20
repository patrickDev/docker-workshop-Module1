# docker-workshop-Module1

#docker compose up --build ingest   (buid and run python file ingest)

#docker compose logs ingest (check the logs)

#uv run pgcli -h localhost -p 5432 -u root -d ny_taxi 

#docker network create pg-network (create a network named pg-network)

#docker run ls (list all networks)

# SQL 
 ``Implicit INNER JOIN

 SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
    yellow_taxi_trips_2021_1 t,
    taxi_zone_lookup zpu,
    taxi_zone_lookup zdo
WHERE
    t."PULocationID" = zpu."LocationID"
    AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;


 Explicit INNER JOIN
SELECT 
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.total_amount
FROM yellow_taxi_trips_2021_1 t
LEFT JOIN taxi_zone_lookup zpu
    ON t."PULocationID" = zpu."LocationID"
LEFT JOIN taxi_zone_lookup zdo
    ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;
