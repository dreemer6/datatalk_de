docker run -itd \
    --name datatalk_postgres \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -p 5432:5432 \
    -v /ny_taxi_postgres_data:/var/lib/postgresql/data 
    postgres

pgcli -h localhost -p 5432 -u root -d ny_taxi

docker run -itd \
    --name datatalk-pgadmin \
    -p 8080:80 \
    -e 'PGADMIN_DEFAULT_EMAIL=admin@admin.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=root' \
    dpage/pgadmin4

docker network create datatalk_pg
docker network connect --alias pg_database datatalk_postgres
docker network connect --alias pg_admin datatalk_pg datatalk-pgadmin

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=fhv_trips \
    --url=https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-04.parquet

docker run -it --network=datatalk_pg taxi_ingest:0.0.1 \
        --user=root \
        --password=root \
        --host=pg_database \
        --port=5432 \
        --db=ny_taxi \
        --table_name=fhv_trips \
        --url=https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-04.parquet