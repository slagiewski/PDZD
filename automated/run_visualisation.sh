#!/bin/sh
echo "Getting analysis 1 data..."
query1 = `cat driver/hive/10_visualisation.sql`
docker-compose exec hive-server hive -e "${query1}" > ./analysis1_data

echo "Getting analysis 2 data..."
query2 = `cat driver/hive/20_visualisation.sql`
docker-compose exec hive-server hive -e "${query2}" > ./analysis2_data

echo "Getting analysis 3 data..."
query3 = `cat driver/hive/30_visualisation.sql`
docker-compose exec hive-server hive -e "${query3}" > ./analysis3_data

echo "Generating plots..."
python3 ./driver/src/run_visualisation.py

echo "Done!"
python3 ./driver/src/run_visualisation.py