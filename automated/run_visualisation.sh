#!/bin/sh
echo "Getting analysis 1 data..."
query1=`cat driver/hive/10_visualisation.sql`
docker-compose exec hive-server hive -e "${query1}" | sed -n "/OK/,/Time/p" | tail -n +2 | head -n -1 > ./analysis1_data
mv ./analysis1_data visualisation_tmp

echo "Getting analysis 2 data..."
query2=`cat driver/hive/20_visualisation.sql` &&
docker-compose exec hive-server hive -e "${query2}" | sed -n "/OK/,/Time/p" | tail -n +2 | head -n -1 > ./analysis2_data
mv ./analysis2_data visualisation_tmp

echo "Getting analysis 3 data..."
query3=`cat driver/hive/30_visualisation.sql`
docker-compose exec hive-server hive -e "${query3}" | sed -n "/OK/,/Time/p" | tail -n +2 | head -n -1 > ./analysis3_data
mv ./analysis3_data visualisation_tmp

echo "Generating plots..."
python3 ./driver/src/run