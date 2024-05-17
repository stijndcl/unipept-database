#! /usr/bin/env bash

echo "Creating database"
cat ../schemas/structure_no_index_no_constraints.sql | docker exec -i thesis-psql psql
echo "Doading data"
time bash ./parallel_load.sh "/tmp/Unipept Database/out"
echo "Applying constraints"
cat ../schemas/structure_constraints_only.sql | docker exec -i thesis-psql psql
echo "Creating indices"
time bash ./parallel_index.sh
