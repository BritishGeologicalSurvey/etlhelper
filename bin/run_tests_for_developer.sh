#! /bin/sh
echo "Flake8 checks"
flake8 etlhelper test || exit 1

echo "Building container"
docker build \
  --build-arg INSTANT_CLIENT_ZIP=${INSTANT_CLIENT_ZIP} \
  -t etlhelper-test-runner . || exit 1

echo "Unit and integration tests"
docker run \
  -e TEST_PG_PASSWORD="${TEST_PG_PASSWORD}" \
  -e TEST_ORACLE_DBTYPE="${TEST_ORACLE_DBTYPE}" \
  -e TEST_ORACLE_HOST="${TEST_ORACLE_HOST}" \
  -e TEST_ORACLE_PORT="${TEST_ORACLE_PORT}" \
  -e TEST_ORACLE_DBNAME="${TEST_ORACLE_DBNAME}" \
  -e TEST_ORACLE_USER="${TEST_ORACLE_USER}" \
  -e TEST_ORACLE_PASSWORD="${TEST_ORACLE_PASSWORD}" \
  -e TEST_MSSQL_DBTYPE="${TEST_MSSQL_DBTYPE}" \
  -e TEST_MSSQL_DBDRIVER="${TEST_MSSQL_DBDRIVER}" \
  -e TEST_MSSQL_HOST="${TEST_MSSQL_HOST}" \
  -e TEST_MSSQL_PORT="${TEST_MSSQL_PORT}" \
  -e TEST_MSSQL_DBNAME="${TEST_MSSQL_DBNAME}" \
  -e TEST_MSSQL_PASSWORD="${TEST_MSSQL_PASSWORD}" \
  --net=host \
  --name=etlhelper-test-runner \
  -it \
  etlhelper-test-runner \
  pytest -vs -rsx --pdb --cov=etlhelper --cov-report html --cov-report term test/

# Copy coverage files out of container to local if tests passed
if [ $? -eq 0 ]; then
  docker cp etlhelper-test-runner:/app/htmlcov .
fi

# Clear up the container
docker rm etlhelper-test-runner 2>&1 > /dev/null
