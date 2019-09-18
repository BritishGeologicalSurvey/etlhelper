#! /bin/sh
docker build -t etlhelper-test-runner . || exit 1
docker run \
  -e TEST_PG_PASSWORD=${TEST_PG_PASSWORD} \
  --net=host \
  --name=etlhelper-test-runner \
  etlhelper-test-runner \
  pytest -vs --cov=etlhelper --cov-report html --cov-report term test/

# Copy coverage files out of container to local if tests passed
if [ $? -eq 0 ]; then
  docker cp etlhelper-test-runner:/app/htmlcov .
fi

# Clear up the container
docker rm etlhelper-test-runner 2>&1 > /dev/null
