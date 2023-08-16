FROM python:3.9-slim-bullseye

# Install package dependencies
RUN apt-get update -y && \
    apt-get install -y \
     apt-transport-https \
     build-essential \
     curl \
     git \
     libaio1

# Add repo for Microsoft ODBC driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc > microsoft.asc && \
    apt-key add microsoft.asc && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -y && \
    ACCEPT_EULA=y apt-get install -y \
      msodbcsql17 \
      unixodbc-dev

# Install Python modules
ENV APP=/app
ENV PYTHONPATH=$APP
WORKDIR $APP
RUN mkdir etlhelper

# Install requirements
COPY requirements.txt $APP/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy app files to container
COPY setup.py versioneer.py setup.cfg .flake8 .coveragerc README.md pytest.ini $APP/
COPY etlhelper/ $APP/etlhelper
COPY test/ $APP/test

# Clear old caches, if present
RUN find . -regextype posix-egrep -regex '.*/__pycache__.*' -delete

# Set up Oracle Client
RUN python -m pip install .