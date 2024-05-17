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
      msodbcsql18 \
      unixodbc-dev

# Install Python modules
ENV APP=/app
ENV PYTHONPATH=$APP
WORKDIR $APP
RUN mkdir etlhelper

# Install requirements
COPY requirements-dev.txt $APP/
RUN pip install --upgrade pip
RUN pip install -r requirements-dev.txt

# Copy files required to package for PyPI
COPY .git/ $APP/.git
COPY pyproject.toml README.md $APP/

# Copy files required for testing
COPY .flake8 pytest.ini $APP/
COPY test/ $APP/test

# Copy app files to container
COPY etlhelper/ $APP/etlhelper

# Clear old caches, if present
RUN find . -regextype posix-egrep -regex '.*/__pycache__.*' -delete

# Install ETL Helper
RUN python -m pip install .