FROM cassandra:latest

# Working directory
WORKDIR /usr/src/app

# Copy files
COPY . .

# Install dependencies
RUN apt-get update && apt-get install -y \
    apt-get update \
    apt-get install python3 python3-pip

RUN pip3 install -r requirements.txt


# Add a Docker volume in the working directory
VOLUME /usr/src/app

CMD [ "python3", "first_create_tables.py"]

