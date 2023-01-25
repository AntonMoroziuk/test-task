FROM ubuntu:latest

RUN apt-get update && apt-get -y install cron python3 python3-pip

RUN pip3 install pandas sqlalchemy psycopg2-binary

COPY task_1_2.py .

ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG POSTGRES_PORT

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/task_1_2

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/task_1_2

# Apply cron job
RUN crontab /etc/cron.d/task_1_2

# Copy env variables because cron can't access them otherwise
RUN touch .env
RUN echo "POSTGRES_USER=${POSTGRES_USER}" >> .env
RUN echo "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" >> .env
RUN echo "POSTGRES_PORT=${POSTGRES_PORT}" >> .env

# Run the command on container startup
CMD ["cron", "-f"]