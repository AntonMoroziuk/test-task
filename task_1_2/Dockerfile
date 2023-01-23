FROM ubuntu:latest

RUN apt-get update && apt-get -y install cron python3 python3-pip

RUN pip3 install pandas sqlalchemy psycopg2-binary

COPY task_1_2.py .

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/task_1_2

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/task_1_2

# Apply cron job
RUN crontab /etc/cron.d/task_1_2

# Run the command on container startup
CMD ["cron", "-f"]