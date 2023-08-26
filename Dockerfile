FROM python:3.9

COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

RUN apt-get update && apt-get install -y wget vim cron

RUN echo "alias ll='ls -lrt'" >> ~/.bashrc

WORKDIR /opt/
COPY keep_alive.py /opt/keep_alive.py
COPY main.py /opt/main.py
COPY postgresql-42.5.2.jar /opt/postgresql-42.5.2.jar
RUN chmod +x /opt/keep_alive.py
RUN chmod +x /opt/main.py
RUN chmod 777 /opt/postgresql-42.5.2.jar

RUN pip install poetry
COPY pyproject.toml /opt/pyproject.toml
COPY poetry.lock /opt/poetry.lock
RUN poetry install

COPY data /opt/data

CMD ["python", "/opt/keep_alive.py"]