FROM apache/spark:4.1.1-python3

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

RUN pip install pyspark numpy pandas scikit-learn pyarrow matplotlib seaborn liac-arff
#RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar --output /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.1.jar

RUN mkdir -p /opt/spark/conf
RUN echo "spark.driver.bindAddress=0.0.0.0" >> /opt/spark/conf/spark-defaults.conf

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

EXPOSE 4040 7077 8080

USER spark
