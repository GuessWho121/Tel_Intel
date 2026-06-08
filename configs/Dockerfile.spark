FROM apache/spark:4.1.2

USER root

RUN apt-get update && apt-get install -y python3-pip curl && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir \
                pyyaml \
                pyarrow \
                pandas \
                pytest

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar

RUN curl -L -o /opt/spark/jars/aws-sdk-v2-bundle-2.29.52.jar \
     https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar

USER spark