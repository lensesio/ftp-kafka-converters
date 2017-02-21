# Kafka-Connect  FTP -> Kafka

As per tutorial on <a href="http://www.landoop.com/blog/2017/02/ftp-to-kafka/">http://www.landoop.com/blog/2017/02/ftp-to-kafka/</a>

## Usage

If using the docker <a href="https://github.com/Landoop/fast-data-dev">Kafka Development Environment</a> build the code with `sbt clean assembly` and then start the **fast-data-dev** docker by passing the location of the generated JAR file into the classpath of Kafka Connect

    docker run --rm -it -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 \
           -p 9092:9092 -e ADV_HOST=192.168.99.100 -e RUNTESTS=0 -e FORWARDLOGS=0 \
           -v /Users/Antonios/ftp-kafka-converters/target/scala-2.11/:/connectors landoop/fast-data-dev
