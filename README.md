# flink-clojure-examples
Examples of Apache Flink application developed with Clojure

## Build
```shell
lein uberjar
```

## Run
```shell
$FLINK_HOME/bin/flink run \
 --class example.word_count \
 --detached \
 target/flink-clojure-examples-0.1.0-SNAPSHOT-standalone.jar
```