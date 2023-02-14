# flink-clojure-examples
Examples of Apache Flink jobs developed with Clojure.

This is a demonstration 
of [flink-clojure](https://github.com/keytiong/flink-clojure) with 
[flink-training](https://github.com/apache/flink-training) taxi ride examples.

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