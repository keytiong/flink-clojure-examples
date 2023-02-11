(defproject io.kosong.flink/flink-clojure-examples "0.1.0-SNAPSHOT"
  :description "A collection of Apache Flink examples in Flink Clojure"
  :url "https://github.com/keytiong/flink-clojure-examples"
  :license {:name "MIT"}

  :source-paths ["src/main/clojure"]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [io.kosong.flink/flink-clojure "0.2.0"]
                 [org.clojure/tools.logging "1.2.4"]]

  :profiles {
    :provided {
      :dependencies [[org.apache.flink/flink-clients "1.16.1"]]}

    :uberjar {
      :aot [example.word-count
            example.taxi-ride.hourly-tips
            example.taxi-ride.long-ride
            example.taxi-ride.ride-and-fare
            example.taxi-ride.ride-cleansing
            example.taxi-ride.ride-count]}}

  )