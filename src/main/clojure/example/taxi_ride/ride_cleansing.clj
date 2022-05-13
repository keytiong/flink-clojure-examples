(ns example.taxi-ride.ride-cleansing
  (:require [example.taxi-ride.common :as common]
            [io.kosong.flink.clojure.core :as fk])
  (:import (org.apache.flink.streaming.api.functions.sink PrintSinkFunction)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.java.utils ParameterTool))
  (:gen-class))

(def nyc-filter
  (fk/flink-fn
    {:fn      :filter
     :returns (fk/type-info-of {})
     :filter  (fn [this ride]
                (and
                  (common/is-in-nyc (:start-lon ride) (:start-lat ride))
                  (common/is-in-nyc (:end-lon ride) (:end-lat ride))))}))

(defn ride-cleansing [env]
  (-> env
    (.addSource common/taxi-ride-generator)
    (.filter nyc-filter)
    (.addSink (PrintSinkFunction.))))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    (ride-cleansing env)
    (.execute env "Ride Cleansing")))