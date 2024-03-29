(ns example.taxi-ride.ride-count
  (:require [example.taxi-ride.common :as common]
            [io.kosong.flink.clojure.core :as fk])
  (:gen-class)
  (:import (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.java.functions KeySelector)))

(def driver-tuple
  (fk/flink-fn
    {:fn      :map
     :returns (fk/type-info-of [])
     :map     (fn [this ride]
                [(:driver-id ride) 1])}))

(def ^KeySelector driver-id-selector
  (fk/flink-fn
    {:fn      :key-selector
     :returns (fk/type-info-of Long)
     :getKey  (fn [this [driver-id _]]
                driver-id)}))

(def driver-ride-counter
  (fk/flink-fn
    {:fn      :reduce
     :returns (fk/type-info-of [])
     :reduce  (fn [this [driver-id-1 count-1] [driver-id-2 count-2]]
                [driver-id-1 (+ count-1 count-2)])}))

(defn ride-count [env]
  (-> env
    (.addSource common/taxi-ride-generator)
    (.map driver-tuple)
    (.keyBy driver-id-selector)
    (.reduce driver-ride-counter)
    (.print)))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    (ride-count env)
    (.execute env "Ride Count")))