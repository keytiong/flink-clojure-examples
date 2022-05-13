(ns example.taxi-ride.ride-and-fare
  (:require [example.taxi-ride.common :as common]
            [io.kosong.flink.clojure.core :as fk])
  (:import (org.apache.flink.streaming.api.functions.sink PrintSinkFunction)
           (org.apache.flink.api.common.state ValueStateDescriptor)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.java.utils ParameterTool))
  (:gen-class))


(def start-ride-filter
  (fk/flink-fn
    {:fn      :filter
     :returns (fk/type-info-of {})
     :filter  (fn [this ride]
                (:is-start ride))}))

(def ride-id-selector
  (fk/flink-fn
    {:fn      :key-selector
     :returns (fk/type-info-of {})
     :getKey  (fn [this ride]
                (:ride-id ride))}))

(def ride-and-fare-enrichment
  (fk/flink-fn
    {:fn       :co-flat-map
     :returns  (fk/type-info-of {})
     :init     (fn [this] (atom {}))
     :open     (fn [this config]
                 (let [state      (.state this)
                       ride-desc  (ValueStateDescriptor. "saved ride" (fk/type-info-of {}))
                       fare-desc  (ValueStateDescriptor. "saved fare" (fk/type-info-of {}))
                       ride-state (.. this getRuntimeContext (getState ride-desc))
                       fare-state (.. this getRuntimeContext (getState fare-desc))]
                   (swap! state assoc :ride-state ride-state)
                   (swap! state assoc :fare-state fare-state)))
     :flatMap1 (fn [this ride out]
                 (let [state      (.state this)
                       fare-state (:fare-state @state)
                       ride-state (:ride-state @state)
                       fare       (.value fare-state)]
                   (if fare
                     (do
                       (.clear fare-state)
                       (.collect out {:ride ride :fare fare}))
                     (.update ride-state ride))))
     :flatMap2 (fn [this fare out]
                 (let [state      (.state this)
                       fare-state (:fare-state @state)
                       ride-state (:ride-state @state)
                       ride       (.value ride-state)]
                   (if ride
                     (do
                       (.clear ride-state)
                       (.collect out {:ride ride :fare fare}))
                     (.update fare-state fare))))}))

(defn ride-and-fare [env]
  (let [rides (-> env
                (.addSource common/taxi-ride-generator)
                (.filter start-ride-filter)
                (.keyBy ride-id-selector))
        fares (-> env
                (.addSource common/taxi-fare-generator)
                (.keyBy ride-id-selector))]
    (-> rides
      (.connect fares)
      (.flatMap ride-and-fare-enrichment)
      (.uid "enrichment")
      (.name "enrichment")
      (.addSink (PrintSinkFunction.)))))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    (ride-and-fare env)
    (.execute env "Ride and Fare")))