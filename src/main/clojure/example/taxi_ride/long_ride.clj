(ns example.taxi-ride.long-ride
  (:require [example.taxi-ride.common :as common]
            [io.kosong.flink.clojure.core :as fk])
  (:import (java.time Duration)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.streaming.api.functions.sink PrintSinkFunction)
           (org.apache.flink.api.common.state ValueStateDescriptor)
           (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.api.common.eventtime WatermarkStrategy TimestampAssignerSupplier))
  (:gen-class))

(defn ride-too-long [ride-start ride-end]
  (-> (Duration/between (:event-time ride-start) (:event-time ride-end))
    (.compareTo (Duration/ofHours 2))
    (> 0)))

(defn timer-time [ride]
  (if (:is-start ride)
    (.. (:event-time ride) (plusSeconds (* 120 60)) toEpochMilli)
    (throw (RuntimeException. "Can not get start time from END event."))))

(def ride-id-selector
  (fk/flink-fn
    {:fn      :key-selector
     :returns (fk/type-info-of {})
     :getKey  (fn [this ride]
                (:ride-id ride))}))

(def long-ride-alert
  (fk/flink-fn
    {:fn             :keyed-process
     :returns        (fk/type-info-of Long)
     :init           (fn [_] (atom {}))
     :open           (fn [this config]
                       (let [state            (.state this)
                             ride-event-desc  (ValueStateDescriptor. "ride event" (fk/type-info-of {}))
                             ride-event-state (.. this getRuntimeContext (getState ride-event-desc))]
                         (swap! state assoc :ride-event-state ride-event-state)))
     :processElement (fn [this ride context out]
                       (let [state            (.state this)
                             ride-event-state (:ride-event-state @state)
                             first-ride-event (.value ride-event-state)]
                         (if (nil? first-ride-event)
                           (do
                             (.update ride-event-state ride)
                             (when (:is-start ride)
                               (.. context timerService (registerEventTimeTimer (timer-time ride)))))
                           (do
                             (if (:is-start ride)
                               (when (ride-too-long ride first-ride-event)
                                 (.collect out (:ride-id ride)))
                               (do
                                 (.. context timerService (deleteEventTimeTimer (timer-time first-ride-event)))
                                 (when (ride-too-long first-ride-event ride)
                                   (.collect out (:ride-id ride)))))
                             (.clear ride-event-state)))))
     :onTimer        (fn [this timestamp context out]
                       (let [state            (.state this)
                             ride-event-state (:ride-event-state @state)]
                         (.collect out (:ride-id (.value ride-event-state)))
                         (.clear ride-event-state)))}))

(defn long-ride [env]
  (let [ts-assigner (fk/timestamp-assigner
                      :extractTimestamp (fn [ride _]
                                          (some-> ride :event-time .toEpochMilli)))
        watermark   (-> (WatermarkStrategy/forBoundedOutOfOrderness (Duration/ofSeconds 60))
                      (.withTimestampAssigner (TimestampAssignerSupplier/of ts-assigner)))]
    (-> env
      (.addSource common/taxi-ride-generator)
      (.assignTimestampsAndWatermarks watermark)
      (.keyBy ride-id-selector)
      (.process long-ride-alert)
      (.addSink (PrintSinkFunction.)))))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    (long-ride env)
    (.execute env "Long Ride")))
