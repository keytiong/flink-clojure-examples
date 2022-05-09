(ns example.taxi-ride.hourly-tips
  (:require [example.taxi-ride.common :as common]
            [io.kosong.flink.clojure.core :as fk])
  (:import (org.apache.flink.api.common.eventtime WatermarkStrategy TimestampAssignerSupplier)
           (org.apache.flink.streaming.api.windowing.assigners TumblingEventTimeWindows)
           (org.apache.flink.streaming.api.windowing.time Time)
           (org.apache.flink.streaming.api.functions.sink PrintSinkFunction)
           (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment))
  (:gen-class))


(fk/fdef fare-driver-id-selector
  :fn :key-selector
  :returns (fk/type-info-of {})
  :getKey (fn [this fare]
            (:driver-id fare)))


(fk/fdef add-tips
  :fn :process-window
  :returns (fk/type-info-of [])
  :process (fn [this key ctx fares out]
             (let [tips (reduce
                          (fn [a fare]
                            (+ a (:tip fare)))
                          0
                          fares)]
               (.collect out [(-> ctx .window .getEnd) key tips]))))

(fk/fdef max-tip
  :fn :simple-reduce
  :returns (fk/type-info-of [])
  :reduce (fn [this v1 v2]
            (if (< (get v1 2) (get v2 2))
              v2
              v1)))

(defn hourly-tips [env]
  (let [ts-assigner (fk/timestamp-assigner
                      :extractTimestamp (fn [fare _]
                                          (some-> fare :start-time .toEpochMilli)))
        watermark   (-> (WatermarkStrategy/forMonotonousTimestamps)
                      (.withTimestampAssigner (TimestampAssignerSupplier/of ts-assigner)))]
    (-> env
      (.addSource common/taxi-fare-generator)
      (.assignTimestampsAndWatermarks watermark)
      (.keyBy fare-driver-id-selector)
      (.window (TumblingEventTimeWindows/of (Time/hours 1)))
      (.process add-tips)
      (.windowAll (TumblingEventTimeWindows/of (Time/hours 1)))
      (.reduce max-tip)
      (.addSink (PrintSinkFunction.)))))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    (hourly-tips env)
    (.execute env "Hourly Tips")))