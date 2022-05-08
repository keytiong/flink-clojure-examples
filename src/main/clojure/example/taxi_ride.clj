(ns example.taxi-ride
  (:require [io.kosong.flink.clojure.core :as fk])
  (:import (java.time Instant Duration)
           (java.util Random PriorityQueue Collections ArrayList)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.streaming.api.functions.sink PrintSinkFunction)
           (org.apache.flink.api.common.state ValueStateDescriptor)
           (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.api.common.eventtime WatermarkStrategy TimestampAssignerSupplier)
           (org.apache.flink.streaming.api.windowing.assigners TumblingEventTimeWindows)
           (org.apache.flink.streaming.api.windowing.time Time)
           (org.apache.flink.api.common.functions ReduceFunction)
           (org.apache.flink.api.java.typeutils ResultTypeQueryable)
           (org.apache.flink.api.common.typeinfo TypeInformation)
           (clojure.lang PersistentVector))
  (:gen-class))

(def seconds-between-rides 20)

(def number-of-drivers 200)

(def lon-east -73.7)

(def lon-west -74.05)

(def lat-north 41.0)

(def lat-south 40.5)

(def lon-width (- 74.05 73.7))

(def lat-height (- 41.0 40.5))

(def delta-lon 0.0014)

(def delta-lat 0.00125)

(def number-of-grid-x 250)

(def number-of-grid-y 400)

(def deg-len 110.25)

(defn a-long
  ([ride-id min max]
   (let [mean   (-> (+ min max) (/ 2.0))
         stddev (-> (- max min) (/ 8.0))]
     (a-long ride-id min max mean stddev)))
  ([ride-id min max mean stddev]
   (let [rnd      (Random. ride-id)
         rnd-long #(Math/round (+ (* stddev (.nextGaussian rnd)) mean))]
     (loop [value (rnd-long)]
       (if (<= min value max)
         value
         (recur (rnd-long)))))))

(defn a-float
  ([ride-id min max]
   (let [mean   (-> (+ min max)
                  (/ 2.0))
         stddev (-> (- max min)
                  (/ 8.0))]
     (a-float ride-id min max mean stddev)))
  ([ride-id min max mean stddev]
   (let [rnd       (Random. ride-id)
         rnd-float #(-> rnd
                      (.nextGaussian)
                      (* stddev)
                      (+ mean)
                      (float))]
     (loop [value (rnd-float)]
       (if (<= min value max)
         value
         (recur (rnd-float)))))))

(defn b-float
  [ride-id min max]
  (let [mean   (-> (+ min max)
                 (/ 2.0))
        stddev (-> (- max min)
                 (/ 8.0))]
    (a-float (+ ride-id 42) min max mean stddev)))

(def beginning (Instant/parse "2020-01-01T12:00:00.00Z"))

(defn start-time [ride-id]
  (.plusSeconds beginning (* seconds-between-rides ride-id)))

(defn ride-duration-minutes [ride-id]
  (a-long ride-id 0 600 20 40))

(defn end-time [ride-id]
  (.plusSeconds (start-time ride-id) (* 60 (ride-duration-minutes ride-id))))

(defn driver-id [ride-id]
  (-> (Random. ride-id)
    (.nextInt number-of-drivers)
    (+ 2013000000)))

(defn taxi-id [ride-id]
  (driver-id ride-id))

(defn start-lat [ride-id]
  (a-float ride-id (- lat-south 0.1) (+ lat-north 0.1)))

(defn start-lon [ride-id]
  (a-float ride-id (- lon-west 0.1) (+ lon-east 0.1)))

(defn end-lat [ride-id]
  (b-float ride-id (- lat-south 0.1) (+ lat-north 0.1)))

(defn end-lon [ride-id]
  (b-float ride-id (- lon-west 0.1) (+ lon-east 0.1)))

(defn passenger-cnt [ride-id]
  (a-long ride-id 1 4))

(defn payment-type [ride-id]
  (if (= (mod ride-id 2) 0)
    "CARD"
    "CASH"))

(defn tip [ride-id]
  (a-long ride-id 0 60 10 15))

(defn tolls [ride-id]
  (if (= (mod ride-id 10) 0)
    (a-long ride-id 0 5)
    0))

(defn total-fare [ride-id]
  (+ 3.0 (* 1.0 (ride-duration-minutes ride-id))
    (tip ride-id)
    (tolls ride-id)))

(defn ->taxi-ride [ride-id is-start]
  {:ride-id       ride-id
   :is-start      is-start
   :event-time    (if is-start (start-time ride-id) (end-time ride-id))
   :start-lon     (start-lon ride-id)
   :start-lat     (start-lat ride-id)
   :end-lon       (end-lon ride-id)
   :end-lat       (end-lat ride-id)
   :passenger-cnt (passenger-cnt ride-id)
   :taxi-id       (taxi-id ride-id)
   :driver-id     (driver-id ride-id)})

(defn ->taxi-fare [ride-id]
  {:ride-id      ride-id
   :taxi-id      (taxi-id ride-id)
   :driver-id    (driver-id ride-id)
   :start-time   (start-time ride-id)
   :payment-type (payment-type ride-id)
   :tip          (tip ride-id)
   :tolls        (tolls ride-id)
   :total-fare   (total-fare ride-id)})

(defn is-in-nyc [lon lat]
  (and
    (not (or (> lon lon-east) (< lon lon-west)))
    (not (or (> lat lat-north) (< lat lat-south)))))


(fk/fdef taxi-ride-generator
  :fn :source

  :returns (fk/type-info-of {})

  :init (fn [this] (atom {}))

  :open (fn [this _]
          (let [state (.state this)
                cmp   (comparator (fn [x y]
                                    (let [time-x (:event-time x)
                                          time-y (:event-time y)]
                                      (.isBefore time-x time-y))))
                q     (PriorityQueue. cmp)]
            (swap! state assoc :running true)
            (swap! state assoc :id 0)
            (swap! state assoc :end-event-q q)
            (swap! state assoc :max-start-time 0)))

  :run (fn [this ctx]
         (let [state                  (.state this)
               batch-size             5
               sleep-millis-per-event 10]
           (while (:running @state)
             (let [id           (:id @state)
                   end-event-q  (:end-event-q @state)
                   start-events (map
                                  (fn [ride-id] (->taxi-ride ride-id true))
                                  (range id (+ id batch-size)))
                   max-start    (reduce
                                  (fn [max-start ride]
                                    (max max-start (.toEpochMilli (:event-time ride))))
                                  0
                                  start-events)]

               (doseq [ride-id (range id (+ id batch-size))]
                 (.add end-event-q (->taxi-ride ride-id false)))

               (while (<= (-> end-event-q .peek :event-time .toEpochMilli) max-start)
                 (.collect ctx (.poll end-event-q)))

               (let [out-of-order-events (ArrayList. start-events)]
                 (Collections/shuffle out-of-order-events (Random. id))
                 (doseq [event out-of-order-events]
                   (.collect ctx event)))

               (swap! state assoc :end-event-q end-event-q)
               (swap! state assoc :id (+ id batch-size))

               (Thread/sleep (* batch-size sleep-millis-per-event))

               ))))

  :cancel (fn [this]
            (let [state (.state this)]
              (swap! state assoc :running false))))

(fk/fdef taxi-fare-generator
  :fn :source

  :returns (fk/type-info-of {})

  :init (fn [this] (atom {}))

  :open (fn [this _]
          (let [state (.state this)]
            (swap! state assoc :running true)))

  :run (fn [this ctx]
         (let [state                  (.state this)
               sleep-millis-per-event 10]
           (loop [running (:running @state)
                  ride-id 1]
             (when running
               (let [fare (->taxi-fare ride-id)]
                 (.collect ctx fare)
                 (Thread/sleep sleep-millis-per-event)))
             (recur (:running @state) (inc ride-id)))))

  :cancel (fn [this]
            (let [state (.state this)]
              (swap! state assoc :running false))))


(fk/fdef driver-tuple
  :fn :map
  :returns (fk/type-info-of [])
  :map (fn [this ride]
         [(:driver-id ride) 1]))

(fk/fdef driver-id-selector
  :fn :key-selector
  :returns (fk/type-info-of Long)
  :getKey (fn [this [driver-id _]]
            driver-id))

(fk/fdef fare-driver-id-selector
  :fn :key-selector
  :returns (fk/type-info-of {})
  :getKey (fn [this fare]
            (:driver-id fare)))

(fk/fdef ride-id-selector
  :fn :key-selector
  :returns (fk/type-info-of {})
  :getKey (fn [this ride]
            (:ride-id ride)))

(fk/fdef driver-ride-counter
  :fn :reduce
  :returns (fk/type-info-of [])
  :reduce (fn [this [driver-id-1 count-1] [driver-id-2 count-2]]
            [driver-id-1 (+ count-1 count-2)]))

(fk/fdef nyc-filter
  :fn :filter
  :returns (fk/type-info-of {})
  :filter (fn [this ride]
            (and
              (is-in-nyc (:start-lon ride) (:start-lat ride))
              (is-in-nyc (:end-lon ride) (:end-lat ride)))))

(fk/fdef start-ride-filter
  :fn :filter
  :returns (fk/type-info-of {})
  :filter (fn [this ride]
            (:is-start ride)))

(fk/fdef ride-and-fare-enrichment
  :fn :co-flat-map
  :returns (fk/type-info-of {})
  :init (fn [this] (atom {}))
  :open (fn [this config]
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
                  (.update fare-state fare)))))

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

(defn ride-too-long [ride-start ride-end]
  (-> (Duration/between (:event-time ride-start) (:event-time ride-end))
    (.compareTo (Duration/ofHours 2))
    (> 0)))

(defn timer-time [ride]
  (if (:is-start ride)
    (.. (:event-time ride) (plusSeconds (* 120 60)) toEpochMilli)
    (throw (RuntimeException. "Can not get start time from END event."))))

(fk/fdef long-ride-alert
  :fn :keyed-process
  :returns (fk/type-info-of Long)
  :init (fn [_] (atom {}))
  :open (fn [this config]
          (let [state            (.state this)
                ride-event-desc  (ValueStateDescriptor. "ride event" (fk/type-info-of {}))
                ride-event-state (.. this getRuntimeContext (getState ride-event-desc))]
            (swap! state assoc :ride-event-state ride-event-state)))
  :processElement (fn [this ride context out]
                    (let [state            (.state this)
                          ride-event-state (:ride-event-state @state)
                          first-ride-event (.value ride-event-state)]
                      (if first-ride-event
                        (do
                          (if (:is-start ride)
                            (when (ride-too-long first-ride-event ride)
                              (.collect out (:ride-id ride)))
                            (do
                              (.. context timerService (deleteEventTimTimer (timer-time first-ride-event)))
                              (when (ride-too-long first-ride-event ride)
                                (.collect out (:ride-id ride)))))
                          (.clear ride-event-state)))))
  :onTimer (fn [this timestamp context out]
             (let [state            (.state this)
                   ride-event-state (:ride-event-state @state)]
               (.collect out (:ride-id (.value ride-event-state)))
               (.clear ride-event-state))))

;;
;;
;;

(defn ride-count [env]
  (-> env
    (.addSource taxi-ride-generator)
    (.map driver-tuple)
    (.keyBy driver-id-selector)
    (.reduce driver-ride-counter)
    (.print)))

(defn ride-cleansing [env]
  (-> env
    (.addSource taxi-ride-generator)
    (.filter nyc-filter)
    (.addSink (PrintSinkFunction.))))

(defn ride-and-fare [env]
  (let [rides (-> env
                (.addSource taxi-ride-generator)
                (.filter start-ride-filter)
                (.keyBy ride-id-selector))
        fares (-> env
                (.addSource taxi-fare-generator)
                (.keyBy ride-id-selector))]
    (-> rides
      (.connect fares)
      (.flatMap ride-and-fare-enrichment)
      (.uid "enrichment")
      (.name "enrichment")
      (.addSink (PrintSinkFunction.)))))

(defn hourly-tips [env]
  (let [ts-assigner (fk/timestamp-assigner
                      :extractTimestamp (fn [fare _]
                                          (some-> fare :start-time .toEpochMilli)))
        watermark   (-> (WatermarkStrategy/forMonotonousTimestamps)
                      (.withTimestampAssigner (TimestampAssignerSupplier/of ts-assigner)))]
    (-> env
      (.addSource taxi-fare-generator)
      (.assignTimestampsAndWatermarks watermark)
      (.keyBy fare-driver-id-selector)
      (.window (TumblingEventTimeWindows/of (Time/hours 1)))
      (.process add-tips)
      (.windowAll (TumblingEventTimeWindows/of (Time/hours 1)))
      (.reduce max-tip)
      (.addSink (PrintSinkFunction.)))))

(defn long-ride [env]
  (let [ts-assigner (fk/timestamp-assigner
                      :extractTimestamp (fn [ride _]
                                          (some-> ride :event-time .toEpochMilli)))
        watermark   (-> (WatermarkStrategy/forBoundedOutOfOrderness (Duration/ofSeconds 60))
                      (.withTimestampAssigner (TimestampAssignerSupplier/of ts-assigner)))]
    (-> env
      (.addSource taxi-ride-generator)
      (.assignTimestampsAndWatermarks watermark)
      (.keyBy ride-id-selector)
      (.process long-ride-alert)
      (.addSink (PrintSinkFunction.)))))

(defn -main [& args]
  (let [params (ParameterTool/fromArgs (into-array String args))
        conf   (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (fk/register-clojure-types env)
    #_(ride-count env)
    #_(ride-cleansing env)
    #_(ride-and-fare env)
    #_(hourly-tips env)
    (long-ride env)
    (.execute env "Taxi Ride")))
