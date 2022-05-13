(ns example.taxi-ride.common
  (:require [io.kosong.flink.clojure.core :as fk])
  (:import (java.time Instant)
           (java.util Random PriorityQueue Collections ArrayList Collection Comparator)
           (org.apache.flink.streaming.api.functions.source SourceFunction)))

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
         rnd-long #(Math/round ^double (+ (* stddev (.nextGaussian rnd)) mean))]
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


(def ^SourceFunction taxi-ride-generator
  (fk/flink-fn
    {:fn      :source

     :returns (fk/type-info-of {})

     :init    (fn [this] (atom {}))

     :open    (fn [this _]
                (let [state (.state this)
                      cmp   (comparator (fn [x y]
                                          (let [time-x (:event-time x)
                                                time-y (:event-time y)]
                                            (.isBefore time-x time-y))))
                      q     (PriorityQueue. ^Comparator cmp)]
                  (swap! state assoc :running true)
                  (swap! state assoc :id 0)
                  (swap! state assoc :end-event-q q)
                  (swap! state assoc :max-start-time 0)))

     :run     (fn [this ctx]
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

                      (let [out-of-order-events (ArrayList. ^Collection start-events)]
                        (Collections/shuffle out-of-order-events (Random. id))
                        (doseq [event out-of-order-events]
                          (.collect ctx event)))

                      (swap! state assoc :end-event-q end-event-q)
                      (swap! state assoc :id (+ id batch-size))

                      (Thread/sleep (* batch-size sleep-millis-per-event))))))

     :cancel  (fn [this]
                (let [state (.state this)]
                  (swap! state assoc :running false)))}))

(def ^SourceFunction taxi-fare-generator
  (fk/flink-fn
    {:fn      :source

     :returns (fk/type-info-of {})

     :init    (fn [this] (atom {}))

     :open    (fn [this _]
                (let [state (.state this)]
                  (swap! state assoc :running true)))

     :run     (fn [this ctx]
                (let [state                  (.state this)
                      sleep-millis-per-event 10]
                  (loop [running (:running @state)
                         ride-id 1]
                    (when running
                      (let [fare (->taxi-fare ride-id)]
                        (.collect ctx fare)
                        (Thread/sleep sleep-millis-per-event)))
                    (recur (:running @state) (inc ride-id)))))

     :cancel  (fn [this]
                (let [state (.state this)]
                  (swap! state assoc :running false)))}))