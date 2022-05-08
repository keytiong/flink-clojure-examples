(ns example.word-count
  (:require [io.kosong.flink.clojure.core :as fk]
            [clojure.tools.logging :as log])
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.java.utils ParameterTool))
  (:gen-class))

(def word-count-data
  ["To be, or not to be,--that is the question:--",
   "Whether 'tis nobler in the mind to suffer",
   "The slings and arrows of outrageous fortune",
   "Or to take arms against a sea of troubles,",
   "And by opposing end them?--To die,--to sleep,--",
   "No more; and by a sleep to say we end",
   "The heartache, and the thousand natural shocks",
   "That flesh is heir to,--'tis a consummation",
   "Devoutly to be wish'd. To die,--to sleep;--",
   "To sleep! perchance to dream:--ay, there's the rub;",
   "For in that sleep of death what dreams may come,",
   "When we have shuffled off this mortal coil,",
   "Must give us pause: there's the respect",
   "That makes calamity of so long life;",
   "For who would bear the whips and scorns of time,",
   "The oppressor's wrong, the proud man's contumely,",
   "The pangs of despis'd love, the law's delay,",
   "The insolence of office, and the spurns",
   "That patient merit of the unworthy takes,",
   "When he himself might his quietus make",
   "With a bare bodkin? who would these fardels bear,",
   "To grunt and sweat under a weary life,",
   "But that the dread of something after death,--",
   "The undiscover'd country, from whose bourn",
   "No traveller returns,--puzzles the will,",
   "And makes us rather bear those ills we have",
   "Than fly to others that we know not of?",
   "Thus conscience does make cowards of us all;",
   "And thus the native hue of resolution",
   "Is sicklied o'er with the pale cast of thought;",
   "And enterprises of great pith and moment,",
   "With this regard, their currents turn awry,",
   "And lose the name of action.--Soft you now!",
   "The fair Ophelia!--Nymph, in thy orisons",
   "Be all my sins remember'd."])

(fk/fdef tokenizer
  :fn :flat-map
  :returns (fk/type-info-of [])
  :flatMap (fn [_ value collector]
             (doseq [v (-> value .toLowerCase (.split "\\W+"))]
               (.collect collector [v 1]))))

(fk/fdef counter
  :fn :reduce
  :returns (fk/type-info-of [])
  :reduce (fn [_ [word-1 count-1] [_ count-2]]
            [word-1 (+ count-1 count-2)]))

(fk/fdef output
  :fn :sink
  :invoke (fn [_ value _]
            (log/info value)))

(defn my-key [value]
  (first value))

(fk/fdef word-selector
  :fn :key-selector
  :returns (fk/type-info-of String)
  :getKey (fn [_ value]
            (my-key value)))

(defn job-graph [env]
  (-> env
    (.fromCollection word-count-data)
    (.flatMap tokenizer)
    (.keyBy word-selector)
    (.reduce counter)
    (.print)
    #_(.addSink output))
  env)

(defn -main [& args]
  (let [args   (into-array String args)
        params (ParameterTool/fromArgs args)
        config (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment config)]
    (.. env getConfig (setGlobalJobParameters params))
    (fk/register-clojure-types env)
    (job-graph env)
    (.execute env "Word Count")))
