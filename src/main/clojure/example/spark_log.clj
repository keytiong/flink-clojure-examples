(ns example.spark-log
  (:require [clojure.data.json :as json]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [io.kosong.flink.clojure.core :as fk]
            [camel-snake-kebab.core :as csk]
            [clojure.tools.logging :as log])
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.common.typeinfo Types)
           (org.elasticsearch.client RestClient Request)
           (org.apache.http HttpHost)
           (org.apache.http.util EntityUtils)
           (java.io FileNotFoundException)
           (org.apache.flink.api.common.state ListStateDescriptor)
           (org.apache.flink.streaming.connectors.elasticsearch ElasticsearchSinkBase$FlushBackoffType ElasticsearchSinkFunction)
           (org.elasticsearch.action.index IndexRequest)
           (org.elasticsearch.common.xcontent XContentType)
           (org.apache.flink.streaming.connectors.elasticsearch7 ElasticsearchSink$Builder)
           (org.apache.flink.api.java.utils ParameterTool))
  (:gen-class))


(def timestamp-kw (keyword "@timestamp"))

(defn elasticsearch-client [{:keys [host port scheme]}]
  (let [http-host (HttpHost. host port scheme)]
    (-> (RestClient/builder (into-array HttpHost [http-host]))
      (.build))))

;;
;; parameters
;;

(def job-history-server "http://localhost:18080/api/v1")

;;
;; Spark History Server Source
;;

(defn- transform-spark-application-record [app]
  (let [now      (System/currentTimeMillis)
        app-id   (:id app)
        app-name (:name app)
        f        (fn [app-attempt]
                   (let [app-attempt-id (:attempt_id app-attempt)]
                     (-> app-attempt
                       (assoc timestamp-kw now)
                       (assoc :name app-name)
                       (assoc :application_id app-id)
                       (assoc :application_attempt_id app-attempt-id)
                       (assoc :spark_type "application"))))]
    (map f (:attempts app))))

(defn- transform-spark-job-record [app job]
  (let [now            (System/currentTimeMillis)
        app-id         (:application_id app)
        app-attempt-id (:application_attempt_id app)]
    (-> job
      (assoc timestamp-kw now)
      (assoc :application_id app-id)
      (assoc :application_attempt_id app-attempt-id)
      (assoc :spark_type "job"))))

(defn- transform-spark-stage-record [app job stage]
  (let [now                          (System/currentTimeMillis)
        app-id                       (:application_id app)
        app-attempt-id               (:application_attempt_id app)
        job-id                       (:job_id job)
        stage-attempt-id             (:attempt_id stage)
        flatten-task-map             (fn [m]
                                       (->> m
                                         (map (fn [[k t]] (assoc t :task_id k)))
                                         vec))
        flatten-executor-summary-map (fn [m]
                                       (->> m
                                         (map (fn [[k s]] (assoc s :executor_id k)))
                                         vec))]
    (-> stage
      (assoc timestamp-kw now)
      (update-in [:tasks] flatten-task-map)
      (update-in [:executor_summary] flatten-executor-summary-map)
      (assoc :application_id app-id)
      (assoc :application_attempt_id app-attempt-id)
      (assoc :job_id job-id)
      (assoc :stage_attempt_id stage-attempt-id)
      (assoc :spark_type "stage")
      (dissoc :attempt_id))))

(defn- transform-spark-task-record [app job stage task]
  (let [now              (System/currentTimeMillis)
        app-id           (:application_id app)
        app-attempt-id   (:application_attempt_id app)
        job-id           (:job_id job)
        stage_id         (:stage_id stage)
        stage-attempt-id (:stage_attempt_id stage)
        task-attempt-id  (:attempt task)]
    (-> task
      (assoc timestamp-kw now)
      (assoc :application_id app-id)
      (assoc :application_attempt_id app-attempt-id)
      (assoc :job_id job-id)
      (assoc :stage_id stage_id)
      (assoc :stage_attempt_id stage-attempt-id)
      (assoc :task_attempt_id task-attempt-id)
      (assoc :spark_type "task")
      (dissoc :attempt))))

(defn transform-spark-environment-record [app env]
  (let [app-id         (:application_id app)
        app-attempt-id (:application_attempt_id app)
        prop-key-xform (partial reduce (fn [m [key value]]
                                         (assoc m (str/replace key "." "_") value))
                         {})
        collect-first  (partial reduce (fn [v [t0 _]] (conj v t0)) [])]
    (-> env
      (assoc :application_id app-id)
      (assoc :application_attempt_id app-attempt-id)
      (assoc :spark_type "environment")
      (update-in [:spark_properties] prop-key-xform)
      (update-in [:hadoop_properties] prop-key-xform)
      (update-in [:system_properties] prop-key-xform)
      (update-in [:classpath_entries] collect-first))))

(defn fetch-application-records [last-update-time]
  (let [now  (System/currentTimeMillis)
        url  (str job-history-server "/applications")
        resp (slurp url)
        apps (->> (json/read-str resp :key-fn csk/->snake_case_keyword))]
    (->> (mapcat transform-spark-application-record apps)
      (filter #(< last-update-time (:last_updated_epoch %)))
      (map (fn [app] (assoc app timestamp-kw now))))))

(defn fetch-environment-record [app]
  (let [app-id (:application_id app)
        url    (str job-history-server "/applications/" app-id "/environment")
        resp   (slurp url)
        now    (System/currentTimeMillis)
        env    (some-> resp
                 (json/read-str :key-fn csk/->snake_case_keyword)
                 (assoc timestamp-kw now))]
    (transform-spark-environment-record app env)))

(defn fetch-job-records [app]
  (let [app-id         (:application_id app)
        app-attempt-id (:application_attempt_id app)
        url            (if app-attempt-id
                         (str job-history-server "/applications/" app-id "/" app-attempt-id "/jobs")
                         (str job-history-server "/applications/" app-id "/jobs"))
        resp           (slurp url)
        now            (System/currentTimeMillis)
        jobs           (json/read-str resp :key-fn csk/->snake_case_keyword)]
    (->> jobs
      (map (partial transform-spark-job-record app))
      (map (fn [job] (assoc job timestamp-kw now))))))


(defn fetch-stage-records [app job]
  (let [app-id         (:application_id app)
        app-attempt-id (:application_attempt_id app)
        fetch-fn       (fn [stage-id]
                         (let [url            (if app-attempt-id
                                                (str job-history-server "/applications/" app-id "/" app-attempt-id "/stages/" stage-id)
                                                (str job-history-server "/applications/" app-id "/stages/" stage-id))
                               resp           (try
                                                (slurp url)
                                                (catch FileNotFoundException e
                                                  (log/error e)))
                               now            (System/currentTimeMillis)
                               stage-attempts (some-> resp
                                                (json/read-str :key-fn csk/->snake_case_keyword))]
                           (map #(assoc % timestamp-kw now) stage-attempts)))
        stages         (mapcat fetch-fn (:stage_ids job))]
    (map (partial transform-spark-stage-record app job) stages)))

(defn history-server-initialize-state [this context]
  (let [state                       (.state this)
        checkpoint-state-descriptor (ListStateDescriptor. "last-updated" Types/LONG)
        checkpoint-state            (-> context
                                      (.getOperatorStateStore)
                                      (.getListState checkpoint-state-descriptor))]
    (swap! state assoc :checkpoint-state checkpoint-state)
    (when-not (-> checkpoint-state .get first)
      (.update checkpoint-state [0]))
    (when (.isRestored context)
      (let [last-update-time (-> checkpoint-state .get first)]
        (swap! state assoc :last-update-time last-update-time)))))

(defn history-server-snapshot-state [this context]
  (let [state            (.state this)
        last-update-time (:last-update-time @state)
        checkpoint-state (:checkpoint-state @state)]
    (.clear checkpoint-state)
    (.add checkpoint-state last-update-time)))

(defn history-server-run [this context]
  (let [state (.state this)]
    (swap! state assoc :running true)
    (while (:running @state)
      (let [prev-update-time (or (:last-update-time @state) 0)
            next-update-time (ref prev-update-time)
            apps             (->> (fetch-application-records prev-update-time)
                               (sort-by :last_updated_epoch))]
        (doseq [app apps]
          (let [this-update-time (:last_updated_epoch app)]
            (when (< prev-update-time this-update-time)
              (dosync (ref-set next-update-time this-update-time))
              (.collect context app #_(json/json-str app :key-fn name)))))
        (swap! state assoc :last-update-time @next-update-time))
      (Thread/sleep 5000))))

(defn history-server-cancel [this]
  (let [state (.state this)]
    (swap! state assoc :running false)))

;;
;; Spark Log Processor
;;

(defn same-document? [doc-1 doc-2]
  (= (dissoc doc-1 timestamp-kw) (dissoc doc-2 timestamp-kw)))

(defn ->index-id [prefix bucket-size time]
  (let [bucket-id (* (quot time bucket-size) bucket-size)]
    (str prefix bucket-id)))

(defn app-doc-id [app]
  (let [app-id         (:application_id app)
        app-attempt-id (or (:application_attempt_id app) "")]
    (str app-id ":" app-attempt-id)))

(defn env-doc-id [env]
  (let [app-id (:application_id env)]
    (str app-id ":_environment")))

(defn job-doc-id [app job]
  (let [job-id (:job_id job)]
    (str (app-doc-id app) ":" job-id)))

(defn stage-doc-id [app job stage]
  (let [stage-id         (:stage_id stage)
        stage-attempt-id (:stage_attempt_id stage)]
    (str (job-doc-id app job) ":" stage-id ":" stage-attempt-id)))

(defn task-doc-id [app job stage task]
  (let [task-id         (:task_id task)
        task-attempt-id (:task_attempt_id task)]
    (str (stage-doc-id app job stage) ":" task-id ":" task-attempt-id)))

(defn es-lookup-doc [processor index-id doc-id]
  (let [state     (.state processor)
        path      (str "/" index-id "/_source/" doc-id)
        es-client (:es-client @state)
        head-req  (doto (Request. "HEAD" path)
                    (.addParameter "ignore" "404"))
        found     (-> es-client
                    (.performRequest head-req)
                    (.getStatusLine)
                    (.getStatusCode)
                    (= 200))
        get-req   (doto (Request. "GET" path)
                    (.addParameter "ignore" "404"))
        body-json (when found
                    (-> es-client
                      (.performRequest get-req)
                      (.getEntity)
                      (EntityUtils/toString)
                      (json/read-str :key-fn keyword)))]
    body-json))

(defn- through-cache-lookup [processor cache-id index-id doc-id]
  (let [state (.state processor)
        exist (cache/has? (cache-id @state) doc-id)]
    (if exist
      (swap! state update-in [cache-id] cache/hit doc-id)
      (if-let [doc (es-lookup-doc processor index-id doc-id)]
        (swap! state update-in [cache-id] cache/miss doc-id doc)))
    (cache/lookup (cache-id @state) doc-id)))

(defn lookup-task-doc [processor index-id doc-id]
  (through-cache-lookup processor :task-cache index-id doc-id))

(defn lookup-stage-doc [processor index-id doc-id]
  (through-cache-lookup processor :stage-cache index-id doc-id))

(defn lookup-job-doc [processor index-id doc-id]
  (through-cache-lookup processor :job-cache index-id doc-id))

(defn lookup-app-doc [processor index-id doc-id]
  (through-cache-lookup processor :app-cache index-id doc-id))

(defn invalidate-cache [processor cache-id doc-id]
  (let [state (.state processor)
        exist (cache/has? (cache-id @state) doc-id)]
    (when exist
      (swap! state update-in [cache-id] cache/evict doc-id))))

(defn invalidate-app-cache [processor doc-id]
  (invalidate-cache processor :app-cache doc-id))

(defn invalidate-job-cache [processor doc-id]
  (invalidate-cache processor :job-cache doc-id))

(defn invalidate-stage-cache [processor doc-id]
  (invalidate-cache processor :stage-cache doc-id))

(defn invalidate-task-cache [processor doc-id]
  (invalidate-cache processor :task-cache doc-id))

(defn es-persist-doc [processor index-id doc-id doc]
  (let [state     (.state processor)
        path      (str "/" index-id "/_update/" doc-id)
        body      {:doc           doc
                   :doc_as_upsert true}
        body-json (json/json-str body :key-fn name)
        request   (doto (Request. "POST" path)
                    (.setJsonEntity body-json))
        es-client (:es-client @state)]
    (.performRequest es-client request)))

(defn process-environment [processor app context out]
  (let [state       (.state processor)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (env-doc-id app)
        env         (fetch-environment-record app)]
    (.collect out {:index-id index-id
                   :doc-id   doc-id
                   :source   env})))

(defn process-task [processor app job stage task context out]
  (let [state       (.state processor)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (task-doc-id app job stage task)
        existing    (lookup-task-doc processor index-id doc-id)]
    (when (or (= "RUNNING" (:status task))
            (not (same-document? existing task)))
      (invalidate-task-cache processor doc-id)
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   task}))))

(defn process-stage [processor app job stage context out]
  (let [state       (.state processor)
        tasks       (map
                      (fn [task]
                        (transform-spark-task-record app job stage task))
                      (:tasks stage))
        stage       (dissoc stage :tasks)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (stage-doc-id app job stage)
        existing    (lookup-stage-doc processor index-id doc-id)]
    (when (or (= "ACTIVE" (:status stage))
            (not (same-document? existing stage)))
      (invalidate-stage-cache processor doc-id)
      (doseq [task tasks]
        (process-task processor app job stage task context out))
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   stage}))))

(defn process-job [processor app job context out]
  (let [state       (.state processor)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (job-doc-id app job)
        existing    (lookup-job-doc processor index-id doc-id)]
    (when (or (= "RUNNING" (:status job))
            (not (same-document? existing job)))
      (invalidate-job-cache processor doc-id)
      (let [stages (fetch-stage-records app job)]
        (doseq [stage stages]
          (process-stage processor app job stage context out)))
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   job}))))

(defn process-app [processor app context out]
  (let [state       (.state processor)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (app-doc-id app)
        existing    (lookup-app-doc processor index-id doc-id)]
    (when-not existing
      (process-environment processor app context out))
    (when (or (not (:completed app))
            (not (same-document? existing app)))
      (invalidate-app-cache processor doc-id)
      (doseq [job (fetch-job-records app)]
        (process-job processor app job context out))
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   app}))))

(defn spark-log-processor-open [this config]
  (let [config-map  (.toMap config)
        es-host     (get config-map "elastic.host.1" "localhost")
        es-port     (Integer/parseInt (get config-map "elastic.port.1" "9200"))
        es-scheme   (get config-map "elastic.scheme.1" "http")
        es-client   (elasticsearch-client {:host es-host :port es-port :scheme es-scheme})
        app-cache   (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        job-cache   (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        stage-cache (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        task-cache  (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        index-id-fn (partial ->index-id "sparklog-" (* 24 60 60 1000))
        state-1     (-> (deref (.state this))
                      (assoc :es-client es-client)
                      (assoc :app-cache app-cache)
                      (assoc :job-cache job-cache)
                      (assoc :stage-cache stage-cache)
                      (assoc :task-cache task-cache)
                      (assoc :index-id-fn index-id-fn))]
    (reset! (.state this) state-1)))

(defn spark-log-processor-processElement [this value context out]
  (process-app this value context out))

(defn spark-log-processor-close [this]
  (let [state     (deref (.state this))
        es-client (:es-client state)
        state-1   (-> state
                    (dissoc :es-client :app-cache))]
    (when es-client
      (.close es-client))
    (reset! (.state this) state-1)))

;;
;; Simple Elastic Sink
;;

(defn es-sink-open [processor config]
  (let [state      (.state processor)
        config-map (.toMap config)
        es-host    (get config-map "elastic.host.1" "localhost")
        es-port    (Integer/parseInt (get config-map "elastic.port.1" "9200"))
        es-scheme  (get config-map "elastic.scheme.1" "http")
        es-client  (elasticsearch-client {:host es-host :port es-port :scheme es-scheme})]
    (swap! state assoc :es-client es-client)))

(defn es-sink-invoke [processor value context]
  (let [state     (.state processor)
        index-id  (:index-id value)
        doc       (:source value)
        doc-id    (:doc-id value)
        path      (str "/" index-id "/_update/" doc-id)
        body      {:doc           doc
                   :doc_as_upsert true}
        body-json (json/json-str body :key-fn name)
        request   (doto (Request. "POST" path)
                    (.setJsonEntity body-json))
        es-client (:es-client @state)]
    (.performRequest es-client request)))

(fk/fdef es-sink
  :fn :sink
  :init (fn [_] (atom {}))
  :open es-sink-open
  :invoke es-sink-invoke)

;;
;; Elastisearch connector
;;

(fk/fdef spark-history-loader
  :fn :source
  :returns (fk/type-info-of {})
  :init (fn [_] (atom {}))
  :run history-server-run
  :cancel history-server-cancel
  :initializeState history-server-initialize-state
  :snapshotState history-server-snapshot-state)

(fk/fdef application-id-selector
  :fn :key-selector
  :returns Types/STRING
  :getKey (fn [_ value]
            (:application_id value)))

(fk/fdef spark-log-processor
  :fn :keyed-process
  :returns (fk/type-info-of {})
  :open spark-log-processor-open
  :close spark-log-processor-close
  :init (fn [_] (atom {}))
  :processElement spark-log-processor-processElement)

(defn elassticsearch-emitter []
  (reify ElasticsearchSinkFunction
    (process [this record context indexer]
      (let [index-id (:index-id record)
            doc      (some-> (:source record)
                       (json/json-str :key-fn name))
            doc-id   (:doc-id record)
            req      (doto (IndexRequest.)
                       (.id doc-id)
                       (.index index-id)
                       (.source doc XContentType/JSON))]
        (.add indexer (into-array IndexRequest [req]))))))

(def elasticsearch-sink
  (let [emitter (elassticsearch-emitter)
        hosts   [(HttpHost. "localhost" 9200 "http")]
        builder (doto (ElasticsearchSink$Builder. hosts emitter)
                  (.setBulkFlushMaxActions 64)
                  (.setBulkFlushInterval 5000)
                  (.setBulkFlushBackoff true)
                  (.setBulkFlushBackoffRetries 10)
                  (.setBulkFlushBackoffDelay 2000)
                  (.setBulkFlushBackoffType ElasticsearchSinkBase$FlushBackoffType/EXPONENTIAL))]
    (.build builder)))

(defn job-graph [env]
  (-> env
    (.addSource spark-history-loader)
    (.uid "6aaaf00d-a133-4cb5-b0c3-c625c7ee1f20")
    (.name "Spark History Server")
    (.keyBy application-id-selector)
    (.process spark-log-processor)
    (.uid "4db0c481-cd89-4bd7-abd9-8351c505830f")
    (.name "Spark Log Processor")
    (.addSink elasticsearch-sink)
    (.disableChaining)
    (.uid "a8d91e4b-a621-4364-96a9-48913b531224")
    (.name "Elasticsearch Store"))
  env)

(defn -main [& args]
  (let [args   (into-array String args)
        params (ParameterTool/fromArgs args)
        config (.getConfiguration params)]
    (-> (StreamExecutionEnvironment/createLocalEnvironmentWithWebUI config)
      (.enableCheckpointing 60000)
      (.setParallelism 2)
      (fk/register-clojure-types)
      (job-graph)
      (.execute))))
