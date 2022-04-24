(ns example.spark-log
  (:require [clojure.data.json :as json]
            [clojure.core.cache :as cache]
            [clojure.string :as str])
  (:import (org.apache.flink.configuration Configuration)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (io.kosong.flink.clojure.function KeyedProcessFunction SourceFunction SinkFunction KeySelector)
           (org.apache.flink.api.common.typeinfo Types TypeInformation)
           (org.elasticsearch.client RestClient Request)
           (org.apache.http HttpHost)
           (org.apache.http.util EntityUtils)
           (java.io FileNotFoundException)
           (org.apache.flink.api.common.state ListStateDescriptor)
           (org.apache.flink.streaming.connectors.elasticsearch ElasticsearchSinkBase$FlushBackoffType ElasticsearchSinkFunction)
           (org.elasticsearch.action.index IndexRequest)
           (org.elasticsearch.common.xcontent XContentType)
           (org.apache.flink.streaming.connectors.elasticsearch7 ElasticsearchSink$Builder))
  (:gen-class))


(defn elasticsearch-client [{:keys [host port scheme]}]
  (let [http-host (HttpHost. host port scheme)]
    (-> (RestClient/builder (into-array HttpHost [http-host]))
      (.build))))

;;
;; parameters
;;

(def job-history-server "http://localhost:18080/api/v1")

(deftype elastic-document
  [^String index-id
   ^String document-id
   ^String document])


;;
;; Spark History Server Source
;;

(defn- transform-spark-application-record [app]
  (let [app-id   (:id app)
        app-name (:name app)
        f        (fn [app-attempt]
                   (let [app-attempt-id (:attemptId app-attempt)]
                     (-> app-attempt
                       (assoc :name app-name)
                       (assoc :applicationId app-id)
                       (assoc :applicationAttemptId app-attempt-id)
                       (assoc :sparkType "application"))))]
    (map f (:attempts app))))

(defn- transform-spark-job-record [app job]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)]
    (-> job
      (assoc :applicationId app-id)
      (assoc :applicationAttemptId app-attempt-id)
      (assoc :sparkType "job"))))

(defn- transform-spark-stage-record [app job stage]
  (let [app-id           (:applicationId app)
        app-attempt-id   (:applicationAttemptId app)
        job-id           (:jobId job)
        stage-attempt-id (:attemptId stage)]
    (-> stage
      (assoc :applicationId app-id)
      (assoc :applicationAttemptId app-attempt-id)
      (assoc :jobId job-id)
      (assoc :stageAttemptId stage-attempt-id)
      (assoc :sparkType "stage"))))

(defn- transform-spark-task-record [app job stage task]
  (let [app-id           (:applicationId app)
        app-attempt-id   (:applicationAttemptId app)
        job-id           (:jobId job)
        stage-attempt-id (:attemptId stage)
        task-attempt-id  (:attempt task)]
    (-> task
      (assoc :applicationId app-id)
      (assoc :applicationAttemptId app-attempt-id)
      (assoc :jobId job-id)
      (assoc :stageAttemptId stage-attempt-id)
      (assoc :taskAttemptId task-attempt-id)
      (assoc :sparkType "task"))))

(defn transform-spark-environment-record [app env]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)
        prop-key-xform (partial reduce (fn [m [key value]]
                                         (assoc m (str/replace key "." "_") value))
                         {})
        collect-first  (partial reduce (fn [v [t0 _]] (conj v t0)) [])]
    (-> env
      (assoc :applicationId app-id)
      (assoc :applicationAttemptId app-attempt-id)
      (assoc :sparkType "environment")
      (update-in [:sparkProperties] prop-key-xform)
      (update-in [:hadoopProperties] prop-key-xform)
      (update-in [:systemProperties] prop-key-xform)
      (update-in [:classpathEntries] collect-first))))

(defn fetch-application-records [last-update-time]
  (let [url  (str job-history-server "/applications")
        resp (slurp url)
        apps (->> (json/read-str resp :key-fn keyword))]
    (->> (mapcat transform-spark-application-record apps)
      (filter #(< last-update-time (:lastUpdatedEpoch %))))))

(defn fetch-environment-record [app]
  (let [app-id (:applicationId app)
        url    (str job-history-server "/applications/" app-id "/environment")
        resp   (slurp url)
        env    (json/read-str resp :key-fn keyword)]
    (transform-spark-environment-record app env)))

(defn fetch-job-records [app]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)
        url            (if app-attempt-id
                         (str job-history-server "/applications/" app-id "/" app-attempt-id "/jobs")
                         (str job-history-server "/applications/" app-id "/jobs"))
        resp           (slurp url)
        jobs           (json/read-str resp :key-fn keyword)]
    (map (partial transform-spark-job-record app) jobs)))


(defn fetch-stage-records [app job]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)
        fetch-fn       (fn [stage-id]
                         (let [url   (if app-attempt-id
                                       (str job-history-server "/applications/" app-id "/" app-attempt-id "/stages/" stage-id)
                                       (str job-history-server "/applications/" app-id "/stages/" stage-id))
                               resp  (try
                                       (slurp url)
                                       (catch FileNotFoundException e))
                               stage (some-> resp (json/read-str :key-fn keyword))]
                           stage))
        stages         (mapcat fetch-fn (:stageIds job))]
    (map (partial transform-spark-stage-record app job) stages)))

(defn history-server-init [this]
  (atom {}))

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

(defn history-server-open [this config]
  )

(defn history-server-close [this]
  )

(defn history-server-run [this context]
  (let [state (.state this)]
    (swap! state assoc :running true)
    (while (:running @state)
      (let [last-update-time (or (:last-update-time @state) 0)
            next-update-time (ref last-update-time)
            apps             (fetch-application-records last-update-time)]
        (doseq [app apps]
          (let [this-update-time (:lastUpdatedEpoch app)]
            (when (< @next-update-time this-update-time)
              (dosync (ref-set next-update-time this-update-time)))
            (.collect context (json/json-str app :key-fn name))))
        (swap! state assoc :last-update-time @next-update-time))
      (Thread/sleep 5000))))

(defn history-server-cancel [this]
  (let [state (.state this)]
    (swap! state assoc :running false)))

(def spark-history-loader
  (SourceFunction.
    {:init            'example.spark-log/history-server-init
     :open            'example.spark-log/history-server-open
     :run             'example.spark-log/history-server-run
     :cancel          'example.spark-log/history-server-cancel
     :close           'example.spark-log/history-server-close
     :initializeState 'example.spark-log/history-server-initialize-state
     :snapshotState   'example.spark-log/history-server-snapshot-state
     :returns         Types/STRING}))

;;
;; Spark Log Processor
;;

(defn ->index-id [prefix bucket-size time]
  (let [bucket-id (* (quot time bucket-size) bucket-size)]
    (str prefix bucket-id)))

(defn app-doc-id [app]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)]
    (if app-attempt-id
      (str app-id ":" app-attempt-id)
      (str app-id ":"))))

(defn env-doc-id [env]
  (let [app-id (:applicationId env)]
    (str app-id ":_environment")))

(defn job-doc-id [app job]
  (let [app-id         (:applicationId app)
        app-attempt-id (:applicationAttemptId app)
        job-id         (:jobId job)]
    (if app-attempt-id
      (str app-id ":" app-attempt-id ":" job-id)
      (str app-id "::" job-id))))

(defn stage-doc-id [app job stage]
  (let [app-id           (:applicationId app)
        app-attempt-id   (:applicationAttemptId app)
        job-id           (:jobId job)
        stage-id         (:stageId stage)
        stage-attempt-id (:stageAttemptId stage)]
    (if app-attempt-id
      (str app-id ":" app-attempt-id ":" job-id ":" stage-id ":" stage-attempt-id)
      (str app-id "::" job-id ":" stage-id ":" stage-attempt-id))))

(defn task-doc-id [app job stage task]
  (let [app-id           (:applicationId app)
        app-attempt-id   (:applicationAttemptId app)
        job-id           (:jobId job)
        stage-id         (:stageId stage)
        stage-attempt-id (:stageAttemptId stage)
        task-id          (:taskId task)
        task-attempt-id  (:taskAttemptId task)]
    (if app-attempt-id
      (str app-id ":" app-attempt-id ":" job-id ":" stage-id ":" stage-attempt-id ":" task-id ":" task-attempt-id)
      (str app-id "::" job-id ":" stage-id ":" stage-attempt-id ":" task-id ":" task-attempt-id))))

(defn es-lookup-doc [processor index-id doc-id]
  (let [state     (.state processor)
        path      (str "/" index-id "/_source/" doc-id)
        es-client (:es-client @state)
        exist     (-> es-client
                    (.performRequest (doto (Request. "HEAD" path)
                                       (.addParameter "ignore" "404")))
                    (.getStatusLine)
                    (.getStatusCode)
                    (= 200))
        body-json (when exist
                    (-> es-client
                      (.performRequest (doto (Request. "GET" path)
                                         (.addParameter "ignore" "404")))
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
        start-time  (:startTimeEpoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (env-doc-id app)
        env         (fetch-environment-record app)]
    (.collect out (->elastic-document index-id doc-id
                    (json/json-str env :key-fn name)))))

(defn process-task [processor app job stage task context out]
  (let [state       (.state processor)
        start-time  (:startTimeEpoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (task-doc-id app job stage task)
        existing    (lookup-task-doc processor index-id doc-id)]
    (when (or (= "RUNNING" (:status task))
            (not= existing task))
      (.collect out (->elastic-document index-id doc-id
                      (json/json-str task :key-fn name))))))

(defn process-stage [processor app job stage context out]
  (let [state       (.state processor)
        tasks       (map (fn [[_ task]]
                           (transform-spark-task-record app job stage task)) (:tasks stage))
        stage       (dissoc stage :tasks)
        start-time  (:startTimeEpoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (stage-doc-id app job stage)
        existing    (lookup-stage-doc processor index-id doc-id)]
    (when (or (= "ACTIVE" (:status stage))
            (not= existing stage))
      (doseq [task tasks]
        (process-task processor app job stage task context out))
      (.collect out (->elastic-document index-id doc-id
                      (json/json-str stage :key-fn name))))))

(defn process-job [processor app job context out]
  (let [state       (.state processor)
        start-time  (:startTimeEpoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (job-doc-id app job)
        existing    (lookup-job-doc processor index-id doc-id)]
    (when (or (= "RUNNING" (:status job))
            (not= existing job))
      (let [stages (fetch-stage-records app job)]
        (doseq [stage stages]
          (process-stage processor app job stage context out)))
      (.collect out (->elastic-document index-id doc-id
                      (json/json-str job :key-fn name))))))

(defn process-app [processor app context out]
  (let [state       (.state processor)
        start-time  (:startTimeEpoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (app-doc-id app)
        existing    (lookup-app-doc processor index-id doc-id)]
    (when-not existing
      (process-environment processor app context out))
    (when (or (not (:completed app))
            (not= existing app))
      (doseq [job (fetch-job-records app)]
        (process-job processor app job context out))
      (.collect out (->elastic-document index-id doc-id (json/json-str app :key-fn name))))))


(defn spark-log-processor-init [this]
  (atom {}))

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
  (let [app (json/read-str value :key-fn keyword)]
    (process-app this app context out)))

(defn spark-log-processor-close [this]
  (let [state     (deref (.state this))
        es-client (:es-client state)
        state-1   (-> state
                    (dissoc :es-client :app-cache))]
    (when es-client
      (.close es-client))
    (reset! (.state this) state-1)))

(def spark-log-processor
  (KeyedProcessFunction.
    {:open           'example.spark-log/spark-log-processor-open
     :close          'example.spark-log/spark-log-processor-close
     :init           'example.spark-log/spark-log-processor-init
     :processElement 'example.spark-log/spark-log-processor-processElement
     :returns        (TypeInformation/of example.spark_log.elastic-document)}))

;;
;; Key Selector
;;

(defn resolve-application-process-key [value]
  (let [m (json/read-str value :key-fn keyword)]
    (:applicationId m)))

(def key-selector
  (KeySelector.
    {:getKey  'example.spark-log/resolve-application-process-key
     :returns Types/STRING}))

;;
;; Simple Elastic Sink
;;

(defn es-sink-init [processor]
  (atom {}))

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
        index-id  (.-index_id value)
        doc       (some-> (.-document value)
                    (json/read-str))
        doc-id    (.-document_id value)
        path      (str "/" index-id "/_update/" doc-id)
        body      {:doc           doc
                   :doc_as_upsert true}
        body-json (json/json-str body :key-fn name)
        request   (doto (Request. "POST" path)
                    (.setJsonEntity body-json))
        es-client (:es-client @state)]
    (.performRequest es-client request)))

(def es-sink
  (SinkFunction.
    {:init   'example.spark-log/es-sink-init
     :open   'example.spark-log/es-sink-open
     :invoke 'example.spark-log/es-sink-invoke}))

;;
;; Elastisearch connector
;;

(defn elassticsearch-emitter []
  (reify ElasticsearchSinkFunction
    (process [this record context indexer]
      (let [index-id  (.-index_id record)
            doc       (.-document record)
            doc-id    (.-document_id record)
            req (doto (IndexRequest.)
                  (.id doc-id)
                  (.index index-id)
                  (.source doc XContentType/JSON))]
        (.add indexer (into-array IndexRequest [req]))))))

(defn elasticsearch-sink []
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
  (let [elasticsearch-sink (elasticsearch-sink)]
    (-> env
      (.enableCheckpointing 5000)
      (.addSource spark-history-loader)
      (.uid "6aaaf00d-a133-4cb5-b0c3-c625c7ee1f20")
      (.name "Spark History Server")
      (.keyBy key-selector)
      (.process spark-log-processor)
      (.uid "4db0c481-cd89-4bd7-abd9-8351c505830f")
      (.name "Spark Log Processor")
      #_(.addSink es-sink)
      (.addSink elasticsearch-sink)
      (.disableChaining)
      (.uid "a8d91e4b-a621-4364-96a9-48913b531224")
      (.name "Elasticsearch Store"))))

(defn load-config []
  (doto (Configuration.)
    (.setString "elastic.host.1" "localhost")
    (.setInteger "elastic.port.1" 9200)
    (.setString "elastic.scheme.1" "http")
    (.setString "history-server.host" "localhost")
    (.setInteger "history-server.port" 18080)
    (.setString "history-server.scheme" "http")))

(defn dev-run []
  (let [conf (load-config)
        env  (StreamExecutionEnvironment/createLocalEnvironmentWithWebUI conf)]
    (job-graph env)
    (.execute env "Spark Log")))

(defn run [env]
  (job-graph env)
  (.execute env "Spark Log"))


(defn -main []
  (let [conf (load-config)
        env  (StreamExecutionEnvironment/getExecutionEnvironment conf)]
    (run env)))