(ns example.spark-log
  (:require [clojure.data.json :as json]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [io.kosong.flink.clojure.core :as fk]
            [camel-snake-kebab.core :as csk]
            [clojure.tools.logging :as log])
  (:import (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.api.common.typeinfo Types)
           (org.elasticsearch.client RestClient Request RestClientBuilder$HttpClientConfigCallback)
           (org.apache.http HttpHost)
           (org.apache.http.util EntityUtils)
           (org.apache.flink.api.common.state ListStateDescriptor)
           (org.apache.flink.streaming.connectors.elasticsearch ElasticsearchSinkBase$FlushBackoffType ElasticsearchSinkFunction)
           (org.elasticsearch.action.index IndexRequest)
           (org.elasticsearch.common.xcontent XContentType)
           (org.apache.flink.api.java.utils ParameterTool)
           (java.net URL)
           (org.apache.http.client CredentialsProvider)
           (org.apache.http.auth UsernamePasswordCredentials)
           (org.apache.flink.connector.elasticsearch.sink Elasticsearch7SinkBuilder ElasticsearchEmitter RequestIndexer FlushBackoffType)
           (org.apache.flink.api.connector.sink2 SinkWriter$Context))
  (:gen-class))

(def timestamp-kw (keyword "@timestamp"))

(deftype ElasticsearchHttpClientConfigCallback [username password]
  :load-ns true
  RestClientBuilder$HttpClientConfigCallback
  (customizeHttpClient [this builder]
    (let [provider (reify CredentialsProvider
                     (getCredentials [this scope]
                       (UsernamePasswordCredentials. username password)))]
      (.setDefaultCredentialsProvider builder provider))))

(defn ->elasticsearch-client [{:keys [urls username password]}]
  (let [http-hosts (map (fn [url-str]
                          (let [url    (URL. url-str)
                                host   (.getHost url)
                                port   (or (.getPort url) 9200)
                                scheme (or (.getProtocol url) "http")]
                            (HttpHost. host port scheme)))
                     urls)
        callback   (->ElasticsearchHttpClientConfigCallback username password)]
    (-> (RestClient/builder (into-array HttpHost http-hosts))
      (.setHttpClientConfigCallback callback)
      (.build))))

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

(defn- transform-spark-stage-record [job stage]
  (let [now                          (System/currentTimeMillis)
        app-id                       (:application_id job)
        app-attempt-id               (:application_attempt_id job)
        job-id                       (:job_id job)
        stage-attempt-id             (:attempt_id stage)
        flatten-task-map             (fn [m]
                                       (->> m
                                         (map (fn [[k t]] (assoc t :task_id (name k))))
                                         vec))
        flatten-executor-summary-map (fn [m]
                                       (->> m
                                         (map (fn [[k s]] (assoc s :executor_id (name k))))
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

(defn- transform-spark-task-record [stage task]
  (let [now              (System/currentTimeMillis)
        app-id           (:application_id stage)
        app-attempt-id   (:application_attempt_id stage)
        job-id           (:job_id stage)
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

(defn fetch-application-records [history-server last-update-time]
  (let [now  (System/currentTimeMillis)
        url  (str history-server "/applications")
        resp (try
               (slurp url)
               (catch Throwable t
                 (log/error t)))
        apps (->> (json/read-str resp :key-fn csk/->snake_case_keyword))]
    (->> (mapcat transform-spark-application-record apps)
      (filter #(< last-update-time (:last_updated_epoch %)))
      (map (fn [app] (assoc app timestamp-kw now))))))

(defn fetch-environment-record [history-server app]
  (let [app-id (:application_id app)
        url    (str history-server "/applications/" app-id "/environment")
        resp   (try
                 (slurp url)
                 (catch Throwable t
                   (log/error t)))
        now    (System/currentTimeMillis)
        env    (some-> resp
                 (json/read-str :key-fn csk/->snake_case_keyword)
                 (assoc timestamp-kw now))]
    (transform-spark-environment-record app env)))

(defn fetch-job-records [history-server app]
  (let [app-id         (:application_id app)
        app-attempt-id (:application_attempt_id app)
        url            (if app-attempt-id
                         (str history-server "/applications/" app-id "/" app-attempt-id "/jobs")
                         (str history-server "/applications/" app-id "/jobs"))
        resp           (try
                         (slurp url)
                         (catch Throwable t
                           (log/error t)))
        now            (System/currentTimeMillis)
        jobs           (some-> resp
                         (json/read-str :key-fn csk/->snake_case_keyword))]
    (->> jobs
      (map (partial transform-spark-job-record app))
      (map (fn [job] (assoc job timestamp-kw now))))))


(defn fetch-stage-records [history-server job]
  (let [app-id         (:application_id job)
        app-attempt-id (:application_attempt_id job)
        fetch-fn       (fn [stage-id]
                         (let [url            (if app-attempt-id
                                                (str history-server "/applications/" app-id "/" app-attempt-id "/stages/" stage-id)
                                                (str history-server "/applications/" app-id "/stages/" stage-id))
                               resp           (try
                                                (slurp url)
                                                (catch Throwable t
                                                  (log/error t)))
                               now            (System/currentTimeMillis)
                               stage-attempts (some-> resp
                                                (json/read-str :key-fn csk/->snake_case_keyword))]
                           (map #(assoc % timestamp-kw now) stage-attempts)))
        stages         (mapcat fetch-fn (:stage_ids job))]
    (map (partial transform-spark-stage-record job) stages)))

(defn history-server-open [this config]
  (let [state  (.state this)
        params (some-> this
                 .getRuntimeContext
                 .getExecutionConfig
                 .getGlobalJobParameters
                 .toMap)
        params (ParameterTool/fromMap params)
        url    (.get params "app.history-server-url")
        sleep  (or (.getLong params "app.history-server-pause-between-poll") 5000)]
    (swap! state assoc :history-server (str url "/api/v1"))
    (swap! state assoc :history-server-sleep sleep)))

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
  (let [state          (.state this)
        history-server (:history-server @state)
        sleep-millis   (:history-server-sleep @state)]
    (swap! state assoc :running true)
    (while (:running @state)
      (let [prev-update-time (or (:last-update-time @state) 0)
            next-update-time (ref prev-update-time)
            apps             (->> (fetch-application-records history-server prev-update-time)
                               (sort-by :last_updated_epoch))]
        (doseq [app apps]
          (let [this-update-time (:last_updated_epoch app)]
            (when (< prev-update-time this-update-time)
              (dosync (ref-set next-update-time this-update-time))
              (.collect context app))))
        (swap! state assoc :last-update-time @next-update-time))
      (Thread/sleep sleep-millis))))

(defn history-server-cancel [this]
  (let [state (.state this)]
    (swap! state assoc :running false)))

;;
;; Spark Log Processor
;;

(defn normalize-document [doc]
  (-> doc
    (dissoc timestamp-kw)
    (dissoc :data_stream)
    (dissoc :ecs)
    (dissoc :host)))

(defn same-document? [doc-1 doc-2]
  (= (normalize-document doc-1) (normalize-document doc-2)))

(defn ->index-id [prefix bucket-size time]
  (let [bucket-id (* (quot time bucket-size) bucket-size)]
    (str prefix bucket-id)))

(defn app-doc-id [doc]
  (let [app-id         (:application_id doc)
        app-attempt-id (or (:application_attempt_id doc) "")]
    (str app-id ":" app-attempt-id)))

(defn env-doc-id [doc]
  (str (app-doc-id doc) ":" "_environment"))

(defn job-doc-id [doc]
  (let [job-id (:job_id doc)]
    (str (app-doc-id doc) ":" job-id)))

(defn stage-doc-id [doc]
  (let [stage-id         (:stage_id doc)
        stage-attempt-id (:stage_attempt_id doc)]
    (str (job-doc-id doc) ":" stage-id ":" stage-attempt-id)))

(defn task-doc-id [doc]
  (let [task-id         (:task_id doc)
        task-attempt-id (:task_attempt_id doc)]
    (str (stage-doc-id doc) ":" task-id ":" task-attempt-id)))

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
  (let [state          (.state processor)
        history-server (:history-server @state)
        start-time     (:start_time_epoch app)
        index-id-fn    (:index-id-fn @state)
        index-id       (index-id-fn start-time)
        doc-id         (env-doc-id app)
        env            (fetch-environment-record history-server app)]
    (.collect out {:index-id index-id
                   :doc-id   doc-id
                   :source   env})))

(defn process-task [processor app job stage task context out]
  (let [state       (.state processor)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (task-doc-id task)
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
                        (transform-spark-task-record stage task))
                      (:tasks stage))
        stage       (dissoc stage :tasks)
        start-time  (:start_time_epoch app)
        index-id-fn (:index-id-fn @state)
        index-id    (index-id-fn start-time)
        doc-id      (stage-doc-id stage)
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
  (let [state          (.state processor)
        history-server (:history-server @state)
        start-time     (:start_time_epoch app)
        index-id-fn    (:index-id-fn @state)
        index-id       (index-id-fn start-time)
        doc-id         (job-doc-id job)
        existing       (lookup-job-doc processor index-id doc-id)]
    (when (or (= "RUNNING" (:status job))
            (not (same-document? existing job)))
      (invalidate-job-cache processor doc-id)
      (let [stages (fetch-stage-records history-server job)]
        (doseq [stage stages]
          (process-stage processor app job stage context out)))
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   job}))))

(defn process-app [processor app context out]
  (let [state          (.state processor)
        history-server (:history-server @state)
        start-time     (:start_time_epoch app)
        index-id-fn    (:index-id-fn @state)
        index-id       (index-id-fn start-time)
        doc-id         (app-doc-id app)
        existing       (lookup-app-doc processor index-id doc-id)]
    (when-not existing
      (process-environment processor app context out))
    (when (or (not (:completed app))
            (not (same-document? existing app)))
      (invalidate-app-cache processor doc-id)
      (doseq [job (fetch-job-records history-server app)]
        (process-job processor app job context out))
      (.collect out {:index-id index-id
                     :doc-id   doc-id
                     :source   app}))))

(defn spark-log-processor-open [this config]
  (let [params            (some-> this
                            .getRuntimeContext
                            .getExecutionConfig
                            .getGlobalJobParameters
                            .toMap)
        state             (.state this)
        params            (ParameterTool/fromMap params)
        es-urls           (some-> (.get params "app.elasticsearch-urls")
                            (str/split #","))
        es-username       (.get params "app.elasticsearch-username")
        es-password       (.get params "app.elasticsearch-password")
        index-prefix      (.get params "app.index-prefix")
        index-bucket-size (.getLong params "app.index-bucket-size")
        es-client         (->elasticsearch-client {:urls es-urls :username es-username :password es-password})
        app-cache         (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        job-cache         (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        stage-cache       (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        task-cache        (clojure.core.cache/ttl-cache-factory {} :ttl (* 30 60 1000))
        index-id-fn       (partial ->index-id index-prefix index-bucket-size)
        history-server    (str (.get params "app.history-server-url") "/api/v1")
        state-1           (-> (deref (.state this))
                            (assoc :es-client es-client)
                            (assoc :app-cache app-cache)
                            (assoc :job-cache job-cache)
                            (assoc :stage-cache stage-cache)
                            (assoc :task-cache task-cache)
                            (assoc :index-id-fn index-id-fn)
                            (assoc :history-server history-server))]
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
;; Elastisearch connector
;;

(def history-server-source
  (fk/flink-fn
    {
     :fn              :source
     :returns         (fk/type-info-of {})
     :init            (fn [_] (atom {}))
     :open            history-server-open
     :run             history-server-run
     :cancel          history-server-cancel
     :initializeState history-server-initialize-state
     :snapshotState   history-server-snapshot-state}))

(def application-id-selector
  (fk/flink-fn
    {:fn      :key-selector
     :returns Types/STRING
     :getKey  (fn [_ value]
                (:application_id value))}))

(def spark-log-processor
  (fk/flink-fn
    {:fn             :keyed-process
     :returns        (fk/type-info-of {})
     :open           spark-log-processor-open
     :close          spark-log-processor-close
     :init           (fn [_] (atom {}))
     :processElement spark-log-processor-processElement}))

(defn elassticsearch-emitter []
  (reify ElasticsearchEmitter
    (^void emit [this record ^SinkWriter$Context context ^RequestIndexer indexer]
      (let [index-id (:index-id record)
            doc      (some-> (:source record)
                       (json/json-str :key-fn name))
            doc-id   (:doc-id record)
            req      (doto (IndexRequest.)
                       (.id doc-id)
                       (.index index-id)
                       (.source doc XContentType/JSON))]
        (.add indexer (into-array IndexRequest [req]))))))

(defn ->elasticsearch-sink [http-hosts username password]
  (let [emitter (elassticsearch-emitter)
        builder (doto (Elasticsearch7SinkBuilder.)
                  (.setHosts (into-array HttpHost http-hosts))
                  (.setEmitter emitter)
                  (.setBulkFlushMaxActions 64)
                  (.setBulkFlushInterval 5000)
                  (.setBulkFlushBackoffStrategy FlushBackoffType/EXPONENTIAL 10 2000)
                  (.setConnectionUsername username)
                  (.setConnectionPassword password))]
    (.build builder)))

(defn job-graph [env params]
  (let [es-username        (.get params "app.elasticsearch-username")
        es-password        (.get params "app.elasticsearch-password")
        http-hosts         (->> (str/split (.get params "app.elasticsearch-urls") #",")
                             (map (fn [url-str]
                                    (let [url    (URL. url-str)
                                          host   (.getHost url)
                                          scheme (or (.getProtocol url) "http")
                                          port   (or (.getPort url) 9200)]
                                      (HttpHost. host port scheme))))
                             (reduce conj []))
        elasticsearch-sink (->elasticsearch-sink http-hosts es-username es-password)]
    (-> env
      (.addSource history-server-source)
      (.uid "6aaaf00d-a133-4cb5-b0c3-c625c7ee1f20")
      (.name "Spark History Server")
      (.keyBy application-id-selector)
      (.process spark-log-processor)
      (.uid "4db0c481-cd89-4bd7-abd9-8351c505830f")
      (.name "Spark Log Processor")
      (.sinkTo elasticsearch-sink)
      (.disableChaining)
      (.uid "a8d91e4b-a621-4364-96a9-48913b531224")
      (.name "Elasticsearch Store")))
  env)


(def default-params
  {"app.data-stream-type"                  "logs"
   "app.data-stream-dataset"               "spark_history"
   "app.data-stream-namespace"             "dev"
   "app.index-prefix"                      "logs-spark_history-dev-"
   "app.index-bucket-size"                 "604800000"
   "app.history-server-url"                "http://localhost:18080"
   "app.history-server-pause-between-poll" "15000"
   "app.elasticsearch-urls"                "http://localhost:9200"
   "app.elasticsearch-username"            ""
   "app.elasticsearch-password"            ""})

(defn -main [& args]
  (let [args   (into-array String args)
        params (-> (ParameterTool/fromMap default-params)
                 (.mergeWith (ParameterTool/fromArgs args)))
        config (.getConfiguration params)
        env    (StreamExecutionEnvironment/getExecutionEnvironment config)]
    (.. env getConfig (setGlobalJobParameters params))
    (fk/register-clojure-types env)
    (-> env
      (job-graph params)
      (.execute))))
