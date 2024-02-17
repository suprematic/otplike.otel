(ns otplike.otel
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.core.async :as async]
   [otplike.util :as u]
   [otplike.process :as p]
   [otplike.proc-util :as pu]
   [otplike.supervisor :as supervisor]
   [otplike.kernel.tracing :as tracing]
   [otplike.kernel.logger :as klogger]
   [otplike.logger :as logger]
   [steffan-westcott.clj-otel.context :as context]
   [steffan-westcott.clj-otel.api.metrics.instrument :as i])
  (:import
   [io.opentelemetry.api GlobalOpenTelemetry]
   [io.opentelemetry.api.common Attributes]
   [io.opentelemetry.api.logs Severity]))

(def ^:private otel-severity
  {:debug   Severity/DEBUG
   :notice   Severity/INFO
   :info  Severity/INFO2
   :warning Severity/INFO
   :error Severity/ERROR
   :critical Severity/FATAL
   :alert Severity/FATAL2
   :emergency Severity/FATAL3})

(defn- flatten-map [m prefix]
  (letfn
   [(fvalue [v]
      (cond
        (or (keyword? v) (symbol? v))
        (u/strks v)
        :else
        (str v)))]

    (reduce
     (fn [acc [k v]]
       (let
        [k (name k)
         new-prefix (if (empty? prefix) k (str prefix "." k))]
         (cond
           (map? v)
           (merge acc (flatten-map v new-prefix))

           (or (sequential? v) (set? v))
           (apply
            merge acc
            (map-indexed
             (fn [i v]
               (cond
                 (map? v)
                 (flatten-map v (str new-prefix "." i))

                 (or (sequential? v) (set? v))
                 (flatten-map {(str i) v} new-prefix)

                 :else
                 {(str new-prefix "." i) (fvalue v)}))
             v))

           :else
           (assoc acc new-prefix (fvalue v)))))
     {}
     m)))

(defn- otel-attributes [attributes]
  (let [flat (flatten-map attributes "")]
    (->>
     flat
     (reduce
      (fn [ac [k v]]
        (.put ac k (str v)))
      (Attributes/builder))
     (.build))))

(defn- fix-id [m]
  (set/rename-keys m {:id "log.record.uid"}))

(defn- fix-message [m]
  (->
   m
   (update
    :message
    (fn [message]
      (if-not (str/blank? message)
        message
        (let
         [{:keys [in what]} m
          in (or (u/strks in) "<undefined>")
          what (u/strks what)]
          (if what
            (format "%s %s" in what)
            in)))))))

(defn- fix-exception [{:keys [exception] :as m}]
  (if (and (some? exception) (instance? java.lang.Throwable exception))
    (-> m
        (dissoc :exception)
        (merge
         {"exception.type" (.getName (class exception))
          "exception.stacktrace" (pr-str exception)}
         (when-let [message (.getMessage exception)]
           {"exception.message" message})))
    m))

(defonce log-count
  (i/instrument
   {:name "otplike.otel.log_count"
    :instrument-type :counter}))

(defn- output [_config m]
  (try
    (let
     [{:keys [level when in message] :as m}
      (-> m fix-id fix-message fix-exception)
      log
      (-> (.getLogsBridge (GlobalOpenTelemetry/get))
          (.get in)
          (.logRecordBuilder)
          (.setAllAttributes (otel-attributes (dissoc m :level :when :message)))
          (.setSeverityText (name level))
          (.setSeverity (get otel-severity level Severity/UNDEFINED_SEVERITY_NUMBER))
          (.setTimestamp (.toInstant when))
          (.setBody message))]
      (.emit log)
      (i/add! log-count {:value 1}))
    (catch Exception ex
      (.printStackTrace ex))))

(defn- sup-fn [_]
  [:ok
   [{:strategy :one-for-one}
    []]])

(defn start [{:keys [logger] :as config}]
  (tracing/set-context-resolver! context/dyn)

  (klogger/register-backend!
   :otel
   (klogger/make-backend logger output))

  (let [rc (supervisor/start-link sup-fn [config])]
    (logger/info
     {:message "OpenTelemetry log bridge started"
      :config config
      :properties
      (->>
       (System/getProperties)
       (filter
        (fn [[p _]]
          (.startsWith p "otel.")))
       (into {}))})
    rc))

(defn stop []
  (tracing/reset-context-resolver!)
  (klogger/unregister-backend! :otel)
  (logger/info
   {:message "OpenTelemetry log bridge stopped"}))
