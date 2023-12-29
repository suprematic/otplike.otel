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
   [steffan-westcott.clj-otel.sdk.autoconfigure :as autoconf])
  (:import
   [io.opentelemetry.api.common Attributes]
   [io.opentelemetry.api.logs Severity]))

(defonce ^:private sdk
  (autoconf/init-otel-sdk!
   {:set-as-default false :set-as-global false}))

(defonce ^:private logs-bridge
  (.getLogsBridge sdk))

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

(defn- output [m]
  (let
   [{:keys [level when in message] :as m}
    (-> m fix-id fix-message fix-exception)
    log
    (-> logs-bridge
        (.get in)
        (.logRecordBuilder)
        (.setAllAttributes (otel-attributes (dissoc m :level :when :message)))
        (.setSeverityText (name level))
        (.setSeverity (get otel-severity level Severity/UNDEFINED_SEVERITY_NUMBER))
        (.setTimestamp (.toInstant when))
        (.setBody message))]
    (.emit log)))

(defonce ^:private in
  (let [ch (async/chan (async/sliding-buffer 16))]
    (async/tap klogger/mult ch)
    ch))

(p/proc-defn- p-log [_config]
  (p/flag :trap-exit true)

  (pu/!chan in)

  (loop [timeout :infinity reason nil]
    (p/receive!
     [:EXIT _ reason']
     (recur 1000 reason')

     message
     (do
       (output message)
       (recur timeout reason))

     (after timeout (p/exit reason)))))

(defn- sup-fn [config]
  [:ok
   [{:strategy :one-for-one}
    [{:id :logger-console
      :start
      [(fn []
         [:ok (p/spawn-link p-log [config])]) []]}]]])

(defn start [config]
  (tracing/set-context-resolver! context/dyn)
  (supervisor/start-link sup-fn [config]))
