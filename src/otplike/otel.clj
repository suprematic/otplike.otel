(ns otplike.otel
  (:require
   [clojure.string :as str]
   [clojure.core.async :as async]
   [otplike.process :as p]
   [otplike.proc-util :as pu]
   [otplike.supervisor :as supervisor]
   [otplike.kernel.tracing :as tracing]
   [otplike.kernel.logger :as klogger]
   [otplike.logger :as logger]
   [steffan-westcott.clj-otel.context :as context]
   [steffan-westcott.clj-otel.sdk.autoconfigure :as autoconf])
  (:import
   [io.opentelemetry.api.common Attributes]))

(defonce ^:private sdk
  (autoconf/init-otel-sdk!
   {:set-as-default true :set-as-global true}))

(defonce ^:private logs-bridge
  (.getLogsBridge sdk))

(def ^:private otel-severity
  (try
    (let [logs-severity (import 'io.opentelemetry.api.logs.Severity)]
      (letfn
       [(severity [s]
          (-> (.getField logs-severity (name s)) (.get nil)))]
        #(get
          {:debug   (severity 'DEBUG)
           :notice  (severity 'INFO)
           :info (severity 'INFO2)
           :warning (severity 'WARN)
           :error (severity 'ERROR)
           :critical (severity 'FATAL)
           :alert (severity 'FATAL2)
           :emergency (severity 'FATAL3)}
          %
          (severity 'UNDEFINED_SEVERITY_NUMBER))))
    (catch ClassNotFoundException _
      nil)))

(defn- flatten-map [m prefix]
  (letfn
   [(fvalue [v]
      (cond
        (or (keyword? v) (symbol? v))
        (name v)
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

(defn- format-body [{:keys [message in what]}]
  (if-not (str/blank? message)
    message
    (let
     [what
      (cond
        (or (keyword? what) (symbol? what))
        (name what)
        :else
        "")]
      (format "%s %s" in  what))))

(defn- output [{:keys [level when in] :as m}]
  (let
   [log
    (-> logs-bridge
        (.get in)
        (.logRecordBuilder)
        (.setAllAttributes (otel-attributes (dissoc m :level :when :message)))
        (.setSeverityText (name level))
        (.setSeverity (otel-severity level))
        (.setTimestamp (.toInstant when))
        (.setBody (format-body m)))]
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
