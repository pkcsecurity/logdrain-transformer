(ns logdrain-transformer.core
  (:require [compojure.core :refer [defroutes GET PUT POST DELETE ANY]]
            [compojure.handler :refer [site]]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.adapter.jetty :as jetty]
            [environ.core :as environ]
            [clojure.string :as string]
            [cheshire.core :as json]
            [logdrain-transformer.integrations.okhttp :as http])
  (:import [java.util.concurrent Executors
                                 TimeUnit]))

(def elastic-url (environ/env :bonsai-url))
(def bulk-index-action (str (json/generate-string {:index {:_index "logs" :_type "_doc"}}) "\n"))

(defonce pool (Executors/newSingleThreadScheduledExecutor))
(def queue (atom []))


(defn parse-syslog-msg [line]
  (let [parts (-> line
                  (string/split #" " 8)
                  (nthnext 2))
        date (first parts)
        host (str (nth parts 2) "[" (nth parts 3) "]")
        message (last parts)]
    {:date date :host host :message message}))


(defn drain-queue []
  (locking queue
    (let [oldval @queue]
      (reset! queue [])
      oldval)))

(defn batch-send []
  (println "Running batch-send")
  (when-let [work (seq (drain-queue))]
    (println "When-let got: " (count work))
    (let [source-maps (map parse-syslog-msg work)
          bulk-request (->> source-maps
                            (map json/generate-string)
                            (string/join (str "\n" bulk-index-action))
                            (str bulk-index-action))]
      (println bulk-request)
      (http/async-req "POST"
                      (str elastic-url "/logs/_doc/_bulk")
                      :media-type "application/x-ndjson; charset=utf-8"
                      :body bulk-request))))


(defroutes app
  (POST "/ingest" {body-stream :body}
    ;; Right now, this function assumes its input is valid. TODO: not that.
    (locking queue
      (swap! queue into (-> body-stream
                            (slurp :encoding "UTF-8")
                            (string/split-lines))))
    {:status 204})
  (ANY "*" []
    (route/not-found "This is not the page you're looking for!")))

(defn -main [& [port]]
  (let [port (Integer. (or port (environ/env :port) 5000))]
    (.scheduleAtFixedRate pool batch-send 0 5 TimeUnit/SECONDS)
    (jetty/run-jetty (site #'app) {:port port :join? false})))

;; For interactive development:
;; (.stop server)
;; (def server (-main))
