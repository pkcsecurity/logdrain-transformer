(ns logdrain-transformer.core
  (:require [compojure.core :refer [defroutes GET PUT POST DELETE ANY]]
            [compojure.handler :refer [site]]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.adapter.jetty :as jetty]
            [environ.core :as environ]
            [clojure.string :as string]
            [clj-http.client :as client]
            [cheshire.core :as json])
  (:import [java.util.concurrent Executors
                                 TimeUnit]))

(def elastic-url (environ/env :bonsai-url))
(def bulk-index-action (str (json/generate-string {:index {:_index "logs"}}) "\n"))

(def pool (Executors/newSingleThreadScheduledExecutor))
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
  (when-let [work (drain-queue)]
    (println "When-let got: " (count work))
    (let [source-maps (map parse-syslog-msg work)
          bulk-request (->> source-maps
                            (map json/generate-string)
                            (string/join (str "\n" bulk-index-action))
                            (str bulk-index-action))
          response (client/post (str elastic-url "/logs/_doc/_bulk")
                                {:content-type :json
                                 :body bulk-request})]
      (println (:status response) " " (:body response)))))


(defroutes app
  (POST "/ingest" {body-stream :body}
    ;; Right now, this function assumes its input is valid. TODO: not that.
    (doseq [line (-> body-stream
                     (slurp)
                     (string/split-lines))]
      (prn "Adding line to queue: " line)
      (locking queue
        (swap! queue conj line)))
    {:status 204})
  (ANY "*" []
    (route/not-found "This is not the page you're looking for!")))

(defn -main [& [port]]
  (let [port (Integer. (or port (environ/env :port) 5000))]
    (jetty/run-jetty (site #'app) {:port port :join? false}))
  (.scheduleAtFixedRate pool batch-send 0 5 TimeUnit/SECONDS))

;; For interactive development:
;; (.stop server)
;; (def server (-main))
