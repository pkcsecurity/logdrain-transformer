(ns logdrain-transformer.core
  (:require [compojure.core :refer [defroutes GET PUT POST DELETE ANY]]
            [compojure.handler :refer [site]]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.adapter.jetty :as jetty]
            [environ.core :as environ]
            [clojure.string :as string]
            [cheshire.core :as json]
            [http.async.client :as http])
  (:import [java.net URL]
           [java.util Base64]
           [java.util.concurrent Executors
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


(defn auth-headers-from-url
  "Takes a URL with embedded credentials and returns an encoded Basic Auth header value with those creds"
  [url-string]
  (->> url-string
       (URL.)
       (.getUserInfo)
       (.getBytes)
       (.encode (Base64/getEncoder))
       (String.)
       (str "Basic ")))


(defn drain-queue []
  (locking queue
    (let [oldval @queue]
      (reset! queue [])
      oldval)))

(defn batch-send []
  (when-let [work (seq (drain-queue))]
    (println "Sending" (count work) "messages for indexing")
    (let [url (str elastic-url "/logs/_doc/_bulk")
          source-maps (map parse-syslog-msg work)
          bulk-request (as-> source-maps $
                            (map json/generate-string $)
                            (string/join (str "\n" bulk-index-action) $)
                            (str bulk-index-action $ "\n"))]
      (with-open [client (http/create-client)]
        (let [response (http/POST
                           client
                           url
                           :headers {:content-type "application/x-ndjson"
                                     :authorization (auth-headers-from-url url)}
                           :body bulk-request)
              status (-> response
                       http/await
                       http/status)]
          (println "Got" (:code status) "from Elasticsearch")
          (when (>= status 400)
            (throw (http/error response))))))))


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
