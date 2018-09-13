(ns logdrain-transformer.core
  (:require [compojure.core :refer [defroutes GET PUT POST DELETE ANY]]
            [compojure.handler :refer [site]]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.adapter.jetty :as jetty]
            [environ.core :as environ]
            [clojure.string :as string]
            [cheshire.core :as json]
            [http.async.client :as http]
            [instaparse.core :as insta]
            [clojure.core.match :as m])
  (:import [java.net URL]
           [java.util Base64]
           [java.util.concurrent Executors
                                 TimeUnit]))

(def elastic-url (environ/env :bonsai-url))
(def bulk-index-action (str (json/generate-string {:index {:_index "logs" :_type "_doc"}}) "\n"))

(defonce pool (Executors/newSingleThreadScheduledExecutor))
(def queue (atom []))

(defn printerr [& s]
  (binding [*out* *err*]
    (apply println s)))


(def syslog-parser
  (insta/parser 
   "
    WORK = R+;
    R = HEADER TS HOSTINTRO HOST SP APP DASH MSG;
    HEADER = LEN SP PRI VERSION SP;
    HOSTINTRO = SP \"host\" SP;
    DASH = SP \"-\" SP;
    <LEN> = #\"[1-9][0-9]*\";
    <SP> = \" \";
    PRI = \"<\" LEN \">\";
    VERSION = LEN;
    TS = #\"[0-9\\-T:]{19}\" MS? TZ;
    <MS> = #\"\\.\\d{6}\";
    <TZ> = #\"\\+\\d{2}:\\d{2}\";
    <HOST> = #\"[^ ]+\";
    <APP> = HOST;
    <MSG> = !R (#\"[^\\.]+\" / #\".\")+;
   "))

(defn parse-and-match [line]
  (when-let [tree (syslog-parser line)]
    (for [record (m/match [tree]
                          [[:WORK & rs]] rs
                          :else nil)
          :when record]
      (m/match [record]
               [[:R _ [:TS & ts] _ host _ app _ & msg]]
               {:date (string/join ts) :host (str host "[" app "]") :message (string/join msg)}))))


(defn auth-headers-from-url
  "Takes a URL with embedded credentials and returns an encoded Basic Auth header value with those creds"
  [url-string]
  (->> url-string
       (URL.)
       (.getUserInfo)
       (.getBytes)
       (.encodeToString (Base64/getEncoder))
       (str "Basic ")))


(defn drain-queue []
  (locking queue
    (let [oldval @queue]
      (reset! queue [])
      oldval)))

(defn batch-send []
  (when-let [work (seq (drain-queue))]
    (println "Sending" (count work) "logs to Elasticsearch")
    (let [url (str elastic-url "/logs/_doc/_bulk")
          source-maps (mapcat parse-and-match work)
          bulk-request (as-> source-maps $
                            (map json/generate-string $)
                            (string/join (str "\n" bulk-index-action) $)
                            (str bulk-index-action $ "\n"))]
      (with-open [client (http/create-client :keep-alive false)]
        (let [response (http/POST
                           client
                           url
                           :headers {:content-type "application/x-ndjson"
                                     :authorization (auth-headers-from-url url)}
                           :body bulk-request)
              real-response (http/await response)
              status (http/status real-response)]
          (println "Got" (:code status) "from Elasticsearch")
          (when (>= (:code status) 400)
            (printerr "Got bad status: " status "\n" (http/error real-response))))))))


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
