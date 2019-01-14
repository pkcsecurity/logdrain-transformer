(ns log-shove.core
  (:require [clojure.string :as string]
            [cheshire.core :as json]
            [http.async.client :as http]
            [clojure.java.io :as io]
            [environ.core :as environ]
            [log-shove.s3 :as s3])
  (:import [java.net URL]
           [java.util Base64]
           [java.util.concurrent
            Executors
            TimeUnit]))

(def elastic-url (environ/env :bonsai-url))
(def bulk-index-action (str (json/generate-string {:index {:_index "logs" :_type "_doc"}}) "\n"))

(def batch-count (atom 0))

(defn auth-headers-from-url
  "Takes a URL with embedded credentials and returns an encoded Basic Auth header value with those creds"
  [url-string]
  (->> url-string
       (URL.)
       (.getUserInfo)
       (.getBytes)
       (.encodeToString (Base64/getEncoder))
       (str "Basic ")))

(defn batch-send [work]
  (print "Indexing batch #" (str (swap! batch-count inc) "... "))
  (let [url (str elastic-url "/logs/_doc/_bulk")
        bulk-request (as-> work $
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
        (println (:code status))
        (when (>= (:code status) 400)
          (println "Got bad status: " status "\n" (http/error real-response)))))))

(defn stream-logs-from-reader [reader lazy?]
  (for [log-json (if lazy?
                   (json/parsed-seq reader true)
                   (json/parse-stream reader true))
        :let [log (:_source log-json)]]
    {:date (:timestamp log)
     :host (:_app log)
     :message (:message log)}))

(defn enqueue-file [file & {:keys [lazy?] :or {lazy? true}}]
  (with-open [r (io/reader file)]
    (dorun (map batch-send (partition 50 (stream-logs-from-reader r lazy?))))))

(defn -main [& _]
  #_(println "Retrieving and indexing yesterday's log archive...")
  (println "Retrieving and indexing desired log archives...")
  (with-open [stream (s3/stream-special)] ;; (s3/stream-yesterday-archive)
    (enqueue-file stream :lazy false))
  (println "Done."))

(defn index-local-files [dirpath]
  (println "Checking" dirpath "for files to upload...")
  (let [dir (io/file dirpath)]
    (if (.isDirectory dir)
      (doseq [file (.listFiles dir) :when (.isFile file)]
        (println "Queueing" (str (.getName file) "..."))
        (enqueue-file file))
      (if (.isFile dir)
        (enqueue-file dir)
        (throw (java.lang.IllegalArgumentException. (str dirpath "is neither a file nor a directory")))))))
