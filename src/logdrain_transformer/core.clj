(ns logdrain-transformer.core
  (:require [compojure.core :refer [defroutes GET PUT POST DELETE ANY]]
            [compojure.handler :refer [site]]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.adapter.jetty :as jetty]
            [environ.core :as environ]
            [clojure.string :as string]
            [clj-http.client :as client]))

(def elastic-url (environ/env :bonsai-url))

(defn parse-syslog-msg [line]
  (let [parts (-> line
                  (string/split #" " 8)
                  (nthnext 2))
        date (first parts)
        host (str (nth parts 2) "[" (nth parts 3) "]")
        message (last parts)]
    {:date date :host host :message message}))

(defroutes app
  (POST "/ingest" {body-stream :body}
    (println "Got /ingest")
    ;; Right now, this function assumes its input is valid (no validation)
    ;; and it only forwards the elastic server's response. TODO: not that.
    (doseq [line (-> body-stream
                     (slurp)
                     (string/split-lines))]
      (let [params (parse-syslog-msg line)
            response (client/post (str elastic-url "/logs/_doc/")
                                  {:content-type :json
                                   :form-params params})]
        (println (:status response) " " (:body response))
        {:status 204})))
  (ANY "*" []
    (route/not-found "This is not the page you're looking for!")))

(defn -main [& [port]]
  (let [port (Integer. (or port (environ/env :port) 5000))]
    (jetty/run-jetty (site #'app) {:port port :join? false})))

;; For interactive development:
;; (.stop server)
;; (def server (-main))
