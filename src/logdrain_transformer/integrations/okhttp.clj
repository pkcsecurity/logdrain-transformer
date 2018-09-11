(ns logdrain-transformer.integrations.okhttp
  (:import [okhttp3
            OkHttpClient$Builder
            Request$Builder
            Callback
            RequestBody
            MediaType
            FormBody$Builder]
           [java.util.concurrent Executors TimeUnit])
  (:require [cheshire.core :as json]))

(defonce http-client
  (-> (OkHttpClient$Builder.)
      (.readTimeout 30 TimeUnit/SECONDS)
      (.build)))

(defn response-type-handler [response-type response]
  (case response-type
    :json (json/parse-string (.. response body string))
    :clj (json/parse-string (.. response body string) true)
    :text (.. response body string)
    :raw response))

(defn callback [& {:keys [on-success on-error on-exception response-type]}]
  (reify Callback
    (onFailure [_ call ex]
      (on-exception call ex))
    (onResponse [_ call resp]
      (if (.isSuccessful resp)
        (on-success call (response-type-handler response-type resp))
        (on-error call (response-type-handler response-type resp)))
      (.close resp))))

(defn add-headers [request-builder headers]
  (doseq [[k v] headers]
    (.header request-builder (name k) (str v))))

(defn form-body-builder [forms]
  (let [fbb (FormBody$Builder.)]
    (doseq [[k v] forms]
      (.add fbb (name k) (str v)))
    (.build fbb)))

(defn execute-sync-request [req]
  (-> http-client
      (.newCall req)
      (.execute)))

(defn execute-async-request [req callback-fn]
  (try (-> http-client
           (.newCall req)
           (.enqueue callback-fn))))

(defn create-request [method url {:keys [body
                                         form-body
                                         body-type
                                         media-type
                                         headers
                                         response-type]
                                  :or {media-type "application/json"
                                       headers {}
                                       body {}
                                       form-body {}
                                       response-type :clj
                                       body-type :request-body}}]
  (let [request-builder (Request$Builder.)
        request-body (RequestBody/create
                       (MediaType/parse media-type)
                       (if (= media-type "application/json")
                         (json/generate-string body)
                         body))
        method-upper (.toUpperCase method)]
    (.url request-builder url)
    (add-headers request-builder headers)
    (case body-type
      :request-body (if (or (= method-upper "GET") (= method-upper "HEAD"))
                      (.method request-builder method-upper nil)
                      (.method request-builder method-upper request-body))
      :form-body (.method request-builder method-upper (form-body-builder form-body)))
    (.build request-builder)))

(defmacro with-sync-request [req & body]
  `(let [hclient# http-client
         request# req
         client# (-> hclient#
                     (.newCall request#))]
     (with-open [~'r (.execute client#)]
       ~@body)))

(defn response->body-str [response]
  (-> response
      .body
      .string))

;; Core functions
;; 
(defn async-req [method url & {:keys [body
                                      form-body
                                      media-type
                                      headers
                                      response-type
                                      on-success
                                      on-exception
                                      on-error]
                               :or {media-type "application/json"
                                    headers {}
                                    body {}
                                    response-type :clj
                                    on-success #(println %1 %2)
                                    on-error #(println %1 %2)
                                    on-exception #(println %1 %2)}
                               :as opts}]
  "Executes an asynchronous http request using okhttp3.
  Returns a clojure map, json, or plain text string depending on `response-type`"
  (let [request (create-request method url opts)]
    (execute-async-request request (callback :on-success on-success
                                             :on-error on-error
                                             :on-exception on-exception
                                             :response-type response-type))))

(defn sync-req [method url & {:keys [body
                                     form-body
                                     media-type
                                     headers
                                     response-type]
                              :or {media-type "application/json"
                                   headers {}
                                   body {}
                                   response-type :clj}
                              :as opts}]
  "Executes a synchronous http request using okhttp3.
  Returns a clojure map, json, or plain text string depending on `response-type`"
  (let [request (create-request method url opts)]
    (response-type-handler response-type (execute-sync-request request))))

(defn post-form-request [url form-map]
  (-> (Request$Builder.)
      (.url url)
      (.post
        (form-body-builder form-map))
      (.build)))

