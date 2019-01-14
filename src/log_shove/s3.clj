(ns log-shove.s3
  (:require [clojure.string :as string])
  (:import
   [java.util Calendar]
   [java.util.zip GZIPInputStream]
   [com.amazonaws.auth EnvironmentVariableCredentialsProvider]
   [com.amazonaws.services.s3
    AmazonS3Client
    AmazonS3ClientBuilder]
   [com.amazonaws.services.s3.model
    S3Object
    S3ObjectInputStream]
   [com.amazonaws.regions Regions]))

(def bucket-name "imb-generosity-logs")
(def filename-prefix "b612fd4b4b")

(defn ^AmazonS3Client s3-client []
  (-> (AmazonS3ClientBuilder/standard)
      (.withRegion Regions/US_EAST_1)
      (.withCredentials (EnvironmentVariableCredentialsProvider.))
      (.build)))

(defn nice-calendar [cal field]
  (condp = field
    Calendar/YEAR (.get cal field)
    Calendar/DATE (format "%02d" (.get cal field))
    Calendar/MONTH (->> (.get cal field)
                        (+ 1)
                        (format "%02d"))))

(defn s3-filename-by-calendar [^Calendar cal]
  (let [year (nice-calendar cal Calendar/YEAR)
        month (nice-calendar cal Calendar/MONTH)
        day (nice-calendar cal Calendar/DATE)]
    (str year "/"
         month "/"
         filename-prefix "."
         (string/join "-" [year month day])
         ".23.json.gz")))

(defn s3-filename-by-date [year month day]
  (str year "/"
       month "/"
       filename-prefix "."
       (string/join "-" [year month day])
       ".23.json.gz"))

(defn ^S3ObjectInputStream get-s3-archive [filename]
  (println "Trying to get" filename "from" bucket-name)
  (try
    (-> (s3-client)
        (.getObject bucket-name filename)
        (.getObjectContent))))

(defn stream-yesterday-archive []
  (-> (doto (Calendar/getInstance) (.add Calendar/DATE -1))
      (s3-filename-by-calendar)
      (get-s3-archive)
      (GZIPInputStream.)))

(defn stream-special []
  (let [year "2018"
        month "12"
        days (map (partial format "%02d") [5 6 7 8 9 10 11 12 19 20 21 27 28 29])]
    (for [day days]
      (-> (s3-filename-by-date year month day)
          (get-s3-archive)
          (GZIPInputStream.)))))
