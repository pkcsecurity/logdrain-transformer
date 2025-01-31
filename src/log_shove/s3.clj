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
       (if (and (= year "2018") (#{"08" "09" "10"} month) ((if (= month "10") #{"01" "02" "03" "04" "05"} (constantly true)) day)) "" ".23")
       ".json.gz"))

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

(defn stream-specific-date [year month day]
  (-> (s3-filename-by-date year month day)
      (get-s3-archive)
      (GZIPInputStream.)))

(defn two-digit-range [start end]
  (map (partial format "%02d") (range start end)))

(defn month->maxday [month]
  (cond
    (#{"01 03 05 07 08 10 12"} month) 32
    (= "02" month) 29
    :else 31))

(defn stream-special []
  (let [year "2019"
        month "03"]
    (for [day (two-digit-range 11 15)]
      (-> (s3-filename-by-date year month day)
          (get-s3-archive)
          (GZIPInputStream.)))))
