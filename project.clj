(defproject logdrain-transformer "0.1.0-SNAPSHOT"
  :description "App for transforming from a Heroku log drain into Elasticsearch documents"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [compojure "LATEST"]
                 [ring/ring-jetty-adapter "LATEST"]
                 [environ "LATEST"]
                 [cheshire "LATEST"]
                 [http.async.client "LATEST"]
                 [instaparse "LATEST"]
                 [org.clojure/core.match "LATEST"]]
  :min-lein-version "2.7.1"
  :plugins [[environ/environ.lein "LATEST"]]
  :hooks[environ.leiningen.hooks]
  :uberjar-name "logdrain-transformer-standalone.jar"
  :profiles {:production {:env {:production true}}})
