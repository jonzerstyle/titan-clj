(defproject titan-clj "0.4.0-ALPHA-4-SNAPSHOT"
  :description "Simple library for interacting with Titan DB"
  :url "http://www.github.com/andrew-nguyen/titan-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.typed "0.2.14"]
                 [clj-gremlin "2.4.0-ALPHA-3-SNAPSHOT"]
                 [com.thinkaurelius.titan/titan-all "0.4.0"]
                 [commons-io/commons-io "2.4"]
                 [potemkin "0.3.4"]]
  
  :repositories [["bd-snapshots" {:url "s3p://bd-deps/snapshots/"
                                  :username ~(System/getenv "LEIN_USERNAME")
                                  :passphrase ~(System/getenv "LEIN_PASSPHRASE")}]]
  
  :plugins [[s3-wagon-private "1.1.2"]])
