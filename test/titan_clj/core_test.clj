(ns titan-clj.core-test
  (:require [clojure.test :refer :all]
            [titan-clj.core :refer :all]
            
            [clojure.java.io :as io]
            [clojure.core.typed :as t])
  (:import [org.apache.commons.io FileUtils]
           [com.tinkerpop.blueprints Vertex Edge]))

(deftest type-check
  (testing "Checking typing"
    (t/check-ns 'titan-clj.core)))

(def titan-tmp-dir "/tmp/titan-clj-test")

(defn- init-test-env
  []
  (FileUtils/deleteDirectory (io/as-file titan-tmp-dir)))

(defn- clean-test-env
  []
  (FileUtils/deleteDirectory (io/as-file titan-tmp-dir)))

(defn- fixture
  [f]
  (init-test-env)
  (f)
  (clean-test-env))

(use-fixtures :each fixture)

(def conf {:storage.backend :berkeleyje
           :storage.directory titan-tmp-dir
           :storage.index.search.backend :elasticsearch
           :storage.index.search.directory (str titan-tmp-dir "/searchindex")
           :storage.index.search.client-only "false"
           :storage.index.search.local-mode "true"})

(deftest test-connect!
  (init-test-env)
  (testing "Opening connection"
    (let [g (connect! conf)]
      (is (.isOpen g))
      (shutdown! g)))
  (clean-test-env))

(deftest test-keys
  (init-test-env)
  (let [g (connect! conf)]
    (testing "Creating keys"
      (make-key! g {:name "SomeKey" :data-type String})
      (let [types (get-types g com.thinkaurelius.titan.core.TitanType)]
        (is (= 1 (count types)))
        (let [type (first types)]
          (is (= "SomeKey" (get-name type)))
          (is (property-key? type))
          (is (not (edge-label? type)))
          (is (= String (get-data-type type)))))
      (let [key (make-key! g {:name "Locking Unique Key" :data-type String :unique :lock :indexed-standard Vertex})]
        (is (.isUnique key com.tinkerpop.blueprints.Direction/IN)))
      (let [key (make-key! g {:name "Locking Single Key" :data-type String :single :lock :indexed-standard Vertex})]
        (is (.isUnique key com.tinkerpop.blueprints.Direction/OUT))
        (is (.hasIndex key "standard" Vertex)))
      (let [key (make-key! g {:name "Locking Single Key Some Index" :data-type String :single :lock :indexed ["search" Vertex]})]
        (is (.isUnique key com.tinkerpop.blueprints.Direction/OUT))
        (is (.hasIndex key "search" Vertex)))
      (let [key (make-key! g {:name "Locking Single Key List" :data-type String :list true})]
        ; TODO - not sure if there is a way to test this...
        )
      (is (thrown-with-msg? RuntimeException
                            #"Unsupported unique type: :locky"
                            (make-key! g {:name "test exception" :data-type String :unique :locky})))
    (shutdown! g)))
  (clean-test-env))
