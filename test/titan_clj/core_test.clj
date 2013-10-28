(ns titan-clj.core-test
  (:require [clojure.test :refer :all]
            [titan-clj.core :refer :all]
            
            [clojure.java.io :as io]
            [clojure.core.typed :as t])
  (:import [org.apache.commons.io FileUtils]
           [com.tinkerpop.blueprints Vertex Edge]
           [com.thinkaurelius.titan.core TitanKey TitanLabel]))

(def titan-tmp-dir "/tmp/titan-clj-test")

(defn- delete-dir
  [dir]
  (try (FileUtils/forceDelete (io/as-file dir))
    (catch java.io.FileNotFoundException e)))

(defn- init-test-env
  []
  (delete-dir titan-tmp-dir))

(defn- clean-test-env
  []
  (delete-dir titan-tmp-dir))

(def ^:dynamic g nil)

(def conf {:storage.backend :berkeleyje
           :storage.directory titan-tmp-dir
           :storage.index.search.backend :elasticsearch
           :storage.index.search.directory (str titan-tmp-dir "/searchindex")
           :storage.index.search.client-only "false"
           :storage.index.search.local-mode "true"})

(defn- fixture
  [f]
  (init-test-env)
  (binding [g (connect! conf)]
    (f)
    (shutdown! g))
  (clean-test-env))

(use-fixtures :each fixture)

(deftest type-check
  (testing "Checking typing"
    (t/check-ns 'titan-clj.core)))

(deftest test-connect!
  (testing "Opening connection"
    (is (.isOpen g))))

(deftest test-keys
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
                          (make-key! g {:name "test exception" :data-type String :unique :locky})))))

; TODO - need to test many-/one-* options but this requires testing via actually
; adding vertices/edges
(deftest test-labels
  (testing "Creating labels"
    (make-label! g {:name "label1"})
    (let [key-types (get-types g TitanKey)
          label-types (get-types g TitanLabel)]
      (is (= 0 (count key-types)))
      (is (= 1 (count label-types)))
      (let [label (first label-types)]
        (is (= "label1" (.getName label)))
        (is (.isDirected label))
        (is (not (.isUnidirected label)))))
    (let [label (make-label! g {:name "l2" :edge-type :unidirected})]
      (is (unidirected? label)))
    (let [label (make-label! g {:name "l3" :edge-type :directed})]
      (is (directed? label)))))

(deftest test-vertex
  (testing "Creating vertex"
    (new-vertex! g {:name "vertexname" :some-key "something unique" :key2 2})
    (let [vertices (seq (.getVertices g))]
      (is (= 1 (count vertices)))
      (let [v (first vertices)]
        (if-let [props (get-properties v :some-key)]
          (is (= "something unique" (.getValue (first props))))
          (is false))
        (if-let [props (get-properties v :key2)]
          (is (= 2 (.getValue (first props))))
          (is false))))))
