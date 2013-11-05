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

(def conf {:storage.backend :berkeleyje
           :storage.directory titan-tmp-dir
           :storage.index.search.backend :elasticsearch
           :storage.index.search.directory (str titan-tmp-dir "/searchindex")
           :storage.index.search.client-only "false"
           :storage.index.search.local-mode "true"})

(defn- fixture
  [f]
  (init-test-env)
  (with-graph (connect! (map-to-conf conf))
    (f))
  (clean-test-env))

(use-fixtures :each fixture)

(deftest type-check
  (testing "Checking typing"
    (t/check-ns 'titan-clj.core)))

(deftest test-connect!
  (testing "Opening connection"
    (is (.isOpen *graph*))))

(deftest test-general-types
  (testing "Testing types"
    (is (not (get-type "Something")))
    (make-key! {:name "Something" :data-type String})
    (is (get-type "Something"))))

(deftest test-keys
  (testing "Creating keys"
    (make-key! {:name "SomeKey" :data-type String})
    (is (get-type "SomeKey"))
    (let [types (get-types com.thinkaurelius.titan.core.TitanType)]
      (is (= 1 (count types)))
      (let [type (first types)]
        (is (= "SomeKey" (get-name type)))
        (is (property-key? type))
        (is (not (edge-label? type)))
        (is (= String (get-data-type type)))))
    (let [key (make-key! {:name "Locking Unique Key" :data-type String :unique :lock :indexed-standard Vertex})]
      (is (.isUnique key com.tinkerpop.blueprints.Direction/IN)))
    (let [key (make-key! {:name "Locking Single Key" :data-type String :single :lock :indexed-standard Vertex})]
      (is (.isUnique key com.tinkerpop.blueprints.Direction/OUT))
      (is (.hasIndex key "standard" Vertex)))
    (let [key (make-key! {:name "Locking Single Key Some Index" :data-type String :single :lock :indexed [["search" Vertex]]})]
      (is (.isUnique key com.tinkerpop.blueprints.Direction/OUT))
      (is (.hasIndex key "search" Vertex)))
    (let [key (make-key! {:name "Key Some Index E/V" :data-type String :indexed [["search" Vertex] ["search" Edge]]})]
      (is (.hasIndex key "search" Vertex))
      (is (.hasIndex key "search" Edge)))
    (let [key (make-key! {:name "Locking Single Key List" :data-type String :list true})]
      ; TODO - not sure if there is a way to test this...
      )
    (is (thrown-with-msg? RuntimeException
                          #"Unsupported unique type: :locky"
                          (make-key! {:name "test exception" :data-type String :unique :locky})))))

; TODO - need to test many-/one-* options but this requires testing via actually
; adding vertices/edges
(deftest test-labels
  (testing "Creating labels"
    (make-label! {:name "label1"})
    (let [key-types (get-types TitanKey)
          label-types (get-types TitanLabel)]
      (is (= 0 (count key-types)))
      (is (= 1 (count label-types)))
      (let [label (first label-types)]
        (is (= "label1" (.getName label)))
        (is (.isDirected label))
        (is (not (.isUnidirected label)))))
    (let [label (make-label! {:name "l2" :edge-type :unidirected})]
      (is (unidirected? label)))
    (let [label (make-label! {:name "l3" :edge-type :directed})]
      (is (directed? label)))))

(deftest test-vertex
  (testing "Creating vertex"
    (make-key! {:name "a-list" :data-type Long :list true})
    (let [v (new-vertex! {:name "vertexname" :some-key "something unique" :key2 2 :a-list '(1,2)})
          vertices (seq (.getVertices *graph*))]
      (is (= 1 (count vertices)))
      (let [v (first vertices)]
        (if-let [props (get-properties v :some-key)]
          (is (= "something unique" (.getValue (first props))))
          (is false))
        (if-let [props (get-properties v :key2)]
          (is (= 2 (.getValue (first props))))
          (is false))
        (if-let [props (get-properties v :a-list)]
          (do
            (is (= 2 (count props)))
            (is (= 1 (.getValue (first props))))
            (is (= 2 (.getValue (second props))))))))))

(deftest test-edge
  (testing "Creating edges"
    (let [source (new-vertex! {:name "source"})
          dest (new-vertex! {:name "dest"})
          edge (add-edge! source dest "connecting-edge")
          vertices (seq (.getVertices *graph*))]
      (is (= 2 (count vertices)))
      (let [v1 (first vertices)
            v2 (second vertices)
            v1-edges (get-edges v1 :in "connecting-edge")
            not-v1-edges (get-edges v1 :out "connecting-edge")
            v2-edges (get-edges v2 :out "connecting-edge")
            not-v2-edges (get-edges v2 :in "connecting-edge")]
        (is (= 1 (get-edge-count v1)))
        (is (= 1 (get-edge-count v2)))
        (is (= "dest" (.getValue (first (get-properties v1 :name)))))
        (is (= 1 (count v1-edges)))
        (is (= 0 (count not-v1-edges)))
        (is (= "source" (.getValue (first (get-properties v2 :name)))))
        (is (= 1 (count v2-edges)))
        (is (= 0 (count not-v2-edges)))))))

(deftest test-tx
  (testing "Graph transactions"
    (try
      (within-tx
        (make-key! {:name "SomeKey" :data-type String})
        (throw (Exception. "Injected error for testing")))
      (catch Exception e))
    (is (not (get-type "SomeKey")))
    (within-tx
       (make-key! {:name "SomeKey" :data-type String}))
    (is (get-type "SomeKey"))))
