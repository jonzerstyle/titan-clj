(ns titan-clj.core-test
  (:require [titan-clj.core :refer :all]
            [titan-clj.gremlin :refer :all] 
            [clojure.java.io :as io]
            [clojure.core.typed :as t]
            [midje.sweet :as ms]
            [clojure.repl :as clj_repl]
            ;[clojure.tools.trace :as tools_trace]
            )
  (:import [org.apache.commons.io FileUtils]
           [com.tinkerpop.blueprints Vertex Edge]
           [com.thinkaurelius.titan.core TitanKey TitanLabel])
  (:refer-clojure :exclude [key])) 

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

(def titan_vertex_key_alias com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex)

(def conf {:storage.backend :berkeleyje
           :storage.directory titan-tmp-dir
           :storage.index.search.backend :elasticsearch
           :storage.index.search.directory (str titan-tmp-dir "/searchindex")
           :storage.index.search.client-only "false"
           :storage.index.search.local-mode "true"})

;====  do an init and cleanup after each fact  ====
(ms/with-state-changes [(ms/before :facts (init-test-env)) 
                        (ms/after :facts (clean-test-env))]
  ;====  around wrappers facts  ====
  (ms/with-state-changes [(around :facts 
                                  (with-graph (connect! (map-to-conf conf)) 
                                           ?form))]
    ;=====  do the tests  ====
    (ms/fact "Properties file to Configuration"
      (let [p (prop-to-conf "test/resources/titan-cassandra-es.properties")]
       (.getString p "storage.hostname") => "127.0.0.1"))
    (ms/fact "type-check"
      (t/check-ns 'titan-clj.core))
    (ms/fact "test-props-to-conf"
      (let [p (prop-to-conf "test/resources/titan-cassandra-es.properties")]
       (.getString p "storage.hostname") => "127.0.0.1")
      (let [p (prop-to-conf (java.io.File. "test/resources/titan-cassandra-es.properties"))]
       (.getString p "storage.hostname") => "127.0.0.1")) 
    (ms/fact "Opening connection"
      (.isOpen *graph*) => true)
    (ms/fact "Testing types"
      (not (get-type "Something")) => true
      (make-key! {:name "Something" :data-type String})
      (type (get-type "Something")) => titan_vertex_key_alias)
    (ms/fact "Creating keys"
      (make-key! {:name "SomeKey" :data-type String})
      (type (get-type "SomeKey")) => titan_vertex_key_alias
      (let [types (get-types com.thinkaurelius.titan.core.TitanType)]
        (count types) => 1
        (let [type (first types)]
          (get-name type) => "SomeKey"
          (property-key? type) => true
          (not (edge-label? type)) => true
          (= String (get-data-type type)) = true))
      (let [key (make-key! {:name "Locking Unique Key" :data-type String :unique :lock :indexed-standard [Vertex]})]
        (.isUnique key com.tinkerpop.blueprints.Direction/IN) => true)
      (let [key (make-key! {:name "Locking Single Key" :data-type String :single :lock :indexed-standard [Vertex]})]
        (.isUnique key com.tinkerpop.blueprints.Direction/OUT) => true
        (.hasIndex key "standard" Vertex) => true)
      (let [key (make-key! {:name "Locking Single Key Some Index" :data-type 
                            String :single :lock :indexed [["search" Vertex]]})]
        (.isUnique key com.tinkerpop.blueprints.Direction/OUT) = true
        (.hasIndex key "search" Vertex) = true)
      (let [key (make-key! {:name "Key Some Index E/V" :data-type String :indexed [["search" Vertex] ["search" Edge]]})]
        (.hasIndex key "search" Vertex) => true
        (.hasIndex key "search" Edge) => true)
      (let [key (make-key! {:name "Key List" :data-type String :list true})]
        ; TODO - not sure if there is a way to test this...
        )
      (let [key (make-key! {:name "Locking Single Key List" :data-type String :single :lock :list true})]
        ; TODO - not sure if there is a way to test this...
        (.isUnique key com.tinkerpop.blueprints.Direction/OUT)
        )
      (let [key (make-key! {:name "Non-locking Single Key List" :data-type String :single :no-lock :list true})]
        ; TODO - not sure if there is a way to test this...
        (.isUnique key com.tinkerpop.blueprints.Direction/OUT) => true
        )
      (ms/fact "test exception"
       (make-key! {:name "test exception" :data-type String :unique :locky})
        => (ms/throws #"Unsupported unique consistency type: :locky")))
    ; TODO - need to test many-/one-* options but this requires testing via actually
    ; adding vertices/edges...
    (ms/fact "Creating labels"
      (make-label! {:name "label1"})
      (let [key-types (get-types TitanKey)
            label-types (get-types TitanLabel)]
        (= 0 (count key-types)) => true
        (= 1 (count label-types)) => true
        (let [label (first label-types)]
          (= "label1" (.getName label)) => true
          (.isDirected label) => true
          (not (.isUnidirected label)) => true))
      (let [label (make-label! {:name "l2" :edge-type :unidirected})]
        (unidirected? label) => true)
      (let [label (make-label! {:name "l3" :edge-type :directed})]
        (directed? label) => true))
    (ms/fact "Creating vertex"
      (make-key! {:name "a-list" :data-type Long :list true})
      (let [v (new-vertex! {:name "vertexname" :some-key "something unique" :key2 2 :a-list '(1,2)})
            vertices (seq (.getVertices *graph*))]
        (= 1 (count vertices)) => true
        (let [v (first vertices)]
          (if-let [props (get-properties v :some-key)]
            (= "something unique" (.getValue (first props))) => true
            (true? false) => true)
          (if-let [props (get-properties v :key2)]
            (= 2 (.getValue (first props))) => true
            (true? false) => true)
          (if-let [props (get-properties v :a-list)]
            (do
              (= 2 (count props)) => true
              (= 1 (.getValue (first props))) => true
              (= 2 (.getValue (second props))) => true)))))
    (ms/fact "Creating edges"
      (let [source (new-vertex! {:name "source"})
            dest (new-vertex! {:name "dest"})
            edge (add-edge! source dest "connecting-edge")
            vertices (seq (.getVertices *graph*))]
        (= 2 (count vertices)) => true
        (let [v1 (first vertices)
              v2 (second vertices)
              v1-edges (get-edges v1 :in "connecting-edge")
              not-v1-edges (get-edges v1 :out "connecting-edge")
              v2-edges (get-edges v2 :out "connecting-edge")
              not-v2-edges (get-edges v2 :in "connecting-edge")]
          (= 1 (get-edge-count v1)) => true
          (= 1 (get-edge-count v2)) => true
          (= "dest" (.getValue (first (get-properties v1 :name)))) => true
          (= 1 (count v1-edges)) => true
          (= 0 (count not-v1-edges)) => true
          (= "source" (.getValue (first (get-properties v2 :name)))) => true
          (= 1 (count v2-edges)) => true
          (= 0 (count not-v2-edges)) => true)))
    (ms/fact "Graph transactions"
      (try
        (within-tx
          (make-key! {:name "SomeKey" :data-type String})
          (throw (Exception. "Injected error for testing")))
        (catch Exception e))
      (not (get-type "SomeKey")) => true
      (within-tx
         (make-key! {:name "SomeKey" :data-type String}))
      (type (get-type "SomeKey")) => titan_vertex_key_alias)
    (ms/fact "Queries"
      (within-tx
        (make-key! {:name "name" :data-type String})
        (make-label! {:name "related-to"})
        (let [v1 (new-vertex! {:name "v1"})
              v2 (new-vertex! {:name "v2"})
              v3 (new-vertex! {:name "v3"})]
          (add-edge! v1 v2 "related-to")
          (add-edge! v2 v3 "related-to")))
      (let [vertices (seq (gremlin
                            *graph*
                            V))
            v1 (first vertices)
            v1-out-edges (seq (gremlin v1 outE))
            v1-in-edges (seq (gremlin v1 inE))
            v1-edges (seq (gremlin v1 bothE))
            e1 (first v1-out-edges)]
        (= 3 (count (seq vertices))) => true
        (= {:name "v1"} (first (gremlin v1 props))) => true
        (= "v1" (prop v1 "name")) => true
        (= 1 (count (seq v1-out-edges))) => true
        (= 0 (count (seq v1-in-edges))) => true
        (= 1 (count (seq v1-edges))) => true
        (= "related-to" (first (gremlin e1 label))) => true))
  ))
