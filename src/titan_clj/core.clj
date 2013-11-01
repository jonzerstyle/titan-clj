(ns titan-clj.core
  (:require [clojure.core.typed :as t])
  (:import [com.tinkerpop.blueprints Direction Edge Element Vertex]
           [com.thinkaurelius.titan.core
            KeyMaker
            LabelMaker
            TitanEdge
            TitanFactory
            TitanGraph
            TitanKey
            TitanLabel
            TitanProperty
            TitanTransaction
            TitanType
            TitanVertex
            TypeMaker
            TypeMaker$UniquenessConsistency]
           [org.apache.commons.configuration Configuration BaseConfiguration])
  (:refer-clojure :exclude [name]))

;;; Util stuff
(defmacro if-run 
  [obj condition func & args] ;obj always needs to be first since this fn is used with ->
  `(if ~condition
     (~func ~obj ~@args)
     (identity ~obj)))

(t/ann ^:no-check map-to-conf [Connection-Map -> Configuration])
(defn map-to-conf
  "Converts a clojure map to a apache Configuration"
  [params]
  (let [conf (BaseConfiguration.)]
    (doseq [[k v] params]
      (.addProperty conf (clojure.core/name k) (clojure.core/name v)))
    conf))

(t/ann unique-converter [(U (Value :lock) (Value :no-lock)) -> (U TypeMaker$UniquenessConsistency)])
(defn- unique-converter
  "Takes :lock and :no-lock and converts to appropriate type"
  [lock-type]
  (case lock-type
    :lock (TypeMaker$UniquenessConsistency/LOCK)
    :no-lock (TypeMaker$UniquenessConsistency/NO_LOCK)
    (throw (IllegalArgumentException. (str "Unsupported unique type: " lock-type)))))

(t/ann direction-converter [(U (Value :in) (Value :out) (Value :both)) ->  Direction])
(defn- direction-converter
  "Takes :in, :out, :both and converts to Direction"
  [dir]
  (case dir
    :in (Direction/IN)
    :out (Direction/OUT)
    :both (Direction/BOTH)
    (throw (IllegalArgumentException. (str "Unsupported direction: " dir)))))

;;; TitanGraph

(t/def-alias Connection-Map (HMap :mandatory {:storage.backend String}
                                  :optional  {:storage.directory String}))
(t/ann connect! [Connection-Map -> TitanGraph])
(defn connect!
  "Connects to a TitanGraph"
  [params]
  (let [^Configuration conf (map-to-conf params)]
    (if-let [^TitanGraph g (TitanFactory/open conf)]
      g
      (throw (Exception. (str "Unable to open graph with params:" params))))))

(t/ann shutdown! [TitanGraph -> nil])
(defn shutdown!
  "Shutdowns the graph"
  [^TitanGraph g]
  (.shutdown g))

(t/ann commit! [TitanGraph -> nil])
(defn commit!
  "Commits current open transaction"
  [^TitanGraph g]
  (.commit g))

(t/ann rollback! [TitanGraph -> nil])
(defn rollback!
  "Rolls back current open transaction"
  [^TitanGraph g]
  (.rollback g))

(t/ann *graph* (t/Option (U TitanGraph TitanTransaction)))
(def ^:dynamic *graph* nil)

(defmacro with-graph [connection & body]
  `(binding [*graph* ~connection]
     (let [ret# (do ~@body)]
       (shutdown! *graph*)
       ret#)))

(defmacro with-existing-graph [connection & body]
  `(binding [*graph* ~connection]
     (let [ret# (do ~@body)]
       ret#)))

(defmacro within-tx [& body]
  `(try
     (let [ret# (do ~@body)]
       (.commit *graph*)
       ret#)
     (catch Exception e#
       (.rollback *graph*) (throw e#))))

; TODO - untested and not sure if implemented properly
(defmacro within-threadbound-tx [& body]
  `(let [tx# (.newTransaction *graph*)]
     (binding [*graph* tx#]
       (try
         (let [ret# (do ~@body)]
           (.commit tx#)
           ret#)
         (catch Exception e#
           (.rollback tx#) (throw e#))))))

;;; TitanKey

; TODO - need to figure out what's wrong with type checking and the c-a macro
; TODO - add check for unique + indexed and throw error otherwise?
(t/def-alias Key-Map
  (HMap :mandatory {:name String :data-type Class}
        ;:optional {:unique (U (Value :lock) (Value(:no-lock)))}))
        :optional {:unique (U (Value :lock) (Value :no-lock))
                   :single (U (Value :lock) (Value :no-lock))
                   :list boolean
                   :indexed-standard Class
                   :indexed '[String Class]}))
(t/ann ^:no-check make-key! [Key-Map -> TitanKey])
(t/non-nil-return com.thinkaurelius.titan.core.TitanGraph/makeKey :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/make :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/dataType :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/unique :all)
(defn make-key!
  "Creates a TitanKey"
  [{:keys [name data-type indexed-standard indexed unique single list]}]
  (let [^KeyMaker maker (-> (.makeKey *graph* name)
                            (.dataType data-type)
                            (if-run indexed-standard .indexed indexed-standard)
                            (if-run indexed .indexed (first indexed) (second indexed))
                            (if-run unique .unique (unique-converter unique))
                            (if-run single .single (unique-converter single))
                            (if-run list .list))]
    (.make maker)))

;;; TitanLabel
; TODO - not sure how to best type check make-label!
(t/def-alias Label-Map
  (HMap :mandatory {:name String}
        ;:optional {:unique (U (Value :lock) (Value(:no-lock)))}))
        :optional {:many-to-one (U (Value :lock) (Value :no-lock))
                   :one-to-many (U (Value :lock) (Value :no-lock))
                   :one-to-one (U (Value :lock) (Value :no-lock))
                   :signature '[TitanType]
                   :sort-key '[TitanType]
                   :edge-type (U (Value :directed) (Value :unidirected))}))
(t/ann ^:no-check make-label! [Label-Map -> TitanLabel])
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/manyToMany :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/manyToOne :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/oneToMany :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/oneToOne :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/signature :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/sortKey :all)
(defn make-label!
  [{:keys [name edge-type many-to-many many-to-one one-to-many one-to-one signature sort-key]}]
  (let [^LabelMaker maker (-> (.makeLabel *graph* name)
                              (if-run many-to-many .manyToMany)
                              (if-run many-to-one .manyToOne (unique-converter many-to-one))
                              (if-run one-to-many .manyToOne (unique-converter one-to-many))
                              (if-run one-to-one .manyToOne (unique-converter one-to-one))
                              (if-run (and signature (not-empty signature)) .signature (into-array signature))
                              (if-run (and sort-key (not-empty sort-key)) .sortKey ((into-array sort-key)))
                              (if-run (= edge-type :directed) .directed)
                              (if-run (= edge-type :unidirected) .unidirected))]
    (.make maker)))

(t/ann directed? [TitanLabel -> boolean])
(defn directed?
  [^TitanLabel label]
  (.isDirected label))

(t/ann unidirected? [TitanLabel -> boolean])
(defn unidirected?
  [^TitanLabel label]
  (.isUnidirected label))

;;; TitanType

(t/ann transaction? (predicate TitanTransaction))
(defn transaction?
  [t]
  (instance? TitanTransaction t))

; TODO - do we want semantic such that nil can be returned?
(t/ann get-types [Class -> (t/Option (t/NonEmptySeq Any))])
(defn get-types
  "Gets types"
  [class]
  (if-let [g *graph*] ; to satisfy core.typed checker since it can be nil if not properly bound
    (if (transaction? g)
      (if-let [types (.getTypes ^TitanTransaction g class)]
        (seq types))
      (if-let [types (.getTypes ^TitanGraph g class)]
        (seq types)))
    (throw (RuntimeException. "Should only be called from 'with-graph'"))))

(t/ann get-type [String -> (t/Option TitanType)])
(defn get-type
  "Gets type by name, returns nil if none found"
  [name]
  (get-types com.thinkaurelius.titan.core.TitanType); TODO - figure out why get-type doesn't work until we call get-types  
  (if-let [g *graph*] ; to satisfy core.typed checker since it can be nil if not properly bound
    (if (transaction? g)
      (.getType ^TitanTransaction g name)
      (.getType ^TitanGraph g name))
    (throw (RuntimeException. "Should only be called from 'with-graph'"))))

(t/ann get-name [TitanType -> String])
(t/non-nil-return com.thinkaurelius.titan.core.TitanType/getName :all)
(defn get-name
  [^TitanType titan-type]
  (.getName titan-type))

(t/ann get-data-type [TitanKey -> Class])
(t/non-nil-return com.thinkaurelius.titan.core.TitanKey/getDataType :all)
(defn get-data-type
  [^TitanKey titan-key]
  (.getDataType titan-key))

(t/ann modifiable? [TitanType -> boolean])
(defn modifiable?
  [^TitanType titan-type]
  (.isModifiable titan-type))

(t/ann edge-label? [TitanType -> boolean])
(defn edge-label? 
  [^TitanType titan-type]
  (.isEdgeLabel titan-type))

(t/ann property-key? [TitanType -> boolean])
(defn property-key?
  [^TitanType titan-type]
  (.isPropertyKey titan-type))

(t/def-alias Direction-Keywords (U (Value :in) (Value :out) (Value :both)))

(t/ann unique? [TitanType Direction-Keywords -> boolean])
(defn unique?
  [^TitanType titan-type direction]
  (.isUnique titan-type (direction-converter direction)))

;;; TitanVertex

; TODO - fix doseq type checking
(t/ann ^:no-check add-property! [TitanVertex String Object -> TitanVertex])
(defn add-property!
  [^TitanVertex v ^String name value]
  (if (coll? value)
    (doseq [p value]
      (.addProperty v name p))
    (.addProperty v name value))
  v)

; TODO - improve type check when I can figure how to specify something like
; generics
(t/ann get-properties [TitanVertex t/Keyword -> (t/Option (t/NonEmptySeq Any))])
(t/non-nil-return com.thinkaurelius.titan.core.TitanVertex/getProperties :all)
(defn get-properties
  [^TitanVertex v name]
  (if-let [props (.getProperties v (clojure.core/name name))]
    (seq props)))

; TODO - need to figure out how to type check addVertex since we pass null but
; core.typed expects Object
(t/def-alias Vertex-Map (t/Map t/Keyword Object))
(t/ann ^:no-check new-vertex! [Vertex-Map -> TitanVertex])
(defn new-vertex!
  [properties]
  (let [^TitanVertex v (.addVertex *graph* nil)]
    (doseq [prop properties]
      (add-property! v (clojure.core/name (first prop)) (second prop)))
    v))

;;; TitanEdge

; TODO - need to figure out how to type check since we pass null
(t/ann  ^:no-check add-edge! [TitanVertex TitanVertex String -> TitanEdge])
(t/non-nil-return com.thinkaurelius.titan.core.TitanVertex/addEdge :all)
(defn add-edge!
  [^TitanVertex source ^TitanVertex dest ^String label]
  (let [edge (.addEdge *graph* nil source dest label)]
    edge))

(t/ann get-edge-count [TitanVertex -> Long])
(defn get-edge-count
  [^TitanVertex v]
  (.getEdgeCount v))

; TODO - no-checking because into-array requires array support
(t/ann ^:no-check get-edges
  (Fn [TitanVertex -> (t/Option (t/NonEmptySeq Any))]
      [TitanVertex Direction-Keywords String String * -> (t/Option (t/NonEmptySeq Any))]))
(t/non-nil-return com.thinkaurelius.titan.core.TitanVertex/getEdges :all)
(defn get-edges
  ([^TitanVertex v]
   (seq (.getEdges v)))
  ([^TitanVertex v direction ^String label & labels]
   (let [label-array  (into-array String (concat (list label) labels))]
     (seq (.getEdges v (direction-converter direction) label-array)))))
