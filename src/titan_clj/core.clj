(ns titan-clj.core
  (:require [clojure.core.typed :as t])
  (:import [com.tinkerpop.blueprints Direction Element]
           [com.thinkaurelius.titan.core
            KeyMaker
            LabelMaker
            TitanFactory
            TitanGraph
            TitanKey
            TitanLabel
            TitanType
            TypeMaker
            TypeMaker$UniquenessConsistency]
           [org.apache.commons.configuration Configuration BaseConfiguration]))

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
      (.addProperty conf (name k) (name v)))
    conf))

(t/ann unique-converter [(U (Value :lock) (Value :no-lock)) -> (U TypeMaker$UniquenessConsistency)])
(defn- unique-converter
  "Takes :lock and :no-lock and converts to appropriate type"
  [lock-type]
  (case lock-type
    :lock (TypeMaker$UniquenessConsistency/LOCK)
    :no-lock (TypeMaker$UniquenessConsistency/NO_LOCK)
    (throw (IllegalArgumentException. (str "Unsupported unique type: " lock-type)))))

(t/ann direction-converter [(U nil (Value :in) (Value :out) (Value :both)) ->  Direction])
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
(t/ann ^:no-check make-key! [TitanGraph Key-Map -> TitanKey])
(t/non-nil-return com.thinkaurelius.titan.core.TitanGraph/makeKey :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/make :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/dataType :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/unique :all)
(defn make-key!
  "Creates a TitanKey"
  [^TitanGraph g {:keys [name data-type indexed-standard indexed unique single list]}]
  (let [^KeyMaker maker (-> (.makeKey g name)
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
(t/ann ^:no-check make-label! [TitanGraph Label-Map -> TitanLabel])
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/manyToMany :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/manyToOne :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/oneToMany :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/oneToOne :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/signature :all)
(t/non-nil-return com.thinkaurelius.titan.core.LabelMaker/sortKey :all)
(defn make-label!
  [^TitanGraph g {:keys [name edge-type many-to-many many-to-one one-to-many one-to-one signature sort-key]}]
  (let [^LabelMaker maker (-> (.makeLabel g name)
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

(t/ann get-types [TitanGraph Class -> (t/Option (t/NonEmptySeq Any))])
(defn get-types
  "Gets types"
  [^TitanGraph g class]
  (if-let [types (.getTypes g class)]
    (seq types)))

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

(t/ann unique? [TitanType (U (Value :in) (Value :out) (Value :both)) -> boolean])
(defn unique?
  [^TitanType titan-type direction]
  (.isUnique titan-type (direction-converter direction)))
