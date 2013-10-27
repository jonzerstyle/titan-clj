(ns titan-clj.core
  (:require [clojure.core.typed :as t])
  (:import [com.tinkerpop.blueprints Direction Element]
           [com.thinkaurelius.titan.core
            KeyMaker
            TitanFactory
            TitanGraph
            TitanKey
            TitanType
            TypeMaker
            TypeMaker$UniquenessConsistency]
           [org.apache.commons.configuration Configuration BaseConfiguration]))

(t/def-alias Connection-Map (HMap :mandatory {:storage.backend String}
                                  :optional  {:storage.directory String}))

(t/ann ^:no-check map-to-conf [Connection-Map -> Configuration])
(defn map-to-conf
  "Converts a clojure map to a apache Configuration"
  [params]
  (let [conf (BaseConfiguration.)]
    (doseq [[k v] params]
      (.addProperty conf (name k) (name v)))
    conf))

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

(t/ann indexed (Fn [KeyMaker Class -> KeyMaker]
                   [KeyMaker String Class -> KeyMaker]))
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/indexed :all)
(defn indexed
  ([^KeyMaker key-maker element] (.indexed key-maker element))
  ([^KeyMaker key-maker name element] (.indexed key-maker name element)))

(t/def-alias Key-Map
  (HMap :mandatory {:name String :data-type Class}
        ;:optional {:unique (U (Value :lock) (Value(:no-lock)))}))
        :optional {:unique (U (Value :lock) (Value :no-lock))
                   :single (U (Value :lock) (Value :no-lock))
                   :list boolean
                   :indexed-standard Class
                   :indexed '[String Class]}))


(defmacro if-run 
  [obj condition func & args] ;obj always needs to be first since this fn is used with ->
  `(if ~condition
     (~func ~obj ~@args)
     (identity ~obj)))

(t/ann unique-converter [(U (Value :lock) (Value :no-lock)) -> (U TypeMaker$UniquenessConsistency)])
(defn- unique-converter
  "Takes :lock and :no-lock and converts to appropriate type"
  [lock-type]
  (case lock-type
    :lock (TypeMaker$UniquenessConsistency/LOCK)
    :no-lock (TypeMaker$UniquenessConsistency/NO_LOCK)
    (throw (IllegalArgumentException. (str "Unsupported unique type: " lock-type)))))

; TODO - need to figure out what's wrong with type checking and the c-a macro
; TODO - add check for unique + indexed and throw error otherwise?
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

(t/ann direction-converter [(U nil (Value :in) (Value :out) (Value :both)) ->  Direction])
(defn- direction-converter
  "Takes :in, :out, :both and converts to Direction"
  [dir]
  (case dir
    :in (Direction/IN)
    :out (Direction/OUT)
    :both (Direction/BOTH)
    (throw (IllegalArgumentException. (str "Unsupported direction: " dir)))))

(t/ann unique? [TitanType (U (Value :in) (Value :out) (Value :both)) -> boolean])
(defn unique?
  [^TitanType titan-type direction]
  (.isUnique titan-type (direction-converter direction)))
