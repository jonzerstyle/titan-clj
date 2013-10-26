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
                   :single (U (Value :lock) (Value :no-lock))}))


(defmacro if-not-nil 
  [obj func & args]
  (if (not-any? nil? args)
     `(~func ~obj ~@args)
     `(identity ~obj)))

(t/ann unique-converter [(U nil (Value :lock) (Value :no-lock)) -> (U nil TypeMaker$UniquenessConsistency)])
(defn- unique-converter
  "Takes :lock and :no-lock and converts to appropriate type"
  [locker]
  (case locker
    :lock (TypeMaker$UniquenessConsistency/LOCK)
    :no-lock (TypeMaker$UniquenessConsistency/NO_LOCK)
    nil nil
    (throw (IllegalArgumentException. (str "Unsupported unique type: " locker)))))

; TODO - need to figure out what's wrong with type checking and the c-a macro
; TODO - add check for unique + indexed and throw error otherwise?
(t/ann ^:no-check make-key! [TitanGraph Key-Map -> TitanKey])
(t/non-nil-return com.thinkaurelius.titan.core.TitanGraph/makeKey :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/make :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/dataType :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/unique :all)
(defn make-key!
  "Creates a TitanKey"
  [^TitanGraph g {:keys [name data-type standard-index unique single]}]
  (let [^KeyMaker maker (-> (.makeKey g name)
                            (.dataType data-type)
                            (if-not-nil .indexed com.tinkerpop.blueprints.Vertex)
                            (if-not-nil .unique (unique-converter unique))
                            (if-not-nil .single (unique-converter single)))]
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

(t/ann unique? [TitanType (U (Value :in) (Value :out) (Value :both)) -> boolean])
(defn unique?
  [^TitanType titan-type direction]
  (.isUnique titan-type (direction-converter direction)))

(t/ann direction-converter [(U nil (Value :in) (Value :out) (Value :both)) ->  Direction])
(defn- direction-converter
  "Takes :in, :out, :both and converts to Direction"
  [dir]
  (case dir
    :in (Direction/IN)
    :out (Direction/OUT)
    :both (Direction/BOTH)
    (throw (IllegalArgumentException. (str "Unsupported direction: " dir)))))
