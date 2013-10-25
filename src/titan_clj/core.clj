(ns titan-clj.core
  (:require [clojure.core.typed :as t])
  (:import [com.thinkaurelius.titan.core TitanFactory TitanGraph TitanKey TitanType]
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

(t/ann make-key! [TitanGraph '{:name String :data-type Class} -> TitanKey])
(t/non-nil-return com.thinkaurelius.titan.core.TitanGraph/makeKey :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/make :all)
(t/non-nil-return com.thinkaurelius.titan.core.KeyMaker/dataType :all)
(defn make-key!
  "Creates a TitanKey"
  [^TitanGraph g {:keys [name data-type]}]
  (let [maker (.makeKey g name)]
    (-> maker
        (.dataType data-type)
        .make)))

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

(t/ann is-edge-label? [TitanType -> boolean])
(defn is-edge-label? 
  [^TitanType titan-type]
  (.isEdgeLabel titan-type))

(t/ann is-property-key? [TitanType -> boolean])
(defn is-property-key?
  [^TitanType titan-type]
  (.isPropertyKey titan-type))
