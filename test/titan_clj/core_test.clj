(ns titan-clj.core-test
  (:require [clojure.test :refer :all]
            [titan-clj.core :refer :all]
            
            [clojure.java.io :as io]
            [clojure.core.typed :as t])
  (:import [org.apache.commons.io FileUtils]))

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

(def conf {:storage.backend :berkeleyje :storage.directory titan-tmp-dir})

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
          (is (is-property-key? type))
          (is (not (is-edge-label? type)))
          (is (= String (get-data-type type)))
    (shutdown! g)))))
  (clean-test-env))
