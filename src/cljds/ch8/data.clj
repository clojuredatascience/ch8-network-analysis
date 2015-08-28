(ns cljds.ch8.data
  (:require [clojure.java.io :as io]
            [cljds.ch8.util :refer [to-long]]
            [clojure.string :as str]
            [loom [graph :as loom]
             [io :as lio]
             [alg-generic :as gen]
             [alg :as alg]
             [gen :as generate]
             [attr :as attr]
             [label :as label]]
            [sparkling.scalaInterop :as scala])
  (:import [org.apache.spark.api.java JavaRDD JavaPairRDD]))


(defn line->edge [line]
  (->> (str/split line #" ")
       (mapv to-long)))

(defn load-edges [file]
  (->> (io/resource file)
       (io/reader)
       (line-seq)
       (map line->edge)))

(defn to-java-rdd [rdd]
  (JavaRDD/fromRDD rdd scala/OBJECT-CLASS-TAG))

(defn to-java-pair-rdd [rdd]
  (JavaPairRDD/fromRDD rdd
                       scala/OBJECT-CLASS-TAG
                       scala/OBJECT-CLASS-TAG))
