(ns cljds.ch8.glittering
  (:gen-class)
  (:require [cljds.ch8.util :refer :all]
            [clojure.string :as str]
            [glittering.algorithms :as ga]
            [glittering.core :as glitter]
            [glittering.pregel :as p]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]))

(defn line->edge [line]
  (let [[from to] (map to-long (str/split line #" "))]
    (glitter/edge from to 1.0)))

(defn load-edgelist [sc path]
  (let [edges (->> (spark/text-file sc path)
                   (spark/map line->edge))]
    (glitter/graph-from-edges edges 1.0)))

(defn line->canonical-edge [line]
  (let [[from to] (sort (map to-long (str/split line #" ")))]
    (glitter/edge from to 1.0)))

(defn load-canonical-edgelist [sc path]
  (let [edges (->> (spark/text-file sc path)
                   (spark/map line->canonical-edge))]
    (glitter/graph-from-edges edges 1.0)))


;; Connected Components

(defn connected-component-m [{:keys [src-attr dst-attr]}]
  (cond
    (< src-attr dst-attr) {:dst src-attr}
    (> src-attr dst-attr) {:src dst-attr}))

(defn connected-components [graph]
  (->> (glitter/map-vertices (fn [id attr] id) graph)
       (p/pregel {:vertex-fn (fn [id attr msg]
                               (min attr msg))
                  :message-fn connected-component-m
                  :combiner min})))

;; Label Propagation

(defn label-propagation-v [id attr msg]
  (key (apply max-key val msg)))

(defn label-propagation-m [{:keys [src-attr dst-attr]}]
  {:src {dst-attr 1}
   :dst {src-attr 1}})

(defn label-propagation [graph]
  (->> (glitter/map-vertices (fn [vid attr] vid) graph)
       (p/pregel {:message-fn label-propagation-m
                  :combiner (partial merge-with +)
                  :vertex-fn label-propagation-v
                  :max-iterations 10})))

;; Page Rank

(def damping-factor 0.85)

(defn page-rank-v [id prev msgsum]
  (let [[rank delta] prev
        new-rank (+ rank (* damping-factor msgsum))]
    [new-rank (- new-rank rank)]))

(defn page-rank-m [{:keys [src-attr attr]}]
  (let [delta (second src-attr)]
    (when (> delta 0.1)
      {:dst (* delta attr)})))

(defn page-rank [graph]
  (->> (glitter/outer-join-vertices (fn [id attr deg] (or deg 0))
                                    (glitter/out-degrees graph)
                                    graph)
       (glitter/map-triplets (fn [edge]
                               (/ 1.0 (glitter/src-attr edge))))
       (glitter/map-vertices (fn [id attr] (vector 0 0)))
       (p/pregel {:initial-message (/ (- 1 damping-factor)
                                      damping-factor)
                  :direction :out
                  :vertex-fn page-rank-v
                  :message-fn page-rank-m
                  :combiner +
                  :max-iterations 20})
       (glitter/map-vertices (fn [id attr] (first attr)))))


;; Semi-clustering

(def cmax 3)

(defn default-cluster [vid]
  {:id vid
   :ic 0
   :bc 0
   :score 1.0
   :vertices #{vid}})

(defn cluster-score [{:keys [ic bc vertices]}]
  (let [vc (count vertices)
        fb 0.0]
    (if (= vc 1)
      1.0
      (/ (- ic (* fb bc))
         (/ (* vc (dec vc))
            1)))))

(defn assoc-vertex-to-cluster [vid edges cluster]
  (let [vertices (:vertices cluster)
        grouped-edges (group-by (fn [[id weight]]
                                  (contains? vertices id)) edges)
        ic (reduce + (map second (get grouped-edges true)))
        bc (reduce + (map second (get grouped-edges false)))
        cluster (-> cluster
                    (update-in [:vertices] conj vid)
                    (update-in [:ic] + ic)
                    (update-in [:bc] + bc))]
    (assoc cluster :score (cluster-score cluster))))

(defn vertex-in-cluster? [vertex cluster]
  (-> cluster :vertices (contains? vertex)))

(defn sc-vertex-fn [vid attr {:keys [clusters edges] :as message}]
  (if (empty? message)
    #{(default-cluster vid)}
    (let [potential-clusters (->> clusters
                                  (remove (fn [cluster]
                                            (vertex-in-cluster? vid cluster)))
                                  (map (fn [cluster]
                                         (assoc-vertex-to-cluster vid edges cluster))))]
      (->> (concat clusters potential-clusters)
           (sort-by :score >)
           (take cmax)
           (set)))))

(defn sc-message-fn
  ;; Caclculate boundary and internal weights
  [{:keys [src-id src-attr dst-id dst-attr attr]}]
  [[:dst {:clusters src-attr
          :edges [[src-id attr]]}]])

(defn sc-merge-fn [a b]
  {:clusters (clojure.set/union (:clusters a) (:clusters b))
   :edges (concat (:edges a) (:edges b))})

(defn semi-clustering [graph]
  (->> (p/pregel {:initial-message {}
                  :edge-fn sc-message-fn
                  :combiner sc-merge-fn
                  :vertex-fn sc-vertex-fn
                  :max-iterations 10}
                 graph)
       (glitter/map-vertices
        (fn [id clusters]
          (sort (map :score clusters))))))
