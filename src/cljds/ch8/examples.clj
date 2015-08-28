(ns cljds.ch8.examples
  (:require [cljds.ch8.data :refer [load-edges to-java-rdd to-java-pair-rdd]]
            [cljds.ch8.glittering :refer :all]
            [cljds.ch8.traversal :refer :all]
            [clojure.set :as set]
            [clojure.string :as str]
            [glittering.algorithms :as ga]
            [glittering.core :as g]
            [glittering.destructuring :as g-de]
            [glittering.pregel :as p]
            [incanter.charts :as c]
            [incanter.core :as i]
            [incanter.stats :as s]
            [incanter.svg :as svg]
            [loom
             [graph :as loom]
             [io :as lio]
             [alg-generic :as gen]
             [alg :as alg]
             [gen :as generate]
             [attr :as attr]
             [label :as label]]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [t6.from-scala.core :as $]))

(defn ex-8-1 []
  (load-edges "twitter/98801140.edges"))

(defn ex-8-2 []
  (->> (load-edges "twitter/98801140.edges")
       (apply loom/graph)
       (lio/view)))

(defn ex-8-3 []
  (->> (load-edges "twitter/98801140.edges")
       (apply loom/digraph)
       (lio/view)))

(defn ex-8-4 []
  (->> (load-edges "twitter/98801140.edges")
       (apply loom/weighted-digraph)
       (lio/view)))

(defn ex-8-5 []
  (let [graph (->> (load-edges "twitter/98801140.edges")
                   (apply loom/digraph))]
    (alg/bf-traverse graph 100742942)))

(defn ex-8-6 []
  (let [graph (->> (load-edges "twitter/98801140.edges")
                   (apply loom/digraph))]
    (alg/pre-traverse graph 100742942)))

(defn ex-8-7 []
  (->> (load-edges "twitter/396721965.edges")
       (apply loom/digraph)
       (lio/view)))

(defn ex-8-8 []
  (let [graph (->> (load-edges "twitter/396721965.edges")
                   (apply loom/digraph))]
    (alg/bf-path graph 75914648 32122637)))


(defn ex-8-9 []
  (let [graph (->> (load-edges "twitter/396721965.edges")
                   (apply loom/weighted-digraph))]
    (-> (loom/add-edges graph [28719244 163629705 100])
        (alg/dijkstra-path 75914648 32122637))))


(defn ex-8-10 []
  (let [graph (->> (load-edges "twitter/396721965.edges")
                   (apply loom/weighted-graph))]
    (-> (alg/prim-mst graph)
        (lio/view))))


(defn ex-8-11 []
  (let [graph (->> (load-edges "twitter/396721965.edges")
                   (apply loom/weighted-graph))]
    (-> (loom/add-edges graph [28719244 163629705 100])
        (alg/prim-mst)
        (lio/view))))

(defn ex-8-12 []
  (->> (load-edges "twitter/15053535.edges")
       (apply loom/graph)
       (lio/view)))

(defn ex-8-13 []
  (->> (load-edges "twitter/15053535.edges")
       (apply loom/graph)
       (alg/connected-components)))

(defn ex-8-14 []
  (->> (load-edges "twitter/15053535.edges")
       (apply loom/digraph)
       (lio/view)))

(defn ex-8-15 []
  (->> (load-edges "twitter/15053535.edges")
       (apply loom/digraph)
       (alg/scc)
       (count)))

(defn ex-8-16 []
  (->> (load-edges "twitter/15053535.edges")
       (apply loom/digraph)
       (alg/scc)
       (sort-by count >)
       (first)))

(defn ex-8-17 []
  (->> (load-edges "twitter_combined.txt")
       (apply loom/digraph)
       (alg/density)
       (count)))

(defn ex-8-18 []
  (let [graph (->> (load-edges "twitter_combined.txt")
                   (apply loom/digraph))
        out-degrees (map #(loom/out-degree graph %)
                         (loom/nodes graph))]
    (-> (c/histogram out-degrees :nbins 50
                     :x-label "Twitter out degrees")
        (i/view))))

(defn ex-8-19 []
  (let [graph (->> (load-edges "twitter_combined.txt")
                   (apply loom/digraph))
        out-degrees (map #(loom/in-degree graph %)
                         (loom/nodes graph))]
    (-> (c/histogram out-degrees :nbins 50
                     :x-label "Twitter in degrees")
        (i/view))))

(defn ex-8-20 []
  (let [graph (generate/gen-rand (loom/graph) 10000 1000000)
        out-degrees (map #(loom/out-degree graph %)
                         (loom/nodes graph))]
    (-> (c/histogram out-degrees :nbins 50
                     :x-label "Random graph out degrees")
        (i/view))))

(defn ex-8-21 []
  (let [graph (->> (load-edges "twitter_combined.txt")
                   (apply loom/digraph))
        out-degrees (map #(loom/out-degree graph %)
                         (loom/nodes graph))
        points (frequencies out-degrees)]
    (-> (c/scatter-plot (keys points) (vals points))
        (c/set-axis :x (c/log-axis :label "log(out-degree)"))
        (c/set-axis :y (c/log-axis :label "log(frequency)"))
        (i/view))))

(defn ex-8-22 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (let [vertices [[1 "A"] [2 "B"] [3 "C"]]
          edges [(g/edge 1 2 0.5)
                 (g/edge 2 1 0.5)
                 (g/edge 3 1 1.0)]]
      (g/graph (spark/parallelize sc vertices)
               (spark/parallelize sc edges)))))


(defn ex-8-23 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (load-edgelist sc "data/twitter_combined.txt")))


(defn ex-8-24 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local[*]")
                             (conf/app-name "ch8"))
    (let [triangles (->> (load-canonical-edgelist
                          sc "data/twitter_combined.txt")
                         (g/partition-by :random-vertex-cut)
                         (ga/triangle-count)
                         (g/vertices)
                         (to-java-pair-rdd)
                         (spark/values)
                         (spark/collect)
                         (into []))
          data (frequencies triangles)]
      (-> (c/scatter-plot (keys data) (vals data))
          (c/set-axis :x (c/log-axis :label "# Triangles"))
          (c/set-axis :y (c/log-axis :label "# Vertices"))
          (i/view)))))


(defn triangle-m [{:keys [src-id src-attr dst-id dst-attr]}]
  (let [c (count (set/intersection src-attr dst-attr))]
    {:src c :dst c}))

(defn triangle-count [graph]
  (let [graph (->> (g/partition-by :random-vertex-cut graph)
                   (g/group-edges (fn [a b] a)))
        adjacent (->> (g/collect-neighbor-ids :either graph)
                      (to-java-pair-rdd)
                      (spark/map-values set))
        graph (g/outer-join-vertices
               (fn [vid attr adj] adj) adjacent graph)
        counters (g/aggregate-messages triangle-m + graph)]
    (->> (g/outer-join-vertices (fn  [vid vattr counter] (/ counter 2))
                                counters graph)
         (g/vertices))))

(defn ex-8-25 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (->> (load-canonical-edgelist
          sc "data/twitter/396721965.edges")
         (triangle-count)
         (spark/collect)
         (into []))))

(defn ex-8-26 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (let [triangles (->> (load-canonical-edgelist
                          sc "data/twitter_combined.txt")
                         (triangle-count)
                         (to-java-pair-rdd)
                         (spark/values)
                         (spark/reduce +))]
      (/ triangles 3))))


(defn ex-8-27 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "cljds.ch8"))
    (->> (load-edgelist sc "data/twitter/396721965.edges")
         (connected-components)
         (g/vertices)
         (spark/collect)
         (into []))))

(defn ex-8-28 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (->> (load-canonical-edgelist
          sc "data/twitter_combined.txt")
         (connected-components)
         (g/vertices)
         (to-java-pair-rdd)
         (spark/values)
         (spark/count-by-value)
         (into []))))

(defn ex-8-29 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (let [xs (->> (load-canonical-edgelist
                   sc "data/twitter_combined.txt")
                  (label-propagation)
                  (g/vertices)
                  (to-java-pair-rdd)
                  (spark/values)
                  (spark/count-by-value)
                  (vals)
                  (frequencies))]
      (-> (c/scatter-plot (keys xs) (vals xs))
          (c/set-axis :x (c/log-axis :label "Community Size"))
          (c/set-axis :y (c/log-axis :label "# Communities"))
          (i/view)))))

(defn top-n-by-pagerank [n graph]
  (->> (page-rank graph)
       (g/vertices)
       (to-java-pair-rdd)
       (spark/map-to-pair
        (s-de/key-value-fn
         (fn [k v]
           (spark/tuple v k))))
       (spark/sort-by-key false)
       (spark/take n)
       (into [])))

(defn most-frequent-attributes [n graph]
  (->> (g/vertices graph)
       (to-java-pair-rdd)
       (spark/values)
       (spark/count-by-value)
       (sort-by second >)
       (map first)))

(defn pagerank-for-community [community-id graph]
  (->> (g/subgraph (constantly true)
                   (fn [id attr] (= attr community-id))
                   graph)
       (top-n-by-pagerank 10)))

(defn ex-8-30 []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local")
                             (conf/app-name "ch8"))
    (let [communities (->> (load-edgelist
                            sc "data/twitter_combined.txt")
                           (label-propagation))
          by-popularity (most-frequent-attributes 2 communities)]
      (doseq [community (take 10 by-popularity)]
        (println
         (pagerank-for-community community communities))))))
