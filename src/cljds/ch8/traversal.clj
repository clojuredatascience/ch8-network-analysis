(ns cljds.ch8.traversal
  (:require [loom.graph :as loom]))

(defn euler-tour? [graph]
  (let [degree (partial loom/out-degree graph)]
    (->> (loom/nodes graph)
         (filter (comp odd? degree))
         (count)
         (contains? #{0 2}))))
