(defproject cljds/ch8 "0.1.0"
  :description "Example code for the book Clojure for Data Science"
  :url "https://github.com/clojuredatascience/ch8-network-analysis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aysylu/loom "0.5.0"]
                 [incanter "1.5.6"]
                 [t6/from-scala "0.2.1"]
                 [glittering "0.1.2"]
                 [gorillalabs/sparkling "1.2.2"]
                 [org.apache.spark/spark-core_2.11 "1.3.1"]
                 [org.apache.spark/spark-graphx_2.11 "1.3.1"]]
  :main cljds.ch8.core
  :aot [cljds.ch8.core cljds.ch8.glittering]
  :profiles {:dev
             {:dependencies [[org.clojure/tools.cli "0.3.1"]]
              :repl-options {:init-ns cljds.ch8.examples}
              :resource-paths ["data" "dev-resources"]}})
