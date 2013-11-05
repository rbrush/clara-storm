(ns clara.rules.storm-java
   "Support for the Java-based API. Users should not use this namespace directly."
  (:require [clara.rules.storm :as storm]
            [clara.rules.engine :as eng])
  (:import [clara.storm QueryClient]
           [clara.rules QueryResult]))


(deftype JavaQueryResult [result]
  QueryResult
  (getResult [_ fieldName] 
    (get result (keyword fieldName)))
  Object
  (toString [_]
    (.toString result)))

(defn- run-query [drpc stream-name rulebase name args]

  (let [results
        (storm/query-storm
         drpc
         stream-name
         rulebase
         (or (deref (resolve (symbol name)))
             (throw (IllegalArgumentException.
                     (str "Unable to resolve symbol to query: " name))))
         args)]
    (map #(JavaQueryResult. %) results)))

(deftype RPCQueryClient [drpc stream-name rulebase]
  QueryClient
   (query [this name args]
     (run-query drpc stream-name rulebase name args))

    (query [this name]
      (run-query drpc stream-name rulebase name {})))

(defn attach-topology
  "Attach to the storm topology, and return a QueryClient instance to run queries against it."
  [builder drpc fact-source-ids query-source-id rulesets]  

   (storm/attach-topology
    builder
    {:fact-source-ids fact-source-ids
     :query-source-id query-source-id
     :rulesets (map symbol rulesets)})

   (RPCQueryClient. drpc "test" (apply eng/load-rules (map symbol rulesets)))) ; FIXME: use correct stream name.

