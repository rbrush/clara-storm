(ns clara.rules.storm
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [backtype.storm.clojure :refer [emit-bolt! ack! defbolt bolt bolt-spec]])
  (:import [clara.rules.engine ITransport LocalTransport]
           [backtype.storm.drpc ReturnResults DRPCSpout LinearDRPCTopologyBuilder]))

(def FACT-STREAM "fact")
(def TOKEN-STREAM "token")
(def WME-STREAM "wme")
(def QUERY-STREAM "query")

;; TODO: transport should determine if item can be processed locally,
;; e.g. any node that doesn't perform a join operation.

(deftype StormTransport [collector anchor node-to-hash]
  ITransport
  (send-elements [transport memory nodes elements]
    (doseq [[bindings element-group] (group-by :bindings elements)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(node-to-hash node) join-bindings element-group true] :anchor anchor :stream WME-STREAM)))

  (send-tokens [transport memory nodes tokens]
    (doseq [[bindings token-group] (group-by :bindings tokens)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(node-to-hash node) join-bindings token-group true] :anchor anchor :stream TOKEN-STREAM)))

  (retract-elements [transport memory nodes elements]
    (doseq [[bindings element-group] (group-by :bindings elements)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(node-to-hash node) join-bindings element-group false] :anchor anchor :stream WME-STREAM)))

  (retract-tokens [transport memory nodes tokens]
      (doseq [[bindings token-group] (group-by :bindings tokens)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(node-to-hash node) join-bindings token-group false] :anchor anchor :stream TOKEN-STREAM))))


(defbolt clara-bolt {WME-STREAM ["node-id" "bindings" "elements" "activation"]
                     TOKEN-STREAM ["node-id" "bindings" "tokens" "activation"]
                     QUERY-STREAM ["result" "return-info"]} 
                    {:prepare :true :params [rules]}
  [conf context collector]
  (let [network (eng/load-rules rules)
        bolt-memory (atom (eng/local-memory network (LocalTransport.)))
        hash-to-node (:id-to-node network)
        node-to-hash (:node-to-id network)]
    (bolt
     (execute [tuple]
      (let [memory (mem/to-transient @bolt-memory)
            transport (StormTransport. collector tuple node-to-hash)]       
        (condp = (.getSourceStreamId tuple)

          FACT-STREAM
          (doseq [[cls fact-group] (group-by class (.getValue tuple 0)) 
                  root (get-in network [:alpha-roots cls])]
            (eng/alpha-activate root fact-group memory transport))

          TOKEN-STREAM 
          (let [node (hash-to-node (.getValue tuple 0))
                bindings (.getValue tuple 1)
                tokens (.getValue tuple 2)
                activation (.getValue tuple 3)]
            
            (if activation 
              (eng/left-activate node bindings tokens memory transport)
              (eng/left-retract node bindings tokens memory transport)))
          
          WME-STREAM
          (let [node (hash-to-node (.getValue tuple 0))
                join-bindings (.getValue tuple 1)
                elements (.getValue tuple 2)
                activation (.getValue tuple 3)]

            (if activation
              (eng/right-activate node join-bindings elements memory transport)
              (eng/right-retract node join-bindings elements memory transport)))

          QUERY-STREAM
          (let [node (hash-to-node (.getValue tuple 0))
                params (.getValue tuple 1)
                return-info (.getValue tuple 2)
                result (map :bindings (mem/get-tokens memory node params))]

            (emit-bolt! collector
                        [(pr-str result) return-info]
                        :anchor tuple
                        :stream QUERY-STREAM)))

        ;; Update the node memory to include the changes.
        (reset! bolt-memory (mem/to-persistent! memory))
        (ack! collector tuple))))))

(defbolt query-bolt {QUERY-STREAM ["node-id" "bindings" "return-info"]} 
                    {:params [rules]}

  [tuple collector]
  (let [[node-id bindings] (edn/read-string (.getValue tuple 0))
        return-info (.getValue tuple 1)]    
    (emit-bolt! collector
                [node-id bindings return-info]
                :anchor tuple
                :stream QUERY-STREAM)
    (ack! collector tuple)))

(defn mk-clara-bolts 
  "Returns a bolt map that can be conj'd onto the rest of a topology structure
   that can be passed in as the bolt argument to the storm's topology Clojure function."
  [ruleset fact-source-id query-source-id]

  {"query-bolt" (bolt-spec {query-source-id :shuffle} (query-bolt ruleset))

   "clara-bolt" (bolt-spec
                 {["query-bolt" QUERY-STREAM] ["node-id" "bindings"],
                  [fact-source-id FACT-STREAM] :shuffle,
                  ["clara-bolt" WME-STREAM] ["node-id" "bindings"],
                  ["clara-bolt" TOKEN-STREAM] ["node-id" "bindings"]}
                 (clara-bolt ruleset)),

   "query-returner" (bolt-spec {["clara-bolt" QUERY-STREAM] :shuffle} (new ReturnResults))})

(defn query-storm [drpc name network query params]
   (let [query-node (get-in network [:query-nodes query])]

     (edn/read-string (.execute drpc name (pr-str [((:node-to-id network) query-node) params])))))