(ns clara.rules.storm
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [backtype.storm.clojure :refer [emit-bolt! ack! defbolt bolt bolt-spec]])
  (:import [clara.rules.engine ITransport LocalTransport ProductionNode]
           [backtype.storm.drpc ReturnResults DRPCSpout LinearDRPCTopologyBuilder]
           [backtype.storm.generated GlobalStreamId Grouping NullStruct]
           [backtype.storm.utils Utils]))

(def FACT-STREAM "fact")
(def TOKEN-STREAM "token")
(def WME-STREAM "wme")
(def QUERY-STREAM "query")

;; TODO: transport should determine if item can be processed locally,
;; e.g. any node that doesn't perform a join operation.

(deftype StormTransport [collector anchor]
  ITransport
  (send-elements [transport memory nodes elements]
    (doseq [[bindings element-group] (group-by :bindings elements)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(:id node) join-bindings element-group true] :anchor anchor :stream WME-STREAM)))

  (send-tokens [transport memory nodes tokens]
    (doseq [[bindings token-group] (group-by :bindings tokens)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(:id node) join-bindings token-group true] :anchor anchor :stream TOKEN-STREAM)))

  (retract-elements [transport memory nodes elements]
    (doseq [[bindings element-group] (group-by :bindings elements)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(:id node) join-bindings element-group false] :anchor anchor :stream WME-STREAM)))

  (retract-tokens [transport memory nodes tokens]
      (doseq [[bindings token-group] (group-by :bindings tokens)
            node nodes
            :let [join-bindings (select-keys bindings (eng/get-join-keys node))]]
      (emit-bolt! collector [(:id node) join-bindings token-group false] :anchor anchor :stream TOKEN-STREAM))))


(defbolt clara-bolt {WME-STREAM ["node-id" "bindings" "elements" "activation"]
                     TOKEN-STREAM ["node-id" "bindings" "tokens" "activation"]
                     QUERY-STREAM ["result" "return-info"]} 
                    {:prepare :true :params [rules]}
  [conf context collector]
  (let [session (apply eng/load-rules rules)
        bolt-memory (atom (eng/local-memory session (LocalTransport.)))
        get-alphas-fn (eng/create-get-alphas-fn type session) ;; TODO: use a different type function.
        hash-to-node (:id-to-node session)]
    (bolt
     (execute [tuple]
      (let [memory (mem/to-transient @bolt-memory)
            transport (StormTransport. collector tuple)]       
        (condp = (.getSourceStreamId tuple)

          FACT-STREAM
          (doseq [[cls fact-group] (group-by class (.getValue tuple 0)) 
                  root (get-in session [:alpha-roots cls])]
            (eng/alpha-activate root fact-group memory transport))

          TOKEN-STREAM 
          (let [node (hash-to-node (.getValue tuple 0))
                bindings (.getValue tuple 1)
                tokens (.getValue tuple 2)
                activation (.getValue tuple 3)]
            
            (if activation 
              (eng/left-activate node bindings tokens memory transport)
              (eng/left-retract node bindings tokens memory transport))

            ;; If the node is a production node, fire the rules.
            (when (isa? (class node) ProductionNode)
              (eng/fire-rules* session [node] memory transport get-alphas-fn)))

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
  (let [[node-id bindings] (read-string (.getValue tuple 0))
        return-info (.getValue tuple 1)]    
    (emit-bolt! collector
                [node-id bindings return-info]
                :anchor tuple
                :stream QUERY-STREAM)
    (ack! collector tuple)))

;; TODO: Create Java API to do so with a specified ruleset and TopologyBuilder.

(defn- mk-inputs [inputs]
  (into {}
    (for [[stream-id grouping-spec] inputs]
      [(if (sequential? stream-id)
         (GlobalStreamId. (first stream-id) (second stream-id))
         (GlobalStreamId. stream-id Utils/DEFAULT_STREAM_ID))
       (Grouping/shuffle (NullStruct.))])))

(defn- add-groupings [declarer inputs]
  (doseq [[id grouping] (mk-inputs inputs)]
    (.grouping declarer id grouping)))


(defn attach-topology
  "Attach the pipeline to the topology, using logic drawn from Storm's mk-topology function"
  [builder {:keys [fact-source-ids query-source-id rulesets]}]
  
  ;; Create a bolt map that includes the query source and returner if specified.
  (let [bolt-map (if query-source-id
                   {"query-bolt" (bolt-spec {query-source-id :shuffle} (query-bolt rulesets))

                    "clara-bolt" (bolt-spec
                                  (into 
                                   {["query-bolt" QUERY-STREAM] ["node-id" "bindings"],
                                    ["clara-bolt" WME-STREAM] ["node-id" "bindings"],
                                    ["clara-bolt" TOKEN-STREAM] ["node-id" "bindings"]}

                                   ;; Add all fact sources to the bolt spec.
                                   (for [fact-source-id fact-source-ids]
                                     [[fact-source-id FACT-STREAM] :shuffle]))

                                  (clara-bolt rulesets))
                    "query-returner" (bolt-spec {["clara-bolt" QUERY-STREAM] :shuffle} (new ReturnResults))}

                   {"clara-bolt"  (bolt-spec
                                   (into
                                    {["clara-bolt" WME-STREAM] ["node-id" "bindings"],
                                     ["clara-bolt" TOKEN-STREAM] ["node-id" "bindings"]}
                                    
                                    ;; Add all fact sources to the bolt spec.
                                    (for [fact-source-id fact-source-ids]
                                      [[fact-source-id FACT-STREAM] :shuffle]))

                                   (clara-bolt rulesets))})]

    ;; Add the bolts to the builder.
    (doseq [[name {bolt :obj p :p conf :conf inputs :inputs}] bolt-map]
      (-> builder 
          (.setBolt name bolt (if-not (nil? p) (int p) p)) 
          (.addConfigurations conf) 
          (add-groupings inputs))))

  builder)

(defn query-storm [drpc name session query params]
   (let [query-node (get-in session [:query-nodes query])]

     ;; TODO: We should probably use the EDN reader here, although this is
     ;; an internal call. Should find a way to register the expected query results with EDN.
     (read-string (.execute drpc name (pr-str [(:id query-node) params])))))