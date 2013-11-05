(ns clara.test-clara-storm
  (:use clojure.test clara.rules.storm)
  (:import [backtype.storm.drpc ReturnResults DRPCSpout
            LinearDRPCTopologyBuilder])
  (:import [backtype.storm LocalDRPC LocalCluster StormSubmitter]
           [backtype.storm.topology TopologyBuilder])
  (:import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy LousyWeather])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm clojure config])
  (:require [clara.rules.engine :as eng]
            [clara.sample-ruleset]
            [clara.rules.testfacts :as facts]))

(defspout fact-spout {FACT-STREAM ["fact"]}
  [conf context collector]
  (let []
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [[(facts/->Temperature 20 "MCI")]] :stream FACT-STREAM)
       (emit-spout! collector [[(facts/->WindSpeed 40 "MCI")]] :stream FACT-STREAM))

     (ack [id]))))

(defn mk-test-topology
  [drpc]

  (let [builder (TopologyBuilder.)]
    (.setSpout builder "drpc" drpc nil)
    (.setSpout builder "facts" fact-spout nil)
    (attach-topology builder
                     {:fact-source-ids ["facts"]
                      :query-source-id (if drpc "drpc" nil)
                      :rulesets ['clara.sample-ruleset]})
    (.createTopology builder)))

(deftest test-simple-query []
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)

        test-topology (mk-test-topology spout)

        rulebase (eng/load-rules 'clara.sample-ruleset)]

    (.submitTopology cluster "test" {} test-topology)

    ;; Let some events process.
    (Thread/sleep 2000)    

    ;; Ensure the query matches as expected.
    (is (= {:?loc "MCI"} 
           (first (query-storm drpc "test" rulebase clara.sample-ruleset/freezing-locations {}))))
    
    (.shutdown cluster)
    (.shutdown drpc)))

(deftest test-join-query []
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)

        test-topology (mk-test-topology spout)

        rulebase (eng/load-rules 'clara.sample-ruleset)]

    (.submitTopology cluster "test" {} test-topology)

    ;; Let some events process.
    (Thread/sleep 4000)    

    ;; Ensure the query matches as expected.
    (is (= {:?fact #clara.rules.testfacts.ColdAndWindy{:temperature 20, :windspeed 40}}
           (first (query-storm drpc "test" rulebase clara.sample-ruleset/find-cold-and-windy {}))))
    
    (.shutdown cluster)
    (.shutdown drpc)))

(deftest test-java-client []
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)

        ; Wire up topology with our drpc and fact spout.
        builder (doto (TopologyBuilder.)
                  (.setSpout "drpc" spout nil)
                  (.setSpout "facts" fact-spout nil))

        ;; Attach our Clara rules to the facts and drpc and
        ;; get our query client.
        client (clara.storm.RuleBolts/attach builder
                                             drpc
                                             ["facts"]
                                             "drpc"
                                             (doto (make-array String 1) 
                                                   (aset 0 "clara.sample-ruleset")))
        test-topology (.createTopology builder)]

    ;; Run the topology.
    (.submitTopology cluster "test" {} test-topology)

    ;; Let some events process.
    (Thread/sleep 2000)    

    ;; Ensure the query matches as expected.
    (is (= "MCI" 
           (.getResult 
            (first (.query client "clara.sample-ruleset/freezing-locations"))
            "?loc")))
    
    (.shutdown cluster)
    (.shutdown drpc)))
