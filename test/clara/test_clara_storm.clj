(ns clara.test-clara-storm
  (:use clojure.test clara.rules.storm)
  (:import [backtype.storm.drpc ReturnResults DRPCSpout
            LinearDRPCTopologyBuilder])
  (:import [backtype.storm LocalDRPC LocalCluster StormSubmitter])
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

(deftest test-simple-query []
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)
        topology (topology 
                  {"drpc" (spout-spec spout)
                   "facts" (spout-spec fact-spout)}

                  ;; Bolt topology defined by Clara.
                  (mk-clara-bolts 'clara.sample-ruleset ["facts"] "drpc"))

        rulebase (eng/load-rules 'clara.sample-ruleset)]

    (.submitTopology cluster "test" {} topology)

    ;; Let some events process.
    (Thread/sleep 2000)    

    ;; Ensure the query matches as expected.
    (is (= {:?loc "MCI"} 
           (first(query-storm drpc "test" rulebase clara.sample-ruleset/freezing-locations {}))))
    
    (.shutdown cluster)
    (.shutdown drpc)))

(deftest test-join-query []
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)
        topology (topology 
                  {"drpc" (spout-spec spout)
                   "facts" (spout-spec fact-spout)}

                  ;; Bolt topology defined by Clara.
                  (mk-clara-bolts 'clara.sample-ruleset ["facts"] "drpc"))

        rulebase (eng/load-rules 'clara.sample-ruleset)]

    (.submitTopology cluster "test" {} topology)

    ;; Let some events process.
    (Thread/sleep 4000)    

    ;; Ensure the query matches as expected.
    (is (= {:?fact #clara.rules.testfacts.ColdAndWindy{:temperature 20, :windspeed 40}}
           (first(query-storm drpc "test" rulebase clara.sample-ruleset/find-cold-and-windy {}))))
    
    (.shutdown cluster)
    (.shutdown drpc)))

