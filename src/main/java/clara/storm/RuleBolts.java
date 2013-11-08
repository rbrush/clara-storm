package clara.storm;

import backtype.storm.LocalDRPC;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.DRPCClient;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.util.Arrays;
import java.util.List;

/**
 * Support for running Clara rules in Storm.
 */
public class RuleBolts {

    /**
     * Function to make a new Clara session.
     */
    private static final IFn attachTopology;

    static {

        IFn require = RT.var("clojure.core", "require");

        require.invoke(Symbol.intern("clara.rules.storm-java"));

        attachTopology = RT.var("clara.rules.storm-java", "attach-topology");
    }

    /** 
     * Attach a set of rules to a topology. This is primary for local testing with the given RPC client.
     * 
     * @param builder the topology builder to attach the rules to the rules session.
     * @param drpc A distributed RPC client used to query
     * @param factSourceIds the identifiers of spouts or bolts that produce items on the "facts" stream used by the rule engine.
     * @param querySourceId the identifier of the DRPC used for issuing queries to the topology
     * @param rulesets One or more Clojure namespaces containing Clara rules. The namespaces must be visible in the caller's classloader
     * @return a client for issuing queries to the rule engine.
     */
    public static QueryClient attach(TopologyBuilder builder,
                                     LocalDRPC drpc,
                                     List<String> factSourceIds,
                                     String querySourceId,
                                     String... rulesets) {

        return (QueryClient) attachTopology.invoke(builder, drpc, factSourceIds, querySourceId, Arrays.asList(rulesets));
    }

    /** 
     * Attach a set of rules to a topology, without using a query client.
     * 
     * @param builder the topology builder to attach the rules to
     * @param factSourceIds the identifiers of spouts or bolts that produce items on the "facts" stream used by the rule engine.
     * @param querySourceId the identifier of the DRPC used for issuing queries to the topology
     * @param rulesets One or more Clojure namespaces containing Clara rules. The namespaces must be visible in the caller's classloader
     */
    public static void attach(TopologyBuilder builder,
                              List<String> factSourceIds,
                              String... rulesets) {

        attachTopology.invoke(builder, null, factSourceIds, null, Arrays.asList(rulesets));
    }
}
