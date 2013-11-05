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

    public static QueryClient attach(TopologyBuilder builder,
                                     LocalDRPC drpc,
                                     List<String> factSourceIds,
                                     String querySourceId,
                                     String... rulesets) {

        return (QueryClient) attachTopology.invoke(builder, drpc, factSourceIds, querySourceId, Arrays.asList(rulesets));
    }
}
