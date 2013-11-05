package clara.storm;

import clara.rules.QueryResult;

import java.util.Map;

/**
 * Client for querying a Clara session running on a distributed cluster.
 */
public interface QueryClient {

    /**
     * Runs the query by the given name against the working memory and returns the matching
     * results. Query names are structured as "namespace/name"
     *
     * @param queryName the name of the query to perform, formatted as "namespace/name".
     * @param arguments query arguments
     * @return a list of query results
     */
    public Iterable<QueryResult> query(String queryName, Map<String,?> arguments);

    /**
     * Runs the query by the given name against the working memory and returns the matching
     * results. Query names are structured as "namespace/name"
     *
     * @param queryName the name of the query to perform, formatted as "namespace/name".
     * @return a list of query results
     */
    public Iterable<QueryResult> query(String queryName);
}
