/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.aeneas.fastcsvloader;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author ccugnasc
 */
public class CqlFrameLoader {

    private final static Logger log = LoggerFactory.getLogger(CqlFrameLoader.class);


    protected Session session;
    protected PreparedStatement query;
    private Cluster cluster = null;
    private final Parser[] parsers;


    /**
     * @param queue
     */

    public CqlFrameLoader(Iterator<String[]> queue, String queryText) {
        this.session = cluster().connect();
        log.info("preparing query \"{}\"", queryText);
        query = session.prepare(checkNotNull(queryText));
        int i = 0;
        parsers=new Parser[query.getVariables().size()];
        for (ColumnDefinitions.Definition cd : query.getVariables()) {
            //TODO not really efficient
            Parser parser = checkNotNull(parserMap.get(cd.getType().asJavaClass()), "Parser not found for " + cd.getType());
            parsers[i++] = parser;
        }
    }

    private final static Map<Class, Parser> parserMap;

    static {
        ImmutableMap.Builder<Class, Parser> builder = ImmutableMap.builder();
        builder.put(Integer.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Integer.parseInt(string);
            }
        });
        builder.put(String.class, new Parser() {
            @Override
            public Object parse(String string) {
                return string;
            }
        });
        builder.put(Double.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Double.parseDouble(string);
            }
        });
        builder.put(Float.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Float.parseFloat(string);
            }
        });
        parserMap=builder.build();

    }


    public void insert(String[] frame) throws Exception {
        Object[] binding = new Object[frame.length];
        for (int i = 0;i<parsers.length;i++) {
             binding[i] = parsers[i].parse(frame[i]);
        }
        session.execute(query.bind(binding));
    }


    public void addToBatch(BatchStatement batchStatement, String[] frame) throws Exception {
        Object[] binding = new Object[frame.length];
        for (int i = 0;i<parsers.length;i++) {
            binding[i] = parsers[i].parse(frame[i]);
        }
        batchStatement.add(query.bind(binding));
    }

    private interface Parser {
        Object parse(String string);
    }

    public void insertBatch(BatchStatement batchStatement) throws Exception {
        if (log.isDebugEnabled())
            log.debug("Inserting batch of size {}", batchStatement.getStatements().size());
        session.execute(batchStatement);
    }

    public Cluster cluster() {

        if (cluster != null) return cluster;
        String[] entryPoints = System.getProperty("cassandra.servers", "localhost").split(",");
        String clusterName = System.getProperty("cassandra.cluster-name", "Test Cluster");
        int port = Integer.getInteger("cassandra.port", 9042);
        log.info("Connecting the cluster {} via hosts {} with port {}", clusterName, Arrays.toString(entryPoints), port);
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(entryPoints)
                .withClusterName(clusterName)
                .withPort(port)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        cluster = builder.build();
        return cluster;

    }
}
