/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.aeneas.fastcsvloader;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author ccugnasc
 */
public class CqlFrameLoader {

    private final static Logger log = LoggerFactory.getLogger(CqlFrameLoader.class);
    private final CqlTypeConverter parser;


    protected Session session;
    protected PreparedStatement query;
    private Cluster cluster = null;


    /**
     * @param queue
     */

    public CqlFrameLoader(Iterator<String[]> queue, String queryText) {
        this.session = cluster().connect();
        log.info("preparing query \"{}\"", queryText);
        query = session.prepare(checkNotNull(queryText));
        parser=new CqlTypeConverter(query.getVariables().asList());

    }




    public void insert(String[] frame) throws Exception {
        Object[] binding = new Object[frame.length];
        for (int i = 0;i<parser.parsers.length;i++) {
             binding[i] = parser.parsers[i].parse(frame[i]);
        }
        session.execute(query.bind(binding));
    }


    public void addToBatch(BatchStatement batchStatement, String[] frame) throws Exception {
        Object[] binding = new Object[frame.length];
        for (int i = 0;i<parser.parsers.length;i++) {
            binding[i] = parser.parsers[i].parse(frame[i]);
        }
        batchStatement.add(query.bind(binding));
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
