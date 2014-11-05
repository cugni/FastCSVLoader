package es.bsc.aeneas.fastcsvloader.sstablewriter;

import com.google.common.util.concurrent.*;
import es.bsc.aeneas.fastcsvloader.CqlTypeConverter;
import es.bsc.aeneas.fastcsvloader.MappedReader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.service.ClientState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class converts a CSV file into a SSTable Cassandra file.
 * The file can be then streamed into Cassandra.
 */
public class SSTableWriter {
    private final static Logger log= LoggerFactory.getLogger(SSTableWriter.class);

    public static void main(String args[]) throws Exception {
        if(args.length!=3)
            throw new IllegalArgumentException("You must provide the name of the file and the query. Found "+args.length);
        String file=checkNotNull(args[0],"Fist argument (file name) missing");
        String query=checkNotNull(args[1],"Second argument (query) missing");
        String schema=checkNotNull(args[2],"Third argument (schema) missing");
        SSTableWriter writer = new SSTableWriter();
        writer.write(file, query, schema);

    }
    final ArrayBlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(1024);
    private volatile boolean done=false;
    public void write(String file, String query, String schema) throws InvalidRequestException, IOException, InterruptedException {
        Matcher matcher = Pattern.compile("insert +into +(?<keyspace>[^. ]+)\\.(?<table>[^. \\(]+).+", Pattern.CASE_INSENSITIVE).matcher(query);
        checkArgument(matcher.matches(),"Impossible to detect keyspace and table name from the query");
        String keyspace=matcher.group("keyspace");
        String table=matcher.group("table");
        File f=new File(file);
        checkArgument(f.exists(),"File not found");
        String fs = System.getProperty("FS", ",");
        checkArgument(fs.length()==1,"Supported only separators of 1 single char");
        char FS=fs.charAt(0);
        Config.setClientMode(true);

        MappedReader trajectoryReader;
        try {
            trajectoryReader   = new MappedReader(f, FS);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open the csv file",e);
        }

        final ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(16));
        final CountDownLatch latch = new CountDownLatch(16);
        done=false;
        for(int i=0;i<16;i++){
            ListenableFuture<String> submit = pool.submit(new SSTableThreadWriter(keyspace,table,schema,query,i));
            Futures.addCallback(submit, new FutureCallback<String>() {
                @Override
                public void onSuccess(String result) {
                    log.info("one node has finished {} ",result);
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    t.printStackTrace();
                    log.error("one thread has dead for"+t);
                    done=true;
                    pool.shutdown();
                    latch.countDown();
                }
            })
            ;
        }

        log.info("Starting to read data");
        while(trajectoryReader.hasNext()){
            //let's reuse the array.
            String[] next = trajectoryReader.next();
            while(!queue.offer(next,10,TimeUnit.SECONDS)){
             log.debug("Reader timeout exhausted");
            }
        }
        done=true;
        log.info("Completed read the data");
        latch.await();
        log.info("All threads ended");
    }

    private class SSTableThreadWriter implements Callable<String>{

        private final CQLSSTableWriter writer;
        private final CqlTypeConverter parser;
        private final int number;

        SSTableThreadWriter(String keyspace,String table,String schema,String query,int number) {
            this.number=number;

            File outputDir = new File("output"+File.separator
                    +"thread-"+number+File.separator+keyspace + File.separator + table);
            if (!outputDir.exists() && !outputDir.mkdirs())
            {
                throw new RuntimeException("Cannot create output directory: " + outputDir);
            }
            writer = CQLSSTableWriter.builder().inDirectory(outputDir)
                    .forTable(schema)
                    .using(query)
                    .withPartitioner(new Murmur3Partitioner())
                    .build();
            List<ColumnSpecification> metaData;
            try {

                ClientState state = ClientState.forInternalCalls();
                ParsedStatement.Prepared prepared = QueryProcessor.getStatement(query, state);
                CQLStatement stmt = prepared.statement;
                stmt.validate(state);
                metaData=prepared.boundNames;
            } catch (Exception e) {
                throw new RuntimeException("Impossible to get the schema",e);
            }
            parser=new CqlTypeConverter(metaData);
        }

        private Object[] binding;

        @Override
        public String call() throws InvalidRequestException, IOException, InterruptedException,NullPointerException {
            String[] line;
            while(!done) {
                while ((line = queue.poll(10, TimeUnit.SECONDS)) == null) {
                    log.debug("Thread-{} timeout expired",number);
                    if (done)
                        return "Done "+number;
                }

                if (binding == null)
                    binding = new Object[line.length];
                for (int i = 0; i < parser.parsers.length; i++) {
                    if(line[i]==null){
                        System.err.println(Arrays.toString(line));
                        throw new IOException("Found a null in thread+"+number);
                    }
                    binding[i] = parser.parsers[i].parse(line[i]);

                }
                writer.addRow(binding);

            }
            writer.close();
            return "Done "+number;
        }
    }

}
