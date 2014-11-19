package es.bsc.aeneas.fastcsvloader.sstablewriter;


import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class converts a CSV file into a SSTable Cassandra file.
 * The file can be then streamed into Cassandra.
 */
public class SSTableWriter implements AtomsWriter {
  private final static Logger log= LoggerFactory.getLogger(SSTableWriter.class);

  public static final String COORDINATES_SCHEMA =  "CREATE TABLE IF NOT EXISTS %s  (\n" +
           "            frame   int,\n" +
           "            atom_id int,\n" +
           "            x   float,\n" +
           "            y   float,\n" +
           "            z   float,\n" +
           "            PRIMARY KEY(frame,atom_id)\n" +
           "    )\n";

   public static final String INSERT_QUERY = "INSERT INTO %s (frame,atom_id,x,y,z)\n" +
           "    VALUES (?,?,?,?,?);";
    private final CQLSSTableWriter writer;


    public static void main(String args[]) throws Exception{
        checkArgument(args.length == 3, "Give me the path to bulkinsertion.txt, the ouput directory and the keyspace");

        File bulkFile = new File(args[0]);
        String output = args[1];
        String keyspace = args[2];

        Scanner s= new Scanner(bulkFile);

        while(s.hasNext()) {
            String trajName=   s.next();
            Integer atomNum= s.nextInt();
            String path=s.next();
            String table=keyspace+"."+trajName+"_ctrj";
            log.info("Starting loading {} of {} atoms in {} ", path,atomNum,table);

            String schema = String.format(COORDINATES_SCHEMA, table);
            String query = String.format(INSERT_QUERY, table);
            SSTableWriter ssWriter = new SSTableWriter(output, schema, query,keyspace,trajName+"_ctrj");
            File trajFile=new File(path);
            TrajectoryReader tr = new TrajectoryReader(trajFile,' ',atomNum,ssWriter);
            int read = tr.call();
            log.info("Finished inserting {} positions",read);

        }

    }


    public SSTableWriter( String output, String schema, String query,String keyspace,String table){
        Config.setClientMode(true);
        log.info(query);
        log.info(schema);
        File outputDir = new File(output + File.separator + keyspace + File.separator + table);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }
        writer = CQLSSTableWriter.builder().inDirectory(outputDir)
                .forTable(schema)
                .using(query)
                .withPartitioner(new Murmur3Partitioner())
                .build();
    }

    @Override
    public void write(int frame, int atomId, float x, float y, float z) throws IOException, InvalidRequestException {

             writer.addRow(frame, atomId, x, y,z);

   }

}
