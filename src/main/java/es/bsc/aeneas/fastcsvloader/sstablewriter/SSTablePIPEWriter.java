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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
public class SSTablePIPEWriter {
    private final static Logger log= LoggerFactory.getLogger(SSTablePIPEWriter.class);

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
    public static void main(String args[]) throws Exception{
        checkArgument(args.length==5,"Give me output,schema,query");
        write(new BufferedInputStream(System.in),args[0],args[1],args[2],args[3],args[4]);
    }


    public static void write(BufferedInputStream io,String output,String schema,String query,String keyspace,String table) throws IOException, InvalidRequestException {

        Config.setClientMode(true);
        log.info(query);
        log.info(schema);



       File outputDir = new File(output+File.separator+keyspace + File.separator + table);
            if (!outputDir.exists() && !outputDir.mkdirs())
            {
                throw new RuntimeException("Cannot create output directory: " + outputDir);
            }
        CQLSSTableWriter writer = CQLSSTableWriter.builder().inDirectory(outputDir)
                .forTable(schema)
                .using(query)
                .withPartitioner(new Murmur3Partitioner())
                .build();

        byte[] b=new byte[20];
        try {
            while (io.available() > 0) {
                int read = 0;
                while (read < 20) {
                    read += io.read(b, read,20 - read);
                }
                ByteBuffer bb = ByteBuffer.wrap(b);
                int frame = bb.getInt(0);
                int atomId = bb.getInt(4);
                float x = bb.getFloat(8);
                float y = bb.getFloat(12);
                float z = bb.getFloat(16);
                log.info("got {} {} {} {} {}",frame,atomId,x,y,z);

                 writer.addRow(frame, atomId, x, y, z);

            }
        } finally {
            writer.close();
        }



    }

}
