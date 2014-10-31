package es.bsc.aeneas.fastcsvloader.sstablewriter;

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
import org.apache.cassandra.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class converts a CSV file into a SSTable Cassandra file.
 * The file can be then streamed into Cassandra.
 */
public class SSTableWriter {

    public static void main(String args[]) throws Exception {
        if(args.length!=3)
            throw new IllegalArgumentException("You must provide the name of the file and the query");
        String file=checkNotNull(args[0],"Fist argument (file name) missing");
        String query=checkNotNull(args[1],"Second argument (query) missing");
        String schema=checkNotNull(args[1],"Second argument (schema) missing");
        write(file,query, schema);

    }
    public static void write(String file, String query, String schema) throws InvalidRequestException, IOException {
        Matcher matcher = Pattern.compile("insert +into +(?<keyspace>[^. ]+)\\.(?<table>[^. \\(]+).+", Pattern.CASE_INSENSITIVE).matcher(query);
        checkArgument(matcher.matches(),"Impossible to detect keyspace and table name from the query");
        String keyspace=matcher.group("keyspace");
        String table=matcher.group("table");
        File f=new File(file);
        checkArgument(f.exists(),"File not found");
        String fs = System.getProperty("FS", ",");
        checkArgument(fs.length()==1,"Supported only separators of 1 single char");
        char FS=fs.charAt(0);

        MappedReader trajectoryReader;
        try {
            trajectoryReader   = new MappedReader(f, FS);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open the csv file",e);
        }
        Config.setClientMode(true);

        File outputDir = new File(keyspace + File.separator + table);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }
        CQLSSTableWriter writer = CQLSSTableWriter.builder().inDirectory(outputDir)
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
        CqlTypeConverter parser=new CqlTypeConverter(metaData);
        String[] line=null;
        Object[] binding=null;
         while(trajectoryReader.hasNext()){
            //let's reuse the array.
            line =line==null?trajectoryReader.next():trajectoryReader.next(line);
            if(binding==null)
             binding  = new Object[line.length];
            for (int i = 0;i<parser.parsers.length;i++) {
                binding[i] = parser.parsers[i].parse(line[i]);
            }
            writer.addRow(binding);


        }
    }
}
