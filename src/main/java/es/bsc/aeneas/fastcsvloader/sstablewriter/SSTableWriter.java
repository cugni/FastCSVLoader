package es.bsc.aeneas.fastcsvloader.sstablewriter;


import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class converts a CSV file into a SSTable Cassandra file.
 * The file can be then streamed into Cassandra.
 */
public class SSTableWriter {
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
        checkArgument(args.length==2,"Give me the path to bulkinsertion.txt, the ouput directory and the keyspace");
        File bulkFile = new File(args[0]);
        String output = args[1];
        String keyspaceDotTable = args[2];


        String schema=String.format(COORDINATES_SCHEMA,keyspaceDotTable);
        String query=String.format(INSERT_QUERY,keyspaceDotTable);
        SSTableWriter ssWriter = new SSTableWriter(output, schema, query);
        //Reading bulkFile
		BufferedReader readerBulk = null;
		try {
		    readerBulk = new BufferedReader(new FileReader(bulkFile));
		    String textBulk = null;
		    while ((textBulk = readerBulk.readLine()) != null) {
		    	List<String> arguments = new LinkedList<String>(Arrays.asList(textBulk.split("\\s+")));
		    	String idTraj = arguments.get(0);
		    	String natoms = arguments.get(1);
		    	File trajPath = new File(arguments.get(2));
		    	String table = idTraj+".traj"; 
		    	System.out.println("idTraj: "+idTraj+" natoms: "+natoms+" trajPath: "+trajPath);
		    	// Reading traj and mdcrd parser
		    	BufferedReader reader = null;
		    	try {
				    reader = new BufferedReader(new FileReader(trajPath));
				    String text = null;
				    text = reader.readLine(); //Ditch the first line (the title)
				    int cont = 0;
				    int frame = 0;
				    while ((text = reader.readLine()) != null) {
				    	List<String> textfloats = new LinkedList<String>(Arrays.asList(text.split("\\s+")));
				    	textfloats.remove(0);
                        List<Float> list=new ArrayList();
				    	for (String tfloat : textfloats)
				    		list.add(Float.parseFloat(tfloat));
				    	cont++;
				    	if(cont == 10){ //lread? what it was? the field in a line? it wasn't set.
				    		if (frame%1000	 == 0) System.out.println("Frame: "+Integer.toString(frame));
				    		//Writting
                            ssWriter.write(list, frame);
				    		frame++;
				    		cont = 0;
				    		list.clear();
				    	}
				    }
				} 
		    	catch (FileNotFoundException e) {   e.printStackTrace(); } 
		    	catch (IOException e) { e.printStackTrace(); } 
		    	finally { try { if (reader != null) reader.close(); } catch (IOException e) {} }
		    }
		    	    
		}
		catch (FileNotFoundException e) {e.printStackTrace();} 
		catch (IOException e) {e.printStackTrace();	} 
		finally {try { if (readerBulk != null) readerBulk.close(); } catch (IOException e) {} }
		
    }


    public SSTableWriter( String output, String schema, String query){
        Config.setClientMode(true);
        Matcher matcher = Pattern.compile("insert +into +(?<keyspace>[^. ]+)\\.(?<table>[^. \\(]+).+", Pattern.CASE_INSENSITIVE).matcher(query);
        checkArgument(matcher.matches(),"Impossible to detect keyspace and table name from the query");
        String keyspace=matcher.group("keyspace");
        String table=matcher.group("table");
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

    public void write(List<Float> frameList, int frame) throws IOException, InvalidRequestException {


       int atomId = 0;
       for (int i = 0; i < frameList.size()-2; i = i + 3) {
                 writer.addRow(frame, atomId, frameList.get(i), frameList.get(i+1), frameList.get(i+2));
                 atomId++;

       }
   }

}
