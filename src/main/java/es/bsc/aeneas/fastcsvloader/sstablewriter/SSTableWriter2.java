package es.bsc.aeneas.fastcsvloader.sstablewriter;


import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class converts a CSV file into a SSTable Cassandra file. The file can be
 * then streamed into Cassandra.
 */
public class SSTableWriter2 {

    public static void write(ArrayList<Float> frameList, String output,	String keyspace, String table, int frame) throws IOException, InvalidRequestException {

        String schema =  "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+"  (\n" +
                "            frame   int,\n" +
                "            atom_id int,\n" +
                "            x   float,\n" +
                "            y   float,\n" +
                "            z   float,\n" +
                "            PRIMARY KEY(frame,atom_id)\n" +
                "    )\n";

        String query = "INSERT INTO "+keyspace+"."+table+" (frame,atom_id,x,y,z)\n" +
                "    VALUES (?,?,?,?,?);";

       Config.setClientMode(true);

        File outputDir = new File(output + File.separator + keyspace + File.separator + table);
        if (!outputDir.exists() && !outputDir.mkdirs())  throw new RuntimeException("Cannot create output directory: " + outputDir);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir)
                .forTable(schema)
                .using(query)
                .withPartitioner(new Murmur3Partitioner())
                .build();

        int atomId = 0;
        for (int i = 0; i < frameList.size() - 2; i = i + 3) {
            writer.addRow(frame, atomId, frameList.get(i),frameList.get(i + 1), frameList.get(i + 2));
            atomId++;

        }
    }

    public static void main(String args[]) throws Exception {
        checkArgument(args.length == 3, "Give me the path to bulkinsertion.txt, the ouput directory and the keyspace");
        File bulkFile = new File(args[0]);
        String output = args[1];
        String keyspace = args[2].toLowerCase();


        // Reading bulkFile
        BufferedReader readerBulk = null;
        try {
            readerBulk = new BufferedReader(new FileReader(bulkFile));
            String textBulk = null;
            while ((textBulk = readerBulk.readLine()) != null) {
                List<String> arguments = new LinkedList<String>(Arrays.asList(textBulk.split("\\s+")));
                String idTraj = arguments.get(0);
                int natoms = Integer.parseInt(arguments.get(1));
                File trajPath = new File(arguments.get(2));
                String table = "traj" + idTraj.toLowerCase();
                System.out.println("idTraj: " + idTraj + " natoms: " + natoms+ " trajPath: " + trajPath);
                // Reading traj and mdcrd parser
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(trajPath));
                    String text = null;
                    text = reader.readLine(); // Ditch the first line (the title)
                    int cont = 0;
                    int frame = 0;
                    int lread = ((natoms * 3) / 10); // 10 is the number of columns of the mdcrd format
                    if((natoms*3)%10 > 0) lread++;
                    ArrayList<Float> list = new ArrayList<Float>();
                    while ((text = reader.readLine()) != null) {
                        List<String> textfloats = new LinkedList<String>(Arrays.asList(text.split("\\s+")));
                        textfloats.remove(0);
                        for (String tfloat : textfloats)
                            list.add(Float.parseFloat(tfloat));
                        cont++;
                        if (cont == lread) {
                            if (list.size() != (natoms*3)) System.out.println("List has a wrong number of atoms");
                            if (frame%1000 == 0) System.out.println("Frame: "+ Integer.toString(frame));
                            // Writting
                            write(list, output, keyspace, table, frame);
                            frame++;
                            cont = 0;
                            list = new ArrayList<Float>();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (reader != null)
                            reader.close();
                    } catch (IOException e) {
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (readerBulk != null)
                    readerBulk.close();
            } catch (IOException e) {
            }
        }
        System.exit(0);

    }

}
