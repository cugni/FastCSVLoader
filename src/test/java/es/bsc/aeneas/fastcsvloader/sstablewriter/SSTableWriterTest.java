package es.bsc.aeneas.fastcsvloader.sstablewriter;

import java.io.File;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

import java.io.IOException;
import org.apache.commons.io.FileUtils;


public class SSTableWriterTest {
    @Test
    public void testWrite() throws InvalidRequestException, IOException {
        String query=  "INSERT INTO casedep.particle(" +
                "time,part_id," +
                "xcoord ,ycoord,zcoord," +
                "xvelo,yvelo,zvelo," +
                "par_type,subdom,family )" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        String schema="CREATE TABLE casedep.particle (\n" +
                "  part_id int,\n" +
                "  time double,\n" +
                "  block text,\n" +
                "  family int,\n" +
                "  par_type int,\n" +
                "  subdom int,\n" +
                "  xcoord double,\n" +
                "  xvelo double,\n" +
                "  ycoord double,\n" +
                "  yvelo double,\n" +
                "  zcoord double,\n" +
                "  zvelo double,\n" +
                "  PRIMARY KEY ((part_id), time)\n" +
                ")";
        
       File dir=new File("casedep/particle");
       FileUtils.deleteDirectory(dir);
       FileUtils.deleteDirectory(dir.getParentFile());
       System.out.println("directory deleted:"+dir.delete());
        long start = System.currentTimeMillis();
        SSTableWriter.write("src/test/resources/particles.csv",query,schema);
        /*SSTableWriter.write("/home/ccugnasc/Desktop/particleSintenticResults_60G.csv",query,schema);*/
        System.out.print("Inserted file in "+(System.currentTimeMillis()-start)/1000+" seconds");
    }
}
