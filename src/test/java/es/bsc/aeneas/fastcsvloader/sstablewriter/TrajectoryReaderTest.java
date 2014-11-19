package es.bsc.aeneas.fastcsvloader.sstablewriter;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TrajectoryReaderTest {

    @Test

    public void testCall() throws Exception {
        TrajectoryReader tr=new TrajectoryReader(new File("/home/ccugnasc/ACGA_last250ns_nowatReimg.trj")
                ,' '
                ,814,
                new AtomsWriter() {
                    @Override
                    public void write(int frame, int atomId, float x, float y, float z) throws IOException, InvalidRequestException {
                        //do nothing
                    }
                });

        long start=System.currentTimeMillis();
        tr.call();
        System.out.println("Read all in " + (System.currentTimeMillis() - start) / 1000 + " s");


    }
}