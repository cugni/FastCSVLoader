package es.bsc.aeneas.fastcsvloader.sstablewriter;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by ccugnasc on 13/11/14.
 */
public class SSTablePIPEWriterTest {

    @Test
    public void testWrite() throws IOException, InvalidRequestException {

        ByteBuffer bb = ByteBuffer.allocate(200);
        Random r=new Random();
        for(int i=0;i<20;i++){
            bb.putInt(0,r.nextInt());
            bb.putInt(4,r.nextInt());
            bb.putFloat(8,r.nextFloat());
            bb.putFloat(12,r.nextFloat());
            bb.putFloat(16,r.nextFloat());
        }


        SSTablePIPEWriter.write(
                new BufferedInputStream(new ByteArrayInputStream( bb.array())),
                "pipeTest",
                String.format(SSTablePIPEWriter.COORDINATES_SCHEMA,"test.particle"),
                String.format(SSTablePIPEWriter.INSERT_QUERY,"test.particle"),
                "test",
                "particle"
                );

    }
}
