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

        for(int i=0;i<10;i++){
            bb.putInt(i);
            bb.putInt(i*100);
            bb.putFloat(1.23F);
            bb.putFloat(2.34F);
            bb.putFloat(3.45F);
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
