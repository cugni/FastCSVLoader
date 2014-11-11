package es.bsc.aeneas.fastcsvloader;

import org.junit.Test;

/**
 * Created by ccugnasc on 11/11/14.
 */
public class RandomTextWriterTest {


    @Test
    public void testWrite100M() throws Exception {
        System.setProperty(RandomTextWriter.TOTAL_BYTES,100*1024*1024+"");
        System.setProperty("query",   "INSERT INTO test.\"Text\"  (word, position, text) VALUES ( ?,?,?) ");
        RandomTextWriter.main(new String[0]);
    }
}
