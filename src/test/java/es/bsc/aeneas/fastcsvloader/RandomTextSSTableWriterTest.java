package es.bsc.aeneas.fastcsvloader;

import org.junit.Test;

/**
 * Created by ccugnasc on 11/11/14.
 */
public class RandomTextSSTableWriterTest {


    @Test
    public void testWrite100M() throws Exception {
        System.setProperty(RandomTextWriter.TOTAL_BYTES,100*1024*1024+"");
        System.setProperty("query",   "INSERT INTO test.\"Text\"  (word, position, text) VALUES ( ?,?,?) ");
        System.setProperty("schema",   "CREATE TABLE test.\"Text\" (\n" +
                "    position int,\n" +
                "    word text,\n" +
                "    text text,\n" +
                "    PRIMARY KEY (position, word)\n" +
                ") ");
        RandomTextSSTableWriter.main(new String[0]);
    }
}
