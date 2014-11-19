package es.bsc.aeneas.fastcsvloader.sstablewriter;

import org.junit.Test;

public class SSTableWriterTest {

    @Test
    public void testMain() throws Exception {
        SSTableWriter.main(new String[]{"src/test/resources/bulkFile","output","cioa"});

    }
}