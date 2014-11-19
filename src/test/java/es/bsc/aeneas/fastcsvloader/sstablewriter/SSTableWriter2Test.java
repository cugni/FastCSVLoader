package es.bsc.aeneas.fastcsvloader.sstablewriter;

import org.junit.Test;

public class SSTableWriter2Test {

    @Test
    public void testMain() throws Exception {
        SSTableWriter2.main(new String[]{"src/test/resources/bulkFile","output","ciao"});

    }
}