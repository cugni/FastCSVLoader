/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.aeneas.fastcsvloader;

import com.datastax.driver.core.BatchStatement;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ccugnasc
 */
public class CqlFrameHandler implements EventHandler<String[]> {
    private final static Logger log = LoggerFactory.getLogger(CqlFrameHandler.class);

    private final CqlFrameLoader cqlFrameLoader;
    private final int mask;
    private final int id;
    private volatile int counter = 0;

    public CqlFrameHandler(CqlFrameLoader cqlFrameLoader, int concurrents, int id) {

        checkArgument((concurrents & 0x1) == 0 || concurrents == 1, "The number of concurrents must be even");
        this.cqlFrameLoader = cqlFrameLoader;
        this.mask = concurrents - 1;
        this.id = id;
    }


    private long last = System.currentTimeMillis();

    /**
     * Called when a publisher has published an event to the {@link com.lmax.disruptor.RingBuffer}
     *
     * @param event      published to the {@link com.lmax.disruptor.RingBuffer}
     * @param sequence   of the event being processed
     * @param endOfBatch flag to indicate if this is the last event in a batch from the {@link com.lmax.disruptor.RingBuffer}
     * @throws Exception if the EventHandler would like the exception handled further up the chain.
     */
    @Override
    public void onEvent(String[] frame, long l, boolean b) throws Exception {
        if ((l & mask) != id)
            return;
        /*
        * no need for synch.. Always the same thread will update that statistic
        * (Unless the print mask is minor then the mask
        */
        if ((counter++ & 0x0FFF) == 0) {
            log.info("At line {}  after {} ms ", counter, System.currentTimeMillis() - last);
            last = System.currentTimeMillis();
        }
        cqlFrameLoader.addToBatch(batchStatement, frame);
        if (b) {
            cqlFrameLoader.insertBatch(batchStatement);
            batchStatement = new BatchStatement();
        }


    }

    BatchStatement batchStatement = new BatchStatement();


}
