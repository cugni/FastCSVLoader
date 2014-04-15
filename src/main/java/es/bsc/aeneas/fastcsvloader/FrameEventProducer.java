package es.bsc.aeneas.fastcsvloader;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
 import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ccugnasc on 2/28/14.
 */
public class FrameEventProducer implements Runnable{
    private final static Logger log= LoggerFactory.getLogger(FrameEventProducer.class);
    TrajectoryReader trajectoryReader;

    private static final EventTranslatorOneArg<String[], TrajectoryReader> TRANSLATOR =
            new EventTranslatorOneArg<String[], TrajectoryReader>() {
                @Override
                public void translateTo(String[] event, long sequence, TrajectoryReader bb) {
                    log.trace("Translating sequence {}", sequence);
                    bb.next(event);
                }
            };

    private final RingBuffer<String[]> ringBuffer;

//    @Inject
    public FrameEventProducer(RingBuffer<String[]> ringBuffer,TrajectoryReader trajectoryReader) {
        this.ringBuffer = ringBuffer;
        this.trajectoryReader=trajectoryReader;
    }




    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p/>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        while (trajectoryReader.hasNext()) {
            ringBuffer.publishEvent(TRANSLATOR, trajectoryReader);
        }
        log.info("Reading completed");
    }
}

