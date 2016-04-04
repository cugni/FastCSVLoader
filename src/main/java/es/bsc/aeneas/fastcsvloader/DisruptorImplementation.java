package es.bsc.aeneas.fastcsvloader;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ccugnasc on 3/3/14.
 */
public class DisruptorImplementation {
    public final static MetricRegistry metrics = new MetricRegistry();
    Logger log = LoggerFactory.getLogger(DisruptorImplementation.class);
    final private CqlFrameLoader cqlFrameLoader;
    final private TrajectoryReader trajectoryReader;

    public DisruptorImplementation(File file, char FS, String queryText) throws IOException {
        trajectoryReader = new MappedReader(file, FS);
        this.cqlFrameLoader = new CqlFrameLoader(trajectoryReader, queryText);
    }

    public static void main(String args[]) throws InterruptedException, IOException {
        if(args.length!=2)
            throw new IllegalArgumentException("You must provide the name of the file and the query");
        String file=checkNotNull(args[0],"Fist argument missing");
        String query=checkNotNull(args[1],"Second argument missing");
        File f=new File(file);
        checkArgument(f.exists(),"File not found");
        String fs = System.getProperty("FS", ",");
        checkArgument(fs.length()==1,"Supported only separators of 1 single char");
        char FS=fs.charAt(0);
        DisruptorImplementation implementation = new DisruptorImplementation(f, FS, query);
        implementation.execute();


    }

    public void execute() throws InterruptedException {
        // Executor that will be used to construct new threads for consumers
        int nConsumers = Integer.getInteger("disruptor.consumers",16);
        checkArgument(((nConsumers != 0) && ((nConsumers & (~nConsumers + 1)) == nConsumers)),"Number of consumers must be a power of 2");
        Executor executor = Executors.newFixedThreadPool(nConsumers, new ThreadFactory() {
            private int i = 0;

            public Thread newThread(Runnable r) {
                log.info("Created thread {}", i);
                return new Thread(r, "DisThread-" + i++);

            }

        });

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = Integer.getInteger("bufferSize",1024);
        checkArgument(((bufferSize != 0) && ((bufferSize & (~bufferSize + 1)) == bufferSize)),"Buffer size must be a power of 2");

        log.info("Using {} concurrent consumers with a buffer size of {}",nConsumers,bufferSize);
// Construct the Disruptor

        Disruptor disruptor = new Disruptor(new EventFactory<String[]>() {
            @Override
            public String[] newInstance() {

                return new String[trajectoryReader.numberOfFields];
            }
        }, bufferSize, executor,
                ProducerType.SINGLE, PhasedBackoffWaitStrategy.withLiteLock(
                500,
                5000,
                TimeUnit.MILLISECONDS
        ));
        RingBuffer<String[]> ringBuffer = disruptor.getRingBuffer();


        for (int i = 0; i < nConsumers; i++) {
            CqlFrameHandler cqlFrameHandler = new CqlFrameHandler(cqlFrameLoader, nConsumers, i);
            disruptor.handleEventsWith(cqlFrameHandler);
        }

        // Start the Disruptor, starts all threads running


        // Get the ring buffer from the Disruptor to be used for publishing.
        FrameEventProducer producer = new FrameEventProducer(ringBuffer, trajectoryReader);
        Thread t = new Thread(producer, "TrajReader");
        long time = System.currentTimeMillis();
        disruptor.start();
        t.start();

        t.join();
        log.info("Load completed in {} ms", System.currentTimeMillis() - time);
    }
}
