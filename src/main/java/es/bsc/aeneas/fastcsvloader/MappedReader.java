/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.aeneas.fastcsvloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * @author ccugnasc
 *         This class read only the text but doesn't convert to numbers.
 */
public class MappedReader extends TrajectoryReader {


    private final static Logger log = LoggerFactory.getLogger(MappedReader.class);
    private final File trajfile;
    private final long filesize;
    private FileChannel fc0;
    private MappedByteBuffer buffer;
    private final int MAX_WINDOW_SIZE = Integer.MAX_VALUE;
    private final boolean windowed;
    private long position = 0;


    private ByteBuffer bb = ByteBuffer.allocate(64);


    public MappedReader(File trajfile, char FS) throws IOException {
        super(trajfile, FS);
        this.trajfile = trajfile;
        this.filesize = trajfile.length();
        fc0 = new FileInputStream(trajfile).getChannel();
        if (filesize > MAX_WINDOW_SIZE) {
            windowed = true;
            buffer = fc0.map(FileChannel.MapMode.READ_ONLY, 0, MAX_WINDOW_SIZE);
        } else {
            windowed = false;
            buffer = fc0.map(FileChannel.MapMode.READ_ONLY, 0, filesize);
        }

    }




    @Override
    public boolean hasNext() {
        //To check
        return buffer.remaining() > 0;

    }

  

    @Override
    public String[] next(String[] f) throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("file completed");
        }


        // Get the atoms from all the residue pointers


        boolean word = false;
        byte[] point = new byte[256];
        int ppos = 0;
        float[] position = new float[3];
        int xyz = 0;

        int count = 0;
        while (buffer.remaining() > 0 || rebuffer()) {
            byte c = buffer.get();
            /**
             * That's a state machine with 2 states. Word and not word
             */

            if (c == FS || c == '\n') {
                if (word) {
                    word = false;
                    String num = new String(point, 0, ppos);
                    log.trace("scanned  {}", num);
                    // position.putFloat(54444);
                    f[xyz++] = num;
                    ppos = 0;
                    count++;
                    if (c == '\n') {
                        //line completed.
                        if (log.isTraceEnabled())
                            log.trace("Read line  {}", Arrays.toString(f));
                        return f;
                    }
                }
                    //do nothing: repeated whitespace

                } else {
                    //inserting each single byte
                    word = true;
                    point[ppos++] = c;
                }

                //else do nothing, it is just another white space


            }
            return f;


        }

    private boolean rebuffer() {
        if (!windowed) {
            //No need to rebuffer, the file is complete
            return false;
        } else {
            position += MAX_WINDOW_SIZE;
            long size;
            if (filesize - position > MAX_WINDOW_SIZE)
                size = MAX_WINDOW_SIZE;
            else
                size = filesize - position;
            log.info("ReBuffering with a new position {} with size {}", position, size);
            try {
                fc0.close();
                fc0 = new FileInputStream(trajfile).getChannel();
                buffer = fc0.map(FileChannel.MapMode.READ_ONLY, position, size);
            } catch (IOException e) {
                log.error("IOException", e);
                throw new AssertionError(e);
            }
            return buffer.remaining() > 0;
        }
    }


}
