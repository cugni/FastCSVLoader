package es.bsc.aeneas.fastcsvloader.sstablewriter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * @author ccugnasc
 *         This class read only the text but doesn't convert to numbers.
 */
public class TrajectoryReader implements Callable<Integer> {



    private final static Logger log = LoggerFactory.getLogger(TrajectoryReader.class);
    private final File trajfile;
    private AtomsWriter writer;
    private final long filesize;
    private final char FS;
    private final int numberOfAtoms;
    private BufferedReader fc0;;
    private long position = 0;


    private ByteBuffer bb = ByteBuffer.allocate(64);


    public TrajectoryReader(File trajfile, char FS, int numberOfAtoms, AtomsWriter writer) throws IOException {
        this.FS = FS;
        this.numberOfAtoms = numberOfAtoms;

        this.trajfile = trajfile;
        this.writer = writer;
        this.filesize = trajfile.length();
        fc0 = new BufferedReader(new FileReader(trajfile));



    }


    public Integer call() throws Exception {


        // Get the atoms from all the residue pointers


        boolean word = false;
        byte[] point = new byte[256];
        int ppos = 0;
        float[] position = new float[3];
        int xyz = 0;

        int frame = 0;
        int count = 0;

        while(fc0.read()!='\n');

        byte c;
        while ((c = (byte) fc0.read())!=-1) {
            /**
             * That's a state machine with 2 states. Word and not word
             */

            if (c == FS || c == '\n') {
                if (word) {
                    word = false;
                    float num = Float.parseFloat(new String(point, 0, ppos));
                    log.trace("scanned  {}", num);
                    // position.putFloat(54444);
                    position[xyz++] = num;
                    ppos = 0;
                    count++;

                    if (count % 3 == 0) {
                        int atomid = count / 3 - frame * numberOfAtoms;
                        writer.write(frame, atomid, position[0], position[1], position[2]);
                        xyz = 0;
                        if (atomid >= numberOfAtoms) {
                            frame++;
                            if(frame%1000==0)
                                log.info("Written frame number {}",frame);
                        }

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
        return count;


    }




}