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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * @author ccugnasc
 *         This class read only the text but doesn't convert to numbers.
 */
public class NIOReader extends TrajectoryReader {


    private final static Logger log = LoggerFactory.getLogger(NIOReader.class);
    private final Scanner scanner;
    private long count;
    private final String fs;


    public NIOReader(File trajfile, char FS) throws IOException {
        super(trajfile, FS);
        scanner=new Scanner(trajfile);
        fs=FS+"";

    }


    public boolean hasNext() {
         return scanner.hasNext();
    }


    @Override
    public String[] next(String[] f) throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("file completed");
        }
        if(count++ % 1e6==0){
            log.info("Read {} lines",count);
        }
       return scanner.nextLine().split(fs);

    }


}
