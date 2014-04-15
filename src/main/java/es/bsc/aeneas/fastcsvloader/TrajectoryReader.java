package es.bsc.aeneas.fastcsvloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class TrajectoryReader implements Iterator<String[]> {

    private final static Logger log = LoggerFactory.getLogger(TrajectoryReader.class);
    protected final File trajectory;
    public final char FS;
    public final int numberOfFields;


    public TrajectoryReader(File trajfile, char FS) throws FileNotFoundException {
        this.FS = checkNotNull(FS);
        log.info("Reading trajectory file {}", checkNotNull(trajfile).getAbsoluteFile());
        log.info("TrajectoryReader implementation: {}", this.getClass().getSimpleName());
        this.trajectory = trajfile;
        Scanner scanner = new Scanner(trajfile);
        String line = scanner.nextLine();
        StringTokenizer tokenizer = new StringTokenizer(line, FS + "");
        this.numberOfFields = tokenizer.countTokens();


    }

    public abstract String[] next(String[] toresuse);

    public String[] next() {
        String[] f = new String[numberOfFields];
        return next(f);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("ReadOnly list");
    }


}
