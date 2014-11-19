package es.bsc.aeneas.fastcsvloader.sstablewriter;

import org.apache.cassandra.exceptions.InvalidRequestException;

import java.io.IOException;

/**
 * Created by ccugnasc on 19/11/14.
 */
public interface AtomsWriter {
    void write(int frame, int atomId, float x, float y, float z) throws IOException, InvalidRequestException;
}
