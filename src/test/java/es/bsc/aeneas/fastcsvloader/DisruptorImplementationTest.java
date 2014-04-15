package es.bsc.aeneas.fastcsvloader;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Created by ccugnasc on 4/15/14.
 */
public class DisruptorImplementationTest {

    String query=  "INSERT INTO test.particle(" +
            "time,part_id," +
            "xcoord,ycoord,zcoord," +
            "xvelo,yvelo,zvelo," +
            "par_type,subdom,family )" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)";

    @Test
    public void testMain() throws Exception {
        DisruptorImplementation.main(new String[]{this.getClass().getResource("test.csv").getFile(),query});

    }

    @Test
    public void testExecute() throws Exception {
        File file = new File(this.getClass().getResource("test.csv").toURI());

        DisruptorImplementation disruptorImplementation = new DisruptorImplementation(file, ',',   query);

        disruptorImplementation.execute();
    }
}
