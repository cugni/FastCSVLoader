package es.bsc.aeneas.fastcsvloader;

import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ccugnasc on 4/15/14.
 */
public class MappedReaderTest {

    Object[][] data=new Object[][]{
            {0.00016,11650,0.0838947,0.0253881,0.00635271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11651,0.0839947,0.0254881,0.00645271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11652,0.0840947,0.0255881,0.00655271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11653,0.0841947,0.0256881,0.00665271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11654,0.0842947,0.0257881,0.00675271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11655,0.0843947,0.0258881,0.00685271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11656,0.0844947,0.0259881,0.00695271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11657,0.0845947,0.0260881,0.00705271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11658,0.0846947,0.0261881,0.00715271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2},
            {0.00016,11659,0.0847947,0.0262881,0.00725271,0.001569789950735867,0.003401139983907342,0.001958969980478287,2,1520,2}};


    @Test
    public void testNext() throws Exception {
        File file = new File(this.getClass().getResource("test.csv").toURI());
        assertTrue(file.exists());
        MappedReader reader = new MappedReader(file, ',');
        assertEquals(11,reader.numberOfFields);
        int nline=0;
        while(reader.hasNext()){
            String[]    next = reader.next();
            for(int i=0;i<next.length;i++){
                Object o = data[nline][i];
                if(o instanceof Double)
                assertEquals((Double)o ,Double.parseDouble(next[i]),0.01);
                else if (o instanceof Integer)
                    assertEquals(o ,Integer.parseInt(next[i]));
            }
            nline++;
        }
        assertEquals(10,nline);


    }


}
