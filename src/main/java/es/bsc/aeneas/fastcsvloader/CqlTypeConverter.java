package es.bsc.aeneas.fastcsvloader;

import com.datastax.driver.core.ColumnDefinitions;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.cql3.ColumnSpecification;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class converts strings to the correct Cassandra type according
 * to the schema.
 *
 */
public class CqlTypeConverter {
    /**
     * This creates the parser for each type of object.
     * The argument can be a list of
     * @param cols
     */
    public CqlTypeConverter(List cols) {

       checkArgument(cols.size()>0,"Empty types");
       Object first = cols.get(0);

        /**
         * Very ugly, but just a workaround because the constructor
         * signature doesn't take in account the typed elements of
         * the list.
         */
       if(first instanceof ColumnSpecification) {
           int i = 0;
           List<ColumnSpecification> columns=cols;
            parsers = new Parser[columns.size()];
           for (ColumnSpecification cd : columns) {
               //TODO not really efficient
               Class<?> type = cd.type.getSerializer().getType();
               Parser parser = checkNotNull(parserMap.get(type), "Parser not found for " + type);
               parsers[i++] = parser;
           }
       }else if(first instanceof ColumnDefinitions.Definition){
        List<ColumnDefinitions.Definition> columns=cols;
        int i = 0;
        parsers=new Parser[columns.size()];
        for (ColumnDefinitions.Definition cd : columns) {
            //TODO not really efficient
            Class<?> type = cd.getType().asJavaClass();
            Parser parser = checkNotNull(parserMap.get(type), "Parser not found for " + type);
            parsers[i++] = parser;
        }
       }else{
           throw new RuntimeException("Unknown type "+first.getClass().getSimpleName());
       }
    }

    public interface Parser {
        public  Object parse(String string);
    }
    private final static Map<Class, Parser> parserMap;

    static {
        ImmutableMap.Builder<Class, Parser> builder = ImmutableMap.builder();
        builder.put(Integer.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Integer.parseInt(string);
            }
        });
        builder.put(String.class, new Parser() {
            @Override
            public Object parse(String string) {
                return string;
            }
        });
        builder.put(Double.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Double.parseDouble(string);
            }
        });
        builder.put(Float.class, new Parser() {
            @Override
            public Object parse(String string) {
                return Float.parseFloat(string);
            }
        });
        parserMap=builder.build();

    }
    public final Parser[] parsers;

}
