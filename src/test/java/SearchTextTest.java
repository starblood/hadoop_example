import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: starblood
 * Date: 13. 10. 5.
 * Time: 오후 5:21
 */
public class SearchTextTest {
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    private String searchText = "MapReduce";
    private String testStr = "MapReduce abc MapReduce def geh";

    @Before
    public void setUp() {
        SearchText.MyMapper mapper = new SearchText.MyMapper();
        SearchText.MyReducer reducer = new SearchText.MyReducer();

        mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(mapper);
        mapDriver.getConfiguration().setStrings("string_to_search", searchText);

        reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.getConfiguration().setStrings("string_to_search", searchText);
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() {

        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        mapDriver.withOutput(new Text("MapReduce"), new LongWritable(1));
        mapDriver.withOutput(new Text("MapReduce"), new LongWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(1));
        values.add(new LongWritable(1));

        reduceDriver.withInput(new Text("MapReduce"), values);
        reduceDriver.withOutput(new Text("MapReduce"), new LongWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(1), new Text(testStr));
        mapReduceDriver.addOutput(new Text("MapReduce"), new LongWritable(2));
        mapReduceDriver.runTest();
    }
}
