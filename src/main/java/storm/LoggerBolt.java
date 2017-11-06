package storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class LoggerBolt extends BaseRichBolt{

    PrintWriter writer;
    int count = 0;
    private OutputCollector collector;

    //@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    //@Override
    public void execute(Tuple tuple) {
        writer.println((count++)+":"+tuple);
        writer.flush();
        // Confirm that this tuple has been treated.
        collector.ack(tuple);
    }

    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    //@Override
    public void cleanup() {
        writer.close();
        super.cleanup();

    }
}