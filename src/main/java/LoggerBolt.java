import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class LoggerBolt extends BaseRichBolt {


    PrintWriter writer;
    int count = 0;
    private OutputCollector collector;
    private String fileName;

    public LoggerBolt(String filename){
        this.fileName = filename;
    }

    //@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    //@Override
    public void execute(Tuple tuple) {
        String timestamp = tuple.getStringByField("timestamp");
        String entity = tuple.getStringByField("entitiy");
        Integer sentiment = tuple.getIntegerByField("sentiment");
        writer.print(timestamp);
        writer.print(entity);
        writer.print(sentiment);
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
