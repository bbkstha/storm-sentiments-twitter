import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.StringTokenizer;

public class HastagBolt extends BaseRichBolt{



    PrintWriter writer;
    int count = 0;

    public HastagBolt(){

    }

    OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {

        collector = outputCollector;
        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log7.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String txt = tuple.getStringByField("englishtweet");
        Integer sentiment = tuple.getIntegerByField("sentiment");


        /********* Hashtag*************/

        StringTokenizer stringTokenizer = new StringTokenizer(txt);
        while (stringTokenizer.hasMoreElements()) {
            String hashTagEntitiy = (String) stringTokenizer.nextElement();
            if (StringUtils.startsWith(hashTagEntitiy, "#")) {
                writer.println((count++)+":"+hashTagEntitiy+" and sentiment is: "+sentiment);
                writer.flush();
                collector.emit(new Values(hashTagEntitiy, sentiment));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "sentiment"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }


}