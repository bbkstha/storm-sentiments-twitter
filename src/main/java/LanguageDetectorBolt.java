import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.tika.language.LanguageIdentifier;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class LanguageDetectorBolt extends BaseRichBolt {


    PrintWriter writer;
    int count = 0;


    public LanguageDetectorBolt(){


    }

    OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {

        collector = outputCollector;

        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log2.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {

            String text = tuple.getStringByField("tweet");
            LanguageIdentifier li = new LanguageIdentifier(text);
            //if (li.isReasonablyCertain())
            String lang = li.getLanguage();




            if(lang.matches("en")) {

                writer.println((count++)+":"+text+" and language is: "+lang);
                writer.flush();
                //System.out.println(lang.get().toString());
                collector.emit(new Values(text));
                collector.ack(tuple);
            }

//            } else {
//                System.out.println("No language matched!");
//            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("englishtweet"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}
