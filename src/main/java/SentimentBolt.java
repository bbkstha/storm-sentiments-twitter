import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.tika.language.LanguageIdentifier;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SentimentBolt extends BaseRichBolt{

    PrintWriter writer;
    int count = 0;

    public SentimentBolt(){

    }

    OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {

        collector = outputCollector;

        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log4.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String txt = tuple.getStringByField("englishtweet");


        /*********Sentiment************/
        //String txt = "@IronStache: Paul Ryan is in Boston doesn't know what it's like to live paycheck to paycheck or what it's like to not have #healthcare. I do.";

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text in the text variable
        //String text = "...";

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(txt);

        // run all Annotators on this text
        int sentimentScore = 0;
        int totalScore = 0;
        int sentenceCount = 0;
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {

            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);

            if (sentiment.toLowerCase().matches("very positive"))
                sentimentScore = 2;
            else if (sentiment.toLowerCase().matches("positive"))
                sentimentScore = 1;
            else if (sentiment.toLowerCase().matches("neutral"))
                sentimentScore = 0;
            else if (sentiment.toLowerCase().matches("negative"))
                sentimentScore = -1;
            else if (sentiment.toLowerCase().matches("very negative"))
                sentimentScore = -2;

            sentenceCount++;
            totalScore += sentimentScore;
        }


        int avgScore = totalScore/sentenceCount;
        //    System.out.println(sentiment);
        //writer.println((count++)+":"+txt+" and sentiment is: "+avgScore);
        //writer.flush();
        //System.out.println(lang.get().toString());
        collector.emit(new Values(txt , avgScore));
        collector.ack(tuple);







//            } else {
//                System.out.println("No language matched!");
//            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("englishtweet", "sentiment"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }


        }

