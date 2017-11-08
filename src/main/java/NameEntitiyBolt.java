import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;


public class NameEntitiyBolt extends BaseRichBolt {



    PrintWriter writer;
    int count = 0;

    public NameEntitiyBolt(){

    }

    OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {

        collector = outputCollector;

        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log5.txt", "UTF-8");
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


        /********* Name Entitiy*************/

        String serializedClassifer = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
        AbstractSequenceClassifier<CoreLabel> classifier = null;
        try{

            classifier = CRFClassifier.getClassifier(serializedClassifer);
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }

        List<List<CoreLabel>> out = classifier.classify(txt);
        for (List<CoreLabel> sentence : out) {
            String s = "";
            String prevLabel = null;
            for (CoreLabel word : sentence) {
                if(prevLabel == null  || prevLabel.equals(word.get(CoreAnnotations.AnswerAnnotation.class)) ) {
                    s = s + " " + word;
                    prevLabel = word.get(CoreAnnotations.AnswerAnnotation.class);
                }
                else {
                    if(!prevLabel.equals("O")){
                    // System.out.println(s.trim());
                       // writer.println((count++)+":"+s.trim()+" and sentiment is: "+sentiment);

                       // writer.flush();
                        //System.out.println(lang.get().toString());
                        collector.emit(new Values(s , sentiment));

                        collector.ack(tuple);

                    }
                    s = " " + word;
                    prevLabel = word.get(CoreAnnotations.AnswerAnnotation.class);
                }
            }
            if(!prevLabel.equals("O"))
                System.out.println(s);
        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "sentiment"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }



}
