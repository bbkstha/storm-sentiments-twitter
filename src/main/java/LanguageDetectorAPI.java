import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Triple;
import org.apache.tika.language.LanguageIdentifier;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.io.IOException;
import java.util.List;
import java.util.Properties;


public class LanguageDetectorAPI {

    public static void main(String[] args)  {

        String text = "For classification of documents based on the language they are written in a multilingual website, a language detection tool is needed. This tool should accept documents without language annotation (metadata) and add that information in the metadata of the document by detecting the language.";
        String language = "UNKNOWN";

        LanguageIdentifier li = new LanguageIdentifier(text);
        //if (li.isReasonablyCertain())
        language = li.getLanguage();

    /*********Sentiment************/
        String txt = "@IronStache: Paul Ryan is in Boston doesn't know what it's like to live paycheck to paycheck or what it's like to not have #healthcare. I do.";

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text in the text variable
        //String text = "...";

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(txt);

        // run all Annotators on this text
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap sentence: sentences){
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            System.out.println(sentiment);
        }

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
                    if(!prevLabel.equals("O"))
                        System.out.println(s.trim());
                    s = " " + word;
                    prevLabel = word.get(CoreAnnotations.AnswerAnnotation.class);
                }
            }
            if(!prevLabel.equals("O"))
                System.out.println(s);
        }




    }

}
