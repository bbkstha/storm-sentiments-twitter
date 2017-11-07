import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import resources.TempObject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LossycountBolt extends BaseRichBolt{

    PrintWriter writer;
    int count = 0;


    OutputCollector collector;
    private HashMap<String, Long> counts = null;
    private Map<String, TempObject> bucket = new ConcurrentHashMap<String,TempObject>();
    private double e= 0.02f; //0.15f; //0.02f;
    private int bucketWidth =(int) Math.ceil(1/e); //50
    private int bucketNumber =1; //starting from 1, not 0
    private int totalEntities =0;
    private double sminuse=0.01f;//s=0.03 so smiu0.03-0.02
    private int totalElements;


    public LossycountBolt(){

    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector outputCollector) {
        collector = outputCollector;
        counts = new HashMap<String, Long>();
        totalElements=0;
        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log6.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String receivedEntity = tuple.getString(0);
        Integer sentimentScore = tuple.getIntegerByField("sentiment");
        totalElements++;
        LossyCountImpl(receivedEntity, sentimentScore);
        collector.ack(tuple);
    }


    public void LossyCountImpl(String entity, Integer sentimentScore)
    {
        if(totalEntities < bucketWidth) {
            if(!bucket.containsKey(entity.toLowerCase())) {
                TempObject tempObj = new TempObject();
                tempObj.delta = bucketNumber -1;
                tempObj.count = 1;
                tempObj.sentimentScore+=sentimentScore;
                tempObj.entity = entity;
                bucket.put(entity.toLowerCase(), tempObj);
            }
            else {
                TempObject tempObj = bucket.get(entity);
                tempObj.count+=1;
                tempObj.sentimentScore+=sentimentScore;
                bucket.put(entity.toLowerCase(), tempObj);
            }
            totalEntities +=1;
        }
        if( totalEntities == bucketWidth) {
            Delete();
            for(String ent: bucket.keySet()) {
                TempObject tempObject = bucket.get(ent);
                //condition if required
                if(tempObject.count >= sminuse*totalElements) {
                    writer.println((count++)+":"+ent+" and running sentiment is: "+tempObject.sentimentScore+" actual count is"+tempObject.count+" and approx. is:"+tempObject.count+tempObject.delta);

                    writer.flush();
                    collector.emit(new Values(ent, ((int)Math.ceil(tempObject.sentimentScore/(float)tempObject.count)), tempObject.count + tempObject.delta));

                }
            }
            totalEntities = 0;
            bucketNumber++;
        }
    }



    public void Delete()
    {
        for(String entity: bucket.keySet()) {
            TempObject bucketElement = bucket.get(entity);
            if(bucketElement.count + bucketElement.delta <= bucketNumber) {
                bucket.remove(entity);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "sentiment","approximateFrequency"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }

}
