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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LossycountBolt extends BaseRichBolt{

    PrintWriter writer;
    int count = 0;


    OutputCollector collector;
    private HashMap<String, Long> counts = null;
    private Map<String, TempObject> bucket = new ConcurrentHashMap<String,TempObject>();
    private double e= 0.002f; //0.15f; //0.02f;
    private int bucketWidth =(int) Math.ceil(1/e); //50
    private int bucketNumber =1; //starting from 1, not 0
    private int totalEntities =0;
    private double sminuse=0.001f;//s=0.03 so smiu0.03-0.02
    private int totalElements;
    private String fileName;

    public LossycountBolt(String file){
        this.fileName = file;
    }


    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        counts = new HashMap<String, Long>();
        totalElements=0;
        try {
            writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {

            String receivedEntity = tuple.getStringByField("entity");
            Integer sentimentScore = tuple.getIntegerByField("sentiment");
            totalElements++;
            LossyCountImpl(receivedEntity, sentimentScore, totalElements);
            collector.ack(tuple);


    }





    public void LossyCountImpl(String entity, Integer sentimentScore, Integer totalEle)
    {
        if(totalEntities < bucketWidth) {
            if(!bucket.containsKey(entity)) {
                TempObject tempObj = new TempObject();
                tempObj.delta = bucketNumber -1;
                tempObj.count = 1;
                tempObj.sentimentScore+=sentimentScore;
                tempObj.entity = entity;
                bucket.put(entity, tempObj);
            }
            else {
                TempObject tempObj = bucket.get(entity);
                tempObj.count+=1;
                tempObj.sentimentScore+=sentimentScore;
                bucket.put(entity, tempObj);
            }
            totalEntities +=1;
        }
        if( totalEntities == bucketWidth) {
            Delete();
            for(String ent: bucket.keySet()) {
                TempObject tempObject = bucket.get(ent);
                //condition if required
                if(tempObject.count >= sminuse*totalEle){  //sminuse*totalEle) {
                    //writer.println((count++)+":"+ent+" and running sentiment is: "+tempObject.sentimentScore+" actual count is"+tempObject.count+" and approx. is:"+tempObject.count+tempObject.delta);
                    //writer.flush();
                    collector.emit(new Values(ent, sentimentScore,tempObject.count + tempObject.delta));//((int)Math.ceil(tempObject.sentimentScore/(float)tempObject.count)), tempObject.count + tempObject.delta));

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
        declarer.declare(new Fields("entity", "sentiment","lossycount"));
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }

}
