package resources;


import org.apache.storm.Config;
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


public class RollingCountingLossyAlg extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    //private static final Logger LOG = Logger.getLogger(RollingCountingLossyAlg.class);
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 2;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";

    private final SlidingWindowCounter<Object> counter;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    private HashMap<String, Long> counts = null;
    private Map<String, TempObject> bucket = new ConcurrentHashMap<String,TempObject>();
    private double e= 0.02f; //0.15f; //0.02f;
    private int bucketWidth =(int) Math.ceil(1/e); //50
    private int bucketNumber =1; //starting from 1, not 0
    private int totalEntities =0;
    private double sminuse=0.01f;//s=0.03 so smiu0.03-0.02
    private int totalElements;


    PrintWriter writer;
    int cou = 0;

    public RollingCountingLossyAlg() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingCountingLossyAlg(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
                this.emitFrequencyInSeconds));

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
        totalElements++;
        if (TupleHelper.isTickTuple(tuple)) {
            emitCurrentWindowCounts();
        }
        else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
           // LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        LossyEmitter(actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        //Object obj = tuple.getValue(0);
        //counter.incrementCount(obj);
        //collector.ack(tuple);
        String receivedEntity = tuple.getString(0);
        Integer sentimentScore = tuple.getIntegerByField("sentiment");
        LossyCountImpl(receivedEntity, sentimentScore);
    }

        public void LossyCountImpl(String entity, Integer sentimentScore)
        {
            if(totalEntities < bucketWidth) {
                if(!bucket.containsKey(entity.toLowerCase())) {
                    TempObject d = new TempObject();
                    d.delta = bucketNumber -1;
                    d.count = 1;
                    d.sentimentScore+=sentimentScore;
                    d.entity = entity;
                    bucket.put(entity.toLowerCase(), d);
                }
                else {
                    TempObject d = bucket.get(entity);
                    d.count+=1;
                    d.sentimentScore+=sentimentScore;
                    bucket.put(entity.toLowerCase(), d);
                }
                totalEntities +=1;
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

    public void LossyEmitter(int actualWindowLengthInSeconds) {

        if( totalEntities == bucketWidth) {
            Delete();
            for(String ent: bucket.keySet()) {
                TempObject d = bucket.get(ent);
                //condition if required
                if(d.count >= sminuse*totalElements) {
                    writer.println((cou++)+":"+ent+" and running sentiment is: "+d.sentimentScore+" actual count is"+d.count+" and approx. is:"+d.count+d.delta);

                    writer.flush();
                    collector.emit(new Values( ent, ((int)Math.ceil(d.sentimentScore/(float)d.count)), d.count + d.delta, actualWindowLengthInSeconds));

                }
            }
            totalEntities = 0;
            bucketNumber++;
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "sentiment","approximateFrequency", "timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
