package shortPath;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class queryResultStat extends ProcessingElement{

    Streamable<Event> downStream;
    static Logger logger = LoggerFactory.getLogger(shortestPathPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;
    
    String outputPath = "";
    int partitionNumber = 100 ;//100/500/1000/1500;
    String searchMethod = "Boundary" ; //"Boundary""Baseline"
    
    HashMap<String, Long> QueryStartTime  ;
    HashMap<String, Long> QueryEndTime  ;
    
    HashMap<String, Long> QueryRuningTime  ;
    HashMap<String, Route> QueryRouter  ;
    HashMap<String, Double> QueryDistance  ;
    HashMap<String, Integer> QueryPathLenght  ;
    
    HashMap<String, Integer> QueryReceiveStat  ;
    HashMap<String, Integer> QuerySendStat  ;

    HashMap<String, Long> QueryReceiveSize  ;
    HashMap<String, Long> QuerySendSize  ;

    HashMap<String, Long> timeCost ;
    HashMap<String, Integer> spaceCost  ;
    
    long queryStarttime = 0;
    long queryEndtime = 0;
    boolean firstevent = false;
    boolean routeTag = false; //the tag to decide storing the route path
    
    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
		if (event.containsKey("route")) {// query route
			Route shortpath = event.get("route", Route.class);
			if(!firstevent){
				queryStarttime = shortpath.getStartTime();
				firstevent=true;
			}
			String queryId = shortpath.getQueryId();

			if (queryId.indexOf("-") == -1) {//not the update query

				Long startTime = shortpath.getStartTime();
				long endTime = shortpath.getEndTime();
				long runtime = endTime - startTime;
				long preruntime = QueryRuningTime.get(queryId) == null ? Long.MIN_VALUE
						: QueryRuningTime.get(queryId);
				if(runtime > preruntime){
					QueryRuningTime.put(queryId, runtime);
					if(routeTag){
						QueryRouter.put(queryId, shortpath);
					}
					queryEndtime = (System.currentTimeMillis() - timeInterval);
				}
				double preDistance = QueryDistance.get(queryId)==null ? Long.MAX_VALUE : QueryDistance.get(queryId);
				double distance = shortpath.getDistance();
				if(preDistance > distance){
					QueryDistance.put(queryId,distance);
					int length = (shortpath.getRoute().split(":").length-1) <= 0 ? 0: (shortpath.getRoute().split(":").length-1);
					QueryPathLenght.put(queryId, length);
				}
			}
		}
		else if (event.containsKey("timeCost")) {// maintain time/space cost
			String partitionId = event.get("PartitionId", String.class);
			timeCost.put(partitionId, event.get("timeCost", Long.class));
			spaceCost.put(partitionId, event.get("shortcutCount", Integer.class));
		} 
		else if (event.containsKey("receiveNum")) { // communication stat
			String queryId = event.get("queryId", String.class);
			int receive = event.get("receiveNum", Integer.class);
			int send = event.get("sendNum", Integer.class);
			QueryReceiveStat.put(queryId, receive);
			QuerySendStat.put(queryId, send);
			
			long receivesize = event.get("receiveSize", Long.class);
			long sendsize = event.get("sendSize", Long.class);
			QueryReceiveSize.put(queryId, receivesize);
			QuerySendSize.put(queryId, sendsize);
		}

    }
    
    public void onTime(){
    	
    	outputResult();
    	if(routeTag){
        	outputRoute();
    	}
    }
    
    public void outputResult(){
    	String path = outputPath + "-result.txt";
    	long sum = 0;
    	int count=0;
    	long averageRuntime = 0;
    	int averageReceive = 0;
    	int averageSend = 0;
    	long averageReceiveSize = 0;
    	long averageSendSize = 0;
    	long timecost = 0;
    	int spacecost = 0;
    	int passQueryNumber = 0;
    	
       	File f = new File(path);
       	StringBuilder sb = new StringBuilder();
       	int i = 0;
       	sb.append("\n ----partitionNumber:"+partitionNumber +"	searchMethod:"+searchMethod  + "	"+ (System.currentTimeMillis()-timeInterval)+"\n");
       	
       	sb.append(" -----runtime-----\n");
       	long max = Long.MIN_VALUE;
       	long min = Long.MAX_VALUE;
       	
       	for(Map.Entry<String, Long> entry : QueryRuningTime.entrySet()){
           	//sb.append(entry.getKey()+ "	"+entry.getValue());
           	//sb.append("\n");
           	sum = sum + entry.getValue();
           	count++;
           	if(max < entry.getValue()) max = entry.getValue();
            if(min>entry.getValue()) min = entry.getValue();
            int receive = QueryReceiveStat.get(entry.getKey())==null?0:QueryReceiveStat.get(entry.getKey());
            int send = QuerySendStat.get(entry.getKey())==null?0:QuerySendStat.get(entry.getKey());
            if(receive == send && receive > 0){
            	i++;
            }
       	}
       	if(QueryRuningTime.size()>3){
           	averageRuntime = count == 0 ? 0 : (sum - max - min)/ (count-2);
       	}else{
           	averageRuntime = count == 0 ? 0 : sum/ count;
       	}
       	passQueryNumber = count;
       	
       	//sb.append(" -----receive event-----\n");
       	int sumEvent = 0;
    	count=0;
    	int maxEvent = Integer.MIN_VALUE; 
    	int minEvent = Integer.MAX_VALUE;
       	for(Map.Entry<String, Integer> entry : QueryReceiveStat.entrySet()){
           	//sb.append(entry.getKey()+ "	"+entry.getValue());
           	//sb.append("\n");
           	sumEvent = sumEvent + entry.getValue();
           	count++;
           	if(maxEvent < entry.getValue()) maxEvent = entry.getValue();
            if(minEvent>entry.getValue()) minEvent = entry.getValue();
       	}
       	if(QueryReceiveStat.size()>3){
       		averageReceive = count == 0 ? 0 : (sumEvent - maxEvent - minEvent)/ (count-2);
       	}else{
           	averageReceive = count==0?0:sumEvent/ count;
       	}
       	
       	sb.append(" -----send event-----\n");
       	sumEvent = 0;
    	count=0;
    	maxEvent = Integer.MIN_VALUE; 
    	minEvent = Integer.MAX_VALUE;
       	for(Map.Entry<String, Integer> entry : QuerySendStat.entrySet()){
           	//sb.append(entry.getKey()+ "	"+entry.getValue());
           	//sb.append("\n");
           	sumEvent = sumEvent + entry.getValue();
           	count++;
           	if(maxEvent < entry.getValue()) maxEvent = entry.getValue();
            if(minEvent>entry.getValue()) minEvent = entry.getValue();
       	}
    	if(QuerySendStat.size()>3){
    		averageSend = count == 0 ? 0 : (sumEvent - maxEvent - minEvent)/ (count-2);
       	}else{
       		averageSend =  count==0?0:sumEvent/ count;       
       	}
       	
    	
    	
    	long sumsize = 0;
    	count=0;
    	long maxsize = Integer.MIN_VALUE; 
    	long minsize = Integer.MAX_VALUE;
       	for(Map.Entry<String, Long> entry : QuerySendSize.entrySet()){
           	//sb.append(entry.getKey()+ "	"+entry.getValue());
           	//sb.append("\n");
       		sumsize = sumsize + entry.getValue();
           	count++;
           	if(maxsize < entry.getValue()) maxsize = entry.getValue();
            if(minsize>entry.getValue()) minsize = entry.getValue();
       	}
    	if(QuerySendSize.size()>3){
    		averageSendSize = count == 0 ? 0 : (sumsize - maxsize - minsize)/ (count-2);
       	}else{
       		averageSendSize =  count==0?0:sumsize/ count;       
       	}
       	
    	sumsize = 0;
    	count=0;
    	maxsize = Integer.MIN_VALUE; 
    	minsize = Integer.MAX_VALUE;
       	for(Map.Entry<String, Long> entry : QueryReceiveSize.entrySet()){
           	//sb.append(entry.getKey()+ "	"+entry.getValue());
           	//sb.append("\n");
       		sumsize = sumsize + entry.getValue();
           	count++;
           	if(maxsize < entry.getValue()) maxsize = entry.getValue();
            if(minsize>entry.getValue()) minsize = entry.getValue();
       	}
    	if(QueryReceiveSize.size()>3){
    		averageReceiveSize = count == 0 ? 0 : (sumsize - maxsize - minsize)/ (count-2);
       	}else{
       		averageReceiveSize =  count==0?0:sumsize/ count;       
       	}

       	long sumcost = 0;
    	count=0;
       	
       	for(Map.Entry<String, Long> entry : timeCost.entrySet()){
       		sumcost = sumcost + entry.getValue();
           	count++;
       	}
       	timecost =  count==0?0:sumcost/ count;
       	
       	int sumspacecost = 0;
    	count=0;
       	
       	for(Map.Entry<String, Integer> entry : spaceCost.entrySet()){
       		sumspacecost = sumspacecost + entry.getValue();
           	count++;
       	}
       	spacecost =  count==0?0:sumspacecost/ count;
       	
       	long queryAllruntime = queryEndtime - queryStarttime;
       	//System.out.println("queryEndtime:" + queryEndtime + "	" + "queryStarttime:" + queryStarttime);

    	double sumDistance = 0;
       	count=0;
    	for(Map.Entry<String, Double> entry : QueryDistance.entrySet()){
    		sumDistance = sumDistance + entry.getValue();
           	count++;
       	}
    	double distance = count==0?0:sumDistance/count;
    	
    	int sumlength = 0;
       	count=0;
    	for(Map.Entry<String, Integer> entry : QueryPathLenght.entrySet()){
    		sumlength = sumlength + entry.getValue();
           	count++;
       	}
    	int length = count==0?0:sumlength/count;
       	
       	sb.append("\n");
       	sb.append("averageRuntime:" + averageRuntime + "\n");
       	sb.append("averageReceive:" + averageReceive + "\n");
       	sb.append("averageSend:" + averageSend + "\n");
       	sb.append("averageReceiveSize:" + averageReceiveSize + "\n");
       	sb.append("averageSendSize:" + averageSendSize + "\n");
       	sb.append("average timecost:" + timecost + "\n");
       	sb.append("average spacecost:" + spacecost + "\n");
       	sb.append("pass Query Number:" + passQueryNumber + "\n");
       	sb.append("queryAllruntime:" + queryAllruntime + "\n");
       	sb.append("queryDistance:" + distance + "\n");
       	sb.append("queryLength:" + length + "\n");

       	try {
       		Files.append(sb.toString(), f, Charsets.UTF_8);
       	}catch(IOException e){
       		logger.error("Cannot wordcount to file [{}]",f.getAbsolutePath(),e);
       	}
    }
        
    public void outputRoute(){
    	String path = outputPath + "-routes.txt";
    	File f = new File(path);
       	StringBuilder sb = new StringBuilder();
       	sb.append("\n -------QueryRouter----------\n");
       	sb.append("QueryId	StartTime	StartVertex	EndVertex	Route\n");
       	for(Map.Entry<String, Route> entry : QueryRouter.entrySet()){
       		String id = entry.getKey();
       		Route route = entry.getValue();
       		sb.append(id + "	" + route.getStartTime() + "	" + route.getStartVertex() + "	" + route.getEndVertex() + "	" + route.getRoute() + "\n" );
       	}
       	int i = 0;
       	try {
       		Files.append(sb.toString(), f, Charsets.UTF_8);
       	}catch(IOException e){
       		logger.error("Cannot wordcount to file [{}]",f.getAbsolutePath(),e);
       	}
    }
    
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    public void setParameters(String path, int number, String method){
    	this.outputPath = path;
    	this.partitionNumber = number;
    	this.searchMethod = method;
    }
    
    @Override
    protected void onCreate() {
        QueryStartTime = new HashMap<String, Long>();
        QueryEndTime = new HashMap<String, Long>();
        
        QueryRuningTime = new HashMap<String, Long>();
        QueryRouter = new HashMap<String, Route> ();
        
        QueryDistance = new HashMap<String, Double>();
        QueryPathLenght = new HashMap<String, Integer>();
        
        QueryReceiveStat = new HashMap<String, Integer>();
        QuerySendStat = new HashMap<String, Integer>();

        QueryReceiveSize = new HashMap<String, Long>();
        QuerySendSize = new HashMap<String, Long>();

        timeCost = new HashMap<String, Long>();;
        spaceCost  = new HashMap<String, Integer>(); ;
    }

    @Override
    protected void onRemove() {
    }
}
