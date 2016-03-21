package shortPath;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.apache.commons.io.Charsets;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class queryOutputPE extends ProcessingElement{

    Streamable<Event> downStream;
    Streamable<Event> queryStream;
    Streamable<Event> controlStream;  //control the distance when get a new target distance
    Streamable<Event> updateQueryStream;
    
    static Logger logger = LoggerFactory.getLogger(shortestPathPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;
    HashMap<String, Integer> PartitionRecieveStat = new HashMap<String, Integer>();
    HashMap<String, Integer> PartitionSendStat = new HashMap<String, Integer>();

    String outputPath = "";
    int partitionNumber = 100 ;//100/500/1000/1500;
    String searchMethod = "Boundary" ; //"Boundary""Baseline"
    HashMap<String, TreeSet<Route>> queryRoutes = new HashMap<String, TreeSet<Route>>();
    HashMap<String, Double> QueryDistance = new HashMap<String, Double>();
    HashMap<String, Integer> QueryPathLenght = new HashMap<String, Integer>();
    
    HashMap<String, Route> subqueryRoutes = new HashMap<String, Route>();
    
    HashMap<String, String> queryStatus = new HashMap<String, String>();
    HashMap<String, Long> queryStatusTime = new HashMap<String, Long>();
    
    HashMap<String, Queue<Event>> TasksQueue = new HashMap<String, Queue<Event>>();
    HashMap<String, Queue<Event>> TasksPriotyQueue = new HashMap<String, Queue<Event>>(); //for case 2 and 3 in optimize
    
    HashMap<String, Long> UpdateCost = new HashMap<String, Long>();  //<Startvertex-endVertex-queryId-type[1234],cost>
    HashMap<String, Integer> UpdateCommunication = new HashMap<String, Integer>();  //<Startvertex-endVertex-queryId-type[1234],count>

    
    HashMap<String, Long> UpdateFailureRate = new HashMap<String, Long>();  //<Startvertex-endVertex, success count>
    HashMap<String, String> CacheQueryEdge = new HashMap<String, String>();  //<queryId,Startvertex-endVertex>
    HashMap<String, Integer> CacheQueryCommunication = new HashMap<String, Integer>();  //<queryId,count>
    
    HashMap<String, Integer> UpdateQueryNumber = new HashMap<String, Integer>();  //<Startvertex-endVertex,querycount>

    HashMap<String, Boolean> QuerySuccessNumber = new HashMap<String, Boolean>();//<queryId,false/true>
    
    ExecutorService pool = Executors.newCachedThreadPool();
    
    boolean firstevent = false;
    int K = 1; //compare the top K routers

    long LockTimeout = 120000;
    long updateStarttime = 0;
    long updateEndtime = 0;
    
    String MultipleFlag = "Lock"; //used on multiple edge updates

    String updateMethod = "Baseline";//Baseline  Improved
    
    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
    	//System.out.println("queryOutputPE: 	"+ event.toString());
    	if(event.containsKey("route")){  //query route results
        	Route shortpath = event.get("route", Route.class);

    		//System.out.println("query route: 	"+ shortpath.ToString());
    		String queryId = shortpath.getQueryId();
    		String route = shortpath.getRoute();
    		String startVertex = shortpath.getStartVertex();
    		String endVertex = shortpath.getEndVertex();
    		double distance = shortpath.getDistance();
    		Long startTime = shortpath.getStartTime();
    		Long endTime = shortpath.getEndTime();

        	if(queryId.indexOf("-") > 0){//sub query route
        		Route tmproute = subqueryRoutes.get(queryId)==null ? null: subqueryRoutes.get(queryId);
				if(tmproute==null ||tmproute.getDistance() > distance){
					subqueryRoutes.put(queryId, shortpath);
					try{
						//MergeSubQuery();
						//detectionDeadLock();
					}catch(Exception e){
						
					}					
				}
        	}
        	else{
        		TreeSet<Route> treeset = queryRoutes.get(queryId)==null?new TreeSet<Route>():queryRoutes.get(queryId);
            	treeset.add(shortpath);
            	queryRoutes.put(queryId, treeset);
            	
            	double preDistance = QueryDistance.get(queryId)==null ? Long.MAX_VALUE : QueryDistance.get(queryId);
				if(preDistance > distance){
					QueryDistance.put(queryId,distance);
					int length = (shortpath.getRoute().split(":").length-1) <= 0 ? 0: (shortpath.getRoute().split(":").length-1);
					QueryPathLenght.put(queryId, length);
				}
        	}
    	}
    	else if(event.containsKey("change")){
    		if(MultipleFlag.equals("noLock")){
    			NoLockDispatch(event);
    		}
    		else if(MultipleFlag.equals("Lock")){
    			SingleThreadLockDispatch(event);
    		}
    		else if(MultipleFlag.equals("ParaLock")){
    			ConcurrentDispatch(event);
    		}
    	}
    	else if(event.containsKey("stop")){  //this query is terminated
    		//System.out.println(event.toString());
    		String origrianlqueryId = event.get("queryId", String.class);
    		QuerySuccessNumber.put(origrianlqueryId, true);
    		int receive = event.get("receiveNum", Integer.class);
    		int send = event.get("sendNum", Integer.class);
    		String queryId = origrianlqueryId;
    		
    		if(queryId.indexOf("-") > 0){ //case 1
    			String[] data = queryId.split("-");
    			String start = data[2]+"-"+data[3];
    			String end = data[4]+"-"+data[5];
    			queryId = data[0];
    			UpdateCommunication.put(start + ":" + end + ":" + queryId + ":1", receive);
    			Long rate = UpdateFailureRate.get(start + ":" + end)==null? 0: UpdateFailureRate.get(start + ":" + end);
				UpdateFailureRate.put(start + ":" + end, rate+1);
    			if(updateMethod.equals("Baseline")){
        			MergeSubQuery();
    			}
    			else if (updateMethod.equals("Improved")){
    				MergeSubQueryImproved();
    			}
    			
    		}
    		String status = queryStatus.get(queryId)==null?"ack":queryStatus.get(queryId);
    		
    		if(status.equals("lock")){
    			String updateedge = CacheQueryEdge.get(queryId);
    			if(updateedge != null){//case 4
    				Long rate = UpdateFailureRate.get(updateedge)==null ? 0: UpdateFailureRate.get(updateedge);
					UpdateFailureRate.put(updateedge, rate+1);
					CacheQueryEdge.remove(queryId);
					
					int preCommuCount = CacheQueryCommunication.get(queryId)==null ? 0: CacheQueryCommunication.get(queryId);
					UpdateCommunication.put(updateedge + ":" + queryId + ":4", receive - preCommuCount);
    			}
    			
    			queryStatus.put(queryId,"ack");
    			queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
        		
    			if(MultipleFlag.equals("noLock")){
    				startTaskQueue(queryId);
        		}
        		else if(MultipleFlag.equals("Lock")){
        			startTaskQueue(queryId);
        		}
        		else if(MultipleFlag.equals("ParaLock")){
        			startTaskQueueOptimize(queryId);
        		}
        		
    		}
    		
    		CacheQueryCommunication.put(origrianlqueryId, receive);
    		updateEndtime = System.currentTimeMillis() - timeInterval;

    	}
    }
    
    /**
     * 
     * our Baseline update query algorithm:
     * recomputing the query for Case 1 and 4;
     **/
    public void updateQuery(Event event, String queryId){
    	String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		double distance = event.get("distance", double.class);
		double changed = event.get("change", double.class);
		long startTime = event.get("startTime", Long.class);
		
		long endTime = 0;
		
		String edge = startVertex + ":" + endVertex;
		
		queryStatus.put(queryId, "lock");
		queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
		
		//System.out.println("Start one query task on query:" + queryId + "	" +event.toString());

    	TreeSet<Route> qRoutes = queryRoutes.get(queryId);
		Iterator<Route> iterator = qRoutes.iterator();
		int count = 0;
		String costKey = edge + ":" + queryId + ":";
		
		while(iterator.hasNext() && count < K){
			count++;
			Route data = iterator.next();
			String route = data.getRoute();
    		int indexId = route.indexOf(edge);
    		if(indexId != -1){//this route pass this changed edge
    			//System.out.println("this route pass this changed edge");
    			if(changed > 0){ //recomputing?
    				//System.out.println(costKey+"	1");

    				Event e = new Event();
    				e.put("query", String.class, queryId + "-" +String.valueOf(System.currentTimeMillis() - timeInterval) + "-" + startVertex + "-" + endVertex + "-case1 "  + data.getStartVertex() + " "+ data.getEndVertex()  );
    				queryStream.put(e);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"1", endTime - startTime);
    				UpdateCommunication.put(costKey+"1", 0);
        		}
        		else {//reduce the distance
    				//System.out.println(costKey+"	2");
    				qRoutes.remove(data);
        			data.setDistance(data.getDistance() + changed);
        			qRoutes.add(data);
        			
        			Event result = new Event();
    				result.put("queryId", String.class, queryId);
    				result.put("control", String.class, null);
    				result.put("distance", Double.class, changed);
    				result.put("PE", String.class, "queryOutputPE");
    				controlStream.put(result);
    				
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"2", endTime - startTime);
    				UpdateCommunication.put(costKey+"2", 0);

    				
    				Long rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
					
    				startTaskQueue(queryId);

        			/*data.setDistance(data.getDistance() + changed);
        			qRoutes.add(data);
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis());
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"2", endTime - startTime);
    				UpdateCommunication.put(costKey+"2", 0);
    				
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis());
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"2", endTime - startTime);
    				UpdateCommunication.put(costKey+"2", 0);

    				
    				Long rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
					
    				startTaskQueue(queryId);*/
        		}
    		}
    		else{//not pass
    			if(changed > 0){//do nothing 
    				//System.out.println(costKey+"	3");
    				
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"3", endTime - startTime);
    				UpdateCommunication.put(costKey+"3", 0);
    				Long rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
    				startTaskQueue(queryId);
    				
/*    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis());
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"3", endTime - startTime);
    				
    				startTaskQueue(queryId);*/
    				
        		}
        		else {//split three queries
    				//System.out.println(queryId+ "	4	changed edg" + event.toString());
        			//System.out.println(costKey+"	4");

    				Event e = new Event();
    				e.put("query", String.class, queryId + "-" +String.valueOf(System.currentTimeMillis() - timeInterval) + "-" + startVertex + "-" + endVertex + "-case4 "  + data.getStartVertex() + " "+ data.getEndVertex()  );
    				queryStream.put(e);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"4", endTime - startTime);
        		}
    			
    		} 
		}
    }
    
    /*
     * Our improve algorithm for Case 1 and 4
     * Case 1 : from target t return to the source s to find a path which is smallest
     * Case 4 : from update edge partition to broadcast the affect edge or message to check some latent path
     */
    public void updateQueryImprove(Event event, String queryId){
    	String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		double distance = event.get("distance", double.class);
		double changed = event.get("change", double.class);
		long startTime = event.get("startTime", Long.class);
		
		long endTime = 0;
		
		String edge = startVertex + ":" + endVertex;
		
		queryStatus.put(queryId, "lock");
		queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
		
		
		
		//System.out.println("Start one query task on query:" + queryId + "	" +event.toString());

    	TreeSet<Route> qRoutes = queryRoutes.get(queryId);
		Iterator<Route> iterator = qRoutes.iterator();
		int count = 0;
		String costKey = edge + ":" + queryId + ":";
		
		while(iterator.hasNext() && count < K){
			count++;
			Route data = iterator.next();
			String route = data.getRoute();
    		int indexId = route.indexOf(edge);
    		if(indexId != -1){//this route pass this changed edge
    			//System.out.println("this route pass this changed edge");
    			if(changed > 0){ //recomputing?
    				//System.out.println(costKey+"	1");
    				Event result = new Event();
    				result.put("queryId", String.class, queryId);
    				result.put("control", String.class, null);
    				result.put("distance", Double.class, changed);
    				result.put("PE", String.class, "queryOutputPE");
    				
    				controlStream.put(result);
    				qRoutes.remove(data);
    				data.setDistance(data.getDistance() + changed);
    				qRoutes.add(data);
    				
    				Event e = new Event();
    				e.put("query", String.class, queryId + "-" +String.valueOf(System.currentTimeMillis() - timeInterval) + "-" + startVertex + "-" + endVertex + "-case1 " + data.getEndVertex() + " "+ data.getStartVertex() );
    				queryStream.put(e);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"1", endTime - startTime);
    				UpdateCommunication.put(costKey+"1", 0);

    				//qRoutes.remove(data);
        		}
        		else {//reduce the distance
        			//System.out.println(costKey+"	2");
        			qRoutes.remove(data);
        			data.setDistance(data.getDistance() + changed);
        			qRoutes.add(data);
        			
        			Event result = new Event();
    				result.put("queryId", String.class, queryId);
    				result.put("control", String.class, null);
    				result.put("distance", Double.class, changed);
    				result.put("PE", String.class, "queryOutputPE");
    				controlStream.put(result);
    				
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"2", endTime - startTime);
    				UpdateCommunication.put(costKey+"2", 0);

    				
    				Long rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
					
    				startTaskQueue(queryId);
    				
        		}
    		}
    		else{//not pass
    			if(changed > 0){//do nothing 
    				//System.out.println(costKey+"	3");
    				queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"3", endTime - startTime);
    				UpdateCommunication.put(costKey+"3", 0);
    				Long rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
    				startTaskQueue(queryId);
    				
        		}
        		else {//case 4   updateQueryStream
        			//System.out.println(costKey+"	4");
        			
        			TreeSet<Route> treeset = queryRoutes.get(queryId)==null?new TreeSet<Route>():queryRoutes.get(queryId);
        			Route thisroute = treeset.first();
        			
        			Event message = new Event();
        			message.put("queryId", String.class, queryId);
        			message.put("startTime", Long.class, System.currentTimeMillis() - timeInterval);
        			message.put("updateEdge", String.class, edge);
        			message.put("changed", Double.class, changed);
        			message.put("source", String.class, thisroute.getStartVertex());
        			message.put("target", String.class, thisroute.getEndVertex());
        			message.put("case", String.class, "4");
        			message.put("PE", String.class, "queryOutputPE");
    				updateQueryStream.put(message);

    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"4", endTime - startTime);
    				UpdateCommunication.put(costKey+"4", 0);
    				
    				CacheQueryEdge.put(queryId, edge);
        			
        		}
    			
    		} 
		}
    }

	public void MergeSubQuery(){
		ArrayList<String> tmpSubQuery = new ArrayList<String>();
		Iterator<Map.Entry<String, Route>> ite = subqueryRoutes.entrySet().iterator();
		while(ite.hasNext()){
			Map.Entry<String, Route> map = ite.next();
			String queryId = map.getKey();
			Route qRoutes = map.getValue();
			String[] data = queryId.split("-");
			
			String start = data[2]+"-"+data[3];
			String end = data[4]+"-"+data[5];
			
			if( data[data.length-1].equals("case1")){ // for case1
				Route newroute = new Route(data[0], qRoutes.getStartTime(), qRoutes.getStartVertex(), qRoutes.getEndVertex(), qRoutes.getEndVertex(), qRoutes.getDistance(), qRoutes.getRoute(), qRoutes.getEndTime());
				TreeSet<Route> treeset = queryRoutes.get(data[0])==null?new TreeSet<Route>():queryRoutes.get(data[0]);
				treeset.add(newroute);
				queryRoutes.put(data[0], treeset);
				
				String tmp = queryId;

				tmpSubQuery.add(tmp);

				Long finishTime = qRoutes.getEndTime();
				Long startTime = qRoutes.getStartTime();
				
				String costKey = start + ":" + end + ":" + data[0] + ":1";
				long cost = UpdateCost.get(costKey) + finishTime - startTime;
				UpdateCost.put(costKey, cost);

			}
			else if( data[data.length-1].equals("case4")){ // for case1
				Route newroute = new Route(data[0], qRoutes.getStartTime(), qRoutes.getStartVertex(), qRoutes.getEndVertex(), qRoutes.getEndVertex(), qRoutes.getDistance(), qRoutes.getRoute(), qRoutes.getEndTime());
				TreeSet<Route> treeset = queryRoutes.get(data[0])==null?new TreeSet<Route>():queryRoutes.get(data[0]);
				treeset.add(newroute);
				queryRoutes.put(data[0], treeset);
				
				String tmp = queryId;

				tmpSubQuery.add(tmp);

				Long finishTime = qRoutes.getEndTime();
				Long startTime = qRoutes.getStartTime();
				
				String costKey = start + ":" + end + ":" + data[0] + ":4";
				long cost = UpdateCost.get(costKey) + finishTime - startTime;
				UpdateCost.put(costKey, cost);
			}
		}	
		for(String s : tmpSubQuery){
			subqueryRoutes.remove(s);
		}
		tmpSubQuery.clear();
	}
	
	
	public void MergeSubQueryImproved(){
		ArrayList<String> tmpSubQuery = new ArrayList<String>();
		Iterator<Map.Entry<String, Route>> ite = subqueryRoutes.entrySet().iterator();
		while(ite.hasNext()){
			Map.Entry<String, Route> map = ite.next();
			String queryId = map.getKey();
			Route qRoutes = map.getValue();
			String[] data = queryId.split("-");
			
			String start = data[2]+"-"+data[3];
			String end = data[4]+"-"+data[5];
			if( data[data.length-1].equals("case1")){ // for case1
					Route newroute = new Route(data[0], qRoutes.getStartTime(), qRoutes.getStartVertex(), qRoutes.getEndVertex(), qRoutes.getEndVertex(), qRoutes.getDistance(), qRoutes.getRoute(), qRoutes.getEndTime());
					TreeSet<Route> treeset = queryRoutes.get(data[0])==null?new TreeSet<Route>():queryRoutes.get(data[0]);
					treeset.add(newroute);
					queryRoutes.put(data[0], treeset);
					
					String tmp = queryId;

					tmpSubQuery.add(tmp);

					Long finishTime = qRoutes.getEndTime();
					Long startTime = qRoutes.getStartTime();
					
					String costKey = start + ":" + end + ":" + data[0] + ":1";
					long cost = UpdateCost.get(costKey) + finishTime - startTime;
					UpdateCost.put(costKey, cost);

				}
			}

	
		for(String s : tmpSubQuery){
			subqueryRoutes.remove(s);
		}
		tmpSubQuery.clear();
	}
	
	
	public void detectionDeadLock(){
		for (Map.Entry<String, String> entry : queryStatus.entrySet()){
			String queryId = entry.getKey();
			String status = entry.getValue();
			long time = System.currentTimeMillis() - timeInterval - queryStatusTime.get(queryId);
			if(status.equals("lock") && time > LockTimeout){
				System.out.println("query	" +queryId+ "	lock time out");
				
				queryStatus.put(queryId, "ack");
				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
				if(MultipleFlag.equals("noLock")){
    				startTaskQueue(queryId);
        		}
        		else if(MultipleFlag.equals("Lock")){
        			startTaskQueue(queryId);
        		}
        		else if(MultipleFlag.equals("ParaLock")){
        			startTaskQueueOptimize(queryId);
        		}
			}

		}

	}
	
	public void startTaskQueue(String queryId){
		Queue<Event> events = TasksQueue.get(queryId);
		
		if(events != null && events.size() > 0){
			if(queryStatus.get(queryId) == null || queryStatus.get(queryId).equals("ack")){
				Event e = events.poll();
				if(updateMethod.equals("Baseline")){
					updateQuery(e, queryId);
				}
				else if (updateMethod.equals("Improved")){
    				updateQueryImprove(e, queryId);
				}
				TasksQueue.put(queryId, events);
			}
		}
	}
	
	
	/**
	 * Optimize the waiting event and some events for case 2 and 3 can be done priority
	 * @param queryId
	 */
	public void startTaskQueueOptimize(String queryId){
		Queue<Event> events = TasksPriotyQueue.get(queryId);
		while(events != null && events.size() > 0){
			if(queryStatus.get(queryId) == null || queryStatus.get(queryId).equals("ack")){
				Event e = events.poll();
				int cases = getUpdateCases(e, queryId);
				if(cases == 2 || cases == 3 ){
					if(updateMethod.equals("Baseline")){
						updateQuery(e, queryId);
					}
					else if (updateMethod.equals("Improved")){
						updateQueryImproveOptimize(e, queryId, cases);
					}
				}
				else{ // new case 1 or 4
					Queue<Event> eves = TasksQueue.get(queryId);
					eves.add(e);
					TasksQueue.put(queryId, eves);
				}
			}
		}
		
		Queue<Event> waitevents = TasksQueue.get(queryId);
		
		if(waitevents != null && waitevents.size() > 0){
			if(queryStatus.get(queryId) == null || queryStatus.get(queryId).equals("ack")){
				Event e = waitevents.poll();
				int cases = getUpdateCases(e, queryId);
				if(updateMethod.equals("Baseline")){
					updateQuery(e, queryId);
				}
				else if (updateMethod.equals("Improved")){
					updateQueryImproveOptimize(e, queryId, cases);
				}
				TasksQueue.put(queryId, waitevents);
			}
		}
	}
	
	public int getUpdateCases(Event event, String queryId){
		String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		double distance = event.get("distance", double.class);
		double changed = event.get("change", double.class);
		long startTime = event.get("startTime", Long.class);
		
		String edge = startVertex + ":" + endVertex;
    	TreeSet<Route> qRoutes = queryRoutes.get(queryId);
		Iterator<Route> iterator = qRoutes.iterator();
		int count = 0;
		while(iterator.hasNext() && count < K){
			count++;
			Route data = iterator.next();
			String route = data.getRoute();
    		int indexId = route.indexOf(edge);
    		if(indexId != -1){//this route pass this changed edge
    			//System.out.println("this route pass this changed edge");
    			if(changed > 0){ //recomputing?
    				return 1;
        		}
        		else {//reduce the distance
        			//System.out.println(costKey+"	2");
        			return 2;
    				
        		}
    		}
    		else{//not pass
    			if(changed > 0){//do nothing 
    				//System.out.println(costKey+"	3");
    				return 3;
    				
        		}
        		else {//case 4   updateQueryStream
        			//System.out.println(costKey+"	4");
        			
        			return 4;
        			
        		}
    		}
		}
		return 0;
	}
	
    /*
     * Our improve algorithm for Case 1 and 4
     * Case 1 : from target t return to the source s to find a path which is smallest
     * Case 4 : from update edge partition to broadcast the affect edge or message to check some latent path
     */
    public void updateQueryImproveOptimize(Event event, String queryId, int type){
    	String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		double distance = event.get("distance", double.class);
		double changed = event.get("change", double.class);
		long startTime = event.get("startTime", Long.class);
		
		long endTime = 0;
		
		String edge = startVertex + ":" + endVertex;
		
		queryStatus.put(queryId, "lock");
		queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
		
    	TreeSet<Route> qRoutes = queryRoutes.get(queryId);
		Iterator<Route> iterator = qRoutes.iterator();
		int count = 0;
		String costKey = edge + ":" + queryId + ":";
		
		while(iterator.hasNext() && count < K){
			count++;
			Route data = iterator.next();
			String route = data.getRoute();
			Event result = new Event();
			long rate = 0;
			switch (type)
			{
				case 1:
					
					result.put("queryId", String.class, queryId);
					result.put("control", String.class, null);
					result.put("distance", Double.class, changed);
					result.put("PE", String.class, "queryOutputPE");
					
					controlStream.put(result);
					qRoutes.remove(data);
					data.setDistance(data.getDistance() + changed);
					qRoutes.add(data);
					
					Event e = new Event();
					e.put("query", String.class, queryId + "-" +String.valueOf(System.currentTimeMillis() - timeInterval) + "-" + startVertex + "-" + endVertex + "-case1 " + data.getEndVertex() + " "+ data.getStartVertex() );
					queryStream.put(e);
					
					endTime = System.currentTimeMillis() - timeInterval;
					UpdateCost.put(costKey+"1", endTime - startTime);
					UpdateCommunication.put(costKey+"1", 0);
					break;
				case 2:
					qRoutes.remove(data);
	    			data.setDistance(data.getDistance() + changed);
	    			qRoutes.add(data);
	    			
	    			result = new Event();
					result.put("queryId", String.class, queryId);
					result.put("control", String.class, null);
					result.put("distance", Double.class, changed);
					result.put("PE", String.class, "queryOutputPE");
					controlStream.put(result);
					
					queryStatus.put(queryId, "ack");
					queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
					endTime = System.currentTimeMillis() - timeInterval;
					UpdateCost.put(costKey+"2", endTime - startTime);
					UpdateCommunication.put(costKey+"2", 0);
	
					
					rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
					
					startTaskQueueOptimize(queryId);
					break;
				case 3:
					queryStatus.put(queryId, "ack");
    				queryStatusTime.put(queryId, System.currentTimeMillis() - timeInterval);
    				
    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"3", endTime - startTime);
    				UpdateCommunication.put(costKey+"3", 0);
    				rate = UpdateFailureRate.get(edge)==null? 0: UpdateFailureRate.get(edge);
					UpdateFailureRate.put(edge, rate+1);
					startTaskQueueOptimize(queryId);
					break;
				case 4:
					TreeSet<Route> treeset = queryRoutes.get(queryId)==null?new TreeSet<Route>():queryRoutes.get(queryId);
        			Route thisroute = treeset.first();
        			
        			Event message = new Event();
        			message.put("queryId", String.class, queryId);
        			message.put("startTime", Long.class, System.currentTimeMillis() - timeInterval);
        			message.put("updateEdge", String.class, edge);
        			message.put("changed", Double.class, changed);
        			message.put("source", String.class, thisroute.getStartVertex());
        			message.put("target", String.class, thisroute.getEndVertex());
        			message.put("case", String.class, "4");
        			message.put("PE", String.class, "queryOutputPE");
    				updateQueryStream.put(message);

    				endTime = System.currentTimeMillis() - timeInterval;
    				UpdateCost.put(costKey+"4", endTime - startTime);
    				UpdateCommunication.put(costKey+"4", 0);
    				CacheQueryEdge.put(queryId, edge);
					break;
				default:
					break;
			}
    			
    	} 
		
    }
	
	public void onTime(){
		/*if(updateMethod.equals("Baseline")){
			MergeSubQuery();
		}
		else if (updateMethod.equals("Improved")){
			MergeSubQueryImproved();
		}*/
		String path = outputPath;

		File f = new File(path);
		StringBuilder sb = new StringBuilder();
		int i = 0;

		sb.append("-----Query routes---" + (System.currentTimeMillis() - timeInterval)+ "\n");
		
		/*for (Map.Entry<String, TreeSet<Route>> entry : queryRoutes.entrySet()){
			String queryId = entry.getKey();
			TreeSet<Route> qRoutes = entry.getValue();
			Iterator<Route> iterator = qRoutes.iterator();
			int count = 0;
			while(iterator.hasNext() && count < K){
				count++;
				Route r = iterator.next();
				sb.append(r.ToString()+"\n");
			}
		}*/
		sb.append("\n");

		try {
			Files.append(sb.toString(), f, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Cannot wordcount to file [{}]", f.getAbsolutePath(), e);
		}
		
		publishSubquery();
		
		detectionDeadLock();
		
		publishUpdateCost();
		
	}	
	
	
	public void publishSubquery(){
		String path = outputPath+"-sub-query.txt";

		File f = new File(path);
		StringBuilder sb = new StringBuilder();
		int i = 0;

		sb.append("-----Query routes---" + (System.currentTimeMillis() - timeInterval)+ "\n");
		
		for (Map.Entry<String, Route> entry : subqueryRoutes.entrySet()){
			String queryId = entry.getKey();
			Route qRoutes = entry.getValue();
			sb.append(qRoutes.ToString()+"\n");

		}

		sb.append("\n");

		try {
			Files.append(sb.toString(), f, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Cannot wordcount to file [{}]", f.getAbsolutePath(), e);
		}
	}
   
	

	public void publishUpdateCost(){
		String path = outputPath+"-update-cost.txt";

		File f = new File(path);
		StringBuilder sb = new StringBuilder();
		int i = 0;
		double sumCost = 0;
		
		sb.append("-----Query Cost---" + (System.currentTimeMillis() - timeInterval)+ "\n");
		
		
		for (Map.Entry<String, Integer> entry : UpdateCommunication.entrySet()){
			String queryId = entry.getKey();
			int cost = entry.getValue();
			//sb.append(queryId + "	" + cost +"\n");
			sumCost = sumCost + cost;
			i++;
		}
		double avgCommuni = i==0? 0 :sumCost/i;
		sb.append("\n");
		
		sumCost = 0; 
		i=0;
		for (Map.Entry<String, Long> entry : UpdateCost.entrySet()){
			String queryId = entry.getKey();
			Long cost = entry.getValue();
			//sb.append(queryId + "	" + cost +"\n");
			sumCost = sumCost + cost;
			i++;
		}
		double avgCost = i==0? 0 :sumCost/i;
		sb.append("\n");
		
		int updateRate = 0;
		for(Map.Entry<String, Long> entry : UpdateFailureRate.entrySet()){
			String updateEdge = entry.getKey();
			Long successCount = entry.getValue();
			int queryCount = UpdateQueryNumber.get(updateEdge);
			double rate = successCount * 1.00 / queryCount;
			//System.out.println(updateEdge + "	"+successCount + "	"+ queryCount);
			if(rate >= 1){
				updateRate++;
			}
			//sb.append(updateEdge + "	" + rate + "\n");
		}
		
		double sumDistance = 0;
       	int count=0;
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
    	
		long updatealltime = updateEndtime - updateStarttime;
		sb.append("\n");
		sb.append("Communication:" + avgCommuni + "\n");
		sb.append("Cost:" + avgCost + "\n");
		sb.append("UpdateEdge:" +UpdateFailureRate.size()  + "\n");
		sb.append("UpdateRate:" +updateRate  + "\n");
		sb.append("Rate:" +(UpdateFailureRate.size()==0?0:updateRate * 100/UpdateFailureRate.size()) + "\n");
		sb.append("UpdateAllRuntime:" +updatealltime + "\n");
		sb.append("UpdateDistance:" +distance + "\n");
		sb.append("UpdateLength:" +length + "\n");
		
		try {
			Files.append(sb.toString(), f, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Cannot wordcount to file [{}]", f.getAbsolutePath(),
					e);
		}
	}
   
	/**
	 * NoLockDispatch:  dispatch change events one by one. There is no lock mechanism
	 * @param event
	 */
	public void NoLockDispatch(Event event){
		if(!firstevent){
			firstevent = true;
			updateStarttime= System.currentTimeMillis() - timeInterval;
			updateEndtime = System.currentTimeMillis() - timeInterval;
		}
		int queryNumber = 0;

		// System.out.println("receive update: 	"+ event.toString());
		for (Map.Entry<String, TreeSet<Route>> entry : queryRoutes.entrySet()) {
			String queryId = entry.getKey();
			boolean querySuccess = QuerySuccessNumber.get(queryId) == null ? false
					: QuerySuccessNumber.get(queryId);
			if (querySuccess) { 
				if (updateMethod.equals("Baseline")) {
					updateQuery(event, queryId);
				} else if (updateMethod.equals("Improved")) {
					updateQueryImprove(event, queryId);
				}
				queryNumber++;
			}
		}
		String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		String edge = startVertex + ":" + endVertex;
		UpdateQueryNumber.put(edge, queryNumber);
	}
	
	/**
	 * SingleThreadDispatch:  dispatch change events one by one. 
	 * For all query, there is only one dispatch thread to lock 
	 * @param event
	 */
	public void SingleThreadLockDispatch(Event event){
		if(!firstevent){
			firstevent = true;
			updateStarttime= System.currentTimeMillis() - timeInterval;
			updateEndtime = System.currentTimeMillis() - timeInterval;
		}
		int queryNumber = 0;

		// System.out.println("receive update: 	"+ event.toString());
		for (Map.Entry<String, TreeSet<Route>> entry : queryRoutes.entrySet()) {
			String queryId = entry.getKey();
			boolean querySuccess = QuerySuccessNumber.get(queryId) == null ? false
					: QuerySuccessNumber.get(queryId);
			if (querySuccess) { 
				String status = queryStatus.get(queryId);
				if (status == null || status.equals("ack")) {
					if (updateMethod.equals("Baseline")) {
						updateQuery(event, queryId);
					} else if (updateMethod.equals("Improved")) {
						updateQueryImprove(event, queryId);
					}
				} else {// the query is lock and waiting
					Queue<Event> es = TasksQueue.get(queryId) == null ? new LinkedList<Event>()
							: TasksQueue.get(queryId);
					es.add(event);
					TasksQueue.put(queryId, es);
				}
				queryNumber++;
			}
		}
		String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		String edge = startVertex + ":" + endVertex;
		UpdateQueryNumber.put(edge, queryNumber);
	}
	
	
	/**
	 * ConcurrentDispatch:  dispatch change events concurrent 
	 * and one query needs one thread to dispatch events. 
	 * @param event
	 */
	public void ConcurrentDispatch(Event event){
		if(!firstevent){
			firstevent = true;
			updateStarttime= System.currentTimeMillis() - timeInterval;
			updateEndtime = System.currentTimeMillis() - timeInterval;
		}
		int queryNumber = 0;

		// System.out.println("receive update: 	"+ event.toString());
		for (Map.Entry<String, TreeSet<Route>> entry : queryRoutes.entrySet()) {
			String queryId = entry.getKey();
			boolean querySuccess = QuerySuccessNumber.get(queryId) == null ? false
					: QuerySuccessNumber.get(queryId);
			if (querySuccess) { 
				pool.submit(new ExecuteTaskUpdateEvent(event, queryId));
				queryNumber++;
			}
		}
		String startVertex = event.get("startVertex", String.class);
		String endVertex = event.get("endVertex", String.class);
		String edge = startVertex + ":" + endVertex;
		UpdateQueryNumber.put(edge, queryNumber);
	}
	
	
	class ExecuteTaskUpdateEvent implements Runnable{
		private Event event;
		private String queryId;
		
		public ExecuteTaskUpdateEvent(Event e, String queryId){
			this.event = e;
			this.queryId = queryId;
		}
		
		@Override
		public void run(){
			String status = queryStatus.get(queryId);
			if (status == null || status.equals("ack")) {
				
				if (updateMethod.equals("Baseline")) {
					updateQuery(event, queryId);
				} else if (updateMethod.equals("Improved")) {
					int cases = getUpdateCases(event, queryId);
					if(cases == 2 || cases == 3 ){
						updateQueryImproveOptimize(event, queryId, cases);
					}
					else{
						updateQueryImproveOptimize(event, queryId, cases);
					}
				}
			} else {// the query is lock and waiting
				int cases = getUpdateCases(event, queryId);
				if(cases == 2 || cases == 3 ){
					Queue<Event> es = TasksPriotyQueue.get(queryId) == null ? new LinkedList<Event>()
							: TasksPriotyQueue.get(queryId);
					es.add(event);
					TasksPriotyQueue.put(queryId, es);
				}
				else{
					Queue<Event> es = TasksQueue.get(queryId) == null ? new LinkedList<Event>()
							: TasksQueue.get(queryId);
					es.add(event);
					TasksQueue.put(queryId, es);
				}
				
			}
		}
		
	}
	
	
	
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    
    public void setQueryStream(Streamable<Event> stream){
    	this.queryStream = stream;
    	
    }
    public void setControStream(Streamable<Event> stream){
    	this.controlStream = stream;
    	
    }
    
    public void setUpdateStream(Streamable<Event> stream){
    	this.updateQueryStream = stream;
    	
    }
    
    
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    public void setParameters(String path, int number, String method, String updateMethod, String lockMthod){
    	this.outputPath = path;
    	this.partitionNumber = number;
    	this.searchMethod = method;
    	this.updateMethod = updateMethod;
    	this.MultipleFlag = lockMthod;

    }
    
    @Override
    protected void onCreate() {
    }

    @Override
    protected void onRemove() {
    }
}
