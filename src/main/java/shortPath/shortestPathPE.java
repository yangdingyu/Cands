package shortPath;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.instrument.Instrumentation;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class shortestPathPE extends ProcessingElement{

    transient Streamable<Event> downStream;   //route stream
    Streamable<Event> resultStream;   //the stream contains the target distance
    Streamable<Event> statResultStream;
    Streamable<Event> controlStream;  //control the distance when get a new target distance
    Streamable<Event> statStream;   //stat the communication of route
    transient  Streamable<Event> LandmarkStream;  //control the distance when get a new target distance

 
    static Logger logger = LoggerFactory.getLogger(shortestPathPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;
    private HashMap<String,Vertex> vertices ;  //map information: vertex and edge
    private HashMap<String,Double> QDistance = new HashMap<String,Double>();  // <QueryID+source, distance> : the shortest distance from the source vertex
    private HashMap<String,String> QRoute = new HashMap<String,String>(); // <QueryID+source, route> : the shortest route from the source vertex
    
    private HashMap<String, BoundaryVertex> Boundaries ;
    
    private HashMap<String,Double> targetDistance;
    boolean ontimeFlag = true;
    
    int sendNum = 0;
    int receiveNum = 0;
    
    String outputPath = "";
    int partitionNumber = 100 ;//100/500/1000/1500;
    String searchMethod = "Boundary" ; //"Boundary""Baseline"
	String isLog = "false";
	String partition = "";//getId().substring(0, getId().length()-4)
	String updateMethod = "Baseline";//Baseline  Improved
	
    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
    	
    	
    	//////map edge update
    	if(event.containsKey("edge")){ 
    		updateEdgeWeight(event);
    		
    	}
    	else if (event.containsKey("control")){
    		
    		if(!event.containsKey("PE")){
    			String queryId = event.get("queryId", String.class);
        		double distance = event.get("distance",Double.class);
        		double curr = targetDistance.get(queryId)==null? Double.MAX_VALUE:targetDistance.get(queryId);
        		if(curr>distance){
        			targetDistance.put(queryId, distance);
        		}
    		}
    		else{// from queryOutputPE  update message
    			String queryId = event.get("queryId", String.class);
        		double distance = event.get("distance",Double.class);
        		double curr = targetDistance.get(queryId)==null? Double.MAX_VALUE:targetDistance.get(queryId);
    			targetDistance.put(queryId, curr + distance);
    		}
    		
    		
    	}
    	
    	
    	///// route map and find shortest path
    	else if (event.containsKey("route")&&!event.containsKey("partition")){ 
    		
    		Route route = event.get("route", Route.class);
    		
    		int length = route.getRoute().split(":").length;
    		String queryId = route.getQueryId();
    		long messageByte =0;
    		
    		Event ev = new Event();
    		ev.put("partition", String.class, partition);
    		ev.put("queryId", String.class, queryId);
			ev.put("receive", Integer.class, 1);
			ev.put("size", Long.class, messageByte);
			ev.put("source", String.class, route.getStartVertex());
			ev.put("From", String.class, route.getNextVertex());
			ev.put("distance", double.class, route.getDistance());
			
			ev.put("pathLength", Integer.class, length);
			statStream.put(ev);

			if (searchMethod.equals("Boundary")) {
				String[] q = queryId.split("-");
				if(q.length > 1 && updateMethod.equals("Improved")){//&& (q[1].equals("case1")||q[1].equals("case4"))
					reverseTraverse(event);
				}
				else{
					boundaryFindShortPath(event); //Boundary
				}
			} 
			else if (searchMethod.equals("Baseline")) {
				graphFindShortestPath(event); // Baseline
			}
			else if (searchMethod.equals("BaselineWithoutControl")){
				graphFindShortestPathWithoutControl(event);
			}

    	}
    	
    	// partition merge shortest path
    	else if (event.containsKey("partition")){ 
    		String queryId = event.get("queryId", String.class);
    		
    		long messageByte =0;
    		Event ev = new Event();
    		ev.put("partition", String.class, partition);
    		ev.put("queryId", String.class, queryId);
			ev.put("receive", Integer.class, 1);
			ev.put("size", Long.class, messageByte);
			statStream.put(ev);
    		
			receiveNum++;
			
			BoundaryMergeShortPath(event);
    	}
    	
    	
    	// landmark partition
    	else if(event.containsKey("partitionRoute")){
    		BoundaryMergeShortPathByLandmarkPartition(event);
    	}
    	
    	
    	//update query case 4
    	else if(event.containsKey("updateEdge")){ 
    		broadcastAffectdMessage(event);
    	}
    	
    	
    	
    	
    }
    
    /**
     * Update the weight of the edge 
     * @param event
     */
    public void updateEdgeWeight(Event event){
    	
    	String[] data = event.get("edge").split(" ");
    	String key = data[1];
    	String FirstPartition = key.split("-")[0];
    	String neighbor = data[2];
    	String NeighborPartition = neighbor.split("-")[0];
    	double distance = Double.parseDouble(data[3]);
    	
    	if(firstEvent == true){
    		partition = FirstPartition;
    		firstEvent = false;

    	}
    	/*if(partition.equals("68")){
			System.out.println("I am  here!");
		}*/
    	Vertex vertex ;
    	if(vertices.get(key)!=null){
    		vertex = vertices.get(key);//==null?new Vertex(key):vertices.get(key);
    		double changeFlag = vertex.put(neighbor, distance);
    		
    		if(changeFlag != 0){  // if flag is not 0, then edge's status changed
    			long startTime = System.currentTimeMillis() -  timeInterval;
    			maintainBoundary();
    			//ShortcutUpdate(key, neighbor,changeFlag);
    			long endTime = System.currentTimeMillis() -  timeInterval;
    			long timecost = endTime - startTime;
    			//System.out.println("The edge status is changed!!	cost:" + timecost +"	"+ vertex.getEdge(neighbor).toString());
    			
    			Event ev = new Event();
        		ev.put("startVertex", String.class, key);
    			ev.put("endVertex", String.class, neighbor);
    			ev.put("distance", double.class, distance);
    			ev.put("change", double.class, changeFlag);
    			ev.put("startTime", Long.class, startTime);
    			resultStream.put(ev);
    		}
    		
    		
    	}
    	else{
    		vertex = new Vertex(key);
    		vertex.put(neighbor, distance);
        	vertices.put(key, vertex);
        	
    	}
    	if(!FirstPartition.equals(NeighborPartition)){  //the status of boundary vertex is stored in vertices sets. Here is no need
    		BoundaryVertex  boundary = Boundaries.get(key) == null? new BoundaryVertex(key) : Boundaries.get(key);
    		boundary.putEdge(neighbor, Double.valueOf(distance));
            Boundaries.put(key, boundary);
            
            
    	}
    	
    }
    

    public void boundaryFindShortPath(Event event){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	Route shortpath = event.get("route", Route.class);
    	
    	//System.out.println(shortpath.ToString());
    	String queryId = shortpath.getQueryId();
    	long starttime = shortpath.getStartTime();
		String source = shortpath.getStartVertex();
		String next = shortpath.getNextVertex();
		String target = shortpath.getEndVertex();
		double distance = shortpath.getDistance();
		String route = shortpath.getRoute();
		
		
		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
		HashMap<String, String> RouteSet = new HashMap<String, String>();
		
		if(vertices.containsKey(target)){ // target is here and find shortest path from source to target
			double currentsourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //boundary distance

			if(currentsourceDistance > distance){
			    QDistance.put(queryId+" "+next, distance);
			    QRoute.put(queryId+" "+next, route);
			    
				if (next.equals(target)) {
					
			    	Route thisroute = new Route(queryId, starttime, source, next, target, distance, route, System.currentTimeMillis()-timeInterval);

					Event ev = new Event();
					ev.put("route", Route.class, thisroute);
					resultStream.put(ev);
					Event evstat = new Event();
					evstat.put("route", Route.class, thisroute);
					statResultStream.put(evstat);
					
					/*Event evt = new Event();
					evt.put("partition", String.class, partition);
					evt.put("queryId", String.class, queryId);
					evt.put("send", Integer.class, 1);
					evt.put("source", String.class, next);
					evt.put("next", String.class, "null");
					evt.put("distance", double.class, distance);
					
					int length = route.split(":").length;
					evt.put("pathLength", Integer.class, length);
					statStream.put(evt);*/
					
					Event result = new Event();
					result.put("queryId", String.class, queryId);
					result.put("control", String.class, null);
					result.put("distance", Double.class, distance);
					controlStream.put(result);

					return;
				}
				else{ //different vertex
					  if(Boundaries.containsKey(target) && Boundaries.containsKey(next)){// target and source are different boundary vertex
							double newDistance = distance + Boundaries.get(next).getShortcutDistance(target);
							String newRoute = route +":"+ Boundaries.get(next).getShortcutRoute(target);
							double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
							if(currentTargetDistance > newDistance) { 
								QDistance.put(queryId+" "+target, newDistance);
								QRoute.put(queryId+" "+target, newRoute);
								
								Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);

								
								resultStream.put(ev);
								Event evstat = new Event();
								evstat.put("route", Route.class, thisroute);
								statResultStream.put(evstat);
								
								/*Event evt = new Event();
								evt.put("partition", String.class, partition);
								evt.put("queryId", String.class, queryId);
								evt.put("send", Integer.class, 1);
								evt.put("source", String.class, next);
								evt.put("next", String.class, "null");
								evt.put("distance", double.class, newDistance);
								
								int length = route.split(":").length;
								evt.put("pathLength", Integer.class, length);
								statStream.put(evt);*/
								
								Event result = new Event();
								result.put("queryId", String.class, queryId);
								result.put("control", String.class, null);
								result.put("distance", Double.class, newDistance);
								controlStream.put(result);
								
								return;
							}
						 }
						 else{
							DistanceSet = LocalShortPathByFbncHeap(next, target, RouteSet);
							double newDistance = distance + DistanceSet.get(target);
							String newRoute = route +":"+ getRoute(RouteSet, next, target);
							double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
							if(currentTargetDistance > newDistance) { 
								QDistance.put(queryId+" "+target, newDistance);
								QRoute.put(queryId+" "+target, newRoute);
								
								Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);
								
								resultStream.put(ev);
								Event evstat = new Event();
								evstat.put("route", Route.class, thisroute);
								statResultStream.put(evstat);
								
								/*Event evt = new Event();
								evt.put("partition", String.class, partition);
								evt.put("queryId", String.class, queryId);
								evt.put("send", Integer.class, 1);
								evt.put("source", String.class, next);
								evt.put("next", String.class, "null");
								evt.put("distance", double.class, newDistance);
								
								int length = route.split(":").length;
								evt.put("pathLength", Integer.class, length);
								statStream.put(evt);*/
								
								Event result = new Event();
								result.put("queryId", String.class, queryId);
								result.put("control", String.class, null);
								result.put("distance", Double.class, newDistance);
								controlStream.put(result);
								
								return;
								
							}
						 }
					
					}
				}
		}

		
		else {  //target is not here
			if(!Boundaries.containsKey(next)){   // the source is internal node and should find the boundary to other partition
				
				//int localPath = LocalShortPath(source, DistanceSet, RouteSet);
				
				try{
					DistanceSet = LocalShortPathByFbncHeap(next, RouteSet);
				}catch(Exception e){
					return;
				}
				
				for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
					String key = entry.getKey();
					double localDistance = DistanceSet.get(key);
					String localRoute = getRoute(RouteSet, next, key);
					if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
						double newDistance = distance + localDistance;
						String newRoute = route + ":" + localRoute;
						
						double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
						if(currentDistance <= newDistance ) continue;
						else {
							QDistance.put(queryId+" "+key, newDistance);
							QRoute.put(queryId+" "+key, newRoute);
							
							for (String nextnode : entry.getValue().getEdgeNeighbors()) {
								long messageByte =0;
						    	long initmemory = Runtime.getRuntime().freeMemory();
						    	
								Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
										+ entry.getValue().getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);

								sendNum++;
								downStream.put(ev);
								
								long endmemory = Runtime.getRuntime().freeMemory();
						    	messageByte = messageByte + initmemory - endmemory ;

								Event evt = new Event();
								evt.put("partition", String.class, partition);
								evt.put("queryId", String.class, queryId);
								evt.put("send", Integer.class, 1);
								evt.put("size", Long.class, messageByte);
								evt.put("source", String.class, next);
								evt.put("next", String.class, nextnode);
								evt.put("distance", double.class, newDistance + entry.getValue().getEdgeDistance(nextnode));
								
								int length = newRoute.split(":").length + 1;
								evt.put("pathLength", Integer.class, length);
								statStream.put(evt);
								sendFlag = "true";
							}
							
							/*Event ev = new Event();
				    		ev.put("partition", String.class, getId());
				    		ev.put("queryId", String.class, queryId);
							ev.put("send", Integer.class, entry.getValue().getEdgeNeighbors().size());
							ev.put("source", String.class, source);
							ev.put("next", String.class, entry.getValue().getEdgeNeighbors().toArray().toString());
							statStream.put(ev);*/
							
						}					
					}
				}
			}
			////boundary to boundary  shortcut and then to next partition
			else{
				BoundaryVertex boundary = Boundaries.get(next);
				double currentSourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //input boundary node distance
				double limiteDistance = targetDistance.get(queryId)==null?Double.MAX_VALUE: targetDistance.get(queryId);
				if(currentSourceDistance > distance && currentSourceDistance <= limiteDistance && (boundary.getShortcut()!= null)) { // sometimes boundary just one and no shortcut
					QDistance.put(queryId+" "+next, distance);
					QRoute.put(queryId+" "+next, route);
	
					for(String nextboundary : boundary.getBoundaries()){ //send to local boundary then to other partition
						
						double localDistance = boundary.getShortcutDistance(nextboundary);
						String localRoute = boundary.getShortcutRoute(nextboundary);
						
						if(localDistance != Double.MAX_VALUE){
							double newDistance = distance + localDistance;
							String newRoute = route + ":" + localRoute;
							double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+nextboundary);
							if(currentDistance <= newDistance) continue;
							else {
								QDistance.put(queryId+" "+nextboundary, newDistance);
								QRoute.put(queryId+" "+nextboundary, newRoute);
								
								for (String nextnode : Boundaries.get(nextboundary).getEdgeNeighbors()){
									long messageByte =0;
							    	long initmemory = Runtime.getRuntime().freeMemory();
									Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
											+ Boundaries.get(nextboundary).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
									Event ev = new Event();
									ev.put("route", Route.class, thisroute);
									
								
									sendNum++;
									downStream.put(ev);
									
									long endmemory = Runtime.getRuntime().freeMemory();
							    	messageByte = messageByte + initmemory - endmemory ;
									
									Event evt = new Event();
						    		evt.put("partition", String.class, partition);
						    		evt.put("queryId", String.class, queryId);
									evt.put("send", Integer.class, 1);
									evt.put("size", Long.class, messageByte);
									evt.put("source", String.class, next);
									evt.put("next", String.class, nextnode);
									evt.put("distance", double.class, newDistance + Boundaries.get(nextboundary).getEdgeDistance(nextnode));
									
									int length = newRoute.split(":").length + 1;
									evt.put("pathLength", Integer.class, length);
									statStream.put(evt);
									sendFlag = "true";
								}
								
								/*Event ev = new Event();
					    		ev.put("partition", String.class, getId());
					    		ev.put("queryId", String.class, queryId);
								ev.put("send", Integer.class, Boundaries.get(nextboundary).getEdgeNeighbors().size());
								ev.put("source", String.class, source);
								ev.put("next", String.class, Boundaries.get(nextboundary).getEdgeNeighbors().toArray().toString());
								statStream.put(ev);*/
							}					
						}
			
					}
					
					if(next.equals(source)){
						for(String nextboundary : boundary.getEdgeNeighbors()){  //directly send  to other partition through edge: when source is boundary
							long messageByte =0;
					    	long initmemory = Runtime.getRuntime().freeMemory();
					    	
							Route thisroute = new Route(queryId, starttime, source, nextboundary, target, distance + boundary.getEdgeDistance(nextboundary), route + ":" + nextboundary, System.currentTimeMillis()-timeInterval);
							Event ev = new Event();
							ev.put("route", Route.class, thisroute);
							sendNum++;
							downStream.put(ev);
							
							long endmemory = Runtime.getRuntime().freeMemory();
					    	messageByte = messageByte + initmemory - endmemory ;
							
							Event evt = new Event();
							evt.put("partition", String.class, partition);
							evt.put("queryId", String.class, queryId);
							evt.put("send", Integer.class, 1);
							evt.put("size", Long.class, messageByte);
							evt.put("source", String.class, next);
							evt.put("next", String.class, nextboundary);
							evt.put("distance", double.class, distance + boundary.getEdgeDistance(nextboundary));
							
							int length = route.split(":").length + 1;
							evt.put("pathLength", Integer.class, length);
							statStream.put(evt);
							sendFlag = "true";
						}
						
						/*Event ev = new Event();
			    		ev.put("partition", String.class, getId());
			    		ev.put("queryId", String.class, queryId);
						ev.put("send", Integer.class, boundary.getEdgeNeighbors().size());
						ev.put("source", String.class, source);
						ev.put("next", String.class, boundary.getEdgeNeighbors().toArray().toString());
						statStream.put(ev);*/
					}
					
				}
				
				
				
			}
		}
		
    	if(sendFlag.equals("false")){
    		Event evt = new Event();
    		evt.put("partition", String.class, partition);
    		evt.put("queryId", String.class, queryId);
    		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
    		evt.put("size", Long.class, Long.valueOf(0));
    		evt.put("source", String.class, target);
    		evt.put("next", String.class, target);
			statStream.put(evt);
    	}
		
    }
    
    
    public void BoundaryMergeShortPath(Event events){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	String queryId ;
		String source ;
		String next ;
		long starttime;
		String target ;
		double distance;
		String route;
		
    	Map<String, String> eventsMap = events.getAttributesAsMap();
    	for(Map.Entry<String, String> entryEvents : eventsMap.entrySet()){
    		String keyEvent = entryEvents.getKey();
    		if(!keyEvent.equals("partition")&&!keyEvent.equals("queryId")){
    			Event event = events.get(keyEvent, Event.class);
    			
    			Route shortpath = event.get("route", Route.class);
    	    	
    	    	queryId = shortpath.getQueryId();
    			source = shortpath.getStartVertex();
    			starttime = shortpath.getStartTime();
    			next = shortpath.getNextVertex();
    			target = shortpath.getEndVertex();
    			distance = shortpath.getDistance();
    			route = shortpath.getRoute();
    			

        		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
        		HashMap<String, String> RouteSet = new HashMap<String, String>();
        		
        		if(vertices.containsKey(target)){ // target is here and find shortest path from source to target
        			double currentsourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //boundary distance

        			if(currentsourceDistance > distance){
        				if(Boundaries.containsKey(target) && Boundaries.containsKey(next)){     //if target is boundary vertex, we can use it directly
        					if(next.equals(target)){
    							QDistance.put(queryId+" "+target, distance);
    							QRoute.put(queryId+" "+target, route);

    							Route thisroute = new Route(queryId, starttime, source, next, target, distance, route, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);        						

        						resultStream.put(ev);
        						Event evstat = new Event();
        						evstat.put("route", Route.class, thisroute);
        						statResultStream.put(evstat);
        						
        						/*Event evt = new Event();
        						evt.put("partition", String.class, partition);
        						evt.put("queryId", String.class, queryId);
        						evt.put("send", Integer.class, 1);
        						evt.put("source", String.class, next);
        						evt.put("next", String.class, "null");
        						evt.put("distance", double.class, distance);
        						
        						int length = route.split(":").length;
        						evt.put("pathLength", Integer.class, length);
        						statStream.put(evt);*/
        						
        						Event result = new Event();
        						result.put("queryId", String.class, queryId);
        						result.put("control", String.class, null);
        						result.put("distance", Double.class, distance);
        						controlStream.put(result);
        						
        						return;
        					}
        					else{ //different boundary vertices and use shortcut
        						double newDistance = distance + Boundaries.get(next).getShortcutDistance(target);
        						String newRoute = route +":"+ Boundaries.get(next).getShortcutRoute(target);
        						double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
        						if(currentTargetDistance > newDistance) { 
        							
        							QDistance.put(queryId+" "+target, newDistance);
        							QRoute.put(queryId+" "+target, newRoute);

        							Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
    								Event ev = new Event();
    								ev.put("route", Route.class, thisroute);  
            						

            						resultStream.put(ev);
            						Event evstat = new Event();
            						evstat.put("route", Route.class, thisroute);
            						statResultStream.put(evstat);

        							
        							/*Event evt = new Event();
            						evt.put("partition", String.class, partition);
            						evt.put("queryId", String.class, queryId);
            						evt.put("send", Integer.class, 1);
            						evt.put("source", String.class, next);
            						evt.put("next", String.class, "null");
            						evt.put("distance", double.class, newDistance);
            						
            						int length = newRoute.split(":").length;
            						evt.put("pathLength", Integer.class, length);
            						statStream.put(evt);*/
        							
        							Event result = new Event();
        							result.put("queryId", String.class, queryId);
        							result.put("control", String.class, null);
        							result.put("distance", Double.class, newDistance);
        							controlStream.put(result);
        							
        							return;
        							
        						}
        					}
        				}
        				else{           //taget is internal vertex, should search
        					//int local = LocalShortPath(source, DistanceSet, RouteSet);
        					DistanceSet = LocalShortPathByFbncHeap(next, target, RouteSet);
        					double newDistance = distance + DistanceSet.get(target);
        					String newRoute = route +":"+ getRoute(RouteSet, next, target);
        					double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
        					if(currentTargetDistance > newDistance) { 
        						QDistance.put(queryId+" "+target, newDistance);
        						QRoute.put(queryId+" "+target, newRoute);
        						
        						Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);

        						resultStream.put(ev);
        						Event evstat = new Event();
        						evstat.put("route", Route.class, thisroute);
        						statResultStream.put(evstat);

    							
    							/*Event evt = new Event();
        						evt.put("partition", String.class, partition);
        						evt.put("queryId", String.class, queryId);
        						evt.put("send", Integer.class, 1);
        						evt.put("source", String.class, next);
        						evt.put("next", String.class, "null");
        						evt.put("distance", double.class, newDistance);
        						
        						int length = newRoute.split(":").length;
        						evt.put("pathLength", Integer.class, length);
        						statStream.put(evt);*/
        						
        						Event result = new Event();
        						result.put("queryId", String.class, queryId);
        						result.put("control", String.class, null);
        						result.put("distance", Double.class, newDistance);
        						controlStream.put(result);
        						
        						return;
        						
        					}
        					
        				}
        			}
        				
        			
        		}
        		else{// target is not here
        			HashMap<String, Event> message = new HashMap<String, Event>(); //this message combine the events to same partition
        			HashMap<String, Long> messageSize = new HashMap<String, Long>();
            		if(!Boundaries.containsKey(next)){   // the source is internal node and should find the boundary to other partition
            			
            			//int localPath = LocalShortPath(source, DistanceSet, RouteSet);
            			DistanceSet = LocalShortPathByFbncHeap(next, RouteSet);
            			for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {

            				String key = entry.getKey();
            				double localDistance = DistanceSet.get(key);
            				String localRoute = getRoute(RouteSet, next, key);
            				if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
            					double newDistance = distance + localDistance;
            					String newRoute = route + ":" + localRoute;
            					
            					double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
            					if(currentDistance <= newDistance ) continue;
            					else {
            						QDistance.put(queryId+" "+key, newDistance);
            						QRoute.put(queryId+" "+key, newRoute);
            						
            						for (String nextnode : entry.getValue().getEdgeNeighbors()) {
            							
                    			    	
            							String partitionId = nextnode.split("-")[0];
            							Event downEvent;
            							if(message.get(partitionId)==null){
            								downEvent =  new Event();
            								downEvent.put("partition", String.class, partitionId);
            								downEvent.put("queryId", String.class, queryId);
            							}
            							else{
            								downEvent = message.get(partitionId);
            							}
            							long messageByte =0;
                    			    	long initmemory = Runtime.getRuntime().freeMemory();
            							Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
    											+ entry.getValue().getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
        								Event ev = new Event();
        								ev.put("route", Route.class, thisroute);
            							downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
            							
            							long endmemory = Runtime.getRuntime().freeMemory();
                    			    	messageByte = messageByte + initmemory - endmemory ;
                    			    	
            							message.put(partitionId, downEvent);
            							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
            							messageSize.put(partitionId, lastSize + messageByte);
            						}
	
            					}
            				}
            			}
            			//send to other partitions
            			for(Map.Entry<String, Event> entryEvent : message.entrySet()){

        			    	Event e = entryEvent.getValue();
            				sendNum++;
            				downStream.put(e);
            				
            				Event ev = new Event();
				    		ev.put("partition", String.class, entryEvent.getKey());
				    		ev.put("queryId", String.class, queryId);
							ev.put("send", Integer.class, 1);
							ev.put("size", Long.class, Long.valueOf(messageSize.get(entryEvent.getKey())==null?0:messageSize.get(entryEvent.getKey())));
							ev.put("source", String.class, next);
							ev.put("next", String.class, entryEvent.getKey());
							statStream.put(ev);
							sendFlag = "true";
            			}
            			
            			
            		}
            		////boundary to boundary  shortcut and then to next partition
            		else{
            			BoundaryVertex boundary = Boundaries.get(next);
            			double currentSourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //input boundary node distance
            			double limiteDistance = targetDistance.get(queryId)==null?Double.MAX_VALUE: targetDistance.get(queryId);
            			
            			if(currentSourceDistance > limiteDistance && currentSourceDistance != Double.MAX_VALUE) return;
            			
            			if(currentSourceDistance > distance && (boundary.getShortcut()!= null)) { // sometimes boundary just one and no shortcut
            				QDistance.put(queryId+" "+next, distance);
            				QRoute.put(queryId+" "+next, route);
            				

            				for(String nextboundary : boundary.getBoundaries()){ //send to local boundary then to other partition
            					
            					double localDistance = boundary.getShortcutDistance(nextboundary);
            					String localRoute = boundary.getShortcutRoute(nextboundary);
            					
            					if(localDistance != Double.MAX_VALUE){
            						double newDistance = distance + localDistance;
            						String newRoute = route + ":" + localRoute;
            						double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+nextboundary);
            						if(currentDistance <= newDistance) continue;
            						else {
            							QDistance.put(queryId+" "+nextboundary, newDistance);
            							QRoute.put(queryId+" "+nextboundary, newRoute);
            							
            							for (String nextnode : Boundaries.get(nextboundary).getEdgeNeighbors()){
            								
                        			    	
            								String partitionId = nextnode.split("-")[0];
                							Event downEvent;
                							if(message.get(partitionId)==null){
                								downEvent =  new Event();
                								downEvent.put("partition", String.class, partitionId);
                								downEvent.put("queryId", String.class, queryId);
                							}
                							else{
                								downEvent = message.get(partitionId);
                							}
                							long messageByte =0;
                        			    	long initmemory = Runtime.getRuntime().freeMemory();
                							Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
        											+ Boundaries.get(nextboundary).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
            								Event ev = new Event();
            								ev.put("route", Route.class, thisroute);
                							downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
                							
                							long endmemory = Runtime.getRuntime().freeMemory();
                        			    	messageByte = messageByte + initmemory - endmemory ;
                        			    	
                							message.put(partitionId, downEvent);
                							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
                							messageSize.put(partitionId, lastSize + messageByte);
            								
            							}
            							
            						}					
            					}
            		
            				}
            				
            				//if(next.equals(source)){ 
        					for(String nextboundary : boundary.getEdgeNeighbors()){  //directly send  to other partition through edge: when source is boundary
        						long messageByte =0;
            			    	long initmemory = Runtime.getRuntime().freeMemory();
            			    	
        						Route thisroute = new Route(queryId, starttime, source, nextboundary, target, distance + boundary.getEdgeDistance(nextboundary), route + ":" + nextboundary, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);
    							
								long endmemory = Runtime.getRuntime().freeMemory();
            			    	messageByte = messageByte + initmemory - endmemory ;
            			    	
        						String partitionId = nextboundary.split("-")[0];
        						Event downEvent;
        						if(message.get(partitionId)==null){
        							downEvent =  new Event();
        							downEvent.put("partition", String.class, partitionId);
            						downEvent.put("queryId", String.class, queryId);
        						}
        						else{
        							downEvent = message.get(partitionId);
        						}
        						downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
        						                			    	
    							message.put(partitionId, downEvent);
    							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
    							messageSize.put(partitionId, lastSize + messageByte);
        					}
            				//}
            				
            				//send to other partition
            				for(Map.Entry<String, Event> entryEvent : message.entrySet()){

            					Event e = entryEvent.getValue();
            					sendNum++;
            					downStream.put(e);	
            					
            					Event ev = new Event();
    				    		ev.put("partition", String.class, partition);
    				    		ev.put("queryId", String.class, queryId);
    							ev.put("send", Integer.class, 1);
    							ev.put("size", Long.class, Long.valueOf(messageSize.get(entryEvent.getKey())==null?0:messageSize.get(entryEvent.getKey())));
    							ev.put("source", String.class, next);
    							ev.put("next", String.class, entryEvent.getKey());
    							statStream.put(ev);
    							sendFlag = "true";
            				}
            			}
            		}
        		}
        		
            	if(sendFlag.equals("false")){
            		Event evt = new Event();
            		evt.put("partition", String.class, partition);
            		evt.put("queryId", String.class, queryId);
            		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
            		evt.put("size", Long.class, Long.valueOf(0));
            		evt.put("source", String.class, target);
            		evt.put("next", String.class, target);
        			statStream.put(evt);
            	}
        	}
    		

        }
    	

    	
    }
    
    
/*    
 *  e.put("route", Route.class, thisroute);    
    downEvent.put("partitionRoute", HashMap.class, partitionRoute);
	downEvent.put("queryId", String.class, queryId);
	downEvent.put("nextPartition", String.class, partitionId);
	downEvent.put(Long.toString(System.nanoTime()), Event.class, e);*/
    /**
     * get one good path by the guild of the partition route.
     * and broadcast the distance of this path to other partitions
     * This path can limit the space of the traversal 
     * 
     * There is only one message between partitions when finding the path
     * Therefore, we do not stat communications.
     * The statStream, stat does not have to send message.
     * When found one path, BoundaryMergeShortPathByLandmark is stopped.
     * 
     * @param events
     */
    public void BoundaryMergeShortPathByLandmarkPartition(Event events){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	String queryId ;
		String source ;
		String next ;
		long starttime;
		String target ;
		double distance;
		String route;
		String nextPartition = events.get("nextPartition", String.class);  //this partition
		HashMap<String, String> partitionRoute = (HashMap<String, String>)events.get("partitionRoute", HashMap.class);
		String realNextPartition = partitionRoute.get(nextPartition); //next partition
		
    	Map<String, String> eventsMap = events.getAttributesAsMap();
    	for(Map.Entry<String, String> entryEvents : eventsMap.entrySet()){
    		String keyEvent = entryEvents.getKey();
    		if(!keyEvent.equals("nextPartition")&&!keyEvent.equals("queryId")&&!keyEvent.equals("partitionRoute")){
    			Event event = events.get(keyEvent, Event.class);
    			
    			Route shortpath = event.get("route", Route.class);
    	    	
    	    	queryId = shortpath.getQueryId(); 
    			source = shortpath.getStartVertex();
    			starttime = shortpath.getStartTime();
    			next = shortpath.getNextVertex();
    			target = shortpath.getEndVertex();
    			distance = shortpath.getDistance();
    			route = shortpath.getRoute();
    			

        		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
        		HashMap<String, String> RouteSet = new HashMap<String, String>();
        		
        		if(vertices.containsKey(target)){ // target is here and find shortest path from source to target
        			double currentsourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //boundary distance

        			if(currentsourceDistance > distance){
        				if(Boundaries.containsKey(target) && Boundaries.containsKey(next)){     //if target is boundary vertex, we can use it directly
        					if(next.equals(target)){
    							//QDistance.put(queryId+" "+target, distance);
    							//QRoute.put(queryId+" "+target, route);

    							Route thisroute = new Route(queryId, starttime, source, next, target, distance, route, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);        						
        						resultStream.put(ev);
        						
        						Event landmarkEvent = new Event();
        						landmarkEvent.put("queryId", String.class, queryId);
        						landmarkEvent.put("partitionRoute", String.class, "partitionRoute");
        						landmarkEvent.put("route", Route.class, thisroute);
        						LandmarkStream.put(landmarkEvent);
        						
        						/*Event evstat = new Event();
        						evstat.put("route", Route.class, thisroute);
        						statResultStream.put(evstat);*/
        						
        						Event result = new Event();
        						result.put("queryId", String.class, queryId);
        						result.put("control", String.class, null);
        						result.put("distance", Double.class, distance);
        						controlStream.put(result);
        						
        						return;
        					}
        					else{ //different boundary vertices and use shortcut
        						double newDistance = distance + Boundaries.get(next).getShortcutDistance(target);
        						String newRoute = route +":"+ Boundaries.get(next).getShortcutRoute(target);
        						double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
        						if(currentTargetDistance <= newDistance) {
        							return;
        						}
        						if(currentTargetDistance > newDistance) { 
        							//QDistance.put(queryId+" "+target, newDistance);
        							//QRoute.put(queryId+" "+target, newRoute);

        							Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
    								Event ev = new Event();
    								ev.put("route", Route.class, thisroute);  
            						resultStream.put(ev);
            						
            						Event landmarkEvent = new Event();
            						landmarkEvent.put("queryId", String.class, queryId);
            						landmarkEvent.put("partitionRoute", String.class, "partitionRoute");
            						landmarkEvent.put("route", Route.class, thisroute);
            						LandmarkStream.put(landmarkEvent);
            						
            						/*Event evstat = new Event();
            						evstat.put("route", Route.class, thisroute);
            						statResultStream.put(evstat);*/
        							
        							Event result = new Event();
        							result.put("queryId", String.class, queryId);
        							result.put("control", String.class, null);
        							result.put("distance", Double.class, newDistance);
        							controlStream.put(result);
        							
        							return;
        						}
        					}
        				}
        				else{           //taget is internal vertex, should search
        					DistanceSet = LocalShortPathByFbncHeap(next, target, RouteSet);
        					double newDistance = distance + DistanceSet.get(target);
        					String newRoute = route +":"+ getRoute(RouteSet, next, target);
        					double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
        					if(currentTargetDistance <= newDistance){
        						return;
        					}
        					if(currentTargetDistance > newDistance) { 
        						//QDistance.put(queryId+" "+target, newDistance);
        						//QRoute.put(queryId+" "+target, newRoute);
        						
        						Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);
        						resultStream.put(ev);
        						
        						Event landmarkEvent = new Event();
        						landmarkEvent.put("queryId", String.class, queryId);
        						landmarkEvent.put("partitionRoute", String.class, "partitionRoute");
        						landmarkEvent.put("route", Route.class, thisroute);
        						LandmarkStream.put(landmarkEvent);
        						
        						/*Event evstat = new Event();
        						evstat.put("route", Route.class, thisroute);
        						statResultStream.put(evstat);*/
        						
        						Event result = new Event();
        						result.put("queryId", String.class, queryId);
        						result.put("control", String.class, null);
        						result.put("distance", Double.class, newDistance);
        						controlStream.put(result);
        						
        						return;
        						
        					}
        					
        				}
        			}        			
        		}
        		else{// target is not here
        			HashMap<String, Event> message = new HashMap<String, Event>(); //this message combine the events to same partition
        			HashMap<String, Long> messageSize = new HashMap<String, Long>();
            		if(!Boundaries.containsKey(next)){   // the source is internal node and should find the boundary to other partition
            			
            			//int localPath = LocalShortPath(source, DistanceSet, RouteSet);
            			DistanceSet = LocalShortPathByFbncHeap(next, RouteSet);
            			for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {

            				String key = entry.getKey();
            				double localDistance = DistanceSet.get(key);
            				String localRoute = getRoute(RouteSet, next, key);
            				if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
            					double newDistance = distance + localDistance;
            					String newRoute = route + ":" + localRoute;
            					
            					double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
            					if(currentDistance <= newDistance ) continue;
            					else {
            						//QDistance.put(queryId+" "+key, newDistance);
            						//QRoute.put(queryId+" "+key, newRoute);
            						
            						for (String nextnode : entry.getValue().getEdgeNeighbors()) {
            							
            							String partitionId = nextnode.split("-")[0];
            							Event downEvent;
            							if(message.get(partitionId)==null){
            								downEvent =  new Event();
            								downEvent.put("nextPartition", String.class, partitionId);
            								downEvent.put("queryId", String.class, queryId);
            								downEvent.put("partitionRoute", HashMap.class, partitionRoute);
            							}
            							else{
            								downEvent = message.get(partitionId);
            							}
            							
            							long messageByte =0;
                    			    	long initmemory = Runtime.getRuntime().freeMemory();
                    			    	
            							Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
    											+ entry.getValue().getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
        								
            							Event ev = new Event();
        								ev.put("route", Route.class, thisroute);
            							downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
            							
            							long endmemory = Runtime.getRuntime().freeMemory();
                    			    	messageByte = messageByte + initmemory - endmemory ;
                    			    	
            							message.put(partitionId, downEvent);
            							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
            							messageSize.put(partitionId, lastSize + messageByte);
            						}
	
            					}
            				}
            			}
            			//send to next partitions

    			    	Event e = message.get(realNextPartition);
        				sendNum++;
        				downStream.put(e);
        				
        				/*Event ev = new Event();
			    		ev.put("partition", String.class, realNextPartition);
			    		ev.put("queryId", String.class, queryId);
						ev.put("send", Integer.class, 1);
						ev.put("size", Long.class, Long.valueOf(messageSize.get(realNextPartition)==null?0:messageSize.get(realNextPartition)));
						ev.put("source", String.class, next);
						ev.put("next", String.class, realNextPartition);
						statStream.put(ev);
						sendFlag = "true";*/
            			
						return;
            			
            		}
            		////boundary to boundary  shortcut and then to next partition
            		else{
            			BoundaryVertex boundary = Boundaries.get(next);
            			double currentSourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //input boundary node distance
            			double limiteDistance = targetDistance.get(queryId)==null?Double.MAX_VALUE: targetDistance.get(queryId);
            			if(currentSourceDistance > distance && currentSourceDistance <= limiteDistance && (boundary.getShortcut()!= null)) { // sometimes boundary just one and no shortcut
            				//QDistance.put(queryId+" "+next, distance);
            				//QRoute.put(queryId+" "+next, route);
            				
            				for(String nextboundary : boundary.getBoundaries()){ //send to local boundary then to other partition
            					
            					double localDistance = boundary.getShortcutDistance(nextboundary);
            					            					
            					String localRoute = boundary.getShortcutRoute(nextboundary);
            					
            					if(localDistance != Double.MAX_VALUE){
            						double newDistance = distance + localDistance;
            						String newRoute = route + ":" + localRoute;
            						double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+nextboundary);
            						if(currentDistance <= newDistance) continue;
            						else {
            							//QDistance.put(queryId+" "+nextboundary, newDistance);
            							//QRoute.put(queryId+" "+nextboundary, newRoute);
            							
            							for (String nextnode : Boundaries.get(nextboundary).getEdgeNeighbors()){
            								
            								String partitionId = nextnode.split("-")[0];
                							Event downEvent;
                							if(message.get(partitionId)==null){
                								downEvent =  new Event();
                								downEvent.put("nextPartition", String.class, partitionId);
                								downEvent.put("queryId", String.class, queryId);
                								downEvent.put("partitionRoute", HashMap.class, partitionRoute);
                							}
                							else{
                								downEvent = message.get(partitionId);
                							}
                							long messageByte =0;
                        			    	long initmemory = Runtime.getRuntime().freeMemory();
                							Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
        											+ Boundaries.get(nextboundary).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
            								Event ev = new Event();
            								ev.put("route", Route.class, thisroute);
                							downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
                							
                							long endmemory = Runtime.getRuntime().freeMemory();
                        			    	messageByte = messageByte + initmemory - endmemory ;
                        			    	
                							message.put(partitionId, downEvent);
                							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
                							messageSize.put(partitionId, lastSize + messageByte);
            							}
            						}					
            					}
            				}
            				
            				//if(next.equals(source)){ //the source is a boundary node, and we can send message to other through this vertex
        					for(String nextboundary : boundary.getEdgeNeighbors()){  //directly send  to other partition through edge: when source is boundary
        						long messageByte =0;
            			    	long initmemory = Runtime.getRuntime().freeMemory();
            			    	
        						Route thisroute = new Route(queryId, starttime, source, nextboundary, target, distance + boundary.getEdgeDistance(nextboundary), route + ":" + nextboundary, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);
    							
								long endmemory = Runtime.getRuntime().freeMemory();
            			    	messageByte = messageByte + initmemory - endmemory ;
            			    	
        						String partitionId = nextboundary.split("-")[0];
        						Event downEvent;
        						if(message.get(partitionId)==null){
        							downEvent =  new Event();
    								downEvent.put("nextPartition", String.class, partitionId);
    								downEvent.put("queryId", String.class, queryId);
    								downEvent.put("partitionRoute", HashMap.class, partitionRoute);
        						}
        						else{
        							downEvent = message.get(partitionId);
        						}
        						downEvent.put(Long.toString(System.nanoTime()), Event.class, ev);
        						                			    	
    							message.put(partitionId, downEvent);
    							long lastSize = messageSize.get(partitionId)==null?0:messageSize.get(partitionId);
    							messageSize.put(partitionId, lastSize + messageByte);
        					}
            				//}
            				
                			//send to next partitions

        			    	Event e = message.get(realNextPartition);
        			    	System.out.println("next partition: " + realNextPartition);
            				sendNum++;
            				downStream.put(e);

                			
    						return;
            			}
            		}
        		}
        		

        	}
    		

        }
    	
    	

    	
    }
    
    
    
	/**
	 * use the method of pregel/graphlab to find the shortest path
	 * @param event
	 */
    public void graphFindShortestPathWithoutControl(Event event){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	Route shortpath = event.get("route", Route.class);

    	String queryId = shortpath.getQueryId();
		String source = shortpath.getStartVertex();
		String next = shortpath.getNextVertex();
		String target = shortpath.getEndVertex();
		double distance = shortpath.getDistance();
		long starttime = shortpath.getStartTime();
		String route = shortpath.getRoute();
		
		
		double currentDistance = QDistance.get(queryId+" "+next)==null?Double.POSITIVE_INFINITY:QDistance.get(queryId+" "+next);
		double limiteDistance = targetDistance.get(queryId)==null?Double.POSITIVE_INFINITY: targetDistance.get(queryId);
		
		
		if(currentDistance > distance && limiteDistance > distance){
			QDistance.put(queryId+" "+next, distance);
			QRoute.put(queryId+" "+next, route);
			
			if(next.equals(target)){
				
		    	Route thisroute = new Route(queryId, starttime, source, next, target, distance, route, System.currentTimeMillis()-timeInterval);
				Event ev = new Event();
				ev.put("route", Route.class, thisroute);
				resultStream.put(ev);
				Event evstat = new Event();
				evstat.put("route", Route.class, thisroute);
				statResultStream.put(evstat);

				/*Event result = new Event();
				result.put("queryId", String.class, queryId);
				result.put("control", String.class, null);
				result.put("distance", Double.class, distance);
				controlStream.put(result);*/
    			return;

    		}
			else{
				Vertex vertex = vertices.get(next);
				String listNext ="";
				long messageByte =0;
				
				for(String s : vertex.getNeighbors()){
					//QDistance.put(queryId+" "+s, distance + vertex.get(s));
					long initmemory = Runtime.getRuntime().freeMemory();
					Route thisroute = new Route(queryId, starttime, source, s, target, distance + vertex.get(s), route + ":" + s, System.currentTimeMillis()-timeInterval);

					Event e = new Event();
					e.put("route", Route.class, thisroute);
			    	downStream.put(e);
			    	long endmemory = Runtime.getRuntime().freeMemory();
			    	messageByte = messageByte + initmemory - endmemory ;
			    	//listNext = listNext + " " + s; 
				}
				
				Event ev = new Event();
	    		ev.put("partition", String.class, partition);
	    		ev.put("queryId", String.class, queryId);
				ev.put("send", Integer.class, vertex.getNeighbors().size());
				ev.put("size", Long.class, messageByte);
				ev.put("source", String.class, next);
				ev.put("next", String.class, listNext);
				statStream.put(ev);
				sendFlag = "true";
			}	
		}
		
		if(sendFlag.equals("false")){
    		Event evt = new Event();
    		evt.put("partition", String.class, partition);
    		evt.put("queryId", String.class, queryId);
    		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
    		evt.put("size", Long.class, Long.valueOf(0));
    		evt.put("source", String.class, target);
    		evt.put("next", String.class, target);
			statStream.put(evt);
    	}
    	

    }
    
    

	/**
	 * use the method of pregel/graphlab to find the shortest path
	 * @param event
	 */
    public void graphFindShortestPath(Event event){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	Route shortpath = event.get("route", Route.class);

    	String queryId = shortpath.getQueryId();
		String source = shortpath.getStartVertex();
		String next = shortpath.getNextVertex();
		String target = shortpath.getEndVertex();
		double distance = shortpath.getDistance();
		long starttime = shortpath.getStartTime();
		String route = shortpath.getRoute();
		
		
		double currentDistance = QDistance.get(queryId+" "+next)==null?Double.POSITIVE_INFINITY:QDistance.get(queryId+" "+next);
		double limiteDistance = targetDistance.get(queryId)==null?Double.POSITIVE_INFINITY: targetDistance.get(queryId);
		
		
		if(currentDistance > distance && limiteDistance > distance){
			QDistance.put(queryId+" "+next, distance);
			QRoute.put(queryId+" "+next, route);
			
			if(next.equals(target)){
				
		    	Route thisroute = new Route(queryId, starttime, source, next, target, distance, route, System.currentTimeMillis()-timeInterval);
				Event ev = new Event();
				ev.put("route", Route.class, thisroute);
				resultStream.put(ev);
				Event evstat = new Event();
				evstat.put("route", Route.class, thisroute);
				statResultStream.put(evstat);

				Event result = new Event();
				result.put("queryId", String.class, queryId);
				result.put("control", String.class, null);
				result.put("distance", Double.class, distance);
				controlStream.put(result);
    			return;

    		}
			else{
				Vertex vertex = vertices.get(next);
				String listNext ="";
				long messageByte =0;
				
				for(String s : vertex.getNeighbors()){
					//QDistance.put(queryId+" "+s, distance + vertex.get(s));
			    	long initmemory = Runtime.getRuntime().freeMemory();
					Route thisroute = new Route(queryId, starttime, source, s, target, distance + vertex.get(s), route + ":" + s, System.currentTimeMillis()-timeInterval);
					Event e = new Event();
					e.put("route", Route.class, thisroute);
			    	downStream.put(e);
			    	long endmemory = Runtime.getRuntime().freeMemory();
			    	messageByte = messageByte + initmemory - endmemory ;
			    	//listNext = listNext + " " + s; 
				}
				
				Event ev = new Event();
	    		ev.put("partition", String.class, partition);
	    		ev.put("queryId", String.class, queryId);
				ev.put("send", Integer.class, vertex.getNeighbors().size());
				ev.put("size", Long.class, messageByte);
				ev.put("source", String.class, next);
				ev.put("next", String.class, listNext);
				statStream.put(ev);
				sendFlag = "true";
			}	
		}
		
		if(sendFlag.equals("false")){
    		Event evt = new Event();
    		evt.put("partition", String.class, partition);
    		evt.put("queryId", String.class, queryId);
    		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
    		evt.put("size", Long.class, Long.valueOf(0));
    		evt.put("source", String.class, target);
    		evt.put("next", String.class, target);
			statStream.put(evt);
    	}
    	

    }
    
    
    /**
     * reverse traverse the graph from target t to source s;
     * This function is for the update query case 1;
     * @param event
     */
    public void reverseTraverse(Event event){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	Route shortpath = event.get("route", Route.class);
    	
    	String queryId = shortpath.getQueryId();    	
    	long starttime = shortpath.getStartTime();
		String source = shortpath.getStartVertex(); //t
		String next = shortpath.getNextVertex();
		String target = shortpath.getEndVertex(); //s
		double distance = shortpath.getDistance();
		String route = shortpath.getRoute();
		
		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
		HashMap<String, String> RouteSet = new HashMap<String, String>();
		
		//queryId + "-" +String.valueOf(System.currentTimeMillis() - timeInterval) + "-" + startVertex + "-" + endVertex + "-case1 "		
		String[] data = queryId.split("-");
		String realQueryId = data[0];
		String startEdge = data[2]+"-"+data[3];
		String endEdge = data[4]+"-"+data[5];
		String updateEdge = startEdge + ":" + endEdge;
		
		double limiteDistance = targetDistance.get(realQueryId)==null?Double.MAX_VALUE: targetDistance.get(realQueryId);
		Double qdistance = QDistance.get(realQueryId+" "+next);
		String qroute ;
		int reverseIndex = route.indexOf(reverseString(updateEdge));  //root t cannot pass this update edge
		if(qdistance == null){
			return;
		}
		else if (reverseIndex ==-1){ //not pass update edge
			qroute = QRoute.get(realQueryId+" "+next);
			int indexId = qroute.indexOf(updateEdge);
			if(indexId != -1){//pass this update edge and we should traversal the graph
				
				if(!Boundaries.containsKey(next)){ //the source is internal node
					try{
						DistanceSet = LocalShortPathByFbncHeap(next, RouteSet);
					}catch(Exception e){
						return;
					}
					for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
						String key = entry.getKey();
						double localDistance = DistanceSet.get(key);
						String localRoute = getRoute(RouteSet, next, key);
						if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
							double newDistance = distance + localDistance;
							String newRoute = route + ":" + localRoute;
							
							double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
							if(currentDistance <= newDistance ) continue;
							else {
								QDistance.put(queryId+" "+key, newDistance);
								QRoute.put(queryId+" "+key, newRoute);
								
								for (String nextnode : entry.getValue().getEdgeNeighbors()) {
									long messageByte =0;
							    	long initmemory = Runtime.getRuntime().freeMemory();
									Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
											+ entry.getValue().getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
									Event ev = new Event();
									ev.put("route", Route.class, thisroute);
									downStream.put(ev);
									
									long endmemory = Runtime.getRuntime().freeMemory();
							    	messageByte = messageByte + initmemory - endmemory ;
									
									Event evt = new Event();
	    				    		evt.put("partition", String.class, partition);
	    				    		evt.put("queryId", String.class, queryId);
	    				    		evt.put("send", Integer.class, 1);
	    				    		evt.put("size", Long.class, messageByte);
	    				    		evt.put("source", String.class, key);
	    				    		evt.put("next", String.class, nextnode);
	    				    		evt.put("distance", double.class, newDistance
											+ entry.getValue().getEdgeDistance(nextnode));
	    							statStream.put(evt);
	    							sendFlag = "true";
								}
							}					
						}
					}
				}
				else{//this is a  boundary node with distance and route from s
					
					BoundaryVertex boundary = Boundaries.get(next);
					double currentSourceDistance = QDistance.get(queryId+" "+next)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+next);  //input boundary node distance
					
					if(currentSourceDistance > distance && currentSourceDistance <= limiteDistance && (boundary.getShortcut()!= null)) { // sometimes boundary just one and no shortcut
						QDistance.put(queryId+" "+next, distance);
						QRoute.put(queryId+" "+next, route);
		
						for(String nextboundary : boundary.getBoundaries()){ //send to local boundary then to other partition
							
							double localDistance = boundary.getShortcutDistance(nextboundary);
							String localRoute = boundary.getShortcutRoute(nextboundary);
							
							if(localDistance != Double.MAX_VALUE){
								double newDistance = distance + localDistance;
								String newRoute = route + ":" + localRoute;
								double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+nextboundary);
								if(currentDistance <= newDistance) continue;
								else {
									QDistance.put(queryId+" "+nextboundary, newDistance);
									QRoute.put(queryId+" "+nextboundary, newRoute);
									
									for (String nextnode : Boundaries.get(nextboundary).getEdgeNeighbors()){
										long messageByte =0;
								    	long initmemory = Runtime.getRuntime().freeMemory();
										Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
												+ Boundaries.get(nextboundary).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
										Event ev = new Event();
										ev.put("route", Route.class, thisroute);									
										sendNum++;
										downStream.put(ev);
										long endmemory = Runtime.getRuntime().freeMemory();
								    	messageByte = messageByte + initmemory - endmemory ;
										Event evt = new Event();
		    				    		evt.put("partition", String.class, partition);
		    				    		evt.put("queryId", String.class, queryId);
		    				    		evt.put("send", Integer.class, 1);
		    				    		evt.put("size", Long.class, messageByte);
		    				    		evt.put("source", String.class, nextboundary);
		    				    		evt.put("next", String.class, nextnode);
		    				    		evt.put("distance", double.class, newDistance
												+ Boundaries.get(nextboundary).getEdgeDistance(nextnode));
		    							statStream.put(evt);
		    							sendFlag = "true";
									}
								}					
							}
				
						}
						
						if(next.equals(source)){
							for(String nextboundary : boundary.getEdgeNeighbors()){  //directly send  to other partition through edge: when source is boundary
								long messageByte =0;
						    	long initmemory = Runtime.getRuntime().freeMemory();
								Route thisroute = new Route(queryId, starttime, source, nextboundary, target, distance + boundary.getEdgeDistance(nextboundary), route + ":" + nextboundary, System.currentTimeMillis()-timeInterval);
								Event ev = new Event();
								ev.put("route", Route.class, thisroute);
								downStream.put(ev);
								long endmemory = Runtime.getRuntime().freeMemory();
						    	messageByte = messageByte + initmemory - endmemory ;
								Event evt = new Event();
    				    		evt.put("partition", String.class, partition);
    				    		evt.put("queryId", String.class, queryId);
    				    		evt.put("send", Integer.class, 1);
    				    		evt.put("size", Long.class, messageByte);
    				    		evt.put("source", String.class, source);
    				    		evt.put("next", String.class, nextboundary);
    				    		evt.put("distance", double.class, distance + boundary.getEdgeDistance(nextboundary));
    							statStream.put(evt);
    							sendFlag = "true";
							}
						}
					}
				}
			}
			else{//not pass this update edge
				double newDistance = qdistance + distance; //<s,next> + <next,t>
				if(newDistance < limiteDistance){//we get one path less than limited Distance, not pass <u,v>
					String newRoute =  qroute + reverseString(route);
					Route thisroute = new Route(queryId, starttime, source, next, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
					Event ev = new Event();
					ev.put("route", Route.class, thisroute);
					resultStream.put(ev);
					/*Event evstat = new Event();
					evstat.put("route", Route.class, thisroute);
					statResultStream.put(evstat);*/
					
					Event result = new Event();
					result.put("queryId", String.class, realQueryId);
					result.put("control", String.class, null);
					result.put("distance", Double.class, newDistance);
					controlStream.put(result);
				}
			}
		}
		
		if(sendFlag.equals("false")){
    		Event evt = new Event();
    		evt.put("partition", String.class, partition);
    		evt.put("queryId", String.class, queryId);
    		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
    		evt.put("size", Long.class,Long.valueOf(0));
    		evt.put("source", String.class, target);
    		evt.put("next", String.class, target);
			statStream.put(evt);
    	}
    	
		

    }
    
    
    /**
     * this partition is affected by update edge and 
     * we need to broadcast the affection to other partition
     * for Case 4
     * @param event
     */
    public void broadcastAffectdMessage(Event event){
    	String sendFlag = "false"; //the flag whether send message to other partition.
    	
    	String queryId = event.get("queryId",String.class);
    	long starttime = event.get("startTime",Long.class);
    	String updateEdge = event.get("updateEdge",String.class);
    	String reverseUpdateEdge = reverseString(updateEdge);
    	Double changed = event.get("changed",Double.class);
    	String source = event.get("source",String.class);
		String target = event.get("target",String.class);
		
		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
		HashMap<String, String> RouteSet = new HashMap<String, String>();
		
    	double limiteDistance = targetDistance.get(queryId)==null?Double.MAX_VALUE: targetDistance.get(queryId);
    	//System.out.println(event.toString());
		
    	if(vertices.containsKey(source)){// the source is here, we cannot use the shortcut 
    		if(vertices.containsKey(target)){ // source and target are all here
    			DistanceSet = LocalShortPathByFbncHeap(source, target, RouteSet);
    			double newDistance = DistanceSet.get(target);
				String newRoute = getRoute(RouteSet, source, target);

				double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
				if(currentTargetDistance > newDistance) { 
					QDistance.put(queryId+" "+target, newDistance);
					QRoute.put(queryId+" "+target, newRoute);
					
					Route thisroute = new Route(queryId, starttime, source, target, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
					Event ev = new Event();
					ev.put("route", Route.class, thisroute);
					resultStream.put(ev);
					
					Event result = new Event();
					result.put("queryId", String.class, queryId);
					result.put("control", String.class, null);
					result.put("distance", Double.class, newDistance);
					controlStream.put(result);
					//return;
				}
			}else{ //soruce is here and target is not here
    			DistanceSet = LocalShortPathByFbncHeap(source, RouteSet);
    			for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
    				String key = entry.getKey();
    				double localDistance = DistanceSet.get(key);
    				String localRoute = getRoute(RouteSet, source, key);
    				if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
    					double newDistance = localDistance;
    					String newRoute = localRoute;
    					double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
    					if(currentDistance <= newDistance ) continue;
    					else {
    						QDistance.put(queryId+" "+key, newDistance);
    						QRoute.put(queryId+" "+key, newRoute);
    						for (String nextnode : entry.getValue().getEdgeNeighbors()) {
    							long messageByte =0;
    					    	long initmemory = Runtime.getRuntime().freeMemory();
    					    	Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
    									+ Boundaries.get(key).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
    							Event ev = new Event();
    							ev.put("route", Route.class, thisroute);									
    							downStream.put(ev);
    							
    							long endmemory = Runtime.getRuntime().freeMemory();
    					    	messageByte = messageByte + initmemory - endmemory ;

    							Event evt = new Event();
    				    		evt.put("partition", String.class, partition);
    				    		evt.put("queryId", String.class, queryId);
    				    		evt.put("send", Integer.class, 1);
    				    		evt.put("size", Long.class, messageByte);
    				    		evt.put("source", String.class, key);
    				    		evt.put("next", String.class, nextnode);
    				    		evt.put("distance", double.class, newDistance
    									+ Boundaries.get(key).getEdgeDistance(nextnode));
    							statStream.put(evt);
    							sendFlag = "true";
    						}
    						
    					}
    				}
    			}
    		}
    	}
    	else if (vertices.containsKey(target)){ //target is on the partition of update edge and source it not here
    		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
    			String key = entry.getKey();
    			DistanceSet = LocalShortPathByFbncHeap(key, target, RouteSet);
    			double currentDistance = QDistance.get(queryId+" "+key)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+key);
    			if(currentDistance == Double.MAX_VALUE ) continue;
    			else{
    				double newDistance = currentDistance + DistanceSet.get(target);
    				String newRoute = QRoute.get(queryId+" "+key) + getRoute(RouteSet, key, target);
    				double limitDistance = QDistance.get(queryId+" "+target)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+target);  //target distance
    				if(limitDistance > newDistance) { 
    					QDistance.put(queryId+" "+target, newDistance);
    					QRoute.put(queryId+" "+target, newRoute);
    					
    					Route thisroute = new Route(queryId, starttime, source, target, target, newDistance, newRoute, System.currentTimeMillis()-timeInterval);
    					Event ev = new Event();
    					ev.put("route", Route.class, thisroute);
    					resultStream.put(ev);
    					
    					Event result = new Event();
    					result.put("queryId", String.class, queryId);
    					result.put("control", String.class, null);
    					result.put("distance", Double.class, newDistance);
    					controlStream.put(result);
    					//return;
    				}
    			}
    		}
    	}
    	else{// this partition is normal partition, not contain source and target
    		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
    			String next = entry.getKey();
    			BoundaryVertex boundary = entry.getValue();
    			Double qDistance = QDistance.get(queryId+" "+next);
    			String route = QRoute.get(queryId+" "+next);
    			if(route == null) continue;  //this query cannot come here
    			else{
    				//String route = QRoute.get(queryId+" "+next);
    				String[] routs = route.split(":");
    				String lastVertex = routs[routs.length-1];
    				if(vertices.containsKey(lastVertex)){//this route is from this partition, we send to other partition
    					int Index = route.indexOf(updateEdge);
    					int reverseIndex = route.indexOf(reverseUpdateEdge); 
    					if (reverseIndex == -1 && Index == -1){ //not pass update edge from other boundary node
    						//do nothing? , it will be done by other boundary node
    					}
    					else{ // pass this update edge
    						double newDistance = qDistance + changed;
    						QDistance.put(queryId+" "+next, newDistance);//
    						if(newDistance < limiteDistance){
    							for (String nextnode : boundary.getEdgeNeighbors()){
    								long messageByte =0;
    						    	long initmemory = Runtime.getRuntime().freeMemory();
    						    	Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
    										+ boundary.getEdgeDistance(nextnode), route + ":" + nextnode, System.currentTimeMillis()-timeInterval);
    								Event ev = new Event();
    								ev.put("route", Route.class, thisroute);									
    								downStream.put(ev);
    								
    								long endmemory = Runtime.getRuntime().freeMemory();
    						    	messageByte = messageByte + initmemory - endmemory ;

    								Event evt = new Event();
        				    		evt.put("partition", String.class, partition);
        				    		evt.put("queryId", String.class, queryId);
        				    		evt.put("send", Integer.class, 1);
        				    		evt.put("size", Long.class, messageByte);
        				    		evt.put("source", String.class, next);
        				    		evt.put("next", String.class, nextnode);
        				    		evt.put("distance", double.class, newDistance
    										+ boundary.getEdgeDistance(nextnode));
        							statStream.put(evt);
        							sendFlag = "true";
    							}
    						}
    					}
    				}
    				else{//from other partition, we need to send to other boundary nodes
    					
    					for(String nextboundary : boundary.getBoundaries()){ 
    						double localDistance = boundary.getShortcutDistance(nextboundary);
    						String localRoute = boundary.getShortcutRoute(nextboundary);
    						if(localDistance != Double.MAX_VALUE){
    							double newDistance = qDistance + localDistance;
    							String newRoute = route + ":" + localRoute;
    							double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.MAX_VALUE:QDistance.get(queryId+" "+nextboundary);
    							if(currentDistance <= newDistance || newDistance >= limiteDistance) continue;
    							else {
    								QDistance.put(queryId+" "+nextboundary, newDistance);
    								QRoute.put(queryId+" "+nextboundary, newRoute);
    								
    								for (String nextnode : Boundaries.get(nextboundary).getEdgeNeighbors()){
    									long messageByte =0;
    							    	long initmemory = Runtime.getRuntime().freeMemory();
    							    	Route thisroute = new Route(queryId, starttime, source, nextnode, target, newDistance
    											+ Boundaries.get(nextboundary).getEdgeDistance(nextnode), newRoute + ":" + nextnode, System.currentTimeMillis()-timeInterval);
    									Event ev = new Event();
    									ev.put("route", Route.class, thisroute);									
    									downStream.put(ev);	
    									long endmemory = Runtime.getRuntime().freeMemory();
    							    	messageByte = messageByte + initmemory - endmemory ;
    									Event evt = new Event();
            				    		evt.put("partition", String.class, partition);
            				    		evt.put("queryId", String.class, queryId);
            				    		evt.put("send", Integer.class, 1);
            				    		evt.put("size", Long.class, messageByte);
            				    		evt.put("source", String.class, next);
            				    		evt.put("next", String.class, nextnode);
            				    		evt.put("distance", double.class, newDistance
        										+ boundary.getEdgeDistance(nextnode));
            							statStream.put(evt);
            							sendFlag = "true";
    								}
    							}					
    						}
    					}
    					
    				}			
    			}			
        	}
    	}
    	
    	if(sendFlag.equals("false")){
    		Event evt = new Event();
    		evt.put("partition", String.class, partition);
    		evt.put("queryId", String.class, queryId);
    		evt.put("send", Integer.class, 0); //this is special for detection receive = send;
    		evt.put("size", Long.class,Long.valueOf(0));
    		evt.put("source", String.class, target);
    		evt.put("next", String.class, target);
			statStream.put(evt);
    	}
    	
    	
    	
    	
    }
    
    protected void onTime(){
		try {

	    	if(ontimeFlag == true){
		    	maintainBoundary();
		    	
		    	Random time = new Random();
		    	int timeout = time.nextInt(30);
		    	//this.wait(timeout);
		    	Thread.sleep(timeout);
		    	
		    	for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
		    		String key = entry.getKey();
					BoundaryVertex boundary = Boundaries.get(key);
					if (Constant.V_LandMark.equals(Constant.LandMark_Partition)
							|| Constant.V_LandMark
									.equals(Constant.LandMark_Sample)) {
						Event ev = new Event();
						ev.put("key", String.class, key);
						ev.put("boundary", BoundaryVertex.class, boundary);
						LandmarkStream.put(ev);
					}
		    	}
		    	/*
		    	Event ev = new Event();
		    	ev.put("key", String.class, "");
				ev.put("boundary", BoundaryVertex.class, new BoundaryVertex());
				ev.put("sendOver", Boolean.class, true);
		    	LandmarkStream.put(ev);*/
	            
		    	ontimeFlag = false;

	    	}else{
	    		//System.out.println("No maintainBoundary!	Partition:" + partition + "	Boundaries:" + Boundaries.size() + "	vertices:" + vertices.size());
	    	}
	    		
	

	    	
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    
    /**
     * maintain the shortcut between all the boundary nodes: LocalShortPathByFbncHeap algorithm
     * the method is for all boundary, try to find every single boundary node to other nodes
     * time complexity: N^2 * (E+V*logV) : N is boundary node number, E is edge number, V is vertex number
     */
    
    
    public void maintainBoundary(){
    	
    	Long startTime = System.currentTimeMillis();
    	int shortcutCount = 0;
    	
		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = Boundaries.get(key);
			HashMap<String, Double> Distance = new HashMap<String, Double>();
			HashMap<String, String> Route = new HashMap<String, String>();
			//int localPath = LocalShortPath(entry.getKey(), Distance, Route);
			Distance = LocalShortPathByFbncHeap(entry.getKey(), Route);
			for (Map.Entry<String, BoundaryVertex> otherentry : Boundaries
					.entrySet()) {
				if (!entry.getKey().equals(otherentry.getKey())) {
					boundary.putShortcutDistance(otherentry.getKey(), Distance.get(otherentry.getKey()));
					boundary.putShortcutRoute(otherentry.getKey(), getRoute(Route, entry.getKey(), otherentry.getKey()));
					shortcutCount++;
				}
			}
			Boundaries.put(key, boundary);
			
		
		}
		
		Long endTime = System.currentTimeMillis();
		Long timecost = endTime - startTime;
		
		Event ev = new Event();
		ev.put("PartitionId", String.class, partition);
		ev.put("timeCost", Long.class, timecost );
		ev.put("shortcutCount", Integer.class, shortcutCount);
		statResultStream.put(ev);
		
		
		
     
    }
      
    
    public void ShortcutUpdate(String startVertex, String endVertex, double changed){
    	Long startTime = System.currentTimeMillis();
		int shortcutCount = 0;
		String edge = startVertex + ":" + endVertex;
		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = Boundaries.get(key);
			
			for (Map.Entry<String, BoundaryVertex> otherentry : Boundaries
					.entrySet()) {
				if (!entry.getKey().equals(otherentry.getKey())) {
					shortcutCount++;
					String nextBoundary = otherentry.getKey();
					double distance = boundary.getShortcutDistance(nextBoundary);
					String route = boundary.getShortcutRoute(nextBoundary);
					int indexId = route.indexOf(edge);
					if(indexId != -1){//this route pass this changed edge
		    			if(changed > 0){ //recomputing
		    				HashMap<String, Double> Distance = new HashMap<String, Double>();
		    				HashMap<String, String> Route = new HashMap<String, String>();
		    				Distance = LocalShortPathByFbncHeap(key, nextBoundary, Route);
		    				boundary.putShortcutDistance(nextBoundary, Distance.get(nextBoundary));
							boundary.putShortcutRoute(nextBoundary, getRoute(Route, key, nextBoundary));
		    			}
		    			else {//reduce the distance
		    				boundary.putShortcutDistance(nextBoundary, distance + changed);		        			
		        		}
					}
		    		else{//not pass
		        			if(changed > 0){//do nothing 
		        				
		        			}
		        			else{
		        				HashMap<String, Double> Distance = new HashMap<String, Double>();
			    				HashMap<String, String> Route = new HashMap<String, String>();
			    				Distance = LocalShortPathByFbncHeap(key, nextBoundary, Route);
			    				boundary.putShortcutDistance(nextBoundary, Distance.get(nextBoundary));
								boundary.putShortcutRoute(nextBoundary, getRoute(Route, key, nextBoundary));
		        			}
		    		}
					
				}
			}
			Boundaries.put(key, boundary);

		}
		
		Long endTime = System.currentTimeMillis();
		Long timecost = endTime - startTime;
		Event ev = new Event();
		ev.put("PartitionId", String.class, partition);
		ev.put("timeCost", Long.class, timecost );
		ev.put("shortcutCount", Integer.class, shortcutCount);
		statResultStream.put(ev);

    }
   
    
    
/**
 * Using Floyd algorithm to get the shortcut between boundary vertices
 */
    public void maintainBoundaryUsingFloyd(){
    	Long startTime = System.currentTimeMillis();
    	int shortcutCount = 0;
    	int i, j, k;
    	int n = vertices.size();
    	double[][] dist = new double[n][n];
    	String[][] path = new String[n][n];
    	for(i=1; i<=n; i++)
    		for(j=1;j<=n;j++){
    			if(vertices.get(i)==null || vertices.get(i).getEdge(Integer.toString(j))==null){
    				dist[i][j] = Double.MAX_VALUE;
    			}
    			else{
    				dist[i][j] = vertices.get(i).get(Integer.toString(j));
    			}
    			path[i][j]="";
    		}
    	
    	for(k=1; k<=n; k++)
    		for(i=1; i<=n; i++)
    			for(j=1; j<=n; j++){
    				if(dist[i][k]+dist[k][j]<dist[i][j]){
    					dist[i][j] = dist[i][k] + dist[k][j];
    					path[i][j] = Integer.toString(k);
    				}
    			}
    	
    	
		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = Boundaries.get(key);
			HashMap<String, Double> Distance = new HashMap<String, Double>();
			HashMap<String, String> Route = new HashMap<String, String>();
			//int localPath = LocalShortPath(entry.getKey(), Distance, Route);
			Distance = LocalShortPathByFbncHeap(entry.getKey(), Route);
			for (Map.Entry<String, BoundaryVertex> otherentry : Boundaries
					.entrySet()) {
				if (!entry.getKey().equals(otherentry.getKey())) {
					boundary.putShortcutDistance(otherentry.getKey(), Distance.get(otherentry.getKey()));
					boundary.putShortcutRoute(otherentry.getKey(), getRoute(Route, entry.getKey(), otherentry.getKey()));
					shortcutCount++;
				}
			}
			Boundaries.put(key, boundary);

		}
		
		
		Long endTime = System.currentTimeMillis();
		Long timecost = endTime - startTime;
		
		Event ev = new Event();
		ev.put("PartitionId", String.class, partition);
		ev.put("timeCost", Long.class, timecost );
		ev.put("shortcutCount", Integer.class, shortcutCount);
		statResultStream.put(ev);
     
    }
    
    
    
    public void output(int i, int j){
    	int n=0;;
    	int[][] path = new int[n][n];
    	
    	if(i==j) return ;
    	if(path[i][j]==0)  System.out.println(j);
    	else{
    		output(i, path[i][j]);
    		output(path[i][j], j);
    	}
    }
    
    /**
     * Find the shortest path from boundary vertex: Dijkstra algorithm
     * @return
     */
    
    
    public int LocalShortPath(String vSource,HashMap<String,Double> D,HashMap<String,String> Route){
    	int number = vertices.size();
    	ArrayList<String> S = new ArrayList<String>();  //traversal vertices
    	ArrayList<String> Q = new ArrayList<String>();  //not traversal vertices
    	
    	//HashMap<String,Double> D = new HashMap<String,Double>();  //the distance from source
    	//HashMap<String,String> Route = new HashMap<String,String>();  //the route from source

    	
    	for (Map.Entry<String, Vertex> entry : vertices.entrySet()) {
			if (!entry.getKey().equals(vSource)) {
				D.put(entry.getKey(), Double.MAX_VALUE);
				Route.put(entry.getKey(), "");
			} else {
				D.put(vSource, Double.valueOf(0));
				Route.put(entry.getKey(), "");
			}
			Q.add(entry.getKey());
    	}
    	while(Q.size()>0){
    		int h = Q.size();
    		double minV = Double.MAX_VALUE;
    		int index = 0;
    		String vnode ="";
    		
    		for(int i=0; i<h;i++){   // extract min ditance
    			String tmpvertex = Q.get(i);
    			double tmpdistance = D.get(tmpvertex);
    			if(minV > tmpdistance){
    				index = i;
    				vnode = tmpvertex;
    				minV = tmpdistance;
    			}
    		}
    		if(minV == Double.MAX_VALUE ) return 1; //cannot find the least neighbor value
    		Q.remove(vnode);
    		S.add(vnode);

    		Vertex startnode = vertices.get(vnode);
    		for(String s : startnode.getNeighbors()){
				if(vertices.containsKey(s)){
	    			double currentDistance = D.get(s);
					double newDistance = D.get(vnode) + startnode.get(s);
					if(currentDistance > newDistance){
						D.put(s, newDistance);
						Route.put(s, Route.get(vnode)==""? s : Route.get(vnode) + "-"+ s);
					}
				}

			}
    		
    	}
    	
    	return 1;
    	
    }
    
 
    
    /**
     * Find the shortest path from boundary vertex using FibonacciHeap
     * @return
     */
    
    public HashMap<String, Double> LocalShortPathByFbncHeap(String vSource, HashMap<String, String> Route){
    	int number = vertices.size();

    	FibonacciHeap<String> pq = new FibonacciHeap<String>(); //the distances of unvisited nodes
    	HashMap<String, FibonacciHeap.Entry<String>> entries = new HashMap<String, FibonacciHeap.Entry<String>>();
    	HashMap<String, Double> result = new HashMap<String, Double>();

        for (Map.Entry<String, Vertex> node : vertices.entrySet()){
            entries.put(node.getKey(), pq.enqueue(node.getKey(), Double.POSITIVE_INFINITY));
            Route.put(node.getKey(), "");

        }
        try{
        	pq.decreaseKey(entries.get(vSource), 0.0);
        }catch(Exception e){
        	e.printStackTrace();
        	Boolean containSource = vertices.containsKey(vSource);
        	System.out.println( " LocalShortPathByFbncHeap Error: " + "	vSource: " + vSource  + "	partition:" + partition);
        }
        
    	
        /* Keep processing the queue until no nodes remain. */
        while (!pq.isEmpty()) {
            /* Grab the current node.  The algorithm guarantees that we now
             * have the shortest distance to it.
             */
            FibonacciHeap.Entry<String> curr = pq.dequeueMin();

            /* Store this in the result table. */
            result.put(curr.getValue(), curr.getPriority());

            Vertex startnode = vertices.get(curr.getValue());
            /* Update the priorities of all of its edges. */
            for (String arc : startnode.getNeighbors()) {
                /* If we already know the shortest path from the source to
                 * this node, don't add the edge.
                 */
                if (result.containsKey(arc)||!vertices.containsKey(arc)) continue;

                /* Compute the cost of the path from the source to this node,
                 * which is the cost of this node plus the cost of this edge.
                 */
                double pathCost = curr.getPriority() + startnode.get(arc);

                /* If the length of the best-known path from the source to
                 * this node is longer than this potential path cost, update
                 * the cost of the shortest path.
                 */
                FibonacciHeap.Entry<String> dest = entries.get(arc);
                if (pathCost < dest.getPriority()){
                    pq.decreaseKey(dest, pathCost);
                    Route.put(arc, curr.getValue());
                }
                
            }
        }
        
        return result;
    	
    } 
    
    
    
    
    /**
     * Find the shortest path from boundary vertex using FibonacciHeap
     * @return
     */
    
    public HashMap<String, Double> LocalShortPathByFbncHeap(String vSource, String vTarget, HashMap<String, String> Route){
    	int number = vertices.size();

    	FibonacciHeap<String> pq = new FibonacciHeap<String>(); //the distances of unvisited nodes
    	HashMap<String, FibonacciHeap.Entry<String>> entries = new HashMap<String, FibonacciHeap.Entry<String>>();
    	HashMap<String, Double> result = new HashMap<String, Double>();

        for (Map.Entry<String, Vertex> node : vertices.entrySet()){
            entries.put(node.getKey(), pq.enqueue(node.getKey(), Double.POSITIVE_INFINITY));
            Route.put(node.getKey(), "");

        }

        pq.decreaseKey(entries.get(vSource), 0.0);
    	
        /* Keep processing the queue until no nodes remain. */
        while (!pq.isEmpty()) {
            /* Grab the current node.  The algorithm guarantees that we now
             * have the shortest distance to it.
             */
            FibonacciHeap.Entry<String> curr = pq.dequeueMin();

            /* Store this in the result table. */
            result.put(curr.getValue(), curr.getPriority());
            if(curr.getValue().equals(vTarget)){
            	
            	return result;
            }

            
            Vertex startnode = vertices.get(curr.getValue());
            /* Update the priorities of all of its edges. */
            for (String arc : startnode.getNeighbors()) {
                /* If we already know the shortest path from the source to
                 * this node, don't add the edge.
                 */
                if (result.containsKey(arc)||!vertices.containsKey(arc)) continue;

                /* Compute the cost of the path from the source to this node,
                 * which is the cost of this node plus the cost of this edge.
                 */
                double pathCost = curr.getPriority() + startnode.get(arc);

                /* If the length of the best-known path from the source to
                 * this node is longer than this potential path cost, update
                 * the cost of the shortest path.
                 */
                FibonacciHeap.Entry<String> dest = entries.get(arc);
                if (pathCost < dest.getPriority()){
                    pq.decreaseKey(dest, pathCost);
                    Route.put(arc, curr.getValue());  ////<target, start>   /// arc node only has one father
                }
                
            }
        }
        
        return result;
    	
    } 
    
    /*
     * Route<vertex, previous vertex>
     */
    public String getRoute(Map<String, String> Route, String startVertex, String endVertex){
    	String position = endVertex;
		int l = 0;
		String reverseroute = position;
	
		while(!position.equals(startVertex)){
			position = Route.get(position);
			if(position.equals("")) break ;
			reverseroute = reverseroute + ":" + position;
			l++;
		}
		String[] splitroute = reverseroute.split(":");
		String route = splitroute[splitroute.length-1];
		for(int j=splitroute.length-2; j>=0; j--){
			route = route + ":" + splitroute[j];
		}
		return route;
    }
    
    
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    
    public void setResultStream(Streamable<Event> stream){
    	this.resultStream = stream;
    	
    }
    public void setQueryStatStream(Streamable<Event> stream){
    	this.statResultStream = stream;
    	
    }
    
    public void setStatStream(Streamable<Event> stream){
    	this.statStream = stream;
    	
    }
    
    public void setControStream(Streamable<Event> stream){
    	this.controlStream = stream;
    	
    }
    
    public void setBoundaryStream(Streamable<Event> stream){
    	this.LandmarkStream = stream;
    	
    }
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    
    public void setParameters(String path, int number, String method, String log, String updateMethod){
    	this.outputPath = path;
    	this.partitionNumber = number;
    	this.searchMethod = method;
    	this.isLog = log;
    	this.updateMethod = updateMethod;
    }
    @Override
    protected void onCreate() {
    	vertices = new HashMap<String,Vertex>();
    	//QDistance = new HashMap<String,Double>();
    	//QRoute = new HashMap<String,String>();
    	Boundaries = new HashMap<String, BoundaryVertex>();
    	targetDistance  = new HashMap<String,Double>();
    	partition = getId();//getId().substring(0, getId().length()-4)

    }

    @Override
    protected void onRemove() {
    }
    
    /*
     * transfer 2-100:2-50  
     * to 2-50:2-100
     */
    public String reverseString(String original){
    	String reverseString = "";
    	String[] data = original.split(":");
    	reverseString = data[data.length-1];
    	for (int i = data.length-2; i >= 0; i--){
    		reverseString = reverseString +":" + data[i];
    	}
    	return reverseString;
    	
    }
    
    public byte[] ObjectToByte(java.lang.Object obj){
    	byte[] bytes = new byte[1024*10];
    	try{
    		ByteArrayOutputStream bo = new ByteArrayOutputStream();
    		ObjectOutputStream oo = new ObjectOutputStream(bo);
    		oo.writeObject(obj);
    		bytes = bo.toByteArray();
    		bo.close();
    		oo.close();
    	} catch(Exception e){
    		System.out.println("ObjectToByte :" +e.getMessage());
    	}
    	return bytes;
    }
  
    
    
    
}
