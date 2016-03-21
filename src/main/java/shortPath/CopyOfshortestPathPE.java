package shortPath;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyOfshortestPathPE extends ProcessingElement{

    Streamable<Event> downStream;   //route stream
    Streamable<Event> resultStream;   //the stream contains the target distance
    Streamable<Event> controlStream;  //control the distance when get a new target distance
    Streamable<Event> statStream;   //stat the communication of route
    static Logger logger = LoggerFactory.getLogger(CopyOfshortestPathPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;
    HashMap<String,Vertex> vertices ;  //map information: vertex and edge
    
    HashMap<String,Double> QDistance ;  // <QueryID+source, distance> : the shortest distance from the source vertex

    HashMap<String, BoundaryVertex> Boundaries;
    
    HashMap<String,Double> targetDistance;
    
    int sendNum = 0;
    int receiveNum = 0;
    
    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
    	
    	//////map edge update
    	if(event.containsKey("edge")){   
        	String[] data = event.get("edge").split(" ");
        	String key = data[1];
        	String FirstPartition = key.split("-")[0];
        	String neighbor = data[2];
        	String NeighborPartition = neighbor.split("-")[0];
        	double distance = Double.parseDouble(data[3]);
        	
        	Vertex vertex = vertices.get(key)==null?new Vertex(key):vertices.get(key);
        	
        	vertex.put(neighbor, distance); 
        	vertices.put(key, vertex);

        	if(!FirstPartition.equals(NeighborPartition)){
        		BoundaryVertex  boundary = Boundaries.get(key) == null? new BoundaryVertex(key) : Boundaries.get(key);
        		boundary.putEdge(neighbor, Double.valueOf(distance));
                Boundaries.put(key, boundary);
        	}
        	
    	}
    	else if (event.containsKey("control")){
    		String queryId = event.get("queryId", String.class);
    		double distance = event.get("distance",Double.class);
    		double curr = targetDistance.get(queryId)==null? Double.MAX_VALUE:targetDistance.get(queryId);
    		if(curr>distance){
    			targetDistance.put(queryId, distance);
    		}
    		
    	}
    	
    	
    	///// route map and find shortest path
    	else { 
    		Event ev = new Event();
    		ev.put("partition", String.class, getId());
			ev.put("receive", Integer.class, 1);
			statStream.put(ev);
    		
			receiveNum++;
			
			//graphFindShortestPath(event);  //baseline
			boundaryFindShortPath(event);

    	}

    	
    }
    
    public void boundaryFindShortPath(Event event){
    	String queryId = event.get("queryId", String.class);
		String source = event.get("source", String.class);
		String target = event.get("target", String.class);
		double distance = event.get("distance", double.class);
		String route = event.get("route", String.class);
		
		HashMap<String, Double> DistanceSet = new HashMap<String, Double>();
		HashMap<String, String> RouteSet = new HashMap<String, String>();
		
		if(vertices.containsKey(target)){ // target is here and find shortest path from source to target
			double currentsourceDistance = QDistance.get(queryId+" "+source)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+source);  //boundary distance

			if(currentsourceDistance > distance){
				if(Boundaries.containsKey(target) && Boundaries.containsKey(source)){     //if target is boundary vertex, we can use it directly
					if(source.equals(target)){
						Event ev = new Event();
						ev.put(queryId, String.class, route + "	distance:"+ distance);
						resultStream.put(ev);
						
						Event result = new Event();
						result.put("queryId", String.class, queryId);
						result.put("control", String.class, null);
						result.put("distance", Double.class, distance);
						controlStream.put(result);
						
						return;
					}
					else{
						double newDistance = distance + Boundaries.get(source).getShortcutDistance(target);
						String newRoute = route +"-"+ Boundaries.get(source).getShortcutRoute(target);
						double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+target);  //target distance
						if(currentTargetDistance > newDistance) { 
							QDistance.put(queryId+" "+target, newDistance);
							Event ev = new Event();
							ev.put(queryId, String.class, newRoute + "	distance:"+ newDistance);
							resultStream.put(ev);
							
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
					int local = LocalShortPath(source, DistanceSet, RouteSet);
					double newDistance = distance + DistanceSet.get(target);
					String newRoute = route +"-"+ RouteSet.get(target);
					double currentTargetDistance = QDistance.get(queryId+" "+target)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+target);  //target distance
					if(currentTargetDistance > newDistance) { 
						QDistance.put(queryId+" "+target, newDistance);
						Event ev = new Event();
						ev.put(queryId, String.class, newRoute + "	distance:"+ newDistance);
						resultStream.put(ev);
						
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
		
		if(!Boundaries.containsKey(source)){   // the source is internal node and should find the boundary to other partition
			
			int localPath = LocalShortPath(source, DistanceSet, RouteSet);
			
			for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {

				String key = entry.getKey();
				double localDistance = DistanceSet.get(key);
				String localRoute = RouteSet.get(key);
				if (localDistance != Double.MAX_VALUE) { // the source can reach this boundary vertex
					double newDistance = distance + localDistance;
					String newRoute = route + "-" + localRoute;
					
					double currentDistance = QDistance.get(queryId+" "+key)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+key);
					if(currentDistance <= newDistance ) continue;
					else {
						QDistance.put(queryId+" "+key, newDistance);
						for (String next : entry.getValue().getEdgeNeighbors()) {

							Event e = new Event();
							e.put("queryId", String.class, queryId);
							e.put("source", String.class, next);
							e.put("target", String.class, target);
							e.put("distance", double.class, newDistance
									+ entry.getValue().getEdgeDistance(next));
							e.put("route", String.class, newRoute + "-" + next); 
							sendNum++;
							downStream.put(e);
						}
						
						Event ev = new Event();
			    		ev.put("partition", String.class, getId());
						ev.put("send", Integer.class, entry.getValue().getEdgeNeighbors().size());
						statStream.put(ev);
						
					}					
				}
			}
		}
		////boundary to boundary  shortcut and then to next partition
		else{
			BoundaryVertex boundary = Boundaries.get(source);
			double currentSourceDistance = QDistance.get(queryId+" "+source)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+source);  //input boundary node distance
			double limiteDistance = targetDistance.get(queryId)==null?Double.valueOf(Double.MAX_VALUE): targetDistance.get(queryId);
			if(currentSourceDistance > distance && currentSourceDistance <= limiteDistance) { 
				QDistance.put(queryId+" "+source, distance);
				
				for(String nextboundary : boundary.getBoundaries()){ //send to local boundary then to other partition
					
					double localDistance = boundary.getShortcutDistance(nextboundary);
					String localRoute = boundary.getShortcutRoute(nextboundary);
					
					if(localDistance != Double.MAX_VALUE){
						double newDistance = distance + localDistance;
						String newRoute = route + "-" + localRoute;
						double currentDistance = QDistance.get(queryId+" "+nextboundary)==null? Double.valueOf(Double.MAX_VALUE):QDistance.get(queryId+" "+nextboundary);
						if(currentDistance <= newDistance) continue;
						else {
							QDistance.put(queryId+" "+nextboundary, newDistance);
							for (String next : Boundaries.get(nextboundary).getEdgeNeighbors()){
								Event e = new Event();
								e.put("queryId", String.class, queryId);
								e.put("source", String.class, next);
								e.put("target", String.class, target);
								e.put("distance", double.class, newDistance + Boundaries.get(nextboundary).getEdgeDistance(next));
								e.put("route", String.class, newRoute + "-" + next); 
								sendNum++;
								downStream.put(e);
							}
							
							Event ev = new Event();
				    		ev.put("partition", String.class, getId());
							ev.put("send", Integer.class, Boundaries.get(nextboundary).getEdgeNeighbors().size());
							statStream.put(ev);
						}					
					}
		
				}
				
				if(route.equals(source)){
					for(String nextboundary : boundary.getEdgeNeighbors()){  //directly send  to other partition through edge: when source is boundary
						Event e = new Event();
						e.put("queryId", String.class, queryId);
						e.put("source", String.class, nextboundary);
						e.put("target", String.class, target);
						e.put("distance", double.class, distance + boundary.getEdgeDistance(nextboundary));
						e.put("route", String.class, route + "-" + nextboundary); 
						sendNum++;
						downStream.put(e);
					}
					
					Event ev = new Event();
		    		ev.put("partition", String.class, getId());
					ev.put("send", Integer.class, boundary.getEdgeNeighbors().size());
					statStream.put(ev);
				}
				
			}
			
			
			
		}
		
		
    }
    
    
    
    
    /**
     * use the method of pregel/graphlab to find the shortest path
     * @param event
     */
    public void graphFindShortestPath(Event event){
    	String queryId = event.get("queryId", String.class);
		String source = event.get("source", String.class);
		String target = event.get("target", String.class);
		double distance = event.get("distance", double.class);
		String route = event.get("route", String.class);
		
		Double currentDistance = Double.valueOf(Double.MAX_VALUE);
		if(!QDistance.containsKey(queryId+" "+source)){
			currentDistance = distance;
			QDistance.put(queryId+" "+source, currentDistance);
			
			if(source.equals(target) ){
    			Event ev = new Event();
    			ev.put(queryId, String.class, route + "	distance:"+ distance);
    			resultStream.put(ev);
    			return;
    		}
			
			Vertex vertex = vertices.get(source);      
			for(String s : vertex.getNeighbors()){
				Event e = new Event();
		    	e.put("queryId", String.class, queryId);
		    	e.put("source", String.class, s);
		    	e.put("target", String.class, target);
		    	e.put("distance", double.class, distance + vertex.get(s));
		    	e.put("route", String.class, route + "-" + s ); //+"["+(distance + vertex.get(s))+"]"
		    	downStream.put(e);
			}
			Event ev = new Event();
    		ev.put("partition", String.class, getId());
			ev.put("send", Integer.class, vertex.getNeighbors().size());
			statStream.put(ev);
			
			
		}
		else{
			
			currentDistance = QDistance.get(queryId+" "+source);
			if(currentDistance > distance){
				QDistance.put(queryId+" "+source, distance);
				
				if(source.equals(target) ){
					Event ev = new Event();
        			ev.put(queryId, String.class, route + "	distance:"+ distance);
        			resultStream.put(ev);
        			return;
        		}

				Vertex vertex = vertices.get(source);
				for(String s : vertex.getNeighbors()){
					Event e = new Event();
			    	e.put("queryId", String.class, queryId);
			    	e.put("source", String.class, s);
			    	e.put("target", String.class, target);
			    	e.put("distance", double.class, distance + vertex.get(s));
			    	e.put("route", String.class, route + "-" + s ); //+"["+(distance + vertex.get(s))+"]"
			    	downStream.put(e);
				}
				Event ev = new Event();
	    		ev.put("partition", String.class, getId());
				ev.put("send", Integer.class, vertex.getNeighbors().size());
				statStream.put(ev);
				
			}
		}
    }
    
    protected void onTime(){
		try {
			java.net.InetAddress hostname = java.net.InetAddress.getLocalHost();
	    	long time = System.currentTimeMillis();

	
	    	System.out.println("Host "+hostname+"	Partition "+getId()+"	New Maintainance of Boundary Start:"+time);
	    	
	    	maintainBoundary();
	
	    	long cost = System.currentTimeMillis() - time;
	    	System.out.println("Host "+hostname+"	Partition "+getId()+"	New Maintainance of Boundary: End!	Boundaries:"+Boundaries.size()+"	Cost:" + cost);
			
	    	Event ev = new Event();
    		ev.put("partition", String.class, getId());
			ev.put("send event", Integer.class, sendNum);
			ev.put("receive event", Integer.class, receiveNum);
			resultStream.put(ev);
	    	
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    
    //vertices
    public void maintainBoundary(){
    	//Boundaries
/*    	for (Map.Entry<String, Vertex> entry : vertices.entrySet()) {
            String key = entry.getKey();
            Vertex vertex = entry.getValue();
            for(String s : vertex.getNeighbors()){
            	if(!vertices.containsKey(s)){
                    BoundaryVertex  boundary = Boundaries.get(key) == null? new BoundaryVertex(key) : Boundaries.get(key);
            		boundary.putEdge(s, Double.valueOf(vertex.get(s)));
                    Boundaries.put(key, boundary);
            	}
            }	
        }*/
    	
		for (Map.Entry<String, BoundaryVertex> entry : Boundaries.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = Boundaries.get(key);
			HashMap<String, Double> Distance = new HashMap<String, Double>();
			HashMap<String, String> Route = new HashMap<String, String>();
			int localPath = LocalShortPath(entry.getKey(), Distance, Route);
			for (Map.Entry<String, BoundaryVertex> otherentry : Boundaries
					.entrySet()) {
				if (!entry.getKey().equals(otherentry.getKey())) {
					boundary.putShortcutDistance(otherentry.getKey(), Distance.get(otherentry.getKey()));
					boundary.putShortcutRoute(otherentry.getKey(), Route.get(otherentry.getKey()));
				}
			}
			//Boundaries.put(key, boundary);

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
    
    
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    
    public void setResultStream(Streamable<Event> stream){
    	this.resultStream = stream;
    	
    }
    
    public void setStatStream(Streamable<Event> stream){
    	this.statStream = stream;
    	
    }
    
    public void setControStream(Streamable<Event> stream){
    	this.controlStream = stream;
    	
    }
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    @Override
    protected void onCreate() {
    	vertices = new HashMap<String,Vertex>();
    	QDistance = new HashMap<String,Double>();
    	Boundaries = new HashMap<String, BoundaryVertex>();
    	targetDistance  = new HashMap<String,Double>();
    }

    @Override
    protected void onRemove() {
    }
}
