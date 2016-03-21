package shortPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;


public class Vertex {

    private static final Logger logger = LoggerFactory.getLogger(Vertex.class);

    private String vertexId;
    final private long time;
    private String streamId;
    private HashMap<String, Edge> edges ;
    private HashMap<String, Double> edgesStatus ;
    private Double value ;   // the weight of this vertex

    /** Default constructor sets time using system time. */
    public Vertex() {
        this.time = System.currentTimeMillis();
        this.value = new Double(0);
    }
    
    public Vertex(String vertexid) {
    	this.vertexId = vertexid;
        this.time = System.currentTimeMillis();
        this.value = new Double(0);
        //this.edges = Maps.newHashMap();
    }
    /**
     * This constructor explicitly sets the time. Event that need to explicitly set the time must call {super(time)}
     */
    public Vertex(long time) {
        this.time = time;
    }

    /**
     * @return the create time
     */
    public long getTime() {
        return time;
    }

    public void setId(String vertexid){
    	this.vertexId = vertexid;
    }
    
    public String getId(){
    	return vertexId;
    }
    
    public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	/**
     * The stream id is used to identify streams uniquely in a cluster configuration. It is not required to operate in
     * local mode.
     * 
     * @return the target stream id
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * 
     * @param streamName
     *            used to identify streams uniquely in a cluster configuration. It is not required to operate in local
     *            mode.
     */
    public void setStreamId(String streamName) {
        this.streamId = streamName;
    }

    /**
     * update the weight of neighbor edge. return the changed value. 0 is no status changed, else changed status
     * 
     * @param type
     *            the type of the value
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public double put(String endVertex, Double distance) {

        if (edges == null) {
        	edges = Maps.newHashMap();
        }
        Edge edge = edges.get(endVertex);
        if(edge==null){
        	edge = new Edge(this.vertexId, endVertex, distance, "Blue", distance*2);
        	edges.put(endVertex, edge);
        	return 0;
        }
        else{
        	edge = edges.get(endVertex);
        	double previousDistance = edge.getWeight();
        	edge.setWeight(distance);
        	String previousStatus = edge.getStatus();
            double limited = edge.getlimitedWeight();
    		double percentRate = distance/limited;
    		String status = getStatusByWeight(percentRate);
    		edge.setStatus(status);
    		if(previousStatus.equals(status)){
    			return 0;
    		}
    		else{
    			return (distance - previousDistance);
    		}

        }
        
		
		
        
    }

    public String getStatusByWeight(double p){
    	String status = "Green";
    	if(p>=0.8) {
    		status = "Green";
    	}
    	else if(p<0.8 && p>=0.3){
    		status = "Blue";
    	}
    	else if(p<0.3 && p>=0.15){
    		status = "Yellow";
    	}
    	else if(p<0.15){
    		status = "Red";
    	}
    	return status;

    }
    
    /**
     * Get the distance by  neighbor vertex.
     * 
     * @param key
     * @return the value
     */
    public double get(String endVertex) {

        double ditance = edges.get(endVertex).getWeight();

        return ditance;

    }

    /**
     * Get the edge by  neighbor vertex.
     * 
     * @param key
     * @return the value
     */
    public Edge getEdge(String endVertex) {

        Edge edge = edges.get(endVertex);

        return edge;

    }

    
    /**
     * set satus of one edge, if changed, return true; else false (include initial);
     * @param endVertex
     * @param status
     */
    public boolean setStatus(String endVertex, String status){
    	if (edges == null) {
        	edges = Maps.newHashMap();
        }
    	Edge edge = edges.get(endVertex);
    	if(edge == null){
    		edge = new Edge(this.vertexId, endVertex);
    	}
    	String previousStatus = edge.getStatus();
        edge.setStatus(status);

        edges.put(endVertex, edge);
        
        if(previousStatus == null || previousStatus == status ) return false;
        else return true;
    }
    
    /**
     * 
     * @param endVertex
     * @return
     */
    public String getStatus(String endVertex){
    	String status = edges.get(endVertex).getStatus();
    	return status;
    }
    
    /**
     * set this edge's limited weight
     * @param endVertex
     * @param status
     */
    public void setLimitedWeight(String endVertex, Double limitedWeight){
    	if (edges == null) {
        	edges = Maps.newHashMap();
        }
        Edge edge = edges.get(endVertex)==null?new Edge(this.vertexId, endVertex):edges.get(endVertex);
        edge.setlimitedWeight(limitedWeight);

        edges.put(endVertex, edge);
    }
    
    /**
     * get this edge's limited weight
     * @param endVertex
     * @return
     */
    public Double getLimitedWeight(String endVertex){
    	Double status = edges.get(endVertex).getlimitedWeight();
    	return status;
    }
    
    
    /**
     * get the neighbors of this vertex
     * @return
     */
    public List<String> getNeighbors(){
    	List<String> neighbors = new ArrayList<String>();
    	for (Map.Entry<String, Edge> entry : edges.entrySet()) {
            String key = entry.getKey();
            neighbors.add(key);
        }
    	return neighbors;
    }
 
    public boolean containsKey(String endVertex) {
        return edges.containsKey(endVertex);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Map.Entry<String, Edge> entry : edges.entrySet()) {
            sb.append("{" + entry.getKey() + ";" + entry.getValue().toString() + "},");
        }
        sb.append("]");
        return sb.toString();
    }

}
