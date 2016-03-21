package shortPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class BoundaryVertex {

    private static final Logger logger = LoggerFactory.getLogger(Vertex.class);

    private String boundaryVertexId;
    final private long time;
    private String streamId;
    
     private HashMap<String, Edge> edges; //the edge from boundary to  other partition vertex
    
     private HashMap<String, Double> Shortcut ;  //boundary vertex
    
     private HashMap<String, String> ShortcutRoute ;

    /** Default constructor sets time using system time. */
    public BoundaryVertex() {
        this.time = System.currentTimeMillis();
        edges = new HashMap<String, Edge>();
        Shortcut = new HashMap<String, Double> ();
        ShortcutRoute = new HashMap<String, String>();
    }
    
    public BoundaryVertex(String vertexid) {
    	this.boundaryVertexId = vertexid;
    	this.edges = new HashMap<String, Edge>();
    	this.Shortcut = new HashMap<String, Double> ();
    	this.ShortcutRoute = new HashMap<String, String>();
        this.time = System.currentTimeMillis();
    }
    
    /**
     * This constructor explicitly sets the time. Event that need to explicitly set the time must call {super(time)}
     */
    public BoundaryVertex(long time) {
        this.time = time;
    }

    /**
     * @return the create time
     */
    public long getTime() {
        return time;
    }

    public void setId(String boundaryVertexId){
    	this.boundaryVertexId = boundaryVertexId;
    }
    
    public String getId(){
    	return boundaryVertexId;
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
     * put a edge (to other partition)
     * @param vertex
     * @param distance
     */
    public void putEdge(String endVertex, Double distance) {


        if (edges == null) {
        	edges = Maps.newHashMap();
        }
        Edge edge = edges.get(endVertex)==null?new Edge(this.boundaryVertexId, endVertex, distance):edges.get(endVertex);
        edge.setWeight(distance);

        edges.put(endVertex, edge);
    }

    /**
     * get the distance from neighbor vertex (to other partition)
     * @param neighborVertex
     * @return
     */
    public double getEdgeDistance(String neighborVertex) {

        double ditance = edges.get(neighborVertex).getWeight();
        return ditance;
    }

    /** get the distance from neighbor vertex (to other partition)
    * @param neighborVertex
    * @return
    */
   public List<String> getEdgeNeighbors() {
	   
	   List<String> neighbors = new ArrayList<String>();
	   for (Map.Entry<String, Edge> entry : edges.entrySet()) {
           String key = entry.getKey();
           neighbors.add(key);
       }
   	return neighbors;
   }

    /**
     * Put an short cut between boundary vertex.
     * 
     * @param type
     *            the type of the value
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void putShortcutDistance(String nextBoundary, Double distance) {

        if (Shortcut == null) {
        	Shortcut = Maps.newHashMap();
        }

        Shortcut.put(nextBoundary, distance);
    }

    /**
     * Get the distance by  nextBoundary.
     * 
     * @param key
     * @return the value
     */
    public double getShortcutDistance(String nextBoundary) {

        double ditance = Shortcut.get(nextBoundary);
        return ditance;
    }
    
    /**
     * put the internal route between this boundary with nextBoundary
     * @param nextBoundary
     * @param route
     */
    public void putShortcutRoute(String nextBoundary, String route){
    	
        if (ShortcutRoute == null) {
        	ShortcutRoute = Maps.newHashMap();
        }

        ShortcutRoute.put(nextBoundary, route);
    }
  
    public HashMap<String, Double> getShortcut(){
    	return  Shortcut;
    }
    /**
     * get the internal route between this boundary with nextBoundary
     * @param nextBoundary
     * @param route
     */
    public String getShortcutRoute(String nextBoundary){
       
        String route = ShortcutRoute.get(nextBoundary);
        return route;
    }
  
    /**
     * get next boundary vertex  
     * @return
     */
    
    public List<String> getBoundaries(){
    	List<String> neighbors = new ArrayList<String>();
    	for (Map.Entry<String, Double> entry : Shortcut.entrySet()) {
            String key = entry.getKey();
            neighbors.add(key);
        }
    	return neighbors;
    }
 
    public boolean containsKey(String nextBoundary) {
        return Shortcut.containsKey(nextBoundary);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Map.Entry<String, Double> entry : Shortcut.entrySet()) {
            sb.append("{" + entry.getKey() + ";" + entry.getValue() + "},");
        }
        sb.append("]");
        return sb.toString();
    }

}
