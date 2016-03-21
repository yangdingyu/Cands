package shortPath;

public class Route implements Comparable<Route>{
	private String queryId;
	private Long startTime;
	private String startVertex;
	private String nextVertex;
	private String endVertex;
	private double distance;
	private String route;
	private Long endTime;
	
	public Route(){
		
	}
	
	public Route(String queryid, Long starttime, String start, String next, String end, double distance, String route, Long endtime){
		this.queryId = queryid;
		this.startTime = starttime;
		this.startVertex = start;
		this.nextVertex = next;
		this.endVertex = end;
		this.distance = distance;
		this.route = route;
		this.endTime = endtime;
		
	}
	
	public String getQueryId(){
		return this.queryId;
	}
	
	public Long getStartTime(){
		return this.startTime;
	}
	
	public String getStartVertex(){
		return this.startVertex;
	}
	
	public String getNextVertex(){
		return this.nextVertex;
	}
	
	public String getEndVertex(){
		return this.endVertex;
	}
	
	public double getDistance(){
		return this.distance;		
	}
	
	public String getRoute(){
		return this.route;
	}
	public Long getEndTime(){
		return this.endTime;
	}

	public void setNextVertex(String next){
		this.nextVertex = next;		
	}
	
	public void setRoute(String route){
		this.route = route;		
	}
	
	public void setDistance(double distance){
		this.distance = distance;		
	}
	
	public void setendTime(Long endtime){
		this.endTime = endtime;		
	}
	
	
	@Override
	public int compareTo(Route other){
		return 

				   this.endTime < other.endTime?1:
					   this.endTime > other.endTime?-1:
							this.distance < other.distance?-1:
								   this.distance > other.distance?1:0;

	}
 
	boolean equals(Route o){
		return compareTo(o)==0;
	}
	
	public String ToString(){
		return this.queryId + "	" + this.startTime + "	" + this.startVertex + "	" + this.nextVertex +  "	" + this.endVertex + "	"+ this.distance + "	" + this.route + "	" +this.endTime;
	}
	
	
}
