package shortPath;

import org.apache.s4.base.Event;

public class UpdateTask implements Comparable<UpdateTask>{
	private Event event;
	private String queryId;
	private Integer caseType;
	
	
	public UpdateTask(Event e, String id, int type){
		this.event = e;
		this.queryId = id;
		this.caseType = type;
	}
	
	public Event getEvent(){
		return event;
	}
	
	public String getQueryId(){
		return queryId;
	}
	
	public int getCaseType(){
		return caseType;
	}
	
	@Override
	public int compareTo(UpdateTask t){
		int i = this.caseType.compareTo(t.caseType);
		return i;
	}
	
}
