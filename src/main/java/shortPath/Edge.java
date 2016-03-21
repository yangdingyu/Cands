package shortPath;

import java.util.Map;

public class Edge {
	private String source;
	private String target;
	private Double weight;
	private String status;  //Green Blue Yellow Red
	private Double limitedWeight;

	private Edge(){
		
	}
	
	public Edge(String source, String target){
		this.source = source;
		this.target = target;
	}
	
	public Edge(String source, String target, Double weight){
		this.source = source;
		this.target = target;
		this.weight = weight;
	}
	
	public Edge(String source, String target, Double weight, String status){
		this.source = source;
		this.target = target;
		this.weight = weight;
		this.status = status;
	}
	
	public Edge(String source, String target, Double weight, String status, Double limitedWeight){
		this.source = source;
		this.target = target;
		this.weight = weight;
		this.status = status;
		this.limitedWeight = limitedWeight;
	}
	
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
	
    public String getTarget() {
        return source;
    }

    public void setTarget(String target) {
        this.target = target;
    }
	
    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }
	
    /**
     * get the status of one edge (Green Blue Yellow Red)
     **/
    public String getStatus() {
        return status;
    }

    /**
     * set Status (Green Blue Yellow Red)
     **/
    public void setStatus(String status) {
        this.status = status;
    }
	
    /***
     * get the limitedWeight of one edge  
     ***/
    public Double getlimitedWeight() {
        return limitedWeight;
    }

    /***
     * set limitedWeight 
     ***/
    public void setlimitedWeight(Double limitedWeight) {
        this.limitedWeight = limitedWeight;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append("{" + this.source + "	" +this.target+ "	"+ this.weight +"	"+ this.status + "	"+ this.limitedWeight+ "	" + "},");      
        sb.append("]");
        return sb.toString();
    }

}
