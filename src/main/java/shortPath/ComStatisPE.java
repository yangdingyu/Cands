package shortPath;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class ComStatisPE  extends ProcessingElement{

    Streamable<Event> downStream;
    Streamable<Event> queryStatStream;
    static Logger logger = LoggerFactory.getLogger(shortestPathPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;
    String outputPath = "";
    int partitionNumber = 100 ;//100/500/1000/1500;
    String searchMethod = "Boundary" ; //"Boundary""Baseline"
	String isLog = "false";

    HashMap<String, Integer> PartitionRecieveStat = new HashMap<String, Integer>();
    HashMap<String, Integer> PartitionSendStat = new HashMap<String, Integer>();
    HashMap<String, Long> PartitionCommunicationReceive = new HashMap<String, Long>();
    HashMap<String, Long> PartitionCommunicationSend = new HashMap<String, Long>();
    int receiveNum = 0;
    int sendNum = 0;
    List<String> send_log_list = new ArrayList<String>();
    List<String> receive_log_list = new ArrayList<String>();

    
    

    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
    	String partition = event.get("partition");
    	String queryId = event.get("queryId");
    	
    	if(event.containsKey("receive") ){ //&& (queryId.indexOf("-") == -1)
    		int data= event.get("receive", Integer.class);
    		int value = PartitionRecieveStat.get(queryId)==null? 0:PartitionRecieveStat.get(queryId);
    		PartitionRecieveStat.put(queryId, value+data);
    		
    		long datasize = event.get("size", Long.class);
    		long valuesize = PartitionCommunicationReceive.get(queryId)==null? 0:PartitionCommunicationReceive.get(queryId);
    		PartitionCommunicationReceive.put(queryId, valuesize+datasize);
    		
    		//receiveNum = receiveNum + data;
    		
    		if(isLog.equals("true") && data>0){
    			String receiveevent = "debugmessage-receive	"+queryId+"	"+event.get("source", String.class) + "	"+ event.get("From", String.class) +"	"
    								+ event.get("distance", double.class) ;
/*    			String debugevent = "debugmessage receive	"+ queryId+"	"+event.get("source", String.class).split("-")[0]==null?"null":event.get("source", String.class).split("-")[0] + "	"
    								+ event.get("From", String.class).split("-")[0]==null?"null":event.get("From", String.class).split("-")[0] 
    								+ "	" + event.get("pathLength", Integer.class)+"	"+ event.get("distance", double.class) ;*/
    			/*String sourcePartition = event.get("source", String.class).split("-")[0] ==null? "null":event.get("source", String.class).split("-")[0];
    			String FromPartition = event.get("From", String.class).split("-")[0] ==null? "null":event.get("From", String.class).split("-")[0];

    			String debugevent = "debugmessage receive	"+ queryId +"	"+ sourcePartition + "	"
						+ FromPartition + "	" + event.get("pathLength", Integer.class) +"	"+ event.get("distance", double.class) ;
        		*/
    			receive_log_list.add(receiveevent);
        		//receive_log_list.add(debugevent);
			}
    		
    		
    	}
    	else if(event.containsKey("send")){ // && (queryId.indexOf("-") == -1)
    		int data = event.get("send", Integer.class);
    		int value = PartitionSendStat.get(queryId)==null? 0:PartitionSendStat.get(queryId);
    		PartitionSendStat.put(queryId, value+data);
    		
    		long datasize = event.get("size", Long.class);
    		long valuesize = PartitionCommunicationSend.get(queryId)==null? 0:PartitionCommunicationSend.get(queryId);
    		PartitionCommunicationSend.put(queryId, valuesize+datasize);
    		
    		//sendNum = sendNum + data;
    		if(isLog.equals("true") && data>0){
    			//System.out.println(event.toString());
    			String sendevent = "debugmessage-send	"+queryId+"	"+event.get("source", String.class) + "	"+ event.get("next", String.class) +"	"
									+ event.get("distance", double.class) ;
    			/*String sourcePartition = event.get("source", String.class).split("-")[0] ==null? "null":event.get("source", String.class).split("-")[0];
    			String NextPartition = event.get("next", String.class).split("-")[0] ==null? "null":event.get("next", String.class).split("-")[0];

    			String debugevent = "debugmessage send	"+ queryId+"	"+ sourcePartition + "	"
    					+ NextPartition +"	"+ event.get("pathLength", Integer.class)+"	"+ event.get("distance", double.class) ;
    			*/
    			send_log_list.add(sendevent);
    			//send_log_list.add(debugevent);
    		}

    		
    	}
    	VoteToHalt(queryId);
    	
/*    	if(receiveNum == sendNum){
    		Event e = new Event();
			e.put("Algorithm Stop at ", Long.class, (System.currentTimeMillis() - timeInterval));
			e.put("receiveNum", Integer.class, receiveNum);
			e.put("sendNum", Integer.class, sendNum);
			downStream.put(e);
    	}*/
    	
    }
     
    public void VoteToHalt(String queryId){
    	int receive = PartitionRecieveStat.get(queryId)==null? 0:PartitionRecieveStat.get(queryId);
    	int send = PartitionSendStat.get(queryId)==null? 0:PartitionSendStat.get(queryId);
    	
    	if(receive > 0 && send > 0){
    		int isHalt = receive- 1 -send;
    		if(isHalt == 0){  //ok
    			//System.out.println("This query " + queryId + " is finished!" );
    			//System.out.println("This query " + queryId + " receive:" + receive + " send:" + send );
    			Event e = new Event();
    			e.put("queryId", String.class, queryId);
    			e.put("stop", String.class, "Stop");
    			e.put("receiveNum", Integer.class, receive-1);
    			e.put("sendNum", Integer.class, send);
    			downStream.put(e);
    		}
    	}
    }
    
    public void onTime(){
    	for(Map.Entry<String, Integer> entry : PartitionRecieveStat.entrySet()){
    		String queryId = entry.getKey();
    		if(queryId.indexOf("-") == -1){//not subquery
    			Event e = new Event();
        		e.put("queryId", String.class, queryId);
        		e.put("receiveNum", Integer.class, entry.getValue()-1);
        		e.put("sendNum", Integer.class, PartitionSendStat.get(queryId)==null? 0:PartitionSendStat.get(queryId));
        		e.put("receiveSize", Long.class, PartitionCommunicationReceive.get(queryId)==null? 0:PartitionCommunicationReceive.get(queryId));
    			e.put("sendSize", Long.class, PartitionCommunicationSend.get(queryId)==null? 0:PartitionCommunicationSend.get(queryId));
        		//downStream.put(e);
        		queryStatStream.put(e);
        		//System.out.println(e.toString());
    			
    		}	
    	}
		if(isLog.equals("true")){
			String path = outputPath+"_log.txt";

	    	File f = new File(path);
	       	StringBuilder sb = new StringBuilder();
	       	int i = 0;
	       	sb.append("\n --Log detail:partitionNumber:"+partitionNumber +"	searchMethod:"+searchMethod  + "	"+ (System.currentTimeMillis()-timeInterval)+"\n");
	       	
	       	sb.append(" -----send-----\n");

	       	for(String entry : send_log_list){
	           	sb.append(entry);
	           	sb.append("\n");
	       	}
	       	send_log_list.clear();
	       	sb.append(" -----receive-----\n");
	       	for(String entry : receive_log_list){
	           	sb.append(entry);
	           	sb.append("\n");
	       	}
	       	receive_log_list.clear();
			try {
				Files.append(sb.toString(), f, Charsets.UTF_8);
			} catch (IOException e) {
				logger.error("Cannot wordcount to file [{}]", f.getAbsolutePath(), e);
			}
		}

    	
    }
    
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    public void setParameters(String path, int number, String method, String log){
    	this.outputPath = path;
    	this.partitionNumber = number;
    	this.searchMethod = method;
    	this.isLog = log;
    }
    
    public void setStatStream(Streamable<Event> stream){
    	this.queryStatStream = stream;
    	
    }
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    @Override
    protected void onCreate() {
    }

    @Override
    protected void onRemove() {
    }
}
