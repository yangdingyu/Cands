package shortPath;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.reporting.CsvReporter;

public class spApp extends App {

	private java.net.InetAddress hostname;

	int splitId = 1;
	long timeInterval = 0;
	
	String outputPath = "";
	int partitionNumber = 0; //100;500;1000;1500
	int node = 0; //node number
	String searchMethod = "" ; //"Boundary""Baseline"
	String isLog = "false";
	String updateMethod = "Baseline";//Baseline  Improved
	String lockFlag = "Lock";//noLock Lock ParaLock
	String landmark = "true";  // Baseline Sample Partition

	@Override
	protected void onStart() {
		

	}
	

	private void prepareMetricsOutputs() throws IOException {
		String output = "/home/dongxiang/dingyu/trafficdata/result/Metrics/" + getPartitionId()  ;
		File metricsDirForPartition = new File(output);
		if (metricsDirForPartition.exists()) {
			FileUtils.deleteDirectory(metricsDirForPartition);
		}
		// activate metrics csv dump
		if (!metricsDirForPartition.mkdirs()) {
			LoggerFactory.getLogger(getClass()).error(
					"Cannot create directory {}",
					new File("metrics").getAbsolutePath());
		}

		CsvReporter.enable(metricsDirForPartition, 10, TimeUnit.SECONDS);
		// ConsoleReporter.enable(20, TimeUnit.SECONDS);
	}

	@Override
	protected void onInit() {
		try {

			
			String configurePath = System.getProperty("user.home")+"/roadConfigure.properties";//"/home/dingyu/trafficdata/USA-road-NY-Metis-1500.txt";
			System.out.println(configurePath);
			File file = new File(configurePath);
			InputStream inputstream = new FileInputStream(configurePath);
			InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
			BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
			String confiRecord = "";
			HashMap<String,String> configureMap = new HashMap<String,String>();
			while((confiRecord = bufferedReader.readLine())!= null){
				String[] re = confiRecord.split("=");
				if(!re[0].equals("")&& re.length==2){
					configureMap.put(re[0], re[1]);
				}
			}
			outputPath = configureMap.get("OutputPath");
			partitionNumber = Integer.parseInt(configureMap.get("PartitionNumber"));
			
			node = Integer.parseInt(configureMap.get("Node"));
			searchMethod = configureMap.get("SearchMethod");
			isLog = configureMap.get("Log");
			updateMethod = configureMap.get("UpdateMethod");
			lockFlag = configureMap.get("LockMethod");
			
			Constant.V_LandMark = configureMap.get("LandMark");
			Constant.PartitionNumber = partitionNumber;
			Constant.DataName = configureMap.get("DataName");
			Constant.LandMarkGraphPath = configureMap.get("LandMarkGraphPath");
			
			timeInterval = System.currentTimeMillis() - Long.parseLong("1369061082145");

			/*if(isLog.equals("true")){
				prepareMetricsOutputs();
				isLog= "false";
			}*/

			queryResultStat queryStat = createPE(queryResultStat.class);// computing the average runtime or route etc. information
			queryStat.setInterval(timeInterval);
			queryStat.setParameters(outputPath, partitionNumber, searchMethod);
			queryStat.setTimerInterval(120, TimeUnit.SECONDS);
			Stream<Event> querStatStream = createStream("queryStatStream", new KeyFinder<Event>() {
	        	
	            @Override
	            public List<String> get(final Event event) {
	            	return ImmutableList.of("static");// partition key: static is one PE
	            }
	        }, queryStat);
			
			
			queryOutputPE queryoutputPE = createPE(queryOutputPE.class);
			queryoutputPE.setInterval(timeInterval);
			queryoutputPE.setParameters(outputPath, partitionNumber, searchMethod, updateMethod, lockFlag);
			
			queryoutputPE.setTimerInterval(60, TimeUnit.SECONDS);
			Stream<Event> queryoutStream = createStream("queryoutStream", new KeyFinder<Event>() {
	        	
	            @Override
	            public List<String> get(final Event event) {
	            	return ImmutableList.of("single");// partition key: static is one PE
	            }
	        }, queryoutputPE);
			
			ComStatisPE statPE = createPE(ComStatisPE.class);
			statPE.setDownStream(queryoutStream);
			statPE.setStatStream(querStatStream);
			statPE.setInterval(timeInterval);
			statPE.setParameters(outputPath, partitionNumber, searchMethod, isLog);
			statPE.setTimerInterval(60, TimeUnit.SECONDS);
			Stream<Event> statStream = createStream("statStream", new KeyFinder<Event>() {
	        	
	            @Override
	            public List<String> get(final Event event) {
	            	return ImmutableList.of("single");// partition key: static is one PE
	            	//String queryId = event.get("queryId");
	            	//return ImmutableList.of(queryId);
	            	
	            }
	        }, statPE);
			
			shortestPathPE shortPathPE = createPE(shortestPathPE.class);
			shortPathPE.setInterval(timeInterval);
			//shortestPathPE.setSingleton(true);
			shortPathPE.setTimerInterval(120, TimeUnit.SECONDS);
			shortPathPE.setParameters(outputPath, partitionNumber, searchMethod, isLog, updateMethod);
			Stream<Event> edgeShortestPathStream = createStream("shortestpath", new KeyFinder<Event>() {
				@Override
				public List<String> get(Event event) {
					String vetex ="";
					if(event.containsKey("edge")){//edge 
						vetex = event.get("edge").split(" ")[1].split("-")[0];
						//vetex = Integer.toString(Integer.parseInt(event.get("edge").split(" ")[1])%10);
						//vetex = Integer.toString(getPartition(Integer.parseInt(event.get("edge").split(" ")[1].split("-")[0])));
					}
					else if(event.containsKey("partition")) //query---boudnaryMerge
					{
						vetex = event.get("partition");
					}
					else if(event.containsKey("route"))//  query --baseline --boundary
					{
						vetex = event.get("route",Route.class).getNextVertex().split("-")[0];
						//vetex = Integer.toString(Integer.parseInt(event.get("source"))%10);
					}
					else if(event.containsKey("partitionRoute")){ // landmark--partition
						/*String source = event.get("source", Route.class).getStartVertex();
						String next =  event.get("route",Route.class).getNextVertex();
						if(source.equals(next)){// first partition
							vetex = source.split("-")[0];
						}
						else{ //next partition
							vetex = (String)event.get("partitionRoute", HashMap.class).get(next);
						}*/
						vetex = event.get("nextPartition");
					}
					else if(event.containsKey("updateEdge"))//  query --baseline --boundary
					{
						vetex = event.get("updateEdge").split("-")[0];
						//vetex = Integer.toString(Integer.parseInt(event.get("source"))%10);
					}
					else{
						System.out.println("Shortest Receive a special Event:" + event.toString());
					}
					String p = vetex;//getPartition(vetex);//getPartition(vetex);//vetex;//
					return ImmutableList.of(p);
					}
			}, shortPathPE);
			
			
			shortPathPE.setDownStream(edgeShortestPathStream);
			
			shortPathPE.setResultStream(queryoutStream);
			
			shortPathPE.setQueryStatStream(querStatStream);
			
			shortPathPE.setStatStream(statStream);

			Stream<Event> controlStream = createStream("controlStream",shortPathPE);
			shortPathPE.setControStream(controlStream);
			queryoutputPE.setControStream(controlStream);
			
			
			edgeInputPE edge =  createPE(edgeInputPE.class);
			edge.setSingleton(true);
			Stream<Event> edgeinputStream = createInputStream("edge" ,edge);
			edge.setDownStream(edgeShortestPathStream);
			
			
			queryPE query = createPE(queryPE.class);
			//query.setSingleton(true);
			query.setDownStream(edgeShortestPathStream);
			//query.setStatStream(querStatStream);
			query.setParameters(outputPath, partitionNumber, searchMethod);
			query.setInterval(timeInterval);
			query.setTimerInterval(60, TimeUnit.SECONDS);
			Stream<Event> queryMasterStream = createStream("queryMasterStream", new KeyFinder<Event>() {
	        	
	            @Override
	            public List<String> get(Event event) {
	            	return ImmutableList.of("key");// partition key: static is one PE
	            }
	        }, query);
			
			queryoutputPE.setQueryStream(queryMasterStream);
			queryoutputPE.setUpdateStream(edgeShortestPathStream);
			
			shortPathPE.setBoundaryStream(queryMasterStream);
			
			queryInputPE queryInput =  createPE(queryInputPE.class);
			queryInput.setSingleton(true);
			Stream<Event> queryInputStream = createInputStream("query" ,queryInput);
			queryInput.setDownStream(queryMasterStream);
			

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	protected void onClose() {
	}

	public String getPartition (String parti){
		int origin_parition = Integer.parseInt(parti);
		int PNumberPerNode = partitionNumber / node;
		String partition = "";
		int index = origin_parition / PNumberPerNode;
		partition = Integer.toString(node * origin_parition + index);
		return partition;
	}
	
	/*int partitionNumber = 5000 ;//1/500/1000/2000/5000/8000/10000;
	public int getPartition(int index){
		return index/partitionNumber;
		if(index<140){
			return 1;
		}
		else if(index>=140 && index<140*2){
			return 2;
		}
		else if(index>=140*2 && index<140*3){
			return 3;
		}
		else if(index>=140*3 && index<140*4){
			return 4;
		}
		else if(index>=140*4 && index<140*5){
			return 5;
		}
		else if(index>=140*5 && index<140*6){
			return 6;
		}
		else if(index>=140*6 && index<140*7){
			return 7;
		}
		else if(index>=140*7 && index<140*8){
			return 8;
		}
		else if(index>=140*8 && index<140*9){
			return 9;
		}
		else if(index>=140*9 && index<140*10){
			return 10;
		}
		else {
			return 11;
		}
		}*/
	
}
