package shortPath;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class queryPE extends ProcessingElement {

	Streamable<Event> downStream;
	Streamable<Event> statStream;
	static Logger logger = LoggerFactory.getLogger(shortestPathPE.class);
	boolean firstEvent = true;
	long timeInterval = 0;

	String outputPath = "";
	int partitionNumber = 100;// 100/500/1000/1500;
	String searchMethod = "Boundary"; // "Boundary""Baseline"

	HashMap<String, BoundaryVertex> BoundariesForQuery;
	HashMap<String, Vertex> PartitionsForQuery;

	HashMap<String, Vertex> SampleForQuery;

	HashMap<String, Event> QueryWaitingQueue; // the query is waiting queue
	HashMap<String, Long> QueryWaitingTime; // the query is waiting queue

	/**
	 * This method is called upon a new Event on an incoming stream
	 */
	public void onEvent(Event event) {

		// boundary edge preprocessing
		if (event.containsKey("boundary")) {
			String key = event.get("key", String.class);
			BoundaryVertex boundary = event.get("boundary",
					BoundaryVertex.class);
			BoundariesForQuery.put(key, boundary);
			
			return;

		}

		// // query event
		else if (event.containsKey("query")
				&& !event.containsKey("partitionRoute")) {

			String[] data = event.get("query").split(" ");
			String queryId = data[0];
			String source = data[1];
			String target = data[2];
			double distance = 0;
			String route = source;

			Route thisroute = new Route(queryId, System.currentTimeMillis()
					- timeInterval, source, source, target, distance, route,
					System.currentTimeMillis() - timeInterval);

			Event routeEvent = new Event();
			routeEvent.put("route", Route.class, thisroute);

			// the method is boundarymerge
			if (searchMethod.equals("BoundaryMerge")) {

				Event queryEventBoundaryMerge = new Event();
				String partitionId = source.split("-")[0];
				queryEventBoundaryMerge.put("partition", String.class,
						partitionId);
				queryEventBoundaryMerge.put("queryId", String.class, queryId);
				queryEventBoundaryMerge.put(Long.toString(System.nanoTime()),
						Event.class, routeEvent);

				// partition preparation processing
				if (Constant.V_LandMark.equals(Constant.LandMark_Partition)) {
					/*
					 * if (firstEvent) { initLandmarkPartitionGraph();
					 * firstEvent = false; }
					 */

					String startPartitionId = source.split("-")[0];
					String endPartitionId = target.split("-")[0];
					
					HashMap<String, Double> distances = new HashMap<String, Double>();
					HashMap<String, String> partitionRoute = new HashMap<String, String> ();
					
					partitionRoute = FindShortPartitionRoute(startPartitionId, endPartitionId, distances);
					
					partitionRoute = getReverseRoute(partitionRoute, startPartitionId, endPartitionId);
					
					//System.out.println("Query: " + startPartitionId + "-" + endPartitionId + ": distance:"+ distances.get(endPartitionId)  + "\n");
					
					//System.out.println( getReverseRoute(partitionRoute, startPartitionId, endPartitionId));

					// send the query message to next partitions

					Event LandmarkEvent = new Event();
					LandmarkEvent.put("partitionRoute", HashMap.class,
							partitionRoute);
					LandmarkEvent.put("queryId", String.class, queryId);
					LandmarkEvent.put("nextPartition", String.class,
							partitionId);
					LandmarkEvent.put(Long.toString(System.nanoTime()),
							Event.class, routeEvent);

					downStream.put(LandmarkEvent);

					QueryWaitingQueue.put(queryId, queryEventBoundaryMerge);
					return;
				} else if (Constant.V_LandMark.equals(Constant.LandMark_Sample)) {
					// find one landmark partition guild
					// if (firstEvent) {
					// initLandmarkSampleGraph();
					// firstEvent = false;
					// }

					String startPartitionId = source.split("-")[0];
					String endPartitionId = target.split("-")[0];
					HashMap<String, Double> distances = new HashMap<String, Double>();
					HashMap<String, String> partitionRoute = new HashMap<String, String> ();
					
					partitionRoute = FindShortPartitionRoute(startPartitionId, endPartitionId, distances);
					partitionRoute = getReverseRoute(partitionRoute, startPartitionId, endPartitionId);
					
					// send the query message to next partitions

					Event LandmarkEvent = new Event();
					LandmarkEvent.put("partitionRoute", HashMap.class,
							partitionRoute);
					LandmarkEvent.put("queryId", String.class, queryId);
					LandmarkEvent.put("nextPartition", String.class,
							partitionId);
					LandmarkEvent.put(Long.toString(System.nanoTime()),
							Event.class, routeEvent);

					downStream.put(LandmarkEvent);

					QueryWaitingQueue.put(queryId, queryEventBoundaryMerge);
					return;

				} else {

					downStream.put(queryEventBoundaryMerge);
					return;
				}

			}
			// other query method
			else {

				downStream.put(routeEvent);
				return;
			}
		}

		// Constant.LandMark_Partition

		// the landmark query return back
		// we will submit a cache query to find the exact shortest path
		else if (event.containsKey("partitionRoute")) {
			String queryId = event.get("queryId", String.class);
			Route returnRoute = event.get("route", Route.class);
			Event queryEventBoundaryMerge = QueryWaitingQueue.get(queryId);
			if (queryEventBoundaryMerge != null) {

				// System.out.println("Start the waiting query:" + queryId);

				long time = returnRoute.getEndTime()
						- returnRoute.getStartTime();
				QueryWaitingTime.put(queryId, time);

				QueryWaitingQueue.remove(queryId);

				downStream.put(queryEventBoundaryMerge);

			}
			return;
		}

	}

	
    /*
     * Route<vertex, previous vertex>
     */
    public String getRoute(Map<String, String> Route, String startVertex, String endVertex){
    	for (Map.Entry<String, String> entry : Route.entrySet()) {
    		System.out.println( entry.getKey() + ":" +  entry.getValue() + ";");
    	}
    	
    	String position = startVertex;
		int l = 0;
		String route = position;
	
		while(!position.equals(endVertex)){
			position = Route.get(position);
			if(position.equals("")) return "";
			route = route + ":" + position;
			l++;
		}

		return route;
    }
    
    
	/**
	 * generate the matrix of the prepare landmark sample graph
	 */
	public void initLandmarkSampleGraph() {

		// read graph from files
		try {
			String graphPath = Constant.LandMarkGraphPath;// "/home/dingyu/trafficdata/USA-road-NY-Metis-1500.txt";
			InputStream inputstream = new FileInputStream(graphPath);
			InputStreamReader inputstreamreader = new InputStreamReader(
					inputstream);
			BufferedReader bufferedReader = new BufferedReader(
					inputstreamreader);
			String record = "";
			int index = 0;
			while ((record = bufferedReader.readLine()) != null) {
				String[] values = record.split("	");

				if (values.length != Constant.PartitionNumber) {
					System.out
							.println("Partition Number does not equale Matrix distance ");
					return;
				}
				Vertex vertex = SampleForQuery.get(String.valueOf(index)) == null ? new Vertex(
						String.valueOf(index))
						: SampleForQuery.get(String.valueOf(index));

				double distance;
				for (int i = 0; i < Constant.PartitionNumber; i++) {
					distance = Double.POSITIVE_INFINITY;
					if (values[i].equals("Infinity")) {

					} else {
						distance = Double.parseDouble(values[i]);
						vertex.put(String.valueOf(i), distance);
					}
				}
				if (vertex == null)
					System.out.println("I am null ");
				SampleForQuery.put(String.valueOf(index), vertex);

				index++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		Map<String, Double> boundaryEdges = new HashMap<String, Double>();
		Map<String, Integer> boundaryEdgesCount = new HashMap<String, Integer>();

		for (Map.Entry<String, BoundaryVertex> entry : BoundariesForQuery
				.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = BoundariesForQuery.get(key);
			String partition = key.split("-")[0];

			List<String> neighbors = boundary.getEdgeNeighbors();

			for (String neighbor : neighbors) {
				String nextPartition = neighbor.split("-")[0];
				double distance = boundary.getEdgeDistance(neighbor);

				String keyPartition = partition + "-" + nextPartition;

				double sumDistance = boundaryEdges.get(keyPartition) == null ? distance
						: boundaryEdges.get(keyPartition) + distance;
				int count = boundaryEdgesCount.get(keyPartition) == null ? 1
						: boundaryEdgesCount.get(keyPartition) + 1;
				boundaryEdges.put(keyPartition, sumDistance);
				boundaryEdgesCount.put(keyPartition, count);
			}

		}
		for (Map.Entry<String, Double> entry : boundaryEdges.entrySet()) {
			String key = entry.getKey();
			double distance = entry.getValue();
			int count = boundaryEdgesCount.get(key);
			double avgDistance = distance / count;

			String startPartition = key.split("-")[0];
			String endPartition = key.split("-")[1];

			double sampleDistance = SampleForQuery.get(startPartition) == null ? avgDistance
					: (SampleForQuery.get(startPartition).getEdge(endPartition) == null ? avgDistance
							: SampleForQuery.get(startPartition).get(
									endPartition));

			Vertex vertex = PartitionsForQuery.get(startPartition) == null ? new Vertex(
					startPartition)
					: PartitionsForQuery.get(startPartition);
			vertex.put(endPartition, sampleDistance);
			PartitionsForQuery.put(startPartition, vertex);

		}

	}

	/**
	 * generate the matrix of the prepare landmark partition graph
	 */
	public void initLandmarkPartitionGraph() {

		System.out.println("----------------initLandmarkPartitionGraph -------------Start\n");
		Map<String, Double> boundaryEdges = new HashMap<String, Double>();
		Map<String, Integer> boundaryEdgesCount = new HashMap<String, Integer>();

		for (Map.Entry<String, BoundaryVertex> entry : BoundariesForQuery
				.entrySet()) {
			String key = entry.getKey();
			BoundaryVertex boundary = entry.getValue();
			
			String partition = key.split("-")[0];

			List<String> neighbors = boundary.getEdgeNeighbors();
			if (neighbors == null) {
				System.out.println("This boundary:" + key
						+ " has none neighbor");
				continue;
			}
			if (neighbors.size() <= 0) {
				System.out.println("This boundary:" + key
						+ " has none neighbor");
				continue;
			}

			for (String neighbor : neighbors) {
				String nextPartition = neighbor.split("-")[0];
				
				double distance = boundary.getEdgeDistance(neighbor);
				if( distance<=0){
					System.out.println("This boundary:" + key
							+ " has none distance");
					continue;
				}

				String keyPartition = partition + "-" + nextPartition;

				double sumDistance = boundaryEdges.get(keyPartition) == null ? distance
						: boundaryEdges.get(keyPartition) + distance;
				int count = boundaryEdgesCount.get(keyPartition) == null ? 1
						: boundaryEdgesCount.get(keyPartition) + 1;
				boundaryEdges.put(keyPartition, sumDistance);
				boundaryEdgesCount.put(keyPartition, count);
			}

		}
		for (Map.Entry<String, Double> entry : boundaryEdges.entrySet()) {
			String key = entry.getKey();
			double distance = entry.getValue();
			int count = boundaryEdgesCount.get(key);
			double avgDistance = distance / count;

			String startPartition = key.split("-")[0];
			String endPartition = key.split("-")[1];
			

			Vertex vertex = PartitionsForQuery.get(startPartition) == null ? new Vertex(
					startPartition)
					: PartitionsForQuery.get(startPartition);
			vertex.put(endPartition, avgDistance);
			PartitionsForQuery.put(startPartition, vertex);

		}
		
		System.out.println("----------------initLandmarkPartitionGraph -------------finished\n");

	}

	/**
	 * Find the shortest path from boundary vertex using FibonacciHeap
	 * 
	 * @return
	 */

	public HashMap<String, String> FindShortPartitionRoute(
			String startPartition, String endPartition,
			HashMap<String, Double> result) {
		int number = PartitionsForQuery.size();
		if (number <= 0) {
			System.out.println("QueryPE: The summary graph is null");
			return null;
		}

		FibonacciHeap<String> pq = new FibonacciHeap<String>(); // the distances
		// of unvisited
		// nodes
		HashMap<String, FibonacciHeap.Entry<String>> entries = new HashMap<String, FibonacciHeap.Entry<String>>();
		HashMap<String, String> Route = new HashMap<String, String>();

		for (Map.Entry<String, Vertex> node : PartitionsForQuery.entrySet()) {
			entries.put(node.getKey(), pq.enqueue(node.getKey(),
					Double.POSITIVE_INFINITY));
			Route.put(node.getKey(), "");

		}

		try {
			pq.decreaseKey(entries.get(startPartition), 0.0);
		} catch (Exception e) {
			e.printStackTrace();
			Boolean containSource = PartitionsForQuery
					.containsKey(startPartition);
			System.out.println(" LocalShortPathByFbncHeap Error: "
					+ "	vSource: " + startPartition
					+ "	PartitionsForQuery containSource:" + containSource);
			return null;
		}
		/* Keep processing the queue until no nodes remain. */
		while (!pq.isEmpty()) {
			/*
			 * Grab the current node. The algorithm guarantees that we now have
			 * the shortest distance to it.
			 */
			FibonacciHeap.Entry<String> curr = pq.dequeueMin();

			/* Store this in the result table. */
			result.put(curr.getValue(), curr.getPriority());
			if (curr.getValue().equals(endPartition)) {

				return Route;
			}

			Vertex startnode = PartitionsForQuery.get(curr.getValue());
			/* Update the priorities of all of its edges. */
			for (String arc : startnode.getNeighbors()) {
				/*
				 * If we already know the shortest path from the source to this
				 * node, don't add the edge.
				 */
				if (result.containsKey(arc)
						|| !PartitionsForQuery.containsKey(arc))
					continue;

				/*
				 * Compute the cost of the path from the source to this node,
				 * which is the cost of this node plus the cost of this edge.
				 */
				double pathCost = curr.getPriority() + startnode.get(arc);

				/*
				 * If the length of the best-known path from the source to this
				 * node is longer than this potential path cost, update the cost
				 * of the shortest path.
				 */
				FibonacciHeap.Entry<String> dest = entries.get(arc);
				if (pathCost < dest.getPriority()) {
					pq.decreaseKey(dest, pathCost);
					 Route.put(arc, curr.getValue());  ////<target, start>
				}

			}
		}

		return Route;

	}

	/**
	 * Find the shortest path from boundary vertex using FibonacciHeap
	 * 
	 * @return
	 */

	public HashMap<String, Double> LocalShortPathByFbncHeap(String vSource,
			HashMap<String, String> Route) {
		int number = PartitionsForQuery.size();

		FibonacciHeap<String> pq = new FibonacciHeap<String>(); // the distances
		// of unvisited
		// nodes
		HashMap<String, FibonacciHeap.Entry<String>> entries = new HashMap<String, FibonacciHeap.Entry<String>>();
		HashMap<String, Double> result = new HashMap<String, Double>();

		for (Map.Entry<String, Vertex> node : PartitionsForQuery.entrySet()) {
			entries.put(node.getKey(), pq.enqueue(node.getKey(),
					Double.POSITIVE_INFINITY));
			Route.put(node.getKey(), "");

		}
		try {
			pq.decreaseKey(entries.get(vSource), 0.0);
		} catch (Exception e) {
			e.printStackTrace();
			Boolean containSource = PartitionsForQuery.containsKey(vSource);
			System.out.println(" LocalShortPathByFbncHeap Error: "
					+ "	vSource: " + vSource + "	vertices containSource:"
					+ containSource);
		}

		/* Keep processing the queue until no nodes remain. */
		while (!pq.isEmpty()) {
			/*
			 * Grab the current node. The algorithm guarantees that we now have
			 * the shortest distance to it.
			 */
			FibonacciHeap.Entry<String> curr = pq.dequeueMin();

			/* Store this in the result table. */
			result.put(curr.getValue(), curr.getPriority());

			Vertex startnode = PartitionsForQuery.get(curr.getValue());
			/* Update the priorities of all of its edges. */
			for (String arc : startnode.getNeighbors()) {
				/*
				 * If we already know the shortest path from the source to this
				 * node, don't add the edge.
				 */
				if (result.containsKey(arc)
						|| !PartitionsForQuery.containsKey(arc))
					continue;

				/*
				 * Compute the cost of the path from the source to this node,
				 * which is the cost of this node plus the cost of this edge.
				 */
				double pathCost = curr.getPriority() + startnode.get(arc);

				/*
				 * If the length of the best-known path from the source to this
				 * node is longer than this potential path cost, update the cost
				 * of the shortest path.
				 */
				FibonacciHeap.Entry<String> dest = entries.get(arc);
				if (pathCost < dest.getPriority()) {
					pq.decreaseKey(dest, pathCost);
					 Route.put(arc, curr.getValue());  ////<target, start>
				}

			}
		}

		return result;

	}
	
	 /*
     * Route<vertex, previous vertex>
     */
    public HashMap<String, String> getReverseRoute(Map<String, String> Route, String startVertex, String endVertex){
    	String position = endVertex;
		int l = 0;
		String reverseroute = position;
	
		while(!position.equals(startVertex)){
			position = Route.get(position);
			if(position.equals("")) return null;
			reverseroute = reverseroute + ":" + position;
			l++;
		}
		String[] splitroute = reverseroute.split(":");
		String route = splitroute[splitroute.length-1];
		for(int j=splitroute.length-2; j>=0; j--){
			route = route + ":" + splitroute[j];
		}
		HashMap<String, String> reouteMap = new HashMap<String,String>();
		splitroute = route.split(":");
		System.out.println("Partition Guild:" + route);
		int length = splitroute.length;
		
		for(int i=0;i<length-1;i++){
			reouteMap.put(splitroute[i], splitroute[i+1]);
		}
				
		return reouteMap;
    }
    
    

	public void setDownStream(Streamable<Event> stream) {
		this.downStream = stream;

	}

	public void onTime() {

		System.out.println("BoundariesForQuery.size():"
				+ BoundariesForQuery.size() + "	firstEvent:" + firstEvent);

		if (BoundariesForQuery.size() > 0) {
			if (firstEvent) {
				if (Constant.V_LandMark.equals(Constant.LandMark_Partition)) {
					initLandmarkPartitionGraph();
					firstEvent = false;
				} else if (Constant.V_LandMark.equals(Constant.LandMark_Sample)) {
					// find one landmark partition guild
					initLandmarkSampleGraph();
					firstEvent = false;
				}

			}
		}

		long sumWaitTime = 0;
		long avgWaitTime = 0;
		int count = QueryWaitingTime.size();
		if (count <= 0)
			return;
		for (Map.Entry<String, Long> entry : QueryWaitingTime.entrySet()) {
			sumWaitTime = sumWaitTime + entry.getValue();
		}
		avgWaitTime = sumWaitTime / count;

		String path = outputPath + "-queryWaitTime.txt";
		File f = new File(path);
		StringBuilder sb = new StringBuilder();

		sb.append("##-----------QueryWaitingTime----  \n");
		sb.append("queryCount:" + count + "\n");
		sb.append("avgWaitTime:" + avgWaitTime + "\n");

		try {
			Files.append(sb.toString(), f, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Cannot wordcount to file [{}]", f.getAbsolutePath(),
					e);
		}

	}

	/*
	 * public void setStatStream(Streamable<Event> stream){ this.statStream =
	 * stream;
	 * 
	 * }
	 */

	public void setInterval(long interval) {
		this.timeInterval = interval;
	}

	public void setParameters(String path, int number, String method) {
		this.outputPath = path;
		this.partitionNumber = number;
		this.searchMethod = method;
	}

	@Override
	protected void onCreate() {

		this.BoundariesForQuery = new HashMap<String, BoundaryVertex>();
		this.SampleForQuery = new HashMap<String, Vertex>();
		this.PartitionsForQuery = new HashMap<String, Vertex>();
		this.QueryWaitingQueue = new HashMap<String, Event>();
		this.QueryWaitingTime = new HashMap<String, Long>();
	}

	@Override
	protected void onRemove() {
	}
}
