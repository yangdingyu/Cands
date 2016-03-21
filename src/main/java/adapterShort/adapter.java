package adapterShort;


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

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.adapter.AdapterApp;

import com.google.common.io.Resources;

public class adapter extends AdapterApp {
	private java.net.InetAddress hostname;

	@Override
	protected void onStart() {

		try {
			hostname = java.net.InetAddress.getLocalHost();
			connectAndRead();

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	/*InputStream adapterConfigFileInputStream = Resources.getResource("configure.txt").openStream();
	InputStreamReader inputstreamreader = new InputStreamReader(adapterConfigFileInputStream);
	BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
	String confiRecord = "";
	HashMap<String,String> configureMap = new HashMap<String,String>();
	while((confiRecord = bufferedReader.readLine())!= null){
		String[] re = confiRecord.split("=");
		if(!re[0].equals("")&& re.length==2){
			configureMap.put(re[0], re[1]);
		}
	}
	String filepath = configureMap.get("Mapfilepath");
	long dataLength = Long.parseLong(configureMap.get("DataLength"));//50000000;
	System.out.println(filepath +"	"+dataLength);*/
	
	public void connectAndRead() throws Exception {
		//String absolutePath = adapter.class.getResource("/").getFile();
		//System.out.println(absolutePath);
		
		
		String configurePath = System.getProperty("user.home")+"/roadConfigure.properties";//"/home/dingyu/trafficdata/USA-road-NY-Metis-1500.txt";
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
		String filepath = configureMap.get("Mapfilepath");
		long dataLength = Long.parseLong(configureMap.get("DataLength"));//50000000;
		System.out.println(filepath +"	"+dataLength  );
		
		
		//String filepath = "/home/yangdy/trafficdata/USA-road-NY-Metis-100.txt";

		//String filepath = "/home/yangdy/trafficdata/map";

		//String filepath = "/data0/dingyu/data/lineitem.tbl";// ;//
		// "/data0/dingyu/worddata/oneword.txt";//"/home/yangdy/worddata/oneword.txt";//data0/dingyu/data/4G.txt";///home/yangdy/tpch/Hadoop.txt";//"/home/yangdy/tpch/data/lineitem.txt";//"/data1/dingyu/data/lineitem.txt";///home/yangdy/tpch/data/lineitem1k.txt";//"/home/yangdy/tpch/PrepareData/lineitem.txt";//"/data0/dingyu/lineitem.txt"; ///"data0/dingyu/lineitem.txt"; //System.getProperty("user.home")

		System.out.println(new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss.SSSS")
				.format(new Date())
				+ "	reading start:	" + "	machine:" + hostname);
		long startTime = System.currentTimeMillis();
		InputStream is = new FileInputStream(filepath);
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String record = "";
		String recordValue = "";
		int recCount = 0;
		int sleepCount= 0;
		while ((record = br.readLine()) != null && recCount< dataLength) { //
			
			if(!record.equals("")){
				Event event = new Event();
				event.put("edge", String.class, record);
				// System.out.println(event.toString());
				getRemoteStream().put(event);
				// recordValue = "";
				recCount = recCount + 1;
			}
			
		}
		long endTime = System.currentTimeMillis();
		System.out.println(new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss.SSSS")
				.format(new Date())
				+ "	reading finished:	count"
				+ recCount
				+ "	machine:"
				+ hostname + "	read Time:" +(endTime - startTime - 5000*sleepCount));
		isr.close();
		is.close();
		br.close();
		System.gc();

	}

	@Override
	protected void onClose() {
		System.gc();
	}
}