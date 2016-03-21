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
import org.apache.s4.core.adapter.AdapterApp;

public class queryAdapter extends AdapterApp {
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

	public void connectAndRead() throws Exception {

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
		long frequency = 1;

		String filepath = configureMap.get("QueryPath");
		long queryNumber = Long.parseLong(configureMap.get("QueryNumber"));//50000000;
		long queryblock = Long.parseLong(configureMap.get("QueryBlock"));//50000000;
		frequency = configureMap.get("QueryFrequency")==null?1:Long.parseLong(configureMap.get("QueryFrequency"));

		System.out.println(filepath +"	"+queryNumber+ "	" + frequency);
		

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
		while ((record = br.readLine()) != null && recCount < queryNumber) { //
			if(!record.equals("")){
				Event event = new Event();
				event.put("query", String.class, record);
				//System.out.println(event.toString());
				getRemoteStream().put(event);
				// recordValue = "";
				
				if(recCount>0&&(recCount%queryblock)==0){
					System.out.println("Query Send Count" + recCount + "	Sleep:"+frequency +"s");
					Thread.sleep(frequency*1000);
				}
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
