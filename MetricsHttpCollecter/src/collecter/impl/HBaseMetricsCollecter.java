package collecter.impl;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.hbase.async.HBaseClient;
import org.hyperic.sigar.Sigar;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import utils.StringHelper;

import collecter.AbstractCollecter;
import collecter.Collecter;

public class HBaseMetricsCollecter extends AbstractCollecter{

	private String metricsString;
	private Map<String, String> metrics;
	private String fetchURL;

	public HBaseMetricsCollecter(Config config, Sigar sigar){
		super(config, sigar);
		fetchURL = getURL();
		metrics = new HashMap<String, String>();
	}

	public void loadMetrics() throws IOException {
		fetch(fetchURL);
		JSONObject json = JSONObject.fromObject(metricsString);
		convert(json, null);
	}

	public void convert(Object obj, Object parentKey) {
		if (obj instanceof JSONObject) {
			Iterator it = ((JSONObject) obj).keys();
			while (it.hasNext()) {
				Object key = it.next();
				Object val = ((JSONObject) obj).get(key);
				if (!(val instanceof JSONObject) && !(val instanceof JSONArray)) {
					metrics.put(String.valueOf(parentKey + "." + key),
							String.valueOf(val));
				} else {
					convert(val, (parentKey == null ? "" : parentKey + ".")
							+ key);
				}
			}
		} else if (obj instanceof JSONArray) {
			JSONArray array = (JSONArray) obj;
			for (Object o : array) {
				convert(o, parentKey);
			}
		}
	}

	public void fetch(String fetchPath) throws IOException {

		try {
			StringBuffer html = new StringBuffer();
			URL url = new URL(fetchPath);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			InputStreamReader isr = new InputStreamReader(conn.getInputStream());
			BufferedReader br = new BufferedReader(isr);
			String temp;
			while ((temp = br.readLine()) != null) {
				html.append(temp).append("\n");
			}
			br.close();
			isr.close();
			metricsString = html.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getDomain(String url) {
		String domain = url.split("/")[2];
		return domain.substring(0, domain.indexOf(":"));
	}

	@Override
	public Map<String, String> build() {
		try {
			loadMetrics();
			return metrics;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<String, String> collect(Map<String, String> metrics) {
		return combineMetrics(metrics);
	}
	
	public String getURL(){
		//TODO modify url
		String url = "http://"+StringHelper.hostname()+":60030/metrics?format=json";
		return url;
	}
	
	public Map<String,String> combineMetrics(Map<String,String> singleMetrics){
		Map<String,String> tableMap = Maps.<String,String>newHashMap();
		
		for(Entry<String,String> attr:singleMetrics.entrySet()){
			String key =attr.getKey();
			String value = attr.getValue();
			if(!key.contains("tbl")){
				tableMap.put(key, value);
				continue;
			}
			//"hbase","RegionServerDynamicStatistics","tbl","tableName","type(region/cf)","region/cf name","metric"
			if(key.contains("region") && !key.contains("META")){
				String[] keys = key.split("\\.",7);
				String combinMetric = Joiner.on(".").join(keys[0],keys[1],keys[6]);
				combinMetric = Joiner.on(",").join(combinMetric,Joiner.on(":").join("table",keys[3]));
				String combinMetricValue = tableMap.get(combinMetric);
				if(combinMetricValue==null){
					tableMap.put(combinMetric, attr.getValue());
				}else{
					try {
						if(combinMetricValue.contains(".")){
							float floatCombinMetricValue = Float.valueOf(combinMetricValue);
							float singleMetricValue = Float.valueOf(value);
							tableMap.put(combinMetric, String.valueOf((floatCombinMetricValue+singleMetricValue)));
						}else {
							long longCombinMetricValue = Long.valueOf(combinMetricValue);
							long singleMetricValue = Long.valueOf(value);
							tableMap.put(combinMetric, String.valueOf((longCombinMetricValue+singleMetricValue)));
						}
					} catch (Exception e) {
						System.out.println(String.format("can't convert metric=[%s],value=[%s]", key,value));
					}
				}
				
			}
		}
		return tableMap;
	}

	public static void main(String[] args) throws IOException {
		Sigar sigar = new Sigar();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Map<String, String>> future = executor
				.submit(new HBaseMetricsCollecter(new Config("tsdb.properties"),new Sigar()));
		try {
			Map<String, String> results = future.get(10 * 1000,
					TimeUnit.MICROSECONDS);
			for (Entry<String, String> entry : results.entrySet()) {
				System.out.println(entry.getKey() + "\t" + entry.getValue());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		executor.shutdown();
	}

}
