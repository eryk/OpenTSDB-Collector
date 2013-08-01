package collecter.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.opentsdb.utils.Config;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import collecter.AbstractCollecter;

public class LoadInfoCollecter extends AbstractCollecter{
	public final String metric = "proc.load";
	
	public LoadInfoCollecter(Config config,Sigar sigar) {
		super(config, sigar);
	}

	@Override
	public Map<String, String> build() {
		Map<String,String> metrics = new HashMap<String,String>();
		
		try {
			double[] values = sigar.getLoadAverage();
			metrics.put(metric+".1min", String.valueOf(values[0]));
			metrics.put(metric+".5min", String.valueOf(values[1]));
			metrics.put(metric+".15min", String.valueOf(values[2]));
		} catch (SigarException e) {
			e.printStackTrace();
		}
		return metrics;
	}

	@Override
	public Map<String, String> collect(Map<String, String> metrics) {
		return metrics;
	}
	
	public static void main(String[] args) throws SigarException, IOException {
		Sigar sigar = new Sigar();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Map<String, String>> future = executor
				.submit(new LoadInfoCollecter(new Config("tsdb.properties"),sigar));
		try {
			Map<String, String> results = future.get(10 * 1000,
					TimeUnit.SECONDS);
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
