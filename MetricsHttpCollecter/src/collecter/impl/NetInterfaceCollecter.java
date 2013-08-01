package collecter.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.opentsdb.utils.Config;

import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarNotImplementedException;

import com.google.common.collect.Maps;

import collecter.AbstractCollecter;

public class NetInterfaceCollecter extends AbstractCollecter {
	public final String metric= "net.stat";

	public NetInterfaceCollecter(Config config,Sigar sigar) {
		super(config, sigar);
	}

	@Override
	public Map<String, String> build() {
		Map<String,String> metrics = Maps.<String,String>newHashMap();
		String ifNames[];
			try {
				ifNames = sigar.getNetInterfaceList();
				for (int i = 0; i < ifNames.length; i++) {  
		            String name = ifNames[i];
		            try {  
		                NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);  
		                Map<String,String> maps = ifstat.toMap();
		                for(Entry<String,String> entry:maps.entrySet()){
		                	metrics.put(metric+"."+entry.getKey()+",eth:"+name, entry.getValue());
		                }
		            } catch (SigarNotImplementedException e) {  
		            	e.printStackTrace();
		            } catch (SigarException e) {  
		            	e.printStackTrace();
		            } 
				}
			} catch (SigarException e1) {
				e1.printStackTrace();
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
				.submit(new NetInterfaceCollecter(new Config("tsdb.properties"),sigar));
		try {
			Map<String, String> results = future.get(40 * 1000,
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
