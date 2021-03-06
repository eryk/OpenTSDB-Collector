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

import com.google.common.base.Joiner;

import collecter.AbstractCollecter;
import collecter.Collecter;

public class CpuInfoCollecter extends AbstractCollecter {

	public final String metric = "proc.stat.cpu";
	public final String tagk = "type";

	public CpuInfoCollecter(Config config, Sigar sigar) {
		super(config, sigar);
	}

	@Override
	public Map<String, String> collect(Map<String, String> metrics) {
		Map<String, String> types = metrics;
		Map<String, String> metricWithTags = new HashMap<String, String>();
		for (Entry<String, String> tagkv : types.entrySet()) {
			String kvPair = Joiner.on(":").join(tagk, tagkv.getKey());
			metricWithTags.put(Joiner.on(",").join(metric, kvPair),
					tagkv.getValue());
		}
		return metricWithTags;
	}

	@Override
	public Map<String, String> build() {
		try {
			return sigar.getCpu().toMap();
		} catch (SigarException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws SigarException, IOException {
		Sigar sigar = new Sigar();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Map<String, String>> future = executor
				.submit(new CpuInfoCollecter(new Config("tsdb.properties"),
						sigar));
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
