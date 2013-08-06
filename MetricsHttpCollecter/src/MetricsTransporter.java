import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hyperic.sigar.Sigar;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.ClassUtil;
import utils.StringHelper;
import collecter.Collecter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

/**
 * 
 * @author eryk xuqi86@gmail.com
 *
 */
public class MetricsTransporter {
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	
	private static TSDB tsdb;
	private Config conf;
	private Sigar sigar;
	
	private List<Collecter> _collecters;
	private ExecutorService exePool;
	private Map<String,String> tags;

	public MetricsTransporter(String path) throws IOException {
		conf = new Config(path);
		sigar = new Sigar();
		tags = new HashMap<String,String>();
	}
	
	public void setup() throws IOException {
		tags.put("host", StringHelper.hostname());
		tags.put("cluster", Joiner.on("_").join(StringHelper.clusterName(conf),conf.getString("tsd.storage.hbase.zk_basedir")));
		tsdb = new TSDB(conf);
		_collecters = new ArrayList<Collecter>(20);
		exePool = Executors.newFixedThreadPool(conf.getInt("metrics.collecter.pool.size"));
	}
	
	public void registerCollecter(){
		Set<Class<?>> collecters =  ClassUtil.getClasses("collecter.impl");
		for(Class cls : collecters){
			try {
				Constructor<Collecter> construcator = cls.getConstructor(Config.class,Sigar.class);
				Collecter collecter = construcator.newInstance(conf,sigar);
				_collecters.add(collecter);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void metricsdump(){
		long timestamp = stamp();
		int count =0;
		System.out.println("timestamp="+timestamp);
		for(Collecter collecter:_collecters){
			Future<Map<String,String>> future = exePool.submit(collecter);
			try {
				Map<String,String> metrics = future.get(conf.getInt("metrics.collecter.thread.interrupt"), TimeUnit.SECONDS);
				for (Entry<String, String> metricWithTag : metrics.entrySet()) {
					Map<String,String> selfTags = new HashMap<String,String>();
					selfTags.putAll(tags);
					Iterator<String> iter =Splitter.on(",").split(metricWithTag.getKey()).iterator();
					String metric = iter.next();
					while(iter.hasNext()){
						String _tmp = iter.next();
						String[] kv = _tmp.split(":");
						selfTags.put(kv[0], kv[1]);
					}
					System.out.println(metric+"~"+metricWithTag.getValue()+"~"+selfTags);
					try {
						count++;
						if (metricWithTag.getValue().contains(".")) {
							tsdb.addPoint(metric, timestamp,Float.valueOf(metricWithTag.getValue()), selfTags);
						} else {
							tsdb.addPoint(metric, timestamp,Long.valueOf(metricWithTag.getValue()), selfTags);
						}
					} catch (Exception e) {
						System.out.println(String.format("can't convert metric=[%s],value=[%s]", metricWithTag.getKey(),metricWithTag.getValue()));
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				LOG.error(String.format("collect metrics timeout,Collecter Class=%s", collecter.getClass()));
			}
		}
		System.out.println("total put="+count);
	}
	
	public long stamp(){
		long timestamp = System.currentTimeMillis() / 1000;
		int interval  = conf.getInt("metrics.collecter.interval");
		long newtimestamp = (timestamp/interval) * interval;
		return newtimestamp;
	}
	
	public void close(){
		tsdb.shutdown();
		exePool.shutdown();
	}
	
	public static void main(String[] args) throws IOException {
		Stopwatch watch = new Stopwatch().start();
		MetricsTransporter trans = new MetricsTransporter(args[0]);
		trans.setup();
		trans.registerCollecter();
		trans.metricsdump();
		trans.close();
		System.out.println("cost time:"+watch.stop());
	}
}
