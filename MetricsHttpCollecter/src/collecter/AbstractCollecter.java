package collecter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.opentsdb.utils.Config;

import org.hyperic.sigar.Sigar;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class AbstractCollecter implements Collecter {
	protected Set<String> filters = new HashSet<String>();
	
	protected Config config;
	protected Sigar sigar;
	
	public AbstractCollecter(Config config ,Sigar sigar) {
		this.config = config;
		this.sigar = sigar;
	}
	
	public Map<String,String> filter(Config config,Map<String,String> metrics){
		String exclude = Joiner.on(".").join(getClass().getSimpleName(),"exclude");
		String keys = config.getString(exclude);
		if(keys==null){
			return metrics;
		}
		filters = Sets.newHashSet(Splitter.on(",").split(keys));
		
		Map<String, String> maps = metrics;
		if(maps==null){
			maps = new HashMap<String,String>();
		}
		return Maps.filterKeys(maps, new Predicate<String>() {

			@Override
			public boolean apply(String key) {
				return !filters.contains(key);
			}
		});
	}
	
	@Override
	public abstract Map<String,String> build();
	
	@Override
	public abstract Map<String, String> collect(Map<String,String> metrics);

	@Override
	public Object call() throws Exception {
		Map<String,String> metrics = filter(config,build());
		return collect(metrics);
	}
}
