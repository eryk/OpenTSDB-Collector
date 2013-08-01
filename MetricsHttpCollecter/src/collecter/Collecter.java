package collecter;
import java.util.Map;
import java.util.concurrent.Callable;

import net.opentsdb.utils.Config;

import org.hyperic.sigar.Sigar;

public interface Collecter extends Callable{
	
	Map<String, String> build();
	
	Map<String, String> collect(Map<String, String> metrics);

}
