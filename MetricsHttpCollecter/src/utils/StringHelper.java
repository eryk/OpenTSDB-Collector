package utils;
import java.io.IOException;
import java.net.InetAddress;

import net.opentsdb.utils.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;


public class StringHelper {
	public static String hostname(){
		String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception exc) {
            Sigar sigar = new Sigar();
            try {
               hostname = sigar.getNetInfo().getHostName();
            } catch (SigarException e) {
               hostname = "localhost.unknown";
            } finally {
               sigar.close();
            }
        }
        return hostname;
	}
	
	public static String clusterName(Config config) throws IOException{
		Configuration hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, config.getString("tsd.storage.hbase.zk_quorum"));
		hbaseConfig.set(HConstants.ZOOKEEPER_ZNODE_PARENT, config.getString("tsd.storage.hbase.zk_basedir"));
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		return admin.getClusterStatus().getMaster().getHostname();
	}
}
