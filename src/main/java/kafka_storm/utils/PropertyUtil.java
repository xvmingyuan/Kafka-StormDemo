package kafka_storm.utils;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 属性配置读取工具
 */
public class PropertyUtil {
	private static final Log log = LogFactory.getLog(PropertyUtil.class);
	private static Properties pros = new Properties();
	static {
		try {
			InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties");
			pros.load(in);
		}catch (Exception e) {
			log.error("load configuration error",e);
		}
	}
	public static String getProperty(String key) {
		return pros.getProperty(key);
	}
}
