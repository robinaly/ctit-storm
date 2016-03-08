package nl.utwente.bigdata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;


public class TopologyTest {
	@Test
	public void testTopology() throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		File f = new File("twitterrc");	
		if (f.exists()) {
			properties.load(new FileInputStream(f));
		}
		AbstractTopologyRunner runner = new KafkaPrinter();
		runner.runLocal("test", properties);
	}
}
