package org.sensoriclife.generator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.electricity.ElectricityGenerator;
import org.sensoriclife.generator.world.WorldGenerator;

/**
 *
 * @author paul
 */
public class WorldGeneratorTest {
	/**
	 * Test of createWorld method, of class WorldGenerator.
	 */
	@Test
	public void testCeateWorld() {
		Map<String, String> defaults = new LinkedHashMap<>();
		defaults.put("realtime", "true");
		defaults.put("storm.debug", "false");
		org.sensoriclife.Config.getInstance().setDefaults(defaults);

		try {
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("generator_helper_table");
		} catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		} catch ( TableExistsException e ) {
			Logger.error("Error while creating table.", e.toString());
		}

		WorldGenerator instance = new WorldGenerator();
		instance.createWorld();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("electricity", new ElectricityGenerator(), 10);
		
		Config conf = new Config();
		conf.setDebug(org.sensoriclife.Config.getBooleanProperty("storm.debug"));
		conf.setNumWorkers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
/*
		try {
			Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scannAll("generator_helper_table");
			int i = 0;
			while ( entries.hasNext() ) {
				Entry<Key, Value> entry = entries.next();
				i++;
			}

			assertNotEquals(i, 0);
		}
		catch ( TableNotFoundException e ) {
			Logger.error("Error while reading data.", e.toString());
		}*/
	}	
}