package org.sensoriclife.generator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import static org.junit.Assert.*;
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
		App.loadConfig();

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
		conf.setDebug(App.getBooleanProperty("storm.debug"));
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