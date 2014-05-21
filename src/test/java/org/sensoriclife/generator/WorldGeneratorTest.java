package org.sensoriclife.generator;

import static org.junit.Assert.assertNotEquals;

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
import org.junit.Test;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.WorldGenerator;

/**
 *
 * @author paul
 */
public class WorldGeneratorTest {
	/**
	 * Test of createWorld method, of class WorldGenerator.
	 * @throws org.apache.accumulo.core.client.AccumuloException
	 * @throws org.apache.accumulo.core.client.AccumuloSecurityException
	 * @throws org.apache.accumulo.core.client.TableExistsException
	 * @throws org.apache.accumulo.core.client.TableNotFoundException
	 */
	@Test
	public void testGenerator() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.realtime", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.timefactor", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("storm.debug", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.cities", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.districts", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.streets", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.buildings", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.residentialUnits", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.users", "99");

		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("generator_helper_table");

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("worldgenerator", new WorldGenerator(false), 1);

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();

		Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scanAll("generator_helper_table");
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertNotEquals(i, 0);

		Accumulo.getInstance().deleteTable("generator_helper_table");
	}

	@Test
	public void testCreateWorld() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.realtime", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.timefactor", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("storm.debug", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.cities", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.districts", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.streets", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.buildings", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.residentialUnits", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.users", "99");

		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("generator_helper_table");

		WorldGenerator instance = new WorldGenerator(false);
		instance.nextTuple();

		Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scanAll("generator_helper_table");
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertNotEquals(i, 0);

		Accumulo.getInstance().deleteTable("generator_helper_table");
	}
}