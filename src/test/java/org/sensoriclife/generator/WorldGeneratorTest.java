package org.sensoriclife.generator;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.WorldGenerator;

/**
 *
 * @author paul
 */
public class WorldGeneratorTest {
	@BeforeClass
	public static void setUp() {
		org.sensoriclife.Config.getInstance().getProperties().setProperty("storm.debug", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.cities", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.districts", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.streets", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.buildings", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.residentialUnits", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.users", "99");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.realtime", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.timefactor", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.table_name_electricity", "sensoriclife_electricity");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.table_name_water", "sensoriclife_water");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.table_name_heating", "sensoriclife_heating");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.table_name", "sensoriclife");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.batch_writer.max_memory", "10000000");
	}

	@Test
	public void testGenerator() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException, InterruptedException {
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_electricity"));
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_water"));
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_heating"));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("worldgenerator", new WorldGenerator(false), 1);

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();

		testEntries("generator.table_name_electricity");
		testEntries("generator.table_name_water");
		testEntries("generator.table_name_heating");
		assertTrue(WorldGenerator.isCreated());

		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_electricity"));
		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_water"));
		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_heating"));
		Accumulo.getInstance().disconnect();
	}

	@Test
	public void testCreateWorld() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException, InterruptedException {
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_electricity"));
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_water"));
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name_heating"));

		WorldGenerator instance = new WorldGenerator(false);
		instance.nextTuple();

		testEntries("generator.table_name_electricity");
		testEntries("generator.table_name_water");
		testEntries("generator.table_name_heating");
		assertTrue(WorldGenerator.isCreated());

		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_electricity"));
		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_water"));
		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name_heating"));
		Accumulo.getInstance().disconnect();
	}
	
	private void testEntries(String tableName) throws TableNotFoundException{
		Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scanAll(org.sensoriclife.Config.getProperty(tableName));
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertNotEquals(i, 0);
	}
}