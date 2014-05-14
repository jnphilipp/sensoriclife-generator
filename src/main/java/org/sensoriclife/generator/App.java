package org.sensoriclife.generator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.electricity.ElectricityGenerator;
import org.sensoriclife.generator.water.WaterGenerator;
import org.sensoriclife.generator.world.WorldGenerator;

/**
 * 
 * @author jnphilipp, paul, stefan
 * @version 0.0.4
 */
public class App {
	/**
	 * properties
	 */
	private static Properties properties;

	public static void main(String[] args) throws AccumuloSecurityException {
		String logFile = "";
		boolean world = false, electricity = false, water = false;

		if (args.length != 0) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();

			while (it.hasNext()) {
				switch (it.next()) {
				case "-log": {
					logFile = it.next();
				}
					break;
				case "world": {
					world = true;
				}
					break;
				case "electricity": {
					electricity = true;
				}
					break;
				case "water": {
					water = true;
				}
					break;
				}
			}
		}

		if (!logFile.isEmpty() && new File(logFile).exists())
			try {
				Logger.getInstance(logFile);
			} catch (IOException e) {
				Logger.error(App.class, "Error while reading log config file.", e.toString());
				System.exit(1);
			}
		else
			Logger.getInstance();

		App.loadConfig();

		try {
			if ( App.getProperty("accumulo.name").isEmpty() && App.getProperty("accumulo.zooServers").isEmpty() && App.getProperty("accumulo.user").isEmpty() && App.getProperty("accumulo.password").isEmpty() ){
				Accumulo.getInstance().connect();
				Accumulo.getInstance().createTable("generator_helper_table");
			}
			else
				Accumulo.getInstance().connect(App.getProperty("accumulo.name"), App.getProperty("accumulo.zooServers"), App.getProperty("accumulo.user"), App.getProperty("accumulo.password"));
		}
		catch ( AccumuloException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		} 
		catch (TableExistsException e) {
			Logger.error("Error by creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();

		if (world)
			builder.setSpout("world", new WorldGenerator(), 1);
		if (electricity)
			builder.setSpout("electricity", new ElectricityGenerator(), 10);
		if (water)
			builder.setSpout("water", new WaterGenerator(), 10);
	
		//for test
		Config conf = new Config();
		conf.setDebug(App.getBooleanProperty("storm.debug"));
		conf.setNumWorkers(2);
		
		try {
			Accumulo.getInstance();
			Accumulo.getInstance().connect();
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while creating mock instance.", e.toString());
		}

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}

	/**
	 * @return the properties
	 */
	public static Properties getProperties() {
		return App.properties;
	}

	/**
	 * @param key
	 *            key
	 * @return values of the given key
	 */
	public static String getProperty(String key) {
		switch (key) {
		case "realtime":
			return App.properties.getProperty(key, "true");
		default:
			return App.properties.getProperty(key, "");
		}
	}

	/**
	 * Returns the value of the given key as boolean;
	 * 
	 * @param key
	 *            key
	 * @return <code>true</code> or <code>false</code>
	 */
	public static boolean getBooleanProperty(String key) {
		return Boolean.valueOf(App.getProperty(key));
	}

	/**
	 * Returns the value of the given key as integer.
	 * 
	 * @param key
	 *            key
	 * @return integer value
	 */
	public static int getIntegerProperty(String key) {
		return Integer.parseInt(App.getProperty(key));
	}

	public static void loadConfig() {
		try {
			App.properties = new Properties();
			App.properties.load(App.class.getResourceAsStream("/config.properties"));
		}
		catch ( IOException e ) {
			Logger.error("Error while loading config file.", e.toString());
			System.exit(1);
		}
	}
}