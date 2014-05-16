package org.sensoriclife.generator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

		Map<String, String> defaults = new LinkedHashMap<>();
		defaults.put("generator.realtime", "true");
		defaults.put("storm.debug", "false");
		org.sensoriclife.Config.getInstance();
		org.sensoriclife.Config.getInstance().setDefaults(defaults);

		try {
			if ( org.sensoriclife.Config.getProperty("accumulo.name").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.zooServers").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.user").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.password").isEmpty() ){
				Accumulo.getInstance().connect();
				Accumulo.getInstance().createTable("generator_helper_table");
			}
			else
				Accumulo.getInstance().connect(org.sensoriclife.Config.getProperty("accumulo.name"), org.sensoriclife.Config.getProperty("accumulo.zooServers"), org.sensoriclife.Config.getProperty("accumulo.user"), org.sensoriclife.Config.getProperty("accumulo.password"));
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
		conf.setDebug(org.sensoriclife.Config.getBooleanProperty("storm.debug"));
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
}