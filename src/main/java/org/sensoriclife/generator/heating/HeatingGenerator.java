package org.sensoriclife.generator.heating;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul
 * @version 0.1.1
 */
public class HeatingGenerator extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	HeatingValueGenerator valueGenerator = new HeatingValueGenerator();
	private Map<String, String> confs;

	public HeatingGenerator() {
		this.confs = Config.toMap();
	}

	public HeatingGenerator(Map<String, String> confs) {
		this.confs = confs;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Logger.debug(HeatingGenerator.class, "Generating next heating values.");

		try {
			if ( (((this.confs.containsKey("accumulo.name") && !this.confs.get("accumulo.name").isEmpty()) || (this.confs.containsKey("accumulo.zooServers") && !this.confs.get("accumulo.zooServers").isEmpty())) || (this.confs.containsKey("accumulo.user") && !this.confs.get("accumulo.user").isEmpty())) || (this.confs.containsKey("accumulo.password") && !this.confs.get("accumulo.password").isEmpty()) )
				Accumulo.getInstance().connect(this.confs.get("accumulo.name"), this.confs.get("accumulo.zooServers"), this.confs.get("accumulo.user"), this.confs.get("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(HeatingGenerator.class, "Error while connecting to accumulo.", e.toString());
		}

		Iterator<Map.Entry<Key, Value>> entries = null;
		try {
			entries = Accumulo.getInstance().scanAll(this.confs.get("generator.table_name"));
		}
		catch ( TableNotFoundException e ) {
			Logger.error(HeatingGenerator.class, "Error while reading data from: " + this.confs.get("generator.table_name"), e.toString());
			return;
		}

		while ( entries.hasNext() ) {
			Map.Entry<Key, Value> entry = entries.next();
			ResidentialUnit unit;
			try {
				unit = (ResidentialUnit) Helpers.toObject(entry.getValue().get());
			} 
			catch ( IOException | ClassNotFoundException e ) {
				Logger.error(HeatingGenerator.class, "Error while converting byte array to object.", e.toString());
				continue;
			}
			
			unit.setHeatingMeters(valueGenerator.generateNextValue(unit, timestamp));			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			String formattedTime = sdf.format(new Date(timestamp.getTime()));

			String xml="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><heating>";
			for ( int i = 0; i < unit.getHeatingIDs().length; i++ )
				xml += "<id>"+unit.getHeatingIDs()[i]+"</id><meter>"+unit.getHeatingMeters()[i]+"</meter>";

			xml += "<time>"+ formattedTime +"</time><heating>";
			this.collector.emit(new Values(xml));
			timestamp.setTime(timestamp.getTime()+15 * 60 * 1000);
		}

		if ( Boolean.parseBoolean(this.confs.get("generator.realtime")) )
			Utils.sleep((900000) / Integer.parseInt(this.confs.get("generator.timefactor")));//15min
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("heating"));
	}
}
