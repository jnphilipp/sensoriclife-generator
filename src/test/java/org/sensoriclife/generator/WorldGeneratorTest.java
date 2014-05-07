package org.sensoriclife.generator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.io.File;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author paul
 */
public class WorldGeneratorTest {
	/**
	 * Test of nextTuple method, of class WorldGenerator.
	 */
	@Test
	public void testNextTuple() {
		System.out.println("nextTuple");
		App.loadConfig();
		
		new File(WorldGenerator.PATH_OUTPUT_FILE).delete();
		WorldGenerator instance = new WorldGenerator();
		instance.nextTuple();
		assertTrue(new File(WorldGenerator.PATH_OUTPUT_FILE).exists());
	}
}