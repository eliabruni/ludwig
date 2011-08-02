/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.s2m.ludwig.conf;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  This is the configuration file for OSS. It uses hadoop's xml schema 
 *  for properties. The default configuration is in $OSS_HOME/oss-conf.xml 
 *  and is a singleton.
 *
 *  Remarks
 *  -------
 *  The logging system gets configuration information from this file and
 *  thus depends on this being initialized. So no logging events are 
 *  allowed when this is being initialized. If logger is used, an infinite 
 *  recursion happens and causes a stack overflow.
 *
 *************************************************************************/
public class LudwigConfiguration extends Configuration {
	protected static final Logger LOG = LoggerFactory
	.getLogger(LudwigConfiguration.class);
	private static LudwigConfiguration singleton;

	/**
	 * Returns the 'OSS_HOME' location. Taken in order of precedence:
	 * 
	 * - Java system property 'oss.home'
	 * 
	 * - $OSS_HOME in the environment.
	 * 
	 * - null if neither of these are set.
	 */
	public static String getOSSHome() {
		String OSSHome = System.getProperty("oss.home");
		if (null == OSSHome) {
			OSSHome = System.getenv("OSS_HOME");
		}

		if (null == OSSHome) {
			LOG.warn("-Doss.home and $OSS_HOME both unset");
		}

		return OSSHome;
	}

	/**
	 * Returns the 'OSS_CONF_DIR' location. Taken in order of precedence:
	 * 
	 * - Java system property 'oss.conf.dir'
	 * 
	 * - $OSS_CONF_DIR in the environment
	 * 
	 * - getOSSHome()/conf
	 * 
	 * - ./conf
	 */
	public static String getOSSConfDir() {
		String OSSConfDir = System.getProperty("oss.conf.dir");
		if (null == OSSConfDir) {
			OSSConfDir = System.getenv("OSS_CONF_DIR");
		}

		if (null == OSSConfDir) {
			String ossHome = getOSSHome();
			if (null != ossHome) {
				OSSConfDir = new File(ossHome, "conf").toString();
			} else {
				OSSConfDir = "./conf";
			}
		}

		return OSSConfDir;
	}

	public synchronized static LudwigConfiguration get() {
		if (singleton == null)
			singleton = new LudwigConfiguration();
		return singleton;
	}

	protected LudwigConfiguration() {
		this(true);
	}


	protected LudwigConfiguration(boolean loadDefaults) {
		super();
		if (loadDefaults) {
			Path conf = new Path(getOSSConfDir());
			LOG.info("Loading configurations from " + conf);
			super.addResource(new Path(conf, "oss-conf.xml"));
			super.addResource(new Path(conf, "oss-site.xml"));
		}
	}

	// COOCCUR COMPUTATION SETTINGS
	public static final String WINDOW_SIZE = "oss.twitter.cooccur.window_size";
	public static final String STOPWORDS_PATH = "oss.twitter.coccur.stopwords_path";

	// AGENT SETTINGS
	public static final String NUMBER_OF_AGENTS = "oss.twitter.agent.number";
	public static final String NUMBER_OF_THREADS_PER_AGENT = "oss.twitter.agent.threads_number";
	
	// TP SETTINGS
	public static final String NUMBER_OF_TERM_TP = "oss.twitter.agent.partitions_number";
	public static final String TP_THRESHOLD = "oss.twitter.agent.tp_threshold";

	// COLLECTOR SETTINGS
	public static final String NUMBER_OF_SAME_COLLECTORS = "oss.twitter.collector.same_number";
	public static final String NUMBER_OF_DIFFERENT_COLLECTORS = "oss.twitter.collector.different_number";
	public static final String NUMBER_OF_THREADS_PER_COLLECTOR = "oss.twitter.collector.threads_number";


	// STREAM SETTINGS
	public static final String MESSAGE_SIZE = "oss.twitter.message_size";
	
	// TWITTER STREAM SETTINGS
	public static final String TWITTER_STREAM_URL = "oss.twitter.url";
	public static final String TWITTER_USER = "oss.twitter.username";
	public static final String TWITTER_PW = "oss.twitter.password";
	
	
	
	



	/**
	 * The size of cooccur window, default = 1. 
	 */
	public int getWindowSize() {
		return getInt(WINDOW_SIZE, 1);
	}	

	public String getStopwordsPath() {
		return STOPWORDS_PATH;
	}

	/**
	 * Number of Agents, default = 3. 
	 */
	public int getNumberOfAgents() {
		return getInt(NUMBER_OF_AGENTS, 3);
	}

	/**
	 * Number of threads per Agent, default = 3. 
	 */
	public int getNumberOfThreadsPerAgent() {
		return getInt(NUMBER_OF_THREADS_PER_AGENT, 3);
	}

	/**
	 * Number of TP for an agent, default = 10000. 
	 */
	public int getNumberOfTP() {
		return getInt(NUMBER_OF_TERM_TP, 10000);
	}

	/**
	 * This is the threshold after which a TP has to be emptied and its 
	 * cooccur send to the broker, default = 100000.
	 */
	public int getTPThreshold() {
		return getInt(TP_THRESHOLD, 100000);
	}

	/**
	 * Number of collectors for each type, default = 3. 
	 */
	public int getNumberOfSameCollectors() {
		return getInt(NUMBER_OF_SAME_COLLECTORS, 3);
	}

	/**
	 * Number of different types of collectors, default = 3. 
	 */
	public int getNumberOfDifferentCollectors() {
		return getInt(NUMBER_OF_SAME_COLLECTORS, 3);
	}

	/**
	 * Number of threads per Agent, default = 3. 
	 */
	public int getNumberOfThreadsPerCollector() {
		return getInt(NUMBER_OF_THREADS_PER_COLLECTOR, 3);
	}

	public String getTwitterName() {
		return get(TWITTER_USER);
	}

	public String getTwitterPW() {
		return get(TWITTER_PW);
	}

	public String getTwitterURL() {
		return get(TWITTER_STREAM_URL,
		"http://stream.twitter.com/1/statuses/sample.json");
	}

	public int getMessageSize() {
		return getInt(NUMBER_OF_THREADS_PER_COLLECTOR, 10);
	}

	public Object getStreamSource() {
		// TODO Auto-generated method stub
		return null;
	}



}
