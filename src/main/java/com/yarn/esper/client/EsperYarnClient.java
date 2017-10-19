package com.yarn.esper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONException;
import org.json.JSONObject;

public class EsperYarnClient {
	
	private static final Log LOG = LogFactory.getLog(EsperYarnClient.class);
	
	// Configuration
	private Configuration conf;
	private YarnClient yarnClient;
	// Application master specific info to register a new Application with RM/ASM
	private String appName;
	// App master priority
	private int amPriority;
	// Queue for App master
	private String amQueue;
	// Amt. of memory resource to request for to run the App Master
	private int amMemory;
	// Amt. of virtual core resource to request for to run the App Master
	private int amVCores;
	
	// Application master jar file
	private String appMasterJarPath;
	// Main class to invoke application master
	private String appMasterMainClass;
	
	// Amt of memory to request for container in which app will be executed
	private int containerMemory;
	// Amt. of virtual cores to request for container in which app will be executed
	private int containerVCores;
	// No. of containers in which the app needs to be executed
	private int containerPriority;
	
	// Start time for client
	private long clientStartTime;
	// Timeout threshold for client. Kill app after time interval expires.
	private long clientTimeOut;
	
	private String esperEngineJarPath;
	private String esperEngineMainClass;
	
	//Kafka server host
	private String kafkaServer;

	//event processing info
	private String nodes;
	
	public EsperYarnClient() {
		conf = new YarnConfiguration();
		conf.setStrings(YarnConfiguration.RM_HOSTNAME, "10.109.253.145");
		
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		
		appName = "";
		amPriority = 0;
		amQueue = "";
		amMemory = 16;
		amVCores = 1;
		
		appMasterJarPath = "";
		appMasterMainClass = "";
		
		containerMemory = 16;
		containerVCores = 1;
		containerPriority = 0;
		
		clientStartTime = System.currentTimeMillis();
		clientTimeOut = 60000;
		
		esperEngineJarPath = "";
		esperEngineMainClass = "";
		
		kafkaServer = "";

		nodes = "";
		
	}
	
	private boolean init(String json) throws JSONException {
		
		kafkaServer = "10.109.253.127:9092";
		
		JSONObject node = new JSONObject(json);
		
		nodes = "\'" + json.replace("\"", "%").replace("\'", "$") + "\'";
		
		appName = "esperApp";
		amPriority = 0;
		amQueue = "default";
		amMemory = 128;
		amVCores = 1;
		
		appMasterJarPath = "/usr/hadoop-yarn/apps/yarn-esper-application-master.jar";
		appMasterMainClass = "com.yarn.esper.app.EsperApplicationMaster";
		/*appMasterJarPath = "hdfs://10.109.253.145:9000/hadoop-yarn/YarnTest.jar";
		appMasterMainClass = "com.yarn.client.YarnApplicationMaster";*/
		
		containerMemory = 128;
		containerVCores = 1;
		containerPriority = 0;
		
		esperEngineJarPath = "/usr/esper/apps/esper-kafka-engine.jar";
		esperEngineMainClass = "com.esper.kafka.adapter.EsperKafkaAdapter";
		
		return true;
	}
	
	private boolean runAppMaster() throws YarnException, IOException {
		
		LOG.info("Running Client");
		yarnClient.start();

		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
	    LOG.info("Got Cluster metric info from ASM" 
	        + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

	    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
	        NodeState.RUNNING);
	    LOG.info("Got Cluster node info from ASM");
	    for (NodeReport node : clusterNodeReports) {
	      LOG.info("Got node report from ASM for"
	          + ", nodeId=" + node.getNodeId() 
	          + ", nodeAddress" + node.getHttpAddress()
	          + ", nodeRackName" + node.getRackName()
	          + ", nodeNumContainers" + node.getNumContainers());
	    }

	    QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
	    LOG.info("Queue info"
	        + ", queueName=" + queueInfo.getQueueName()
	        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
	        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
	        + ", queueApplicationCount=" + queueInfo.getApplications().size()
	        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());		

	    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
	    for (QueueUserACLInfo aclInfo : listAclInfo) {
	      for (QueueACL userAcl : aclInfo.getUserAcls()) {
	        LOG.info("User ACL Info for Queue"
	            + ", queueName=" + aclInfo.getQueueName()			
	            + ", userAcl=" + userAcl.name());
	      }
	    }
		
	    // Get a new application id
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		
		int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMemory);
		
		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
		
		if(amMemory>maxMemory){
			LOG.info("AM memory specified above max threshold of cluster. Using max value."
			          + ", specified=" + amMemory
			          + ", max=" + maxMemory);
			amMemory = maxMemory;
		}
		
		if(amVCores>maxVCores){
			LOG.info("AM virtual cores specified above max threshold of cluster. " 
			          + "Using max value." + ", specified=" + amVCores 
			          + ", max=" + maxVCores);
			amVCores = maxVCores;
		}

		// set the application name
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		appContext.setApplicationName(appName);
		
		// set local resources for the application master
		/*Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		LOG.info("Copy App Master jar from local filesystem and add to local environment");
		// Copy the application master jar to the filesystem
		FileSystem fs = FileSystem.get(conf);
		//addToLocalResources(fs, appMasterJarPath, localResources);
		Path amJarPath = new Path(appMasterJarPath);
		FileStatus jarStatus = fs.getFileStatus(amJarPath);
		LocalResource amJar = Records.newRecord(LocalResource.class);
		amJar.setResource(ConverterUtils.getYarnUrlFromPath(amJarPath));
		amJar.setSize(jarStatus.getLen());
		amJar.setTimestamp(jarStatus.getModificationTime());
		amJar.setType(LocalResourceType.FILE);
		amJar.setVisibility(LocalResourceVisibility.PUBLIC);
		localResources.put("amJar", amJar);*/

		// Set the env variables to be setup in the env where the application master will be run
	    LOG.info("Set the environment for the application master");
		Map<String, String> amEnv = new HashMap<String, String>();
		
		// Add AppMaster.jar location to classpath
		StringBuilder amClasspath = new StringBuilder(Environment.CLASSPATH.$$());
		amClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for(String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, 
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)){
			amClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			amClasspath.append(c.trim());
		}
		amClasspath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
		amClasspath.append(appMasterJarPath);
		amEnv.put("CLASSPATH", amClasspath.toString());
		
		LOG.info("Complete setting up app master env " + amClasspath.toString());
		
		// Set the necessary command to execute the application master
		List<String> commands = new ArrayList<String>();
		// Set java executable command 
		commands.add("$JAVA_HOME/bin/java");
		// Set Xmx based on am memory size
		commands.add("-Xmx" + amMemory + "M");
		// Set class name
		commands.add(appMasterMainClass);
		// Set params for Application Master
		commands.add("--container_memory " + String.valueOf(containerMemory));
		commands.add("--container_vcores " + String.valueOf(containerVCores));
		commands.add("--request_priority " + String.valueOf(containerPriority));
		commands.add("--esper_jar_path " + esperEngineJarPath);
		commands.add("--esper_main_class " + esperEngineMainClass);
		commands.add("--kafka_server " + kafkaServer);
		commands.add("--nodes " + nodes);
		
		LOG.info("Completed setting up app master command " + commands.toString());
		
		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance
				(null, amEnv, commands, null, null, null);
		appContext.setAMContainerSpec(amContainer);
		
		// Set up resource type requirements
		Resource capability = Resource.newInstance(amMemory, amVCores);
		appContext.setResource(capability);
		
		// Set the priority for the application master
		appContext.setPriority(Priority.newInstance(amPriority));
		
		// Set the queue to which this application is to be submitted in the RM
		appContext.setQueue(amQueue);
		
		// Submit the application to the applications manager
		LOG.info("Submitting application to ASM");
		yarnClient.submitApplication(appContext);

		return monitorApplication(appId); 
	}
	
	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		
		while(true){
			
			// Check app status every 1 second.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}
			
			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			LOG.info("Got application report from ASM for"
			          + ", appId=" + appId.getId()
			          + ", clientToAMToken=" + report.getClientToAMToken()
			          + ", appDiagnostics=" + report.getDiagnostics()
			          + ", appMasterHost=" + report.getHost()
			          + ", appQueue=" + report.getQueue()
			          + ", appMasterRpcPort=" + report.getRpcPort()
			          + ", appStartTime=" + report.getStartTime()
			          + ", yarnAppState=" + report.getYarnApplicationState().toString()
			          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
			          + ", appTrackingUrl=" + report.getTrackingUrl()
			          + ", appUser=" + report.getUser());
			
			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus status = report.getFinalApplicationStatus();

			if(state==YarnApplicationState.FINISHED){
				System.out.println(
				        "Application " + appId + " finished with" +
				    		" state " + state + 
				    		" at " + report.getFinishTime());
				System.out.println("status: " + status);
				if(status==FinalApplicationStatus.SUCCEEDED){
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				}
				else{
					LOG.info("Application did finished unsuccessfully."
				              + " YarnState=" + state.toString() + ", DSFinalStatus=" + status.toString()
				              + ". Breaking monitoring loop");
					return false;
				}
			}else if(state==YarnApplicationState.KILLED 
					|| state==YarnApplicationState.FAILED){
				System.out.println(
				        "Application " + appId + " finished with" +
				    		" state " + state + 
				    		" at " + report.getFinishTime());
				System.out.println("status: " + status);
				LOG.info("Application did not finish."
			            + " YarnState=" + state.toString() + ", DSFinalStatus=" + status.toString()
			            + ". Breaking monitoring loop");
				return false;
			}
				
			
			if(System.currentTimeMillis()>(clientStartTime+clientTimeOut)){
				LOG.info("Reached client specified timeout for application. Killing application");
				forceKillApplication(appId);
			}
		}
	}
	
	//Kill a submitted application by sending a call to the ASM
	private void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
		yarnClient.killApplication(appId);
	}
	
	public void runYarnClient(String json) {
		boolean result = false;
		
		try {
			LOG.info("Initializing Client");
			try {
				boolean doRun = init(json);
				if(!doRun){
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				// TODO: handle exception
				System.err.println(e.getLocalizedMessage());
				System.exit(-1);
			}
			
			result = runAppMaster();
		} catch (Throwable t) {
			// TODO: handle exception
			LOG.fatal("Error running Client", t);
			System.exit(1);
		}
		
		if(result){
			LOG.info("Application completed successfully");
			//System.exit(0);
		}else{
			LOG.error("Application failed to complete successfully");
			//System.exit(2);
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
