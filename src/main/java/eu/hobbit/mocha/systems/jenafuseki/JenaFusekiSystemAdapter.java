package eu.hobbit.mocha.systems.jenafuseki;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactoryHttp;
import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.jena.update.UpdateRequest;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.hobbit.mocha.systems.jenafuseki.util.JenaFusekiSystemAdapterConstants;

import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.atlas.web.auth.SimpleAuthenticator;
/**
 * Apache Jena Fuseki System Adapter class for all MOCHA tasks
 */
public class JenaFusekiSystemAdapter extends AbstractSystemAdapter {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JenaFusekiSystemAdapter.class);
	private QueryExecutionFactory queryExecFactory;
	private UpdateExecutionFactory updateExecFactory;
	
	private boolean dataLoadingFinished = false;
	
	private AtomicInteger totalReceived = new AtomicInteger(0);
	private AtomicInteger totalSent = new AtomicInteger(0);
	private Semaphore allDataReceivedMutex = new Semaphore(0);
	private Semaphore fusekiServerStartedMutex = new Semaphore(0);

	private int loadingNumber = 0;
	private AtomicBoolean fusekiServerStarted = new AtomicBoolean(false);
	private String datasetFolderName = "/myvol/datasets";
	
	public JenaFusekiSystemAdapter(int numberOfMessagesInParallel) {
		super(numberOfMessagesInParallel);
	}
	
	public JenaFusekiSystemAdapter() { }
	
	public void init() throws Exception {
		LOGGER.info("Initialization begins.");
		super.init();
		// create query factory
		queryExecFactory = new QueryExecutionFactoryHttp("http://localhost:3030/ds/query");
		// create update factory
		HttpAuthenticator auth = new SimpleAuthenticator("admin", "admin".toCharArray());
		updateExecFactory = new UpdateExecutionFactoryHttp("http://localhost:3030/ds/update", auth);
		LOGGER.info("Initialization is over.");
	}
	
	public void receiveGeneratedData(byte[] data) {
		if (dataLoadingFinished == false) {
			ByteBuffer dataBuffer = ByteBuffer.wrap(data);    	
			String fileName = RabbitMQUtils.readString(dataBuffer);
			
			LOGGER.info("Receiving file: " + fileName);
						
			byte [] content = new byte[dataBuffer.remaining()];
			dataBuffer.get(content, 0, dataBuffer.remaining());
			
			if (content.length != 0) {
				try {
					if (fileName.contains("/"))
						fileName = fileName.replaceAll("[^/]*[/]", "");
					FileUtils.writeByteArrayToFile(new File(datasetFolderName + File.separator + fileName), content);
				} catch (FileNotFoundException e) {
					LOGGER.error("Exception while writing data file", e);
				} catch (IOException e) {
					LOGGER.error("Exception while writing data file", e);
				}
			}

			if(totalReceived.incrementAndGet() == totalSent.get()) {
				allDataReceivedMutex.release();
			}
		}
		else {			
			ByteBuffer buffer = ByteBuffer.wrap(data);
			String insertQuery = RabbitMQUtils.readString(buffer); 
			
			// rewrite insert to let jena fuseki to create the appropriate graphs while inserting
			insertQuery = insertQuery.replaceFirst("INSERT", "").replaceFirst("WITH", "INSERT DATA { GRAPH");
			insertQuery = insertQuery.substring(0, insertQuery.length() - 13).concat(" }");
			
			UpdateRequest updateRequest = UpdateRequestUtils.parse(insertQuery);
			try {
				updateExecFactory.createUpdateProcessor(updateRequest).execute();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void receiveGeneratedTask(String taskId, byte[] data) {
		// before executing a task check if Fuseki Server is up and running and if not wait until it is
		if(!fusekiServerStarted.get()) {
			LOGGER.info("[Task] Waiting until Jena Fuseki Server is online...");
			try {
				fusekiServerStartedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for Fuseki Server to be started.", e);
			}
			LOGGER.info("Jena Fuseki Server started successfully.");
		}
		
		ByteBuffer buffer = ByteBuffer.wrap(data);
		String queryString = RabbitMQUtils.readString(buffer);
		long timestamp1 = System.currentTimeMillis();
		if (queryString.contains("INSERT DATA")) {
			// TODO: check the replacement
			UpdateRequest updateRequest = UpdateRequestUtils.parse(queryString);
			try {
				updateExecFactory.createUpdateProcessor(updateRequest).execute();
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				this.sendResultToEvalStorage(taskId, RabbitMQUtils.writeString(""));
			} catch (IOException e) {
				LOGGER.error("Got an exception while sending results.", e);
			}
		}
		else {
			QueryExecution qe = queryExecFactory.createQueryExecution(queryString);
			ResultSet results = null;
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

			try {
				results = qe.execSelect();
				ResultSetFormatter.outputAsJSON(outputStream, results);
			} catch (Exception e) {
				LOGGER.error("Problem while executing task " + taskId + ": " + queryString, e);
				//TODO: fix this hacking
				try {
					outputStream.write("{\"head\":{\"vars\":[\"xxx\"]},\"results\":{\"bindings\":[{\"xxx\":{\"type\":\"literal\",\"value\":\"XXX\"}}]}}".getBytes());
				} catch (IOException e1) {
					LOGGER.error("Problem while executing task " + taskId + ": " + queryString, e);
				}
			} finally {
				qe.close();
			}

			try {
				this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
			} catch (IOException e) {
				LOGGER.error("Got an exception while sending results.", e);
			}
		}
		long timestamp2 = System.currentTimeMillis();
		LOGGER.info("Task " + taskId + ": " + (timestamp2-timestamp1));
	}

	@Override
	public void receiveCommand(byte command, byte[] data) {
		if (JenaFusekiSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			int numberOfMessages = buffer.getInt();
			boolean lastBulkLoad = buffer.get() != 0;

			LOGGER.info("Bulk loading phase (" + loadingNumber + ") begins");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
			// release before acquire, so it can immediately proceed to bulk loading
			if(totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allDataReceivedMutex.release();
			}

			LOGGER.info("Wait for receiving all data for bulk load " + loadingNumber + ".");
			try {
				allDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for all data for bulk load " + loadingNumber + " to be recieved.", e);
			}
			LOGGER.info("All data for bulk load " + loadingNumber + " received. Proceed to the loading...");

			loadVersion("http://graph.version." + loadingNumber);

			LOGGER.info("Bulk loading phase (" + loadingNumber + ") is over.");
			
			LOGGER.info("Deleting all data files.");

			File theDir = new File(datasetFolderName);
			if(theDir.exists()) {
				for (File f : theDir.listFiles()) {
					f.delete();
				}
			}
			
			loadingNumber++;
			dataLoadingFinished = lastBulkLoad;
			LOGGER.info("dataLoadingFinished: " + dataLoadingFinished);
			
			// after all bulk load phases over start the Apache Jena Fuseki Server
			if(dataLoadingFinished && !fusekiServerStarted.get()) {
				fusekiServerStarted.set(startJenaFuseki());
				fusekiServerStartedMutex.release();
			}
			
			try {
				sendToCmdQueue(JenaFusekiSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		super.receiveCommand(command, data);
	}
	
	/*
	 * Load all files contained in data path using the tdbloader.
	 * tdbloader cannot be run in parallel with fuseki server (due to transaction issues), so the server
	 * have to be started after the loading phase.
	 */
	private void loadVersion(String graphURI) {
		LOGGER.info("Loading data on " + graphURI + "...");
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "scripts" + File.separator + "load.sh";
			String[] command = {"/bin/bash", scriptFilePath, datasetFolderName, graphURI};
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			LOGGER.info(graphURI + " loaded successfully.");
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}
	}
	

	private boolean startJenaFuseki() {
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "scripts" + File.separator + "fuseki-server_start.sh";
			String[] command = {"/bin/bash", scriptFilePath};
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for starting Fuseki Server.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for starting Fuseki Server.", e);
		}
		return true;
	}

	public void close() throws IOException {
		LOGGER.info("Stopping Apache Jena Fuseki.");
		try {
			queryExecFactory.close();
			updateExecFactory.close();
		} catch (Exception e) {
			LOGGER.error("Got an exception while closing query execution factories.", e);
		}
		super.close();
		LOGGER.info("Apache Jena Fuseki has stopped.");
	}

}
