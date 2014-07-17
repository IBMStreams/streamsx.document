/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.document.extract;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.types.Blob;

/**
 * Document extraction core specific imports
 */
import com.ibm.streamsx.document.extract.core.Document;
import com.ibm.streamsx.document.extract.core.ExtractionService;
import com.ibm.streamsx.document.extract.core.OutputOptions;
import com.ibm.streamsx.document.extract.core.OutputOptions.TraceInfoLevel;
import com.ibm.streamsx.document.extract.core.configuration.ExtractionServiceConfiguration;
import com.ibm.streamsx.document.extract.core.exceptions.DECoreFileNotFoundException;
import com.ibm.streamsx.document.extract.core.exceptions.DECoreIOException;
import com.ibm.streamsx.document.extract.core.extractionResults.DocumentInfo;
import com.ibm.streamsx.document.extract.core.extractionResults.Message;
import com.ibm.streamsx.document.extract.core.extractionResults.Properties;
import com.ibm.streamsx.document.extract.core.extractionResults.ResultIterator;
import com.ibm.streamsx.document.extract.core.extractionResults.ResultIterator.CONTENT_TYPE;
import com.ibm.streamsx.document.extract.core.logging.DECoreLogger;


@PrimitiveOperator(name="DocumentSource", 
				  namespace="com.ibm.streamsx.document.extract", 
				  description="Java Operator DocumentSource",
				  // the parameter is a workaround for PDF Box bug on big documents
				  vmArgs={"-Djava.util.Arrays.useLegacyMergeSort=true"})
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"impl/lib/*", "config/","lib/*"})
public class DocumentSource extends AbstractOperator {

	/**
	 * Parameter names
	 */
	private  static final String CONFIG_FOLDER_PNAME = "configFolder";
	private  static final String EXTRACTION_TYPE_PNAME = "extractionType";
	private  static final String IN_FILE_PNAME = "file";
	private  static final String RETURN_FILE_DETAILS_PNAME = "returnFileDetails";
	private  static final String RETURN_EMBEDDED_DOCUMENTS_PNAME = "returnEmbeddedDocuments";
	private  static final String EXTRACTOR_NAME_PNAME = "extractorName";
	private  static final String RETURN_SYSTEM_MESSAGES_PNAME = "returnSystemMessages";
	
	
	private static final CharSequence EMBEDDED_SUBSTR = "EMBEDDED";
	private static final String CONFIG_FOLDER_DEFAULT = "../../../config";
	private static final EnumSupportedExtractionTypes EXTRACTION_TYPE_DEFAULT = EnumSupportedExtractionTypes.Metadata;
	 
	private String configFolder = null;
	private EnumSupportedExtractionTypes extractionType = null;
	private String file = null;
	private boolean returnFileDetails = false;
	private boolean returnEmbeddedDocuments = true;
	private boolean returnSystemMessages = false;
	
	private TraceInfoLevel deCoreLogLevel = TraceInfoLevel.ERROR;
	
	private ExtractionServiceConfiguration config= null;
	private ExtractionService service = null;
	private String extractorName = null;
	
	private final static Logger logger = Logger.getLogger(DocumentSource.class.getName());
	
	private int inPorts = 0;
	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */	
    private Thread processThread;
	

    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        Set<String> paramNames = context.getParameterNames();
        
    	configFolder = context.getParameterValues(CONFIG_FOLDER_PNAME).size() > 0 ? 
    						context.getParameterValues(CONFIG_FOLDER_PNAME).get(0):
    						CONFIG_FOLDER_DEFAULT;	
    						
    	extractionType = context.getParameterValues(EXTRACTION_TYPE_PNAME).size() > 0 ? 
				EnumSupportedExtractionTypes.fromString(context.getParameterValues(EXTRACTION_TYPE_PNAME).get(0)):
					EXTRACTION_TYPE_DEFAULT;	
			 
    	
    	// check parameter existance
    	returnFileDetails = paramNames.contains(RETURN_FILE_DETAILS_PNAME) ?
    		Boolean.valueOf(context.getParameterValues(RETURN_FILE_DETAILS_PNAME).get(0)) :
    		false;	
    	
    	returnEmbeddedDocuments = paramNames.contains(RETURN_EMBEDDED_DOCUMENTS_PNAME) ?
        		Boolean.valueOf(context.getParameterValues(RETURN_EMBEDDED_DOCUMENTS_PNAME).get(0)) :
        		false;	
    	
        extractorName = paramNames.contains(EXTRACTOR_NAME_PNAME) ?
        		context.getParameterValues(EXTRACTOR_NAME_PNAME).get(0) :
        		null;		

    	returnSystemMessages = paramNames.contains(RETURN_SYSTEM_MESSAGES_PNAME) ?
        		Boolean.valueOf(context.getParameterValues(RETURN_SYSTEM_MESSAGES_PNAME).get(0)) :
        		false;
        		

		logger.trace("Operator parametrized with configFolder '" 
    			+ configFolder + "','" 
    			+ "extractionType '" + extractionType + "',"
    			+ "returnFileDetails '" + returnFileDetails + "'," 
    			+ "returnEmbeddedDocuments '" + returnEmbeddedDocuments + "',"
    			+ "extractorName '" + extractorName + "',"
    			+ "returnSystemMessages '" + returnSystemMessages + "',"
    			);
    	
    		
    	// Initialize java logging
    	DECoreLogger.loadJavaLoggingConfiguration(configFolder);	
		
		config = new ExtractionServiceConfiguration(configFolder);
		service = new ExtractionService(config);
		
        inPorts = context.getNumberOfStreamingInputs();
        // source operator
        if (inPorts == 0) {
        	// file parameter is mandatory for source operator 
        	// with no input ports  only
        	file = context.getParameterValues(IN_FILE_PNAME).get(0);

        	/*
        	 * Create the thread for producing tuples if required. 
        	 * The thread is created at initialize time but not started.
        	 * The thread will be started by allPortsReady().
        	 */
			processThread = getOperatorContext().getThreadFactory().newThread(
	                new Runnable() {
	
	                    @Override
	                    public void run() {
	                        try {
	                            produceTuples();
	                        } catch (Exception e) {
	                            logger.error("Operator error", e);
	                            e.printStackTrace();
	                        }                    
	                    }
	                    
	                });
			/*
			 * Set the thread not to be a daemon to ensure that the SPL runtime
			 * will wait for the thread to complete before determining the
			 * operator is complete.
			 */
			processThread.setDaemon(false);
        }
        
        // set de core log level according to the operator log level
        Level effectiveOpLogLevel = logger.getEffectiveLevel();
    	logger.trace("Effective operator log level '"  + effectiveOpLogLevel + "'");

        deCoreLogLevel = toDECoreLogLevel(logger.getEffectiveLevel());
    	logger.trace("Effective DE core log level '"  + deCoreLogLevel + "'");
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    	if (inPorts == 0) {
	        // Start a thread for producing tuples because operator 
	    	// implementations must not block and must return control to the caller.
	        processThread.start();
    	}
    }
    
    /**
     * Process an incoming tuple that arrived on the specified port.
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port. 
     * </P>
     * @param inp-Dfoo=trueutStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public final void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {
    	
    	Document document = null;
    	String documentName = "";
    	Type.MetaType attrType = tuple.getStreamSchema().getAttribute(0).getType().getMetaType();
    	
    	synchronized (inputStream) {
	    	if (attrType == MetaType.RSTRING) {    	
		    	documentName = tuple.getString(new Integer(0));    	
		    	logger.trace("Tuple generation method started for document '" + documentName + "'");
		    	// assumes that the first attribute in the tuple contains 
		        // name of the file to read
		        try {
	        		document = new Document(documentName);
		        } catch(DECoreFileNotFoundException fnfe) {        	
		            logger.error("File '" + file + "' not found");	
		            throw new Exception(fnfe);
		        }
	    	}
	    	else if (attrType == MetaType.BLOB) {
	    		Blob rawDocument = tuple.getBlob(new Integer(0));
	    		InputStream inStream = rawDocument.getInputStream();
	   			document = new Document(inStream);
	    	}
	    	else {
		    	String errMsg = "Unexpected input schema. The operator expects first attribute to be BLOB or RSTRING only. Received " + attrType;
	    		logger.error(errMsg);
	    		throw new Exception(errMsg);
	    	}
	
	    	String documentId = document.getId();
	    	if (documentId == null || documentId.length() == 0) document.setId(document.getFilePath());
	    	if (extractorName != null) document.setRequestedExtractor(extractorName);
	    	logger.trace("An ID '" + document.getId() + "' has set for document with name '" + documentName + "'");	        	
	    	
	    	logger.trace("Document object has been created for file '" + documentName + "'");	
    	}
        try {
        	submitOutTuple(document, false);
        } catch (DECoreIOException deCoreException) {
            logger.error("DE Core IO exception: " + deCoreException);	
            throw new Exception(deCoreException);        	
        } catch (OutOfMemoryError outOfMemoryError) {
            logger.error("DE Core IO error: " + outOfMemoryError);	
            throw new Exception(outOfMemoryError);        	
        }
    }
    
    /**
     * Submit new tuples to the output stream
     * @throws Exception if an error occurs while submitting a tuple
     */
    private void produceTuples() throws Exception  {
    	logger.trace("Tuple generation method started");
    	logger.trace("About to create document object for file '" + file + "'");
    	
        Document document = null;
        String documentPath = null;
        try {
        	document = new Document(file);        	
        	documentPath = document.getFilePath();
        	String documentId = document.getId();
        	if ((documentId == null) || (documentId.length() == 0)) document.setId(documentPath);
        	if (extractorName != null) document.setRequestedExtractor(extractorName);
	    	logger.trace("An ID '" + document.getId() + "' has set for document with name '" + documentPath + "'");	        	
        } catch(DECoreFileNotFoundException fnfe) {        	
            logger.error("File '" + file + "' not found");	
            throw new Exception(fnfe);
        }
        logger.trace("Document object has been created for file '" + file + "'");	
                
        try {
        	submitOutTuple(document, true);
        } catch (DECoreIOException deCoreException) {
            logger.error("DE Core IO exception: " + deCoreException);	
            throw new Exception(deCoreException);        	
        } catch (OutOfMemoryError outOfMemoryError) {
            logger.error("DE Core IO error: " + outOfMemoryError);	
            throw new Exception(outOfMemoryError);        	
        } catch (Exception e) {
        	logger.error("Error occured during text extraction fot document '" + file + "': " + e.getMessage());
        	throw new Exception(e);
        }
    }

    private void submitOutTuple(Document document, boolean submitFinal) throws Exception {
    	
    	final StreamingOutput<OutputTuple> out = getOutput(0);
    	
    	OutputOptions options  = new OutputOptions();
    	synchronized(document) {    	
		 	if (extractionType.equals(EnumSupportedExtractionTypes.Metadata)) {
		 		options.setReturnExtractedText(false);
		 	}
		 	options.setReturnFileDetails(returnFileDetails);
		 	// the parameter defines  
	 	 	// which DE core messages would be returned by DE Core
	 	 	// Note, that INFO should be the highest level
	 	 	// returned. Otherwise document id will become
	 	 	// invisible.
	 	 	options.setTraceInfoLevel(TraceInfoLevel.INFO);
	 	 	// make configurable
	 	 	options.setReturnEmbeddedDocument(returnEmbeddedDocuments);
    	}
    	
    	// Main extraction method
	 	InputStream in = null;
 		in = service.extractDocument(document, options);
		logger.trace("Document loaded sucessfully");			
		
		// Iterating over the response and initializing an output tuple
		ResultIterator resIter = new ResultIterator (in, service);
		String line;
		StringBuffer textBuffer = new StringBuffer();
		String documentId = null;
		List<Message> msgs = null;
		Properties props = null;
        OutputTuple outTuple = null;
        String title = null, contentType = null;
        Date modificationDate = null;        
        
		while (resIter.hasCurrentElement()) {
			
			CONTENT_TYPE t = resIter.getCurrentElementType();
			
			// Current element is a document
			if (t == CONTENT_TYPE.DOCUMENT) {
				//System.out.println("submitOutTuple : content type DOCUMENT");
			    DocumentInfo docInfo = resIter.getCurrentDocumentElement();
			    msgs = docInfo.getMessages();
			    props = docInfo.getProperties();
			    documentId = docInfo.getDocumentId();			    
			    if ((documentId == null) || (documentId.length() == 0)) {					    	
			    	documentId = genDocId(docInfo);
			    }
			    // workaround for detection if document is embedded
			    if (documentId.contains(EMBEDDED_SUBSTR)) {
			    	// extend root document name with name and mimetype to make sure that the name is unique
			    	contentType = props.getContentType();			    	
			    	title = props.getTitle();
			    	modificationDate = props.getModificationDate();
			    	if (contentType != null) documentId +=  "-" + contentType;
			    	if (title != null) documentId +=  "-" + title;
			    	if (modificationDate != null) documentId += "-" + modificationDate; 
			    }
			    logger.trace("Populating out tuple with metadata for document id '" + documentId + "'");	

	        	outTuple = out.newTuple();
	        	outTuple.setString("docId", documentId);		        
		        
				
		        if (returnSystemMessages) {
	        		outTuple.setList("msgs", msgListToStrList(msgs));
		        }
				
		        if (extractionType.equals(EnumSupportedExtractionTypes.All) || 
		        	extractionType.equals(EnumSupportedExtractionTypes.Metadata)) {
					// set metadata properties
		        	if (props != null) {
	        			outTuple.setMap("props", props.getAllProperties());
					}
				}
		        
				if ( extractionType.equals(EnumSupportedExtractionTypes.Metadata) ||
				    ((props != null) && (props.getEmbeddedFilesNumber() != null) && (props.getEmbeddedFilesNumber()  > 0))) {
					
					logger.trace("Submitting document metadata '" + outTuple + "'");
					// submit out tuple
					out.submit(outTuple);
				}
			}
			// Current element is text
			else if (t == CONTENT_TYPE.TEXT && !extractionType.equals(EnumSupportedExtractionTypes.Metadata)) { 				
			    logger.trace("Populating out tuple with text for document id '" + documentId + "'");	

				InputStream txt = resIter.getCurrentTextElement();
			    BufferedReader br = new BufferedReader(new InputStreamReader(txt));
				while ((line=br.readLine())!=null) {
					textBuffer.append(line);
				}
				
				outTuple.setString("text", textBuffer.toString());
				logger.trace("Submitting document '" + outTuple + "'");
				out.submit(outTuple);
			
					

				// clear buffer
				textBuffer.delete(0, textBuffer.length());
			}


			// advance the iterator
			resIter.advance();
		}
		
        // Submit a window and final punctuation when finished        
        if (submitFinal) {
        	logger.trace("Complete - sending window & final marks");
            out.punctuate(Punctuation.WINDOW_MARKER);
        	out.punctuate(Punctuation.FINAL_MARKER);
        }
	     
	}
    
    @ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {
		// TODO!
    }
    
    @ContextCheck(compile = false)
	public static void checkParameters(OperatorContextChecker checker)
			throws Exception {
		// check that extracted entity fits appropriate enum    	
    	List<String> extractionTypes = checker.getOperatorContext().getParameterValues(EXTRACTION_TYPE_PNAME);
    	String extractionType = null;
    	if (extractionTypes != null) {
	    	if (extractionTypes.isEmpty()) {
	    		throw new Exception(
						"Operator parameter extractionType should not be empty");
	    	}
			 else {
				 try {
					 extractionType = extractionTypes.get(0);
					 EnumSupportedExtractionTypes.valueOf(extractionType);					 
				 } catch (IllegalArgumentException e) {
					 checker.setInvalidContext("Invalid value for operator parameter extractionType '" + extractionType + "'. Supported extractionType values are '" + 
							 EnumSupportedExtractionTypes.allToString() + "'",
							 null);				 				 
				 }
			 }
		 }  
    	 
	}
	
    
    /**
     * Shutdown this operator, which will interrupt the thread
     * executing the <code>produceTuples()</code> method.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        if (processThread != null) {
            processThread.interrupt();
            processThread = null;
        }
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
                
    	if (service != null) {
    		service.shutdown(false);
    	}
        // Must call super.shutdown()
        super.shutdown();
    }
    
    /**
     * Generated document id based on some document properties
     */
    private synchronized String genDocId(DocumentInfo docInfo) {
    	StringBuffer docId = new StringBuffer();
    	Properties docProps = docInfo.getProperties();
    	String fileLocation = docProps.getFileLocation();
    	if (fileLocation != null) {
    		docId.append(fileLocation + "/");
    	}
    	String fileName = docProps.getFileName();
    	if (fileName != null) {
    		docId.append(fileName);
    	}
    	
    	// unknown file location - generate document id from available metadata
    	if (docId.length() == 0) {
    		String docTitle = docProps.getTitle();
    		if (docTitle != null) {
    			docId.append(docProps.getTitle());
    		} else {
    			docId.append(docInfo.getProperties());    			
    		}  
    	}
    	
    	return docId.toString();
    }

    private List<String> msgListToStrList(List<Message> msgs) {
    	List<String> res = new ArrayList<String>(msgs.size());
    	Iterator<Message> iterator = msgs.iterator();
    	while (iterator.hasNext()) {
    		res.add((iterator.next()).toString());
    	}
    	
    	return res;
    }

    private TraceInfoLevel toDECoreLogLevel(Level level) {
    	
    	switch(level.toInt()) {
    		case Level.TRACE_INT:
    		case Level.DEBUG_INT:
    			return TraceInfoLevel.DEBUG;
    		case Level.ERROR_INT:
    		case Level.FATAL_INT: 
    			return TraceInfoLevel.ERROR;
    		case Level.INFO_INT: 
    			return TraceInfoLevel.INFO;
    		case Level.WARN_INT:
    			return TraceInfoLevel.WARNING;	
    		default:
    			return TraceInfoLevel.ERROR;	
    	}
    }
    
    @Parameter(name = "configFolder", description = "", optional = true)
    public void setConfigFolder(String configFolder) {
    	this.configFolder  = configFolder;
    }
    
    public String getConfigFolder() {
    	return this.configFolder;
    }

    @Parameter(name = "extractionType", description = "", optional = true)
    public void setExtractionType(String extractionType) {
    	this.extractionType  = EnumSupportedExtractionTypes.fromString(extractionType);
    }
    
    public String getExtractionTYpe() {
    	return this.extractionType.toString();
    }
    
    @Parameter(name = "file", description = "", optional = true)
    public void setFile(String file) {
    	this.file  = file;
    }

    public String getFile() {
    	return this.file;
    }

    @Parameter(name = "returnFileDetails", description = "", optional = true)
    public void setReturnFileDetials(boolean returnFileDetails) {
    	this.returnFileDetails  = returnFileDetails;
    }

    public boolean getReturnFileDetails() {
    	return this.returnFileDetails;
    }

    @Parameter(name = "returnSystemMessages", description = "", optional = true)
    public void setReturnSystemMessages(boolean returnSystemMessages) {
    	this.returnSystemMessages  = returnSystemMessages;
    }

    public boolean getReturnSystemMessages() {
    	return this.returnSystemMessages;
    }

    @Parameter(name = "returnEmbeddedDocuments", description = "", optional = true)
    public void setReturnEmbeddedDocuments(boolean returnEmbeddedDocuments) {
    	this.returnEmbeddedDocuments  = returnEmbeddedDocuments;
    }

    public boolean getReturnEmbeddedDocuments() {
    	return this.returnFileDetails;
    }
    
    @Parameter(name = "extractorName", description = "", optional = true)
    public void setExtractorName(String extractorName) {
    	this.extractorName  = extractorName;
    }

    public String getExtractorName() {
    	return this.extractorName;
    }
 
    
    
    enum EnumSupportedExtractionTypes {
    	Text("Text"), Metadata("Metadata"), All("All");
    	
    	private String txt;

    	EnumSupportedExtractionTypes(String txt) {
    		this.txt = txt;
    	}

    	public String toString() {
    		return this.txt;
    	}

    	public static EnumSupportedExtractionTypes fromString(String text) {
    		if (text != null) {
    			for (EnumSupportedExtractionTypes b : EnumSupportedExtractionTypes.values()) {
    				if (text.equalsIgnoreCase(b.txt)) {
    					return b;
    				}
    			}
    		}
    		return null;
    	}
    
    	public boolean equals(DocumentSource.EnumSupportedExtractionTypes other) {		
    		return this.txt.equalsIgnoreCase(other.toString());    		
    	}
    
    	public static String allToString() {    		
    		StringBuffer res = new StringBuffer();
    		int i = 0;
			for (EnumSupportedExtractionTypes b : EnumSupportedExtractionTypes.values()) {
				if (i > 0) res.append(",");
				res.append(b);
				i++;
			}
    		
    		
    		return res.toString();
    	}
    }

}