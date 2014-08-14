/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.document.extract;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
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
import com.ibm.streamsx.document.extract.core.logging.DECoreLogger;
/**
 * Document extraction core specific imports
 */

/**
 * Class for an operator that receives a tuple and then optionally submits a
 * tuple. This pattern supports one or more input streams and one or more output
 * streams.
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to
 * process and submit tuples</li>
 * <li>process() handles a tuple arriving on an input port
 * <li>processPuncuation() handles a punctuation mark arriving on an input port
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any
 * time, such as a request to stop a PE or cancel a job. Thus the shutdown() may
 * occur while the operator is processing tuples, punctuation marks, or even
 * during port ready notification.</li>
 * </ul>
 * <p>
 * With the exception of operator initialization, all the other events may occur
 * concurrently with each other, which lead to these methods being called
 * concurrently by different threads.
 * </p>
 */
@PrimitiveOperator(name = "DocumentSource", namespace = "com.ibm.streamsx.document.extract", description = DocumentSourceConstants.OPERATOR_DESC,
// the parameter is a workaround for PDF Box bug on big documents
vmArgs = { "-Djava.util.Arrays.useLegacyMergeSort=true" })
@InputPortSet(description = "Optional port for document's ingestion", optional = true, cardinality=1, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious)
@OutputPortSet(description = "Mandatory output port for metadata and text content", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating)
@Libraries({ "impl/lib/*", "impl/ext/lib/*", "config/", "lib/*" })
public class DocumentSource extends AbstractOperator {

	

	private String configFolder = null;
	private EnumSupportedExtractionTypes extractionType = null;
	private String file = null;
	private boolean returnFileDetails = false;
	private boolean returnEmbeddedDocuments = true;
	private boolean returnSystemMessages = false;
	private String binaryDocumentAttr = null;
	private String extractedDocumentAttr = null;

	private TraceInfoLevel deCoreLogLevel = TraceInfoLevel.ERROR;

	private ExtractionServiceConfiguration config = null;
	private ExtractionService service = null;
	private String extractorName = null;

	private final static Logger logger = Logger.getLogger(DocumentSource.class.getName());

	private int inPorts = 0;
	
	
	// Thread for calling <code>produceTuples()</code> to produce tuples
	private Thread processThread;

	
	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		logger.trace("Operator " + context.getName() + " initializing in PE: "
				+ context.getPE().getPEId() + " in Job: "
				+ context.getPE().getJobId());

		configFolder = DocumentSourceUtils.getStringParamValue(context,
				DocumentSourceConstants.CONFIG_FOLDER_PNAME,
				DocumentSourceUtils.getDefaultConfigFolderPath());
		extractionType = EnumSupportedExtractionTypes.fromString(DocumentSourceUtils
				.getStringParamValue(context,
						DocumentSourceConstants.EXTRACTION_TYPE_PNAME,
						DocumentSourceConstants.EXTRACTION_TYPE_DEFAULT));
		returnFileDetails = DocumentSourceUtils.getBoolParamValue(
				context, DocumentSourceConstants.RETURN_FILE_DETAILS_PNAME,
				false);
		returnEmbeddedDocuments = DocumentSourceUtils.getBoolParamValue(
				context, DocumentSourceConstants.RETURN_EMBEDDED_DOCUMENTS_PNAME,
				false);			
		extractorName = DocumentSourceUtils.getStringParamValue(context,
				DocumentSourceConstants.EXTRACTOR_NAME_PNAME, null);		
		returnSystemMessages = DocumentSourceUtils.getBoolParamValue(
				context, DocumentSourceConstants.RETURN_SYSTEM_MESSAGES_PNAME,
				false);
				
		logger.trace("Operator parametrized with configFolder = '" + configFolder
				+ "','" + "extractionType='" + extractionType + "',"
				+ "returnFileDetails='" + returnFileDetails + "',"
				+ "returnEmbeddedDocuments='" + returnEmbeddedDocuments + "',"
				+ "extractorName='" + extractorName + "',"
				+ "returnSystemMessages='" + returnSystemMessages + "'");

		// Initialize jpava logging
		DECoreLogger.loadJavaLoggingConfiguration(configFolder);

		config = new ExtractionServiceConfiguration(configFolder);
		service = new ExtractionService(config);

		inPorts = context.getNumberOfStreamingInputs();
		// source operator
		if (inPorts == 0) {
			// file parameter is mandatory for source operator
			// with no input ports only
			file = DocumentSourceUtils.getStringParamValue(context, DocumentSourceConstants.IN_FILE_PNAME, null);
			// check input file existance
			DocumentSourceUtils.checkFileExistance(file);

			/*
			 * Create the thread for producing tuples if required. The thread is
			 * created at initialize time but not started. The thread will be
			 * started by allPortsReady().
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
		logger.trace("Effective operator log level '" + effectiveOpLogLevel
				+ "'");

		deCoreLogLevel = toDECoreLogLevel(logger.getEffectiveLevel());
		logger.trace("Effective DE core log level '" + deCoreLogLevel + "'");
	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName()
				+ " all ports are ready in PE: " + context.getPE().getPEId()
				+ " in Job: " + context.getPE().getJobId());
		if (inPorts == 0) {
			// Start a thread for producing tuples because operator
			// implementations must not block and must return control to the
			// caller.
			processThread.start();
		}
	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 * <P>
	 * Copy the incoming tuple to a new output tuple and submit to the output
	 * port.
	 * </P>
	 * 
	 * @param inp
	 *            -Dfoo=trueutStream Port the tuple is arriving on.
	 * @param tuple
	 *            Object representing the incoming tuple.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple)
			throws Exception {

		Document document = null;
		String documentName = "";
		StreamSchema streamSchema = tuple.getStreamSchema();
		Type.MetaType attrType = streamSchema.getAttribute(0).getType()
				.getMetaType();
		int attrCount = streamSchema.getAttributeCount();

		synchronized (inputStream) {
			if ((attrType == Type.MetaType.RSTRING) && (attrCount == 1)) {
				documentName = tuple.getString(new Integer(0));
				logger.trace("Tuple generation method started for document '"
						+ documentName + "'");
				// assumes that the first attribute in the tuple contains
				// name of the file to read
				try {
					document = new Document(documentName);
				} catch (DECoreFileNotFoundException fnfe) {
					logger.error("File '" + file + "' not found");
					throw new Exception(fnfe);
				}
			} else if ((attrType == MetaType.BLOB) && (attrCount == 1)) {
				Blob rawDocument = tuple.getBlob(new Integer(0));
				InputStream inStream = rawDocument.getInputStream();
				document = new Document(inStream);
			}
			else {
				logger.trace("Tuple generation method started for a binary document residing in the attribute '"
						+ this.binaryDocumentAttr + "'");
				Blob rawDocument = tuple.getBlob(this.binaryDocumentAttr);
				document = new Document(rawDocument.getInputStream());
			}
			String documentId = document.getId();
			if (documentId == null || documentId.length() == 0)
				document.setId(document.getFilePath());
			if (extractorName != null)
				document.setRequestedExtractor(extractorName);

			logger.trace("An ID '" + document.getId()
					+ "' has set for document with name '" + documentName + "'");
			logger.trace("Document object has been created for file '"
					+ documentName + "'");
		}
		try {
			submitOutTuple(document, false, tuple);
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
	 * 
	 * @throws Exception
	 *             if an error occurs while submitting a tuple
	 */
	private void produceTuples() throws Exception {
		logger.trace("Tuple generation method started");
		logger.trace("About to create document object for file '" + file + "'");

		Document document = null;
		String documentPath = null;
		try {
			document = new Document(file);
			documentPath = document.getFilePath();
			String documentId = document.getId();
			if ((documentId == null) || (documentId.length() == 0))
				document.setId(documentPath);
			if (extractorName != null)
				document.setRequestedExtractor(extractorName);
			logger.trace("An ID '" + document.getId()
					+ "' has set for document with name '" + documentPath + "'");
		} catch (DECoreFileNotFoundException fnfe) {
			logger.error("File '" + file + "' not found");
			throw new Exception(fnfe);
		}
		logger.trace("Document object has been created for file '" + file + "'");

		try {
			submitOutTuple(document, true, null);
		} catch (DECoreIOException deCoreException) {
			logger.error("DE Core IO exception: " + deCoreException);
			throw new Exception(deCoreException);
		} catch (OutOfMemoryError outOfMemoryError) {
			logger.error("DE Core IO error: " + outOfMemoryError);
			throw new Exception(outOfMemoryError);
		} catch (Exception e) {
			logger.error("Error occured during text extraction fot document '"
					+ file + "': " + e.getMessage());
			throw new Exception(e);
		}
	}

	private void submitOutTuple(Document document, boolean submitFinal,
			Tuple tuple) throws Exception {

		StreamingOutput<OutputTuple> out = getOutput(0);
		OutputOptions options = new OutputOptions();
		synchronized (document) {
			if (this.extractionType.equals(EnumSupportedExtractionTypes.Metadata)) {
				options.setReturnExtractedText(false);
			}
			options.setReturnFileDetails(this.returnFileDetails);
			options.setTraceInfoLevel(OutputOptions.TraceInfoLevel.INFO);
			options.setReturnEmbeddedDocument(this.returnEmbeddedDocuments);
		}
		logger.trace("About to extract document with options (subset): " + 
					 "returnFileDetails: '" + options.getReturnFileDetails() + "'" + 
					 ",returnEmbeddedDocuments: '" + options.getReturnEmbeddedDocuments() + "'");
		InputStream in = this.service.extractDocument(document, options);
		logger.trace("Document loaded sucessfully");

		ResultIterator resIter = new ResultIterator(in, this.service);

		StringBuffer textBuffer = new StringBuffer();
		String documentId = null;
		List<Message> msgs = null;
 		Properties props = null;
		OutputTuple outTuple = null;
		String title = null;
		String contentType = null;
		Date modificationDate = null;
		Map<String, Object> documentData = new HashMap<String, Object>();
		while (resIter.hasCurrentElement()) {
			ResultIterator.CONTENT_TYPE t = resIter.getCurrentElementType();
			if (t == ResultIterator.CONTENT_TYPE.DOCUMENT) {
				DocumentInfo docInfo = resIter.getCurrentDocumentElement();
				msgs = docInfo.getMessages();
				props = docInfo.getProperties();
				documentId = docInfo.getDocumentId();
				if ((documentId == null) || (documentId.length() == 0)) {
					documentId = genDocId(docInfo);
				}
				if (documentId.contains(DocumentSourceConstants.EMBEDDED_SUBSTR)) {
					contentType = props.getContentType();
					title = props.getTitle();
					modificationDate = props.getModificationDate();
					if (contentType != null) {
						documentId = documentId + "-" + contentType;
					}
					if (title != null) {
						documentId = documentId + "-" + title;
					}
					if (modificationDate != null) {
						documentId = documentId + "-" + modificationDate;
					}
				}
				logger.trace("Populating out tuple with metadata for document id '" + documentId + "'");
				outTuple = (OutputTuple) out.newTuple();
				logger.trace("Output tuple has been created succesfully");
				documentData.put("docId", new RString(documentId));
				if (this.returnSystemMessages) {
					documentData.put("msgs", msgListToStrList(msgs));
				}
				logger.trace("Output tuple populated with messages  '"
						+ msgListToStrList(msgs) + "'");
				if (this.extractionType.equals(EnumSupportedExtractionTypes.All) ||
					this.extractionType.equals(EnumSupportedExtractionTypes.Metadata)) {
					if (props != null) {
							documentData.put("props", props.getAllProperties());
					}
				}
				logger.trace("Output tuple populated with properties  '"
						+ props.getAllProperties() + "'");
				if ((this.extractionType
						.equals(EnumSupportedExtractionTypes.Metadata))
						|| ((props != null)
								&& (props.getEmbeddedFilesNumber() != null) && (props
								.getEmbeddedFilesNumber().intValue() > 0))) {
						logger.trace("Submitting document metadata '" + outTuple + "'");
						DocumentSourceUtils.populateOutTuple(this.extractedDocumentAttr, outTuple,tuple, documentData);
						logger.trace("Extract document tuple has been created succesfully...");
						out.submit(outTuple);
				}
			} else if (t == ResultIterator.CONTENT_TYPE.TEXT) {
				if (!this.extractionType
						.equals(EnumSupportedExtractionTypes.Metadata)) {
					logger.trace("Populating out tuple with text for document id '"
							+ documentId + "'");

					InputStream txt = resIter.getCurrentTextElement();
					BufferedReader br = new BufferedReader(new InputStreamReader(txt));
					String line;
					while ((line = br.readLine()) != null) {
						textBuffer.append(line);
					}
					documentData.put("text", textBuffer.toString());
					DocumentSourceUtils.populateOutTuple(this.extractedDocumentAttr, outTuple,tuple, documentData);
					logger.trace("Extract document tuple has been created succesfully...");
					logger.trace("Submitting document '" + outTuple + "'");
					out.submit(outTuple);
					
					textBuffer.delete(0, textBuffer.length());
				}
			}
			resIter.advance();
		}
		if (submitFinal) {
			logger.trace("Complete - sending window & final marks");
			out.punctuate(StreamingData.Punctuation.WINDOW_MARKER);
			out.punctuate(StreamingData.Punctuation.FINAL_MARKER);
		}
	}

	@ContextCheck(compile = false)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {

		OperatorContext context = checker.getOperatorContext();

		int inPorts = context.getNumberOfStreamingInputs();
		if (inPorts == 1) {
			StreamSchema inputSchema = ((StreamingInput) checker
					.getOperatorContext().getStreamingInputs().get(0))
					.getStreamSchema();
			if (inputSchema.getAttributeCount() == 1) {
				Type.MetaType attrType = inputSchema.getAttribute(0).getType()
						.getMetaType();
				if (Type.MetaType.RSTRING != attrType
						&& Type.MetaType.BLOB != attrType) {
					checker.setInvalidContext(
							"Expected attribute of type RSTRING or BLOB on input port, found attribute of type "
									+ inputSchema.getAttribute(0).getType()
											.getMetaType(), null);
				}
			} else if (context.getParameterValues("binaryDocumentAttr")
					.isEmpty()) {
				checker.setInvalidContext(
						"Parameter with name binaryDocumentAttr should be specified for DocumentSource operator",
						null);
			} else {
				String documentAttrName = (String) context.getParameterValues(
						"binaryDocumentAttr").get(0);
				if (!checker.checkRequiredAttributes(inputSchema,
						new String[] { documentAttrName })) {
					checker.setInvalidContext("Expected attribute with name "
							+ documentAttrName + " on input port. ", null);
				} else if (Type.MetaType.BLOB != inputSchema
						.getAttribute(documentAttrName).getType().getMetaType()) {
					checker.setInvalidContext("Attribute "
							+ documentAttrName
							+ " should has a BLOB type, found "
							+ documentAttrName
							+ " attribute of type "
							+ inputSchema.getAttribute(0).getType()
									.getMetaType(), null);
				}
			}
		}
	}

	@ContextCheck(compile = false)
	public static void checkParameters(OperatorContextChecker checker)
			throws Exception {
		// check that extracted entity fits appropriate enum
		List<String> extractionTypes = checker.getOperatorContext()
				.getParameterValues(
						DocumentSourceConstants.EXTRACTION_TYPE_PNAME);
		String extractionType = null;
		if (extractionTypes != null) {
			if (extractionTypes.isEmpty()) {
				throw new Exception(
						"Operator parameter extractionType should not be empty");
			} else {
				try {
					extractionType = extractionTypes.get(0);
					EnumSupportedExtractionTypes.valueOf(extractionType);
				} catch (IllegalArgumentException e) {
					checker.setInvalidContext(
							"Invalid value for operator parameter extractionType '"
									+ extractionType
									+ "'. Supported extractionType values are '"
									+ EnumSupportedExtractionTypes
											.allToString() + "'", null);
				}
			}
		}

	}

	@OperatorContext.ContextCheck(compile = false)
	public static void checkOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		if (context.getNumberOfStreamingOutputs() == 1) {
			StreamingOutput<OutputTuple> streamingOutputPort = (StreamingOutput) context
					.getStreamingOutputs().get(0);
			StreamSchema outputSchema = streamingOutputPort.getStreamSchema();
			int outPortAttrsCount = outputSchema.getAttributeCount();
			if (!DocumentSourceConstants.DOCUMENT_SPL_SCHEMA
					.equals(outputSchema.getLanguageType())) {
				for (int i = 0; i < outPortAttrsCount; i++) {
					if (context.getParameterValues("extractedDocumentAttr")
							.isEmpty()) {
						checker.setInvalidContext(
								"Parameter with name extractedDocumentAttr should be specified for DocumentSource operator",
								null);
					} else {
						String extractedDocumentAttrName = (String) context
								.getParameterValues("extractedDocumentAttr")
								.get(0);
						if (!checker.checkRequiredAttributes(outputSchema,
								new String[] { extractedDocumentAttrName })) {
							checker.setInvalidContext(
									"Expected attribute with name "
											+ extractedDocumentAttrName
											+ " on ouput port. ", null);
						} else if (!DocumentSourceConstants.DOCUMENT_SPL_SCHEMA
								.equals(outputSchema
										.getAttribute(extractedDocumentAttrName)
										.getType().getLanguageType())) {
							checker.setInvalidContext(
									"Attribute "
											+ extractedDocumentAttrName
											+ " should has a Document_t type schema, found "
											+ extractedDocumentAttrName
											+ " attribute of type "
											+ outputSchema
													.getAttribute(
															extractedDocumentAttrName)
													.getType()
													.getLanguageType(), null);
						}
					}
				}
			}
		}
	}

	/**
	 * Shutdown this operator, which will interrupt the thread executing the
	 * <code>produceTuples()</code> method.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
		if (processThread != null) {
			processThread.interrupt();
			processThread = null;
		}
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " shutting down in PE: "
				+ context.getPE().getPEId() + " in Job: "
				+ context.getPE().getJobId());

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

	//private List<String> msgListToStrList(List<Message> msgs) {
	private String[] msgListToStrList(List<Message> msgs) {
		//List<String> res = new ArrayList<String>(msgs.size());
		String[] res = new String[msgs.size()];
		Iterator<Message> iterator = msgs.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			//res.add((iterator.next()).toString());
			res[i++] = (iterator.next()).toString();
		}
		
		return res;
	}

	private TraceInfoLevel toDECoreLogLevel(Level level) {

		switch (level.toInt()) {
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

	@Parameter(name = "configFolder", description = DocumentSourceConstants.CONFIG_FOLDER_PARAM_DESCRIPTION, optional = true)
	public void setConfigFolder(String configFolder) {
		this.configFolder = configFolder;
	}

	public String getConfigFolder() {
		return this.configFolder;
	}

	@Parameter(name = "extractionType", description = DocumentSourceConstants.EXTRACTION_TYPE_PARAM_DESCRIPTION, optional = true)
	public void setExtractionType(String extractionType) {
		this.extractionType = EnumSupportedExtractionTypes
				.fromString(extractionType);
	}

	public String getExtractionTYpe() {
		return this.extractionType.toString();
	}

	@Parameter(name = "file", description = DocumentSourceConstants.FILE_PARAM_DESCRIPTION, optional = true)
	public void setFile(String file) {
		this.file = file;
	}

	public String getFile() {
		return this.file;
	}

	@Parameter(name = "returnFileDetails", description = DocumentSourceConstants.RET_FILE_DETAILS_PARAM_DESCRIPTION, optional = true)
	public void setReturnFileDetials(boolean returnFileDetails) {
		this.returnFileDetails = returnFileDetails;
	}

	public boolean getReturnFileDetails() {
		return this.returnFileDetails;
	}

	@Parameter(name = "returnSystemMessages", description = DocumentSourceConstants.RET_SYS_MESSAGES_PARAM_DESCRIPTION, optional = true)
	public void setReturnSystemMessages(boolean returnSystemMessages) {
		this.returnSystemMessages = returnSystemMessages;
	}

	public boolean getReturnSystemMessages() {
		return this.returnSystemMessages;
	}

	@Parameter(name = "returnEmbeddedDocuments", description = DocumentSourceConstants.RET_EMBEDDED_DOCS_PARAM_DESCRIPTION, optional = true)
	public void setReturnEmbeddedDocuments(boolean returnEmbeddedDocuments) {
		this.returnEmbeddedDocuments = returnEmbeddedDocuments;
	}

	public boolean getReturnEmbeddedDocuments() {
		return this.returnFileDetails;
	}

	@Parameter(name = "extractorName", description = DocumentSourceConstants.EXTRACTOR_NAME_PARAM_DESCRIPTION, optional = true)
	public void setExtractorName(String extractorName) {
		this.extractorName = extractorName;
	}

	public String getExtractorName() {
		return this.extractorName;
	}

	@Parameter(name = "binaryDocumentAttr", description = DocumentSourceConstants.BIN_DOC_ATTR_PARAM_DESCRIPTION, optional = true)
	public void setBinaryDocumentAttr(String binaryDocumentAttr) {
		this.binaryDocumentAttr = binaryDocumentAttr;
	}

	public String getBinaryDocumentAttr() {
		return this.binaryDocumentAttr;
	}

	@Parameter(name = "extractedDocumentAttr", description = DocumentSourceConstants.EXTR_DOC_ATTR_PARAM_DESCRIPTION, optional = true)
	public void setExtractedDocumentAttr(String extractedDocumentAttr) {
		this.extractedDocumentAttr = extractedDocumentAttr;
	}

	public String getExtractedDocumentAttr() {
		return this.extractedDocumentAttr;
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
				for (EnumSupportedExtractionTypes b : EnumSupportedExtractionTypes
						.values()) {
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
			for (EnumSupportedExtractionTypes b : EnumSupportedExtractionTypes
					.values()) {
				if (i > 0)
					res.append(",");
				res.append(b);
				i++;
			}

			return res.toString();
		}
	}

}