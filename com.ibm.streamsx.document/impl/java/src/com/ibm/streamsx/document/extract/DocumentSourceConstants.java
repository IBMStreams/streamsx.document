package com.ibm.streamsx.document.extract;

public class DocumentSourceConstants {
	public static final String IBM_LISENCE = "Licensed Materials-Property of IBM                                IL720140015C                                                         (C) Copyright IBM Corp.  2010, 2014    All Rights Reserved.      US Government Users Restricted Rights - Use, duplication or      disclosure restricted by GSA ADP Schedule Contract with          IBM Corp.";
	public static final String DE_TOOLKIT_HOME_VAR_NAME = "DE_TOOLKIT_HOME";
	public static final String CONFIG_FOLDER_PNAME = "configFolder";
//	public static final String CONFIG_FOLDER_DEFAULT = "../../../config";
	
	public static final String BINARY_DOCUMENT_ATTR_PNAME = "binaryDocumentAttr";
	public static final String EXTRACTED_DOCUMENT_ATTR_PNAME = "extractedDocumentAttr";
	public static final String EXTRACTION_TYPE_PNAME = "extractionType";
	public static final String IN_FILE_PNAME = "file";
	public static final String RETURN_FILE_DETAILS_PNAME = "returnFileDetails";
	public static final String RETURN_EMBEDDED_DOCUMENTS_PNAME = "returnEmbeddedDocuments";
	public static final String EXTRACTOR_NAME_PNAME = "extractorName";
	public static final String RETURN_SYSTEM_MESSAGES_PNAME = "returnSystemMessages";
	public static final String ENCODE_TO_JSON_PNAME = "encodeToJSON";
	public static final CharSequence EMBEDDED_SUBSTR = "EMBEDDED";
	public static final DocumentSource.EnumSupportedExtractionTypes EXTRACTION_TYPE_DEFAULT = DocumentSource.EnumSupportedExtractionTypes.Metadata;
	public static final String BINARY_DOCUMENT_ATTR_DEFAULT = "";
	public static final String EXTRACTED_DOCUMENT_ATTR_DEFAULT = "";
	public static final String DOCUMENT_SPL_SCHEMA = "tuple<rstring docId,list<ustring> msgs,map<ustring,list<ustring>> props,ustring text>";
	
	/**
	 * Description constants used in operator's annotations for SPLDOC generation
	 */
	public static final String OPERATOR_DESC = "The DocumentSource operator extracts metadata and text from binary documents." +
								               "For that purpose the operator utilizes different open source libraries to allow parsing of different document types. " +
								               "Out-of-the-box the operator utilizes Tika, PDFBox, JUnrar and TrueZip.\\n\\n" +								                								              
								               "**Rrerequisities**" +
								               "\\n\\n The operator requires **DE_TOOLKIT_HOME** envrionment variable to be defined." +
								               "\\n The environment variable should be set with the com.ibm.streamsx.document toolkit absolute path." +
								               "\\n\\n**Examples**" + 
								               "\\n\\nIn the following example, the DirectorySource operator extracts documents from archived (RAR) file including document content and metadata." +								               
								               "\\n\\n      stream<Document_t> RARDocumentSource = DocumentSource()" +
								       		   "\\n\\n      {" +
								       		   "\\n\\n             param" +
								       		   "\\n\\n                 file : \\\"docs/doc_samples.rar\\\";" +
								       		   "\\n\\n		            extractionType : \\\"All\\\";" +								       				
								       		   "\\n\\n		            returnEmbeddedDocuments: true;" +								
								       		   "\\n\\n      }";								    



	public static final String CONFIG_FOLDER_PARAM_DESCRIPTION = "Allows to specify config folder absolute location. If not specified ${DE_TOOLKIT_HOME}/config folder" +
																 " is used by the operator.";
	
	public static final String BIN_DOC_ATTR_PARAM_DESCRIPTION = "Input schema attribute name with a binary document data.";
	public static final String EXTR_DOC_ATTR_PARAM_DESCRIPTION = "Output schema attribute for an extraction results population.";
	public static final String EXTRACTION_TYPE_PARAM_DESCRIPTION = "Specifies the type of data that should be extracted from a document." +
																   "The type might have one of the following values: 'Text','Metadata' and 'All'." +
																   "If 'Text' value is specified the operator will extract text content only from a given document." +
																   "If 'Metadata' value is specified the operator will extract document metadata only from a given document." +
																   "If 'All' value is specified the operator will extract both text content and document metadata.";
	public static final String FILE_PARAM_DESCRIPTION = "The parameter might be specified only when the operator runs as a source operator, i.e. has no input port. " +
													 	"The paramater value specifies the source file path.";
	public static final String RET_FILE_DETAILS_PARAM_DESCRIPTION = "When the parameter's value is 'true' the operator extracts file details (location, name, size) as a part of document metadata. The default is 'false'.";
	public static final String RET_SYS_MESSAGES_PARAM_DESCRIPTION = "When the parameter's value is 'true' the operator extracts system messages as a part of document metadata. The default is 'false'.";
	public static final String RET_EMBEDDED_DOCS_PARAM_DESCRIPTION = "When the parameter's value is 'true' the operator extracts embedded documents if found." +
																	 "Each embedded document is submitted in a separate tuple.The default is 'false'.";
	public static final String EXTRACTOR_NAME_PARAM_DESCRIPTION = "By default the operator selects extractor per document according to mime-type to extractor mapping " +
																  " specified in the config/extractor_mapping.xml file. The parameter allows to overwrite the mapping by explicit " +
																  " extractor name specification.";
}
