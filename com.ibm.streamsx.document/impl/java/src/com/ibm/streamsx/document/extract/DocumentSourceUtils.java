package com.ibm.streamsx.document.extract;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;

public class DocumentSourceUtils {

	
	private final static Logger logger = Logger.getLogger(DocumentSourceUtils.class.getName());

	/**
	 * Gets config folder path
	 * @return config folder path
	 */
    public static String getDefaultConfigFolderPath() throws Exception
    {
      String res = null;
      
      String DtxToolkitHome = System.getenv(DocumentSourceConstants.DE_TOOLKIT_HOME_VAR_NAME);
      logger.trace("DE_TOOLKIT_HOME environment variable value is " + DtxToolkitHome);
      if (DtxToolkitHome != null) {
        res = DtxToolkitHome + "/config";
      }      
      return res;
    }
    
    private static Tuple createAttrTupleFromData(String attrName, OutputTuple outTuple, Map<String, Object> data) {
      StreamSchema documentTupleSchema = outTuple.getStreamSchema();
      Attribute extractDocumentAttr = documentTupleSchema.getAttribute(attrName);
      if (extractDocumentAttr == null) {
        return documentTupleSchema.getTuple(data);
      }
      TupleType extractDocumentTupleType = (TupleType)extractDocumentAttr
        .getType();
      StreamSchema extractDocumentTupleSchema = extractDocumentTupleType
        .getTupleSchema();
      logger.trace("Document tuple schema :" +  extractDocumentTupleSchema.getLanguageType());
      
      logger.trace("About to populate create tuple with data :");
      for (String key : data.keySet()) {
        logger.trace("key = " + key + ", value = " + data.get(key));
      }
      return extractDocumentTupleSchema.getTuple(data);
    }
    
    
    public static void populateOutTuple(String extractedDocumentAttr, OutputTuple outTuple, Tuple inTuple, Map<String, Object> documentData)
    {
      if ((extractedDocumentAttr != null) && (extractedDocumentAttr.length() > 0)) {
        Tuple extractDocumentTuple = createAttrTupleFromData(extractedDocumentAttr, outTuple, documentData);
        outTuple.setObject(extractedDocumentAttr, extractDocumentTuple);
        outTuple.assign(inTuple);
      }
      else
      {
        outTuple.setString("docId", ((RString)documentData.get("docId")).getString());
        Map<String, List<String>> propsMap = (Map<String, List<String>>)documentData.get("props");
        if (propsMap != null) {
          outTuple.setMap("props", propsMap);
        }
        String[] msgsArr = (String[])documentData.get("msgs");
        if (msgsArr != null) {
        	outTuple.setList("msgs", Arrays.asList(msgsArr));
        }
        String text = (String)documentData.get("text");
        if (text != null) {
          outTuple.setString("text", text);
        }
      }
    }

	
    

}
