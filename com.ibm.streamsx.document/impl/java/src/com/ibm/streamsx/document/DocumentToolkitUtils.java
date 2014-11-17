package com.ibm.streamsx.document;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.document.extract.DocumentSourceUtils;

public class DocumentToolkitUtils {
	
	// logger
	private final static Logger logger = Logger.getLogger(DocumentToolkitUtils.class.getName());

	/**
	 * Gets operator parameter value from context
	 * @param context - operator context
	 * @param paramName - parameter name
	 * @param paramDefaultValue - default parameter value
	 * @return
	 */
	public static Object getParamValue(OperatorContext context, String paramName, Object paramDefaultValue) {
		return  context.getParameterValues(paramName).size() > 0 ? 
				context.getParameterValues(paramName).get(0): paramDefaultValue;
	}

	public static String getStringParamValue(OperatorContext context, String paramName, Object paramDefaultValue) {
		return  (String)getParamValue(context, paramName, paramDefaultValue);
	}

	public static boolean getBoolParamValue(OperatorContext context, String paramName, boolean paramDefaultValue) {
		return Boolean.parseBoolean((String)getParamValue(context, paramName,  String.valueOf(paramDefaultValue)));		
	}

	public static int getIntParamValue(OperatorContext context, String paramName, int paramDefaultValue) {
		return Integer.valueOf((String)getParamValue(context, paramName,  String.valueOf(paramDefaultValue))).intValue();		
	}

	/**
	 * Checks file existence
	 * @param file
	 */
    public static boolean fileExistsAndReadable(String file) {
    	File f = new File(file);
    	boolean res = (!f.isFile() || !f.canRead()) ? false : true;  
    	return res;
    }
    
}
