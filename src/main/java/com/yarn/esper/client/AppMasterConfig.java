package com.yarn.esper.client;

import org.apache.hadoop.util.Shell;

public class AppMasterConfig {
	
	public static final String APPMASTER_CLASSPARH = "esper.application.classpath";
	public static final String DEFAULT_APPMASTER_CLASSPARH = 
			Environment.APPMASTER_LIB.toString() + "/*";
	
	private enum Environment {
		APPMASTER_LIB("/usr/hadoop-yarn/lib");
		
		private final String variable;
	    private Environment(String variable) {
	      this.variable = variable;
	    }
	    
	    public String key() {
	      return variable;
	    }
	    
	    public String toString() {
	      return variable;
	    }

	    public String $() {
	      if (Shell.WINDOWS) {
	        return "%" + variable + "%";
	      } else {
	        return "$" + variable;
	      }
	    }

	    public String $$() {
	      return "{{" + variable + "}}";
	    }
	}

}
