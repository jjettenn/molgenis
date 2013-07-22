package org.molgenis.compute5;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.RandomStringUtils;
import org.molgenis.compute5.model.Parameters;

import com.google.common.base.Joiner;

public class ComputeProperties
{
	public File propertiesFile;

	public String path = Parameters.PATH_DEFAULT;
	public String workFlow = Parameters.WORKFLOW_DEFAULT;
	public String defaults = null;
	public String defaultsCommandLine = null;
	public String[] parameters =
	{ Parameters.PARAMETERS_DEFAULT };
	public String customHeader = null;
	public String customFooter = null;
	public String customSubmit = null;
	public String backend = Parameters.BACKEND_DEFAULT;
	public String runDir = Parameters.RUNDIR_DEFAULT;
	public String runId = Parameters.RUNID_DEFAULT;
	public String database = Parameters.DATABASE_DEFAULT;
	public String port = Parameters.PORT_DEFAULT;
	public String interval = Parameters.INTERVAL_DEFAULT;
	public String molgenisuser = null;
	public String molgenispass = null;
	public String backenduser = null;
	public String backendpass = null;
	public String autogenerated = AUTO_NO;
	public String parametersToOverwrite = null;

	private HashMap<String, String> parametersToOverwriteMap = null;

	public static final String AUTO_NO = "no";
	public static final String AUTO_YES = "yes";

	// parameters not stored in compute.properties file:
	public boolean showHelp = false; // show help?
	public boolean databaseStart = false; // start db?
	public boolean databaseEnd = false; // stop db?
	public boolean generate = false; // should we generate?
	public boolean list = false; // should we list currently generated jobs?
	public boolean create = false;
	public String createDirName = Parameters.CREATE_WORKFLOW_DEFAULT;
	public boolean clear = false;
	public boolean execute = false; // does user want to execute scripts?



	private Options options = null;

	public ComputeProperties(String[] args)
	{
		options = createOptions();
			// validate command line args
		CommandLine cl = null;
		try
		{
			cl = new PosixParser().parse(options, args);
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		this.showHelp = cl.hasOption(Parameters.HELP) || 0 == cl.getOptions().length;

		//setPath(args);

		if (this.showHelp || this.create)
				return;
			// set path

			// prepend path to defaults
			updateDefaultParameterValues(path);

			// get user name and set that one as default
			this.molgenisuser = System.getProperty("user.name");
			this.backenduser = System.getProperty("user.name");

			createPropertiesFile();

			// parse properties file
			parseProperties();

			// overwrite with command line args
			parseCommandLine(args);

			// look for defaults in same folder as workflow
			updateWorkflowParameterDefaultsCSV();

			// save new config
			saveProperties();

	}

	public Options getOptions()
	{
		return options;
	}

	/**
	 * If this.defaultsCommandLine does not exist, then look in workflow folder for [workflow].defaults.csv or else
	 * defaults.csv
	 */
	private void updateWorkflowParameterDefaultsCSV()
	{
		if (null != defaultsCommandLine)
		{
			// validate file exists

			if (!new File(defaultsCommandLine).exists())
			{
				System.err.println(">> ERROR >> '-defaults " + defaultsCommandLine + "' does not exist!");
				System.err.println("Exit with code 1");
				System.exit(1);
			}
			else
			{
				this.defaults = this.defaultsCommandLine;
			}
		}
		else
		{
			File workflowFile = new File(this.workFlow);
			String workflowFilePath = workflowFile.getParent(); // get workflow
																// path
			String workflowName = workflowFile.getName(); // strip workflow name
			workflowName = workflowName.substring(0, workflowName.indexOf('.')); // strip
																					// extension

			File defaultsFileTest = new File(workflowFilePath + File.separator + workflowName + "."
					+ Parameters.DEFAULTS_DEFAULT);
			if (defaultsFileTest.exists())
			{ // first check [workflow].defaults.csv
				this.defaults = defaultsFileTest.toString();
			}
			else
			{ // else check defaults.csv
				defaultsFileTest = new File(workflowFilePath + File.separator + Parameters.DEFAULTS_DEFAULT);
				if (defaultsFileTest.exists())
				{
					this.defaults = defaultsFileTest.toString();
				}
			}
			// else this.defaults stays null
		}
	}

	private void updateDefaultParameterValues(String path)
	{
		this.workFlow = updatePath(path, this.workFlow);
		// this.defaults = updatePath(path, this.defaults);
		// this.runDir = updatePath(path, this.runDir);

		ArrayList<String> pathParameters = new ArrayList<String>();
		for (String p : this.parameters)
			pathParameters.add(updatePath(path, p));
		this.parameters = pathParameters.toArray(new String[pathParameters.size()]);

	}

	/**
	 * Prepend path if fileName has no absolute path
	 * 
	 * @param path
	 * @param fileName
	 * @return
	 */
	private String updatePath(String path, String fileName)
	{
		if (fileName.startsWith("/") || fileName.startsWith("~")) return fileName;
		else return path + (path.endsWith("/") ? "" : "/") + fileName;
	}

	private void setPath(String[] args)
	{
		Options options = createOptions();
		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try
		{
			cmd = parser.parse(options, args);
			this.path = cmd.getOptionValue(Parameters.PATH_CMNDLINE_OPTION, this.path);
			this.path = this.path + (this.path.endsWith("/") ? "" : "/");

			// do we want to create a new workflow? If so: where?
			this.create = cmd.hasOption(Parameters.CREATE);
			if (this.create)
			{
//				this.createWorkflow = cmd.getOptionValue(Parameters.CREATE, this.createWorkflow);
//				this.createWorkflow = updatePath(this.path, this.createWorkflow);
			}
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
	}

	public void createPropertiesFile()
	{
		// get location properties file
		String propFileString = Parameters.PROPERTIES;

		this.propertiesFile = new File(propFileString);

		if (!this.propertiesFile.exists())
		{
			try
			{
				// this.propertiesFile.getParentFile().mkdirs();
				this.propertiesFile.createNewFile();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	public void parseProperties()
	{
		try
		{
			Properties p = new Properties();
			FileInputStream fis = new FileInputStream(this.propertiesFile);
			try
			{
				p.load(fis);
			}
			finally
			{
				fis.close();
			}

			// set this.variables
			this.path = p.getProperty(Parameters.PATH, this.path);
			this.workFlow = p.getProperty(Parameters.WORKFLOW, this.workFlow);
			this.defaults = p.getProperty(Parameters.DEFAULTS, this.defaults);
			this.customHeader = p.getProperty(Parameters.CUSTOM_HEADER_COLUMN, this.customHeader);
			this.customFooter = p.getProperty(Parameters.CUSTOM_FOOTER_COLUMN, this.customFooter);
			this.customSubmit = p.getProperty(Parameters.CUSTOM_SUBMIT_COLUMN, this.customSubmit);
			this.backend = p.getProperty(Parameters.BACKEND, this.backend);
			this.runDir = p.getProperty(Parameters.RUNDIR, this.runDir);
			this.autogenerated = p.getProperty(Parameters.AUTOGENERATED_ID, this.autogenerated);
			if(!autogenerated.equalsIgnoreCase(AUTO_YES))
				this.runId = p.getProperty(Parameters.RUNID, this.runId);
			this.database = p.getProperty(Parameters.DATABASE, this.database);
			this.port = p.getProperty(Parameters.PORT, this.port);
			this.interval = p.getProperty(Parameters.INTERVAL, this.interval);
			this.molgenisuser = p.getProperty(Parameters.MOLGENIS_USER_CMNDLINE, this.molgenisuser);
			this.backenduser = p.getProperty(Parameters.BACKEND_USER_CMNDLINE, this.backenduser);


			String parametersCSVString = p.getProperty(Parameters.PARAMETERS);
			if (null != parametersCSVString) this.parameters = parametersCSVString.split("\\s*,\\s*");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void parseCommandLine(String[] args)
	{
		Options options = createOptions();

		CommandLineParser parser = new PosixParser();
		try
		{
			CommandLine cmd = parser.parse(options, args);

			this.showHelp = cmd.hasOption(Parameters.HELP); // show help?
			if (this.showHelp) throw new ParseException("");

			// set this.variables

			this.path = cmd.getOptionValue(Parameters.PATH_CMNDLINE_OPTION, this.path);
			this.path = this.path + (this.path.endsWith("/") ? "" : "/");

			this.workFlow = getFullPath(cmd, Parameters.WORKFLOW_CMNDLINE_OPTION, this.workFlow);
			this.defaultsCommandLine = getFullPath(cmd, Parameters.DEFAULTS_CMNDLINE_OPTION, null);
			this.customHeader = getFullPath(cmd, Parameters.CUSTOM_HEADER_COLUMN, this.customHeader);
			this.customFooter = getFullPath(cmd, Parameters.CUSTOM_FOOTER_COLUMN, this.customFooter);
			this.customSubmit = getFullPath(cmd, Parameters.CUSTOM_SUBMIT_COLUMN, this.customSubmit);
			this.backend = cmd.getOptionValue(Parameters.BACKEND_CMNDLINE_OPTION, this.backend);
			this.runDir = cmd.getOptionValue(Parameters.RUNDIR_CMNDLINE_OPTION, this.runDir);
			this.database = cmd.getOptionValue(Parameters.DATABASE_CMNDLINE_OPTION, this.database);
			this.port = cmd.getOptionValue(Parameters.PORT_CMNDLINE_OPTION, this.port);
			this.databaseStart = cmd.hasOption(Parameters.DATABASE_START_CMNDLINE_OPTION);
			this.databaseEnd = cmd.hasOption(Parameters.DATABASE_END_CMNDLINE_OPTION);
			this.interval = cmd.getOptionValue(Parameters.INTERVAL_CMNDLINE_OPTION, this.interval);
			this.molgenisuser = cmd.getOptionValue(Parameters.MOLGENIS_USER_CMNDLINE_OPTION, this.molgenisuser);
			this.molgenispass = cmd.getOptionValue(Parameters.MOLGENIS_PASS_CMNDLINE_OPTION, this.molgenispass);
			this.backenduser = cmd.getOptionValue(Parameters.BACKEND_USER_CMNDLINE_OPTION, this.backenduser);
			this.backendpass = cmd.getOptionValue(Parameters.BACKEND_PASS_CMNDLINE_OPTION, this.backendpass);
			this.create = cmd.hasOption(Parameters.CREATE);
			this.createDirName = cmd.getOptionValue(Parameters.CREATE, this.createDirName);

			this.parametersToOverwrite = cmd.getOptionValue(Parameters.PARAMETERS_TO_OVERWRITE_CMDLINE_OPTION);

			if(parametersToOverwrite != null)
			{
				try
				{
					parseParametersToOverwrite(parametersToOverwrite);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}


			this.customFooter = cmd.getOptionValue(Parameters.CUSTOM_FOOTER_COLUMN);

			// generate only if -g or if -w and -p present
			this.generate = cmd.hasOption(Parameters.GENERATE_CMNDLINE_OPTION)
					|| (cmd.hasOption(Parameters.WORKFLOW_CMNDLINE_OPTION) && cmd
							.hasOption(Parameters.PARAMETERS_CMNDLINE_OPTION));

			this.clear = cmd.hasOption(Parameters.CLEAR);

			this.runId = cmd.getOptionValue(Parameters.RUNID_CMNDLINE_OPTION, this.runId);
			// if runId == null then create one
			if (null == this.runId)
			{
				// 4 letters/LETTERS/numbers -> (26*2 + 10)^4 = 14,776,336
				// possibilities
				this.runId = RandomStringUtils.random(4, true, true);
				this.autogenerated =this.AUTO_YES;
			}

			// want to run?
			this.execute = cmd.hasOption(Parameters.RUN_CMNDLINE_OPTION);

			// do we want to list jobs?
			this.list = cmd.hasOption(Parameters.LIST);

			String[] cmdParameters = cmd.getOptionValues(Parameters.PARAMETERS_CMNDLINE_OPTION);
			cmdParameters = getFullPath(cmdParameters);
			if (null != cmdParameters) this.parameters = cmdParameters;
		}
		catch (ParseException e)
		{
			System.err.println(e.getMessage() + "\n");
			new HelpFormatter().printHelp("compute -p parameters.csv", options);
		}
	}

	private void parseParametersToOverwrite(String parametersToOverwrite) throws Exception
	{
		parametersToOverwriteMap = new HashMap<String, String>();


		String[] pairs = parametersToOverwrite.split(";");

		for (String pair : pairs)
		{
			String[] pPair = pair.split("=");
			if (pPair.length == 2)
			{
				parametersToOverwriteMap.put(pPair[0], pPair[1]);
			}
			else
			{
				throw new Exception("Incorrect command-line sequence: \"" + parametersToOverwrite + "\"");
			}
		}




	}

	private String[] getFullPath(String[] cmdParameters)
	{
		if (null == cmdParameters) return null;

		ArrayList<String> pathParameters = new ArrayList<String>();
		for (String p : cmdParameters)
			pathParameters.add(updatePath(this.path, p));
		return pathParameters.toArray(new String[pathParameters.size()]);
	}

	/**
	 * Returns path as specified by this cmndlineOption. If path is relative, then this.path/ will be prepended.
	 * 
	 * @param cmd
	 * @param cmndlineOption
	 * @param defaultValue
	 * @return
	 */
	private String getFullPath(CommandLine cmd, String cmndlineOption, String defaultValue)
	{
		String option = cmd.getOptionValue(cmndlineOption);
		if (null == option)
		{
			return defaultValue;
		}
		else
		{
			return updatePath(this.path, option);
		}
	}

	public void saveProperties()
	{
		Properties p = new Properties();
		try
		{
			// set this.variables
			if(this.customSubmit != null)
			p.setProperty(Parameters.CUSTOM_SUBMIT_COLUMN, this.customSubmit);
			if(this.customFooter != null)
			p.setProperty(Parameters.CUSTOM_FOOTER_COLUMN, this.customFooter);
			if(this.customHeader != null)
			p.setProperty(Parameters.CUSTOM_HEADER_COLUMN, this.customHeader);

			p.setProperty(Parameters.INTERVAL, this.interval);
			p.setProperty(Parameters.PORT, this.port);
			p.setProperty(Parameters.DATABASE, this.database);

			p.setProperty(Parameters.BACKEND_USER_CMNDLINE, this.backenduser);
			p.setProperty(Parameters.BACKEND, this.backend);

			p.setProperty(Parameters.RUNDIR, this.runDir);
			if (null != this.defaults) p.setProperty(Parameters.DEFAULTS, this.defaults);
			p.setProperty(Parameters.WORKFLOW, this.workFlow);
			p.setProperty(Parameters.PARAMETERS, this.parametersString());
			p.setProperty(Parameters.PATH, this.path);

			p.setProperty(Parameters.MOLGENIS_USER_CMNDLINE, this.molgenisuser);
			p.setProperty(Parameters.AUTOGENERATED_ID, this.autogenerated);
			p.setProperty(Parameters.RUNID, this.runId);

			FileOutputStream fos = new FileOutputStream(this.propertiesFile);
			try
			{
				p.store(fos, "This file contains your molgenis-compute properties");
			}
			finally
			{
				fos.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public String parametersString()
	{
		return Joiner.on(",").join(this.parameters);
	}

	/**
	 * Create Options object specifying all possible command line arguments
	 * 
	 * @return Options
	 */
	@SuppressWarnings("static-access")
	public Options createOptions()
	{
		Options options = new Options();
		Option path = OptionBuilder.hasArg()
				.withDescription("Path to directory this generates to. Default: <current dir>.")
				.withLongOpt(Parameters.PATH).create(Parameters.PATH_CMNDLINE_OPTION);
		Option p = OptionBuilder.withArgName(Parameters.PARAMETERS_DEFAULT).hasArgs().withLongOpt("parameters")
				.withDescription("Path to parameter.csv file(s). Default: " + Parameters.PARAMETERS_DEFAULT)
				.create(Parameters.PARAMETERS_CMNDLINE_OPTION);
		Option w = OptionBuilder.withArgName(Parameters.WORKFLOW_DEFAULT).hasArg()
				.withDescription("Path to your workflow file. Default: " + Parameters.WORKFLOW_DEFAULT)
				.withLongOpt(Parameters.WORKFLOW).create(Parameters.WORKFLOW_CMNDLINE_OPTION);
		Option d = OptionBuilder.hasArg()
				.withDescription("Path to your workflow-defaults file. Default: " + Parameters.DEFAULTS_DEFAULT)
				.withLongOpt(Parameters.DEFAULTS).create(Parameters.DEFAULTS);
		options.addOption(OptionBuilder.hasArg()
				.withDescription("Adds a custom header. Default: " + Parameters.CUSTOM_HEADER_DEFAULT)
				.create(Parameters.CUSTOM_HEADER_COLUMN));
		options.addOption(OptionBuilder.hasArg()
				.withDescription("Adds a custom footer. Default: " + Parameters.CUSTOM_FOOTER_DEFAULT)
				.create(Parameters.CUSTOM_FOOTER_COLUMN));
		options.addOption(OptionBuilder.hasArg()
				.withDescription("Set a custom submit.sh template. Default: " + Parameters.CUSTOM_SUBMIT_DEFAULT)
				.create(Parameters.CUSTOM_SUBMIT_COLUMN));
		Option b = OptionBuilder.hasArg()
				.withDescription("Backend for which you generate. Default: " + Parameters.BACKEND_DEFAULT)
				.withLongOpt(Parameters.BACKEND).create(Parameters.BACKEND_CMNDLINE_OPTION);
		Option runDir = OptionBuilder.hasArg().withDescription("Directory where jobs are stored")
				.create(Parameters.RUNDIR);
		Option runId = OptionBuilder.hasArg()
				.withDescription("Id of the task set which you generate. Default: " + Parameters.RUNID_DEFAULT)
				.withLongOpt(Parameters.RUNID).create(Parameters.RUNID_CMNDLINE_OPTION);

		options.addOption(OptionBuilder.withDescription("Shows this help.").withLongOpt(Parameters.HELP)
				.create(Parameters.HELP_CMNDLINE_OPTION));
		options.addOption(path);
		options.addOption(p);
		options.addOption(w);
		options.addOption(d);
		options.addOption(b);
		options.addOption(runDir);
		options.addOption(runId);
		options.addOption(OptionBuilder
				.withDescription("Host, location of database. Default: " + Parameters.DATABASE_DEFAULT).hasArg()
				.withLongOpt(Parameters.DATABASE).create(Parameters.DATABASE_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Port used to connect to databasae. Default: " + Parameters.PORT_DEFAULT).hasArg()
				.withLongOpt(Parameters.PORT).create(Parameters.PORT_CMNDLINE_OPTION));
		options.addOption(OptionBuilder.withDescription("Starts the database").withLongOpt(Parameters.DATABASE_START)
				.create(Parameters.DATABASE_START_CMNDLINE_OPTION));
		options.addOption(OptionBuilder.withDescription("End the database").withLongOpt(Parameters.DATABASE_END)
				.create(Parameters.DATABASE_END_CMNDLINE_OPTION));
		options.addOption(OptionBuilder.withDescription("Generate jobs").withLongOpt(Parameters.GENERATE)
				.create(Parameters.GENERATE_CMNDLINE_OPTION));
		options.addOption(OptionBuilder.withDescription("List jobs, generated, queued, running, completed, failed")
				.withLongOpt(Parameters.LIST).create(Parameters.LIST_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Creates empty workflow. Default name: " + Parameters.CREATE_WORKFLOW_DEFAULT)
				.hasOptionalArg().create(Parameters.CREATE));
		options.addOption(OptionBuilder
				.withDescription(
						"Run jobs from current directory on current backend. When using --database this will return a 'id' for --pilot.")
				.withLongOpt(Parameters.RUN).create(Parameters.RUN_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Supply user name to login to molgenis database. Default is your own user name.").hasArg()
				.withLongOpt(Parameters.MOLGENIS_USER_CMNDLINE).create(Parameters.MOLGENIS_USER_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Supply user pass to login to molgenis. Default is not saved.").hasArg()
				.withLongOpt(Parameters.MOLGENIS_PASS_CMNDLINE).create(Parameters.MOLGENIS_PASS_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Supply user name to login to execution backend. Default is your own user name.").hasArg()
				.withLongOpt(Parameters.BACKEND_USER_CMNDLINE).create(Parameters.BACKEND_USER_CMNDLINE_OPTION));
		options.addOption(OptionBuilder
				.withDescription("Supply user pass to login to execution backend. Default is not saved.").hasArg()
				.withLongOpt(Parameters.BACKEND_PASS_CMNDLINE).create(Parameters.BACKEND_PASS_CMNDLINE_OPTION));

		options.addOption(OptionBuilder.withDescription("Clear properties file").withLongOpt(Parameters.CLEAR)
				.create(Parameters.CLEAR));

		options.addOption(OptionBuilder
				.withDescription(
						"Parameters and values, which will overwritten in the parameters file. Parameters should be " +
								"placed into brackets and listed using equality sign, e.g. \"mem=6GB;queue=long\" ")
				.hasArg()
				.withLongOpt(Parameters.PARAMETERS_TO_OVERWRITE_CMDLINE)
				.create(Parameters.PARAMETERS_TO_OVERWRITE_CMDLINE_OPTION));


		return options;
	}

	public boolean hasParametersToOverwrite()
	{
		if(parametersToOverwriteMap != null)
			return true;
		return false;
	}

	public HashMap getParametersToOverwrite()
	{
		return parametersToOverwriteMap;
	}
}
