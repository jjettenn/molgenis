package org.molgenis.app;

import static org.molgenis.app.ScaleusSampleExporterController.URI;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.molgenis.ui.MolgenisPluginController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Controller that handles home page requests
 */
@Controller
@RequestMapping(URI)
public class ScaleusSampleExporterController extends MolgenisPluginController
{
	public static final String ID = "scaleusexport";
	public static final String URI = MolgenisPluginController.PLUGIN_URI_PREFIX + ID;

	private final ScaleusSampleExporterService scaleusSampleExporterService;

	@Autowired
	public ScaleusSampleExporterController(ScaleusSampleExporterService scaleusSampleExporterService)
	{
		super(URI);
		this.scaleusSampleExporterService = scaleusSampleExporterService;
	}

	@RequestMapping
	public String init() throws ClientProtocolException, IOException
	{
		scaleusSampleExporterService.exportSamples();
		return "view-scaleusexport";
	}
}
