package org.molgenis.data.importer;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.molgenis.data.DatabaseAction;
import org.molgenis.ui.wizard.AbstractWizardPage;
import org.molgenis.ui.wizard.Wizard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindingResult;

@Component
public class PackageWizardPage extends AbstractWizardPage
{
	private static final Logger LOG = LoggerFactory.getLogger(PackageWizardPage.class);

	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String getTitle()
	{
		return "Packages";
	}

	@Override
	public String handleRequest(HttpServletRequest request, BindingResult result, Wizard wizard)
	{
		ImportWizardUtil.validateImportWizard(wizard);
		ImportWizard importWizard = (ImportWizard) wizard;
		String entityImportOption = importWizard.getEntityImportOption();

		if (entityImportOption != null)
		{
			try
			{
				// convert input to database action
				DatabaseAction entityDbAction = ImportWizardUtil.toDatabaseAction(entityImportOption);
				if (entityDbAction == null) throw new IOException("unknown database action: " + entityImportOption);
			}
			catch (RuntimeException e)
			{
				ImportWizardUtil.handleException(e, importWizard, result, LOG, entityImportOption);
			}
			catch (IOException e)
			{
				ImportWizardUtil.handleException(e, importWizard, result, LOG, entityImportOption);
			}
		}
		return null;
	}
}
