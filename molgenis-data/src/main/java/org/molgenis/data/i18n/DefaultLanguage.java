package org.molgenis.data.i18n;

import org.molgenis.data.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultLanguage extends Language
{
	private static final long serialVersionUID = 1L;

	@Autowired
	public DefaultLanguage(LanguageMetaData languageMetaData, DataService dataService)
	{
		super(languageMetaData, dataService);
		setCode("en");
		setName("English");
	}
}
