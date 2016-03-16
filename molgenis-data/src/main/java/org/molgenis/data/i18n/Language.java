package org.molgenis.data.i18n;

import org.molgenis.data.DataService;
import org.molgenis.data.support.DefaultEntity;

public class Language extends DefaultEntity
{
	private static final long serialVersionUID = 1L;

	public Language(LanguageMetaData languageMetaData, DataService dataService)
	{
		super(languageMetaData, dataService);
	}

	public String getCode()
	{
		return getString(LanguageMetaData.CODE);
	}

	public void setCode(String code)
	{
		set(LanguageMetaData.CODE, code);
	}

	public String getName()
	{
		return getString(LanguageMetaData.NAME);
	}

	public void setName(String name)
	{
		set(LanguageMetaData.NAME, name);
	}
}
