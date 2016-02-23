package org.molgenis.data.settings;

import org.molgenis.data.meta.PackageImpl;
import org.springframework.stereotype.Component;

@Component
public class SettingsPackage extends PackageImpl
{
	public static final String PACKAGE_NAME = "settings";

	public static final SettingsPackage INSTANCE = new SettingsPackage();

	public SettingsPackage()
	{
		super(PACKAGE_NAME, "The default package", null);
	}
}
