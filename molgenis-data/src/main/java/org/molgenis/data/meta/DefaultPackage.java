package org.molgenis.data.meta;

import org.molgenis.data.Package;
import org.springframework.stereotype.Component;

@Component
public class DefaultPackage extends PackageImpl
{
	public static final String PACKAGE_NAME = Package.DEFAULT_PACKAGE_NAME;

	public static final DefaultPackage INSTANCE = new DefaultPackage();

	public DefaultPackage()
	{
		super(Package.DEFAULT_PACKAGE_NAME, "The default package", null);
	}
}
