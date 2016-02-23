package org.molgenis.data.support;

import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Package;
import org.molgenis.data.meta.DefaultPackage;

public abstract class SystemEntityMetaData extends DefaultEntityMetaData
{
	public SystemEntityMetaData(EntityMetaData entityMetaData)
	{
		super(entityMetaData);
		setSystem(true);
	}

	public SystemEntityMetaData(String simpleName)
	{
		super(simpleName);
		setSystem(true);
		setPackage(DefaultPackage.INSTANCE);
	}

	public SystemEntityMetaData(String simpleName, EntityMetaData entityMetaData)
	{
		super(simpleName, entityMetaData);
		setSystem(true);
	}

	public SystemEntityMetaData(String simpleName, Package package_)
	{
		super(simpleName, package_);
		setSystem(true);
	}

	public SystemEntityMetaData(String simpleName, Class<? extends Entity> entityClass)
	{
		super(simpleName, entityClass);
		setSystem(true);
		setPackage(DefaultPackage.INSTANCE);
	}

	public SystemEntityMetaData(String simpleName, Class<? extends Entity> entityClass, Package package_)
	{
		super(simpleName, entityClass, package_);
		setSystem(true);
	}
}
