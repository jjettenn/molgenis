package org.molgenis.data;

import static java.lang.String.format;

public class EntityAlreadyExistsException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public EntityAlreadyExistsException(String entityName)
	{
		super(format("Entity [%s] already exists", entityName));
	}
}
