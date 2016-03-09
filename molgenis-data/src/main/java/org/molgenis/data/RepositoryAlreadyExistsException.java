package org.molgenis.data;

import static java.lang.String.format;

public class RepositoryAlreadyExistsException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public RepositoryAlreadyExistsException(String repoName)
	{
		super(format("Repository [%s] already exists", repoName));
	}
}
