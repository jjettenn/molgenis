package org.molgenis.auth;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMetaData extends SystemEntityMetaData
{

	public AuthorityMetaData()
	{
		super("authority");
		setAbstract(true);
		addAttribute(Authority.ROLE).setLabel("role").setNillable(true);
	}
}
