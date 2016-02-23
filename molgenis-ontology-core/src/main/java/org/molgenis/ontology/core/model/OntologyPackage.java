package org.molgenis.ontology.core.model;

import org.molgenis.data.meta.PackageImpl;
import org.springframework.stereotype.Component;

@Component
public class OntologyPackage extends PackageImpl
{
	public final static String PACKAGE_NAME = "Ontology";

	public final static OntologyPackage INSTANCE = new OntologyPackage();

	public OntologyPackage()
	{
		super(PACKAGE_NAME, "This is a pacakge for storing ontology related model", null);
	}
}
