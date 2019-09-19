package gate.serialization;

import java.util.ArrayList;
import java.util.Collection;

import gate.Annotation;
import gate.Document;
import gate.corpora.DocumentContentImpl;
import gate.util.DocumentFormatException;

public class DocumentUtil {

	public static void validateEmptyDocument(Document document) throws DocumentFormatException {
		if (!document.getAnnotations().isEmpty()) {
			throw new DocumentFormatException("document has already annotations in default annotation set");
		}
		if (!document.getAnnotations().getRelations().isEmpty()) {
			throw new DocumentFormatException("document has already relations in default annotation set");
		}
		Collection<String> annotationSetNames = new ArrayList<>();
		Collection<String> relationSetNames = new ArrayList<>();
		for (String annotationSetName : document.getAnnotationSetNames()) {
			if (annotationSetName.length() == 0) {
				continue;
			}
			if (!document.getAnnotations(annotationSetName).isEmpty()) {
				annotationSetNames.add(annotationSetName);
			}
			if (!document.getAnnotations(annotationSetName).getRelations().isEmpty()) {
				relationSetNames.add(annotationSetName);
			}
		}
		if (!annotationSetNames.isEmpty() && !relationSetNames.isEmpty()) {
			throw new DocumentFormatException("document has already annotations in " + annotationSetNames
					+ " and relations in " + relationSetNames);
		} else if (!annotationSetNames.isEmpty()) {
			throw new DocumentFormatException("document has already annotations in " + annotationSetNames);
		} else if (!relationSetNames.isEmpty()) {
			throw new DocumentFormatException("document has already relations in " + relationSetNames);
		}
	}

	public static final void copyDocumentValues(Document fromDocument, Document toDocument) {
		toDocument.setContent(new DocumentContentImpl(fromDocument.getContent().toString()));
		if (!fromDocument.getAnnotations().isEmpty()) {
			for (Annotation annotation : fromDocument.getAnnotations()) {
				toDocument.getAnnotations().add(annotation);
			}
		}
		if (!fromDocument.getAnnotations().getRelations().isEmpty()) {
			toDocument.getAnnotations().getRelations().addAll(fromDocument.getAnnotations().getRelations());
		}
		for (String annotationSetName : fromDocument.getAnnotationSetNames()) {
			if (annotationSetName.length() == 0) {
				continue;
			}
			if (!fromDocument.getAnnotations(annotationSetName).isEmpty()) {
				for (Annotation annotation : fromDocument.getAnnotations(annotationSetName)) {
					toDocument.getAnnotations(annotationSetName).add(annotation);
				}
			}
			if (!fromDocument.getAnnotations(annotationSetName).getRelations().isEmpty()) {
				toDocument.getAnnotations(annotationSetName).getRelations()
						.addAll(fromDocument.getAnnotations(annotationSetName).getRelations());
			}
		}
		if (!fromDocument.getFeatures().isEmpty()) {
			toDocument.getFeatures().putAll(fromDocument.getFeatures());
		}
	}

}
