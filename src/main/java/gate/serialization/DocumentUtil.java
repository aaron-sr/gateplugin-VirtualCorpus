package gate.serialization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

import gate.Annotation;
import gate.Document;
import gate.GateConstants;
import gate.corpora.DocumentContentImpl;
import gate.corpora.DocumentImpl;
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
			if (GateConstants.ORIGINAL_MARKUPS_ANNOT_SET_NAME.contentEquals(annotationSetName)) {
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
			if (GateConstants.ORIGINAL_MARKUPS_ANNOT_SET_NAME.contentEquals(annotationSetName)) {
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
		if (toDocument instanceof DocumentImpl) {
			DocumentImpl toDocumentImpl = (DocumentImpl) toDocument;

			int nextAnnotationId;
			if (fromDocument instanceof DocumentImpl) {
				nextAnnotationId = ((DocumentImpl) fromDocument).peakAtNextAnnotationId();
			} else {
				nextAnnotationId = allAnnotationStream(fromDocument).mapToInt(annotation -> annotation.getId()).max()
						.orElse(-1) + 1;
			}
			toDocumentImpl.setNextAnnotationId(nextAnnotationId);

			int nextNodeId = allAnnotationStream(fromDocument)
					.flatMap(annotation -> Stream.of(annotation.getStartNode(), annotation.getEndNode()))
					.mapToInt(node -> node.getId()).max().orElse(-1) + 1;
			if (nextNodeId > 0) {
				int currentNextNodeId;
				do {
					currentNextNodeId = toDocumentImpl.getNextNodeId() + 1;
				} while (nextNodeId > currentNextNodeId);
			}
		}
	}

	private static Stream<Annotation> allAnnotationStream(Document document) {
		return Stream.concat(document.getAnnotations().stream(),
				document.getAnnotationSetNames().stream().flatMap(name -> document.getAnnotations(name).stream()));
	}

}
