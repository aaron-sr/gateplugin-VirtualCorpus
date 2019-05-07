package gate.format;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import gate.Annotation;
import gate.Document;
import gate.Resource;
import gate.TextualDocument;
import gate.corpora.DocumentContentImpl;
import gate.corpora.MimeType;
import gate.corpora.RepositioningInfo;
import gate.corpora.TextualDocumentFormat;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;
import gate.util.DocumentFormatException;

@CreoleResource(name = "Serialization Format", isPrivate = true, autoinstances = { @AutoInstance(hidden = true) })
public class SerializationFormat extends TextualDocumentFormat {
	private static final long serialVersionUID = 7836956734599945213L;
	private static Logger logger = Logger.getLogger(SerializationFormat.class);

	@Override
	public void unpackMarkup(Document document) throws DocumentFormatException {

		try {
			byte[] bytes;
			if (document instanceof TextualDocument) {
				bytes = document.getContent().toString().getBytes(((TextualDocument) document).getEncoding());
			} else {
				bytes = document.getContent().toString().getBytes();
			}
			if (bytes.length > 0) {
				try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
					Document readDocument = (Document) objectInputStream.readObject();
					validateEmptyDocument(document);
					copyDocumentValues(readDocument, document);
				}
			}
		} catch (Exception e) {
			throw new DocumentFormatException(e);
		}
	}

	private void validateEmptyDocument(Document document) throws DocumentFormatException {
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

	@Override
	public void unpackMarkup(Document document, RepositioningInfo repInfo, RepositioningInfo ampCodingInfo)
			throws DocumentFormatException {
		unpackMarkup(document);
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		MimeType mime = new MimeType("application", "java-serialized-object");
		mimeString2ClassHandlerMap.put(mime.getType() + "/" + mime.getSubtype(), this);
		mimeString2mimeTypeMap.put(mime.getType() + "/" + mime.getSubtype(), mime);
		suffixes2mimeTypeMap.put("ser", mime);
//		magic2mimeTypeMap.put("<?xml", mime);
		setMimeType(mime);
		return this;
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
