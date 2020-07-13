package gate.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import gate.Document;
import gate.Resource;
import gate.TextualDocument;
import gate.corpora.MimeType;
import gate.corpora.RepositioningInfo;
import gate.corpora.TextualDocumentFormat;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;
import gate.serialization.DocumentUtil;
import gate.util.DocumentFormatException;

@CreoleResource(name = "Serialization Format", isPrivate = true, autoinstances = { @AutoInstance(hidden = true) })
public class SerializationFormat extends TextualDocumentFormat {
	private static final long serialVersionUID = 7836956734599945213L;
	private static Logger logger = Logger.getLogger(SerializationFormat.class);

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

	@Override
	public void unpackMarkup(Document document, RepositioningInfo repInfo, RepositioningInfo ampCodingInfo)
			throws DocumentFormatException {
		unpackMarkup(document);
	}

	@Override
	public void unpackMarkup(Document document) throws DocumentFormatException {
		try (InputStream inputStream = openContentInputStream(document)) {
			DocumentUtil.applyDocumentValues(inputStream, false, document);
		} catch (IOException e) {
			throw new DocumentFormatException(e);
		}
	}

	protected InputStream openContentInputStream(Document document) throws UnsupportedEncodingException, IOException {
		byte[] bytes;
		if (document instanceof TextualDocument) {
			bytes = document.getContent().toString().getBytes(((TextualDocument) document).getEncoding());
		} else {
			bytes = document.getContent().toString().getBytes();
		}
		return new ByteArrayInputStream(bytes);
	}

}
