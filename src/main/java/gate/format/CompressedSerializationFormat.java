package gate.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.http.client.entity.DeflateInputStream;
import org.apache.log4j.Logger;

import gate.Document;
import gate.Resource;
import gate.corpora.MimeType;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "Compressed Serialization Format", isPrivate = true, autoinstances = {
		@AutoInstance(hidden = true) })
public class CompressedSerializationFormat extends SerializationFormat {
	private static final long serialVersionUID = -3403673755349121489L;
	private static Logger logger = Logger.getLogger(CompressedSerializationFormat.class);

	@Override
	public Resource init() throws ResourceInstantiationException {
		MimeType mime = new MimeType("application", "compressed-java-serialized-object");
		mimeString2ClassHandlerMap.put(mime.getType() + "/" + mime.getSubtype(), this);
		mimeString2mimeTypeMap.put(mime.getType() + "/" + mime.getSubtype(), mime);
		suffixes2mimeTypeMap.put("ser.zz", mime);
//		magic2mimeTypeMap.put("<?xml", mime);
		setMimeType(mime);
		return this;
	}

	@Override
	protected InputStream openContentInputStream(Document document) throws UnsupportedEncodingException, IOException {
		return new DeflateInputStream(super.openContentInputStream(document));
	}

}
