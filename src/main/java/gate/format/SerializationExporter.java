package gate.format;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.DocumentExporter;
import gate.FeatureMap;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;
import gate.serialization.DocumentUtil;

@CreoleResource(name = "Serialization Exporter", tool = true, autoinstances = @AutoInstance)
public class SerializationExporter extends DocumentExporter {
	private static final long serialVersionUID = -1437154772909153534L;
	private static Logger logger = Logger.getLogger(SerializationExporter.class);

	public SerializationExporter() {
		this("Java Serialization", "ser", "application/java-serialized-object");
	}

	public SerializationExporter(String fileType, String defaultExtension, String mimeType) {
		super(fileType, defaultExtension, mimeType);
	}

	@Override
	public void export(Document document, OutputStream outputStream, FeatureMap options) throws IOException {
		DocumentUtil.writeDocument(document, outputStream, false);
	}

}
