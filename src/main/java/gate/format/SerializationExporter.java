package gate.format;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.DocumentExporter;
import gate.FeatureMap;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "Serialization Exporter", tool = true, autoinstances = @AutoInstance)
public class SerializationExporter extends DocumentExporter {
	private static final long serialVersionUID = -1437154772909153534L;
	private static Logger logger = Logger.getLogger(SerializationExporter.class);

	public SerializationExporter() {
		this("ser", "application/java-serialized-object");
	}

	protected SerializationExporter(String fileExtension, String mimeType) {
		super("Java Serialization", fileExtension, mimeType);
	}

	@Override
	public void export(Document document, OutputStream outputStream, FeatureMap options) throws IOException {
		new ObjectOutputStream(outputStream).writeObject(document);
		outputStream.flush();
	}

}
