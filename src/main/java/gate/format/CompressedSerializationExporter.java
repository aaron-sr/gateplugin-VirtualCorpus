package gate.format;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.FeatureMap;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;
import gate.serialization.DocumentUtil;

@CreoleResource(name = "Compressed Serialization Exporter", tool = true, autoinstances = @AutoInstance)
public class CompressedSerializationExporter extends SerializationExporter {
	private static final long serialVersionUID = -8349637629674835906L;
	private static Logger logger = Logger.getLogger(CompressedSerializationExporter.class);

	public CompressedSerializationExporter() {
		super("Compressed Java Serialization", "ser.zz", "application/compressed-java-serialized-object");
	}

	@Override
	public void export(Document document, OutputStream outputStream, FeatureMap options) throws IOException {
		DocumentUtil.writeDocument(document, outputStream, true);
	}

}
