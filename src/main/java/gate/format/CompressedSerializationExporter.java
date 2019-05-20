package gate.format;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.FeatureMap;
import gate.creole.metadata.AutoInstance;
import gate.creole.metadata.CreoleResource;

@CreoleResource(name = "Compressed Serialization Exporter", tool = true, autoinstances = @AutoInstance)
public class CompressedSerializationExporter extends SerializationExporter {
	private static final long serialVersionUID = -8349637629674835906L;
	private static Logger logger = Logger.getLogger(CompressedSerializationExporter.class);

	public CompressedSerializationExporter() {
		super("ser.zz", "application/compressed-java-serialized-object");
	}

	@Override
	public void export(Document document, OutputStream outputStream, FeatureMap options) throws IOException {
		super.export(document, new DeflaterOutputStream(outputStream, true), options);
	}

}
