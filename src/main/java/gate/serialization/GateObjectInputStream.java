package gate.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import gate.Gate;

public class GateObjectInputStream extends ObjectInputStream {

	public GateObjectInputStream(InputStream in) throws IOException {
		super(in);
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
		try {
			return Class.forName(desc.getName(), false, Gate.getClassLoader());
		} catch (Exception e) {
			return super.resolveClass(desc);
		}
	};
}