package dk.safl.beanstemc;

public class BeanstemcException extends Exception {

	private static final long serialVersionUID = 1981287084441643194L;
	
	public BeanstemcException () {
		this(null, null);
	}

	public BeanstemcException(String message) {
		this(message, null);
	}

	public BeanstemcException(String message, Exception cause) {
		super(message, cause);
	}

	public BeanstemcException(Exception cause) {
		this(null, cause);
	}
}