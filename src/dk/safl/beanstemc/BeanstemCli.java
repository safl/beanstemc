package dk.safl.beanstemc;

public class BeanstemCli {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		String	host = args[0];
		int		port = Integer.parseInt(args[1]);
		
		String tube = args[2];
		String message = args[3];
		
		Beanstemc beanstemc = new Beanstemc(host, port);
		
		beanstemc.use(tube);
		beanstemc.put(message.getBytes());

	}

}
