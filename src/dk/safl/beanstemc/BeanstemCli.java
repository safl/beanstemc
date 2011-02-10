/**
 *	This file is part of Beanstemc.
 *
 *	Copyright (C) 2011 Simon A. F. Lund
 *
 *	Beanstemc is free software: you can redistribute it and/or modify
 *	it under the terms of the GNU Lesser General Public License as published by
 *	the Free Software Foundation, either version 3 of the License, or
 *	(at your option) any later version.
 *	
 *	Beanstemc is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *	GNU Lesser General Public License for more details.
 *
 *	You should have received a copy of the GNU Lesser General Public License
 *	along with Beanstemc.  If not, see <http://www.gnu.org/licenses/>.
 *
 *	@author Simon A. F. Lund
 * 
 */
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
