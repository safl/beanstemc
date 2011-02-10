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