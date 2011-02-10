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
 * @author Simon A. F. Lund
 * 
 */
package dk.safl.beanstemc;

public class Job {

	private long id;
	private byte[] data;	
	
	public Job(long id, byte[] data) {
		this.id		= id;
		this.data	= data;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public long getId() {
		return id;
	}
	
}
