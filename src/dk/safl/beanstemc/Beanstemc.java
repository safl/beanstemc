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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
 
/**
 *	A beanstalkd client.
 *
 *	Based on: https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
 * 
 * TODO:
 * 
 * - verify that int is a proper datatype for ttr, delay and priority. 
 * 
 * Errors produce exceptions. Methods which can either succeed or produce an
 * error returns void.
 * It seems weird to have functions which will either always return true
 * or produce an exception. With such method the return value is never used.
 * Therefore they simply either execute with or without raising an exception. 
 * 
 */
public class Beanstemc {
	
	public static String	DEFAULT_HOST		= "localhost";
	public static int		DEFAULT_PORT		= 11300;
	public static int		DEFAULT_TTR			= 120;
	public static int		DEFAULT_DELAY		= 0;
	public static long		DEFAULT_PRIORITY	= 1024;
	
	public static long	MAX_PRIORITY = 4294967295L;
	public final String	CRLF = "\r\n";
	
	private Socket	c;
	private BufferedInputStream in;
	private OutputStream out;

	public Beanstemc(Socket connection) throws IOException {
		
		c	= connection;
		in	= new BufferedInputStream(c.getInputStream());
		out	= c.getOutputStream();		
		
	}
	
	public Beanstemc() throws UnknownHostException, IOException {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}
	
	public Beanstemc(String host) throws UnknownHostException, IOException {
		this(new Socket(host, DEFAULT_DELAY));		
	}
	
	public Beanstemc(String host, int port) throws UnknownHostException, IOException {
		this(new Socket(host, port));		
	}

		
	/**
	 * A helper function for reading a response line from the current connection.
	 * 
	 * @return Examples: {"WATCHING", "10"}, {"NOT_FOUND"}. 
	 * @throws IOException
	 */
	private String[] readResponseLine() throws IOException {
		
		StringBuilder buf = new StringBuilder();
		
		char cur = ' ';
		char prev;
		
		do {
			
			prev	= cur;
			cur		= (char)in.read();
			buf.append(cur);
			
		} while(prev != '\r' && cur != '\n');
		
		return buf.substring(0, buf.length()-2).split(" ");
		
	}
	
	/**
	 * Read a beanstalkd blob from the current connection.
	 * 
	 * @param bytes Size of the blob.
	 * @return The blob.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	private byte[] readData(int bytes) throws IOException, BeanstemcException {
		
		byte[] data = new byte[bytes];
		int bytes_read	= 0;
		int cur_bytes	= 0;
		
		// Read the blob
		while(bytes_read < bytes) {
			
			cur_bytes = in.read(data);				
			if (cur_bytes > 0) {
				bytes_read += cur_bytes;
			} else {
				throw new IOException("Premature end-of-line when reading job data.");
			}
		}
		
		// Read the CRLF
		in.read();
		in.read();
		
		return data;
		
	}
	
	/**
	 * A helper function for retrieving a job.
	 * 
	 * @return The next job in-bound on the current connection.
	 * @throws IOException
	 * @throws BeanstemcException 
	 */
	private Job readJob() throws IOException, BeanstemcException {
		
		String [] resp = readResponseLine();		
		
		if ((resp.length == 3) && (resp[0].equals("RESERVED") || resp[0].equals("FOUND"))) {
			
			long jobId	= Long.valueOf(resp[1]);
			int bytes	= Long.valueOf(resp[2]).intValue();
			
			return new Job(jobId, readData(bytes));
			
		} else {
			throw new BeanstemcException(String.format("%s", resp[0]));
		}
		
		
	}
	
	private byte[] readYaml() throws IOException, BeanstemcException {
		
		String [] resp = readResponseLine();
		if (resp.length == 2 && resp[0].equals("OK")) {
			
			int bytes	= Long.valueOf(resp[1]).intValue();
			
			return readData(bytes);			
			
		} else {
			throw new BeanstemcException(resp[0]);
		}
		
		
	}
	
	private HashMap<String, String> parseYamlMap(String yaml) throws IOException {
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		String line;
		BufferedReader reader = new BufferedReader(new StringReader(yaml));
		while((line = reader.readLine()) != null) {
			
			String[] kv = line.split(": ");
			if (kv.length == 2) {
				map.put(kv[0], kv[1]);
			}
			
		}
		
		return map;
		
	}
	
	private List<String> parseYamlList(String yaml) throws IOException {
		
		List<String> list = new ArrayList<String>();
		
		String line;
		BufferedReader reader = new BufferedReader(new StringReader(yaml));
		while((line = reader.readLine()) != null) {
			
			String[] kv = line.split("- ");
			if (kv.length == 2) {
				list.add(kv[1]);
			}
			
		}
		
		return list;
	}
	
	/** 
	 * Create and insert a job into the currently used tube (see the "use" method).
	 * 
	 * @param data The payload of the job.
	 * 
	 * @param priority Priority of the job. Smaller priority values will be scheduled before jobs with larger priorities. 
	 * 					<em>Do not use priorities larger 2^32-1</em>
	 * 
	 * @param delay The number of seconds to wait before putting the job in the ready queue.
	 * 				The job will be in the "delayed" state during this time.
	 * 
	 * @param ttr Time to run, the number of seconds to allow a worker to run this job.
	 * 				The time is counted from the moment a worker reserves the job. 
	 *				If the worker does not delete, release, or bury the job within ttr seconds, 
	 *				the job will time out and the server will release the job.
	 *				The minimum ttr is 1. If the client sends 0, the server will silently increase the ttr to 1.
	 * 
	 * @return Id of the job.
	 * 
	 * @throws BeanstemcException
	 * @throws IOException
	 */	
	public long put(byte[] data, long priority, int delay, int ttr) throws IOException, BeanstemcException {
		
		long bytes = data.length;
		
		out.write(String.format("put %d %d %d %d%s", priority, delay, ttr, bytes, CRLF).getBytes());
		out.write(data);
		out.write(CRLF.getBytes());
		out.flush();
		
		long jobId;
		String [] resp = readResponseLine();
		
		if (resp[0].equals("INSERTED") && (resp.length == 2)) {
			jobId = Long.valueOf(resp[1]);
		} else if (resp[0].equals("BURIED") && (resp.length == 2)) {
			throw new BeanstemcException(String.format("%s %d", resp[0], Long.valueOf(resp[1])));
		} else {
			throw new BeanstemcException(resp[0]);
		}
		
		return jobId;
		
	}
	
	/**
	 * @see #put(byte[], long, int, int)
	 */
	public long put(byte[] data) throws IOException, BeanstemcException {		
		return put(data, DEFAULT_PRIORITY, DEFAULT_DELAY, DEFAULT_TTR);		
	}
	
	/**
	 * @see #put(byte[], long, int, int)
	 */
	public long put(byte[] data, long priority) throws IOException, BeanstemcException {		
		return put(data, priority, DEFAULT_DELAY, DEFAULT_TTR);		
	}
	
	/**
	 * @see #put(byte[], long, int, int)
	 */
	public long put(byte[] data, long priority, int delay) throws IOException, BeanstemcException {		
		return put(data, priority, delay, DEFAULT_TTR);		
	}
	
	/**
	 * The "use" command is for producers. Subsequent "put" commands will put jobs into the tube specified by this command.
	 * If no use command has been issued, jobs will be put into the tube named "default".
	 * 
	 * @param tube Name of tube. If the tube does not exist, it will be created. 
	 * 				<em>The name must no greater than 200 bytes</em>.
	 * 
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public void use(String tube) throws IOException, BeanstemcException {
		
		out.write(String.format("use %s%s", tube, CRLF).getBytes());
		out.flush();
		
		String [] resp = readResponseLine();		
		if (!resp[0].equals("USING")) {
			throw new BeanstemcException(String.format("%s", resp[0]));
		}
		
	}
	
	/**
	 * Get a job and exclusive access to it. 
	 * 
	 * If no job is available to be reserved, beanstalkd will wait to send a 
	 * response until one becomes available. Once a job is reserved for the client, 
	 * the client has limited time to run (TTR) before the job times out.
	 * 
	 * When the job times out, the server will put the job back into the ready queue.
	 * Both the TTR and the actual time left can be found in response to the stats-job command.
	 *  
	 * During the TTR of a reserved job, the last second is kept by the server as a safety margin, 
	 * during which the client will not be made to wait for another job. 
	 * If the client issues a reserve command during the safety margin, or if the safety margin 
	 * arrives while the client is waiting on a reserve command the call will throw 
	 * BeanstalkcExption with the message "DEADLINE_SOON".
	 * 
	 * @param timeout A timeout value of 0 will result in a reserve call to immediately return either 
	 * 					a job a throw a BeanstalkcException with the message "TIMED_OUT". 
	 * 					A positive value of timeout will limit the amount of time the client will block 
	 * 					on the reserve request until a job becomes available.
	 * @return Job
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public Job reserve(int timeout) throws IOException, BeanstemcException {
		
		if (timeout > 0) {
			out.write(String.format("reserve-with-timeout %d%s", timeout, CRLF).getBytes());
		} else {
			out.write(String.format("reserve%s", CRLF).getBytes());
		}
		out.flush();
		
		return readJob();
		
	}
	
	/**
	 * Get a job with non-exclusive access.
	 * 
	 * @param jobId
	 * @return Job The next job available.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public Job peek(long jobId) throws IOException, BeanstemcException {
		
		out.write(String.format("peek %d%s", jobId, CRLF).getBytes());
		out.flush();
		
		return readJob();
		
	}
	
	/**
	 * Get non-exclusive access to a <em>ready</em> job.
	 * 
	 * @return Job The next <em>ready</em> job, in the currently used tube.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public Job peekReady() throws IOException, BeanstemcException {
		
		out.write(String.format("peek-ready%s", CRLF).getBytes());
		out.flush();
		
		return readJob();
		
	}
	
	/**
	 * Get non-exclusive access to a <em>delayed</em> job.
	 * 
	 * @return Job The job with the shortest delay left among <em>delayed</em> jobs, in the currently used tube.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public Job peekDelayed() throws IOException, BeanstemcException {
		
		out.write(String.format("peek-delayed%s", CRLF).getBytes());
		out.flush();
		
		return readJob();
		
	}
	
	/**
	 * Get non-exclusive access to a <em>buried</em> job.
	 * 
	 * @return Job The first job in the list of <em>buried</em> jobs, in the currently used tube.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public Job peekBuried() throws IOException, BeanstemcException {
		
		out.write(String.format("peek-buried%s", CRLF).getBytes());
		out.flush();
		
		return readJob();
		
	}
	
	/** 
	 * @see #reserve(int) 
	 */
	public Job reserve() throws IOException, BeanstemcException {
		return reserve(0);
	}
	
	/**
	 * Removes a job from the server entirely.
	 * It is normally used by the client when the job has successfully run to completion.
	 * A client can <em>only</em> delete jobs that it has <em>reserved</em>, jobs in state <em>ready</em>, and jobs in state <em>buried</em>. 
	 * 
	 * @param jobId Id of the job to remove.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public void delete(long jobId) throws IOException, BeanstemcException {
		
		out.write(String.format("delete %d%s", jobId, CRLF).getBytes());
		out.flush();
		
		String [] resp = readResponseLine();
		if (!resp[0].equals("DELETED")) {
			throw new BeanstemcException(String.format("%s, JOBID=[%d]", resp[0], jobId));
		}
		
	}
	
	/** 
	 * @param job Job instance which the caller wishes to delete.
	 * @throws IOException
	 * @throws BeanstemcException
	 * @see #delete(long) 
	 */
	public void delete(Job job) throws IOException, BeanstemcException {
		delete(job.getId());
	}
	
	/**
	 * Put a reserved job back into the ready queue (and marks its state as "ready") to be run by any client.
	 * It is normally used when the job fails because of a transitory error.
	 * 
	 * @param jobId Is the job id to release.
	 * @param priority Is a new priority to assign to the job.
	 * @param delay Number of seconds to wait before putting the job in the ready queue. The job will be in the "delayed" state during this time.
	 * @throws IOException
	 * @throws BeanstemcException 
	 */
	public void release(long jobId, long priority, int delay) throws IOException, BeanstemcException {
		
		// Request
		out.write(String.format("release %d %d %d%s", jobId, priority, delay, CRLF).getBytes());
		out.flush();
		
		// Response	
		String [] resp = readResponseLine();

		if (!resp[0].equals("RELEASED")) {
			throw new BeanstemcException(String.format("%s, [JOBID=%d, PRIORITY=%d, DELAY=%d]", resp[0], jobId, priority, delay));
		}
		
	}
	
	/**
	 * @see #release(long, long, int)
	 */	
	public void release(long jobId, long priority) throws IOException, BeanstemcException {
		release(jobId, priority, DEFAULT_DELAY);
	}
	
	/** 
	 * @see #release(long, long, int)
	 */	
	public void release(long jobId) throws IOException, BeanstemcException {
		release(jobId, DEFAULT_PRIORITY, DEFAULT_DELAY);
	}
	
	/** 
	 * @see #release(long, long, int)
	 */	
	public void release(Job job, long priority, int delay) throws IOException, BeanstemcException {
		release(job.getId(), priority, delay);
	}
	
	/** 
	 * @see #release(long, long, int)
	 */	
	public void release(Job job, long priority) throws IOException, BeanstemcException {
		release(job.getId(), priority, DEFAULT_DELAY);
	}
	
	/** 
	 * @see #release(long, long, int)
	 */	
	public void release(Job job) throws IOException, BeanstemcException {
		release(job.getId(), DEFAULT_PRIORITY, DEFAULT_DELAY);
	}
	
	/** 
	 * Put a job into the "buried" state.
	 * 
	 * Buried jobs are put into a FIFO linked list and will not be touched by the server again 
	 * until a client kicks them with the "kick" command.
	 * 
	 * @param jobId Id of the job to bury.
	 * @param priority A new priority to assign to the job.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public void bury(long jobId, long priority) throws IOException, BeanstemcException {
		
		out.write(String.format("bury %d %d%s", jobId, priority, CRLF).getBytes());
		out.flush();
		
		String [] resp = readResponseLine();
		if (!resp[0].equals("BURIED")) {
			throw new BeanstemcException(String.format("%s, JOBID=[%d, PRIORITY=%d]", resp[0], jobId, priority));
		}
		
	}
	
	/**
	 * @see #bury(long, long)
	 */
	public void bury(long jobId) throws IOException, BeanstemcException {		
		bury(jobId, DEFAULT_PRIORITY);
	}
	
	/** 
	 * Add the named tube to the watch list for the current connection.
	 * 
	 * A reserve command will take a job from any of the tubes in the
	 * watch list. For each new connection, the watch list initially 
	 * consists of one tube, named "default".
	 * 
	 * @param tube Name of the tube to watch, the name can be no longer than 200 bytes. The tube will be created if it does not already exist.
	 * @return The number of tubes currently in the watch list.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public int watch(String tube) throws IOException, BeanstemcException {
		
		out.write(String.format("watch %s%s", tube, CRLF).getBytes());
		out.flush();
			
		String [] resp = readResponseLine();
		if (resp[0].equals("WATCHING")) {
			return Integer.valueOf(resp[1]);
		} else {
			throw new BeanstemcException(String.format("%s", resp[0]));
		}
		
	}
	
	/**
	 * Remove the named tube from the watch list for the current connection.
	 * 
	 * @param tube
	 * @return The number of tubes currently in the watch list.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public int ignore(String tube) throws IOException, BeanstemcException {
		
		out.write(String.format("ignore %s%s", tube, CRLF).getBytes());
		out.flush();
			
		String [] resp = readResponseLine();
		if (resp[0].equals("WATCHING")) {
			return Integer.valueOf(resp[1]);
		} else {
			throw new BeanstemcException(String.format("%s [TUBE=%s]", resp[0], tube));
		}
		
	}
	
	/**
	 * Move buried job(s) into the ready queue.
	 *   
	 * kick only applies to the currently used tube.
	 * 
	 * - If there are any buried jobs, it will only kick buried jobs.
	 * - Otherwise it will kick delayed jobs.
	 * 
	 * @param bound The upper bound on the number of jobs to kick.
	 * 				The server will kick no more than bound jobs.
	 * @return The number of jobs actually kicked.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public int kick(int bound) throws IOException, BeanstemcException {
		
		out.write(String.format("kick %d%s", bound, CRLF).getBytes());
		out.flush();
			
		String [] resp = readResponseLine();
		if (resp[0].equals("KICKED")) {
			return Integer.valueOf(resp[1]);
		} else {
			throw new BeanstemcException(String.format("%s [BOUND=%d]", resp[0], bound));
		}
		
	}
	
	/** 
	 * Request more time to work on a job.
	 * This is useful for jobs that potentially take a long time, but you still want
	 * the benefits of a TTR pulling a job away from an unresponsive worker.
	 * A worker may periodically tell the server that it's still alive and processing a job
	 * (e.g. it may do this on DEADLINE_SOON).
	 * 
	 * @param jobId
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public void touch(long jobId) throws IOException, BeanstemcException {
		
		out.write(String.format("touch %d%s", jobId, CRLF).getBytes());
		out.flush();
			
		String [] resp = readResponseLine();
		if (!resp[0].equals("TOUCHED")) {
			throw new BeanstemcException(String.format("%s [JOBID=%d]", resp[0], jobId));
		}
		
	}	
	
	/**
	 * Delay any new from job being reserved for a given time.
	 * 
	 * @param tube
	 * @param delay
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public void pauseTube(String tube, int delay) throws IOException, BeanstemcException  {
		
		out.write(String.format("pause-tube %s%d%s", tube, delay, CRLF).getBytes());
		out.flush();
			
		String [] resp = readResponseLine();
		if (!resp[0].equals("PAUSED")) {
			throw new BeanstemcException(String.format("%s [TUBE=%s,DELAY=%d]", resp[0], tube, delay));
		}
		
	}
	
	/**
	 * Close the connection.
	 * 
	 * This is done gracefully by sending the server a quit message.
	 * 
	 * @throws IOException
	 */
	public void quit() throws IOException {
		
		out.write(String.format("quit%s", CRLF).getBytes());
		out.flush();
		c.close();
		
	}
	
	/**
	 * @see #quit()
	 * @throws IOException
	 */
	public void close() throws IOException {
		quit();
	}
	
	/**
	 * Statistical information about the system as a whole.
	 * 
	 * @return See protocol documentation for contents of the returned map.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public HashMap<String,String> stats() throws IOException, BeanstemcException {
		
		out.write(String.format("stats%s", CRLF).getBytes());
		out.flush();
		
		return parseYamlMap(new String(readYaml()));
		
	}
	
	/**
	 * Statistical information about the specified job; if it exists.
	 * 
	 * @param jobId
	 * @return See protocol documentation for contents of the returned map.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public HashMap<String,String> statsJob(long jobId) throws IOException, BeanstemcException {
		
		out.write(String.format("stats-job %d%s", jobId, CRLF).getBytes());
		out.flush();
		
		return parseYamlMap(new String(readYaml()));
		
	}
	
	/**
	 * Statistical information about the specified tube; if it exists.
	 * 
	 * @param tube
	 * @return See protocol documentation for contents of the returned map.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public HashMap<String,String> statsTube(String tube) throws IOException, BeanstemcException {
		
		out.write(String.format("stats-tube %s%s", tube, CRLF).getBytes());
		out.flush();
				
		return parseYamlMap(new String(readYaml()));
		
	}
	
	/**
	 * List all tubes.
	 * 
	 * @return List of tube names.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public List<String> listTubes() throws IOException, BeanstemcException {
				
		out.write(String.format("list-tubes%s", CRLF).getBytes());
		out.flush();
		
		return parseYamlList(new String(readYaml()));
		
	}
	
	/**
	 * The tube currently in <em>use</em>.
	 * 
	 * Only one tube can be in use at a time, changing tube is done by invoking: {@link #use(String)}.
	 * 
	 * @return List of tube names.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public String listTubeUsed() throws IOException, BeanstemcException {
		
		out.write(String.format("list-tube-used%s", CRLF).getBytes());
		out.flush();
		
		return parseYamlList(new String(readYaml())).get(0);
	
	}
	
	/**
	 * List all <em>watched</em> tubes.
	 * 
	 * @see #watch(String)
	 * 
	 * @return List of tube names.
	 * @throws IOException
	 * @throws BeanstemcException
	 */
	public List<String> listTubesWatched() throws IOException, BeanstemcException {
		
		out.write(String.format("list-tubes-watched%s", CRLF).getBytes());
		out.flush();
		
		return parseYamlList(new String(readYaml()));
		
	}	

}
