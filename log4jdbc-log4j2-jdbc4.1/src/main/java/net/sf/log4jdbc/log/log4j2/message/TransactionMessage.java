/*
 * Copyright (c) 2009-2015, Architector Inc., Japan
 * All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sf.log4jdbc.log.log4j2.message;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import net.sf.log4jdbc.sql.jdbcapi.ConnectionSpy;
import org.apache.logging.log4j.message.Message;

public class TransactionMessage extends SqlMessage implements Message {

	public static enum Operation {

		SET_TRANSACTION_ISOLEATION,

		SET_AUTO_COMMIT,

		SET_SAVE_POINT,

		ROLLBACL_SAVE_POINT,

		RELEASE_SAVE_POINT,

		COMMIT,

		ROLLBACK,

		CONNECTION_CLOSED,

	}

	private static final long serialVersionUID = 1L;

	private static final Map<String, Operation> operations;

	static {
		Map<String, Operation> map;

		map = new HashMap<String, Operation>();
		map.put(
				"setTransactionIsolation",
				Operation.SET_TRANSACTION_ISOLEATION);
		map.put(
				"setAutoCommit",
				Operation.SET_AUTO_COMMIT);
		map.put(
				"setSavepoint",
				Operation.SET_SAVE_POINT);
		map.put(
				"releaseSavepoint",
				Operation.RELEASE_SAVE_POINT);
		map.put(
				"commit",
				Operation.COMMIT);
		map.put(
				"rollback",
				Operation.ROLLBACK);
		map.put(
				"close",
				Operation.CONNECTION_CLOSED);
		operations = Collections.unmodifiableMap(map);
	}

	private static final Map<String, String> isolations;

	static {
		Map<String, String> map;

		map = new HashMap<String, String>();
		map.put(
				Integer.toString(ConnectionSpy.TRANSACTION_NONE),
				"none");
		map.put(
				Integer.toString(ConnectionSpy.TRANSACTION_READ_UNCOMMITTED),
				"read_uncommitted");
		map.put(
				Integer.toString(ConnectionSpy.TRANSACTION_READ_COMMITTED),
				"read_committed");
		map.put(
				Integer.toString(ConnectionSpy.TRANSACTION_REPEATABLE_READ),
				"repeatable_read");
		map.put(
				Integer.toString(ConnectionSpy.TRANSACTION_SERIALIZABLE),
				"serializable");
		isolations = Collections.unmodifiableMap(map);
	}

	private ConnectionSpy connection;

	private Operation operation;

	private String parameter;

	private String result;

	public TransactionMessage(
			ConnectionSpy connection,
			Operation operation,
			String parameter,
			String result,
			boolean isDebugEnabled) {
		super(isDebugEnabled);
		this.connection = connection;
		this.operation = operation;
		this.parameter = parameter;
		this.result = result;
	}

	public TransactionMessage() {
		this(null, null, null, null, false);
	}

	@Override
	protected void buildMessage() {
		StringBuilder message;

		message = new StringBuilder();
		if (this.isDebugEnabled()) {
			message.append(SqlMessage.getDebugInfo());
			message.append(SqlMessage.nl);
		}
		message.append(this.connection.getConnectionNumber());
		message.append(". Transaction ");
		switch (this.operation) {
		case SET_TRANSACTION_ISOLEATION :
			message.append("set transaction isolation to ");
			message.append(this.parameter);
			message.append('.');
			break;
		case SET_AUTO_COMMIT :
			message.append("set auto commit to ");
			message.append(this.parameter);
			message.append('.');
			break;
		case SET_SAVE_POINT :
			message.append("set save point ");
			message.append(this.result);
			if (parameter.length() > 0)
				message.append(' ').append(parameter);
			message.append('.');
			break;
		case ROLLBACL_SAVE_POINT :
			message.append("rollbacked save point ");
			message.append(this.parameter);
			message.append('.');
			break;
		case RELEASE_SAVE_POINT :
			message.append("released save point ");
			message.append(this.parameter);
			message.append('.');
			break;
		case COMMIT :
			message.append("committed.");
			break;
		case ROLLBACK :
			message.append("rollbacked.");
			break;
		case CONNECTION_CLOSED :
			message.append("aborted (connection closed).");
			break;
		}
		this.setMessage(message.toString());
	}

	public static TransactionMessage apply(
			ConnectionSpy connection,
			String method,
			String result,
			boolean isDebugEnabled) {
		int open;
		Operation operation;
		String parameter;

		open = method.indexOf('(');
		if (open < 0 || !method.endsWith(")")) return null;
		operation = operations.get(method.substring(0, open));
		if (operation == null) return null;
		parameter = method.substring(open + 1, method.length() - 1);
		switch (operation) {
		case SET_TRANSACTION_ISOLEATION :
			if (isolations.containsKey(parameter))
				parameter = isolations.get(parameter);
			else
				parameter = "unknown (" + parameter + ")";
			break;
		case ROLLBACK :
			if (parameter.length() > 0)
				operation = Operation.ROLLBACL_SAVE_POINT;
			break;
		case CONNECTION_CLOSED :
			if (connection.getLastAutoCommit()) return null;
			break;
		default :
			break;
		}
		return new TransactionMessage(
				connection,
				operation,
				parameter,
				result,
				isDebugEnabled);
	}

}
