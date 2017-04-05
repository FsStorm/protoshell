//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.prolucid.protoshell;

import java.io.*;
import java.util.*;

import com.google.protobuf.*;
import com.prolucid.protoshell.messages.Multilang;
import org.apache.log4j.Logger;
import org.apache.storm.multilang.*;
import org.apache.storm.task.TopologyContext;

public class ProtoSerializer implements ISerializer {
    public static Logger LOG = Logger.getLogger(ProtoSerializer.class);
	OutputStream processIn;
	InputStream processOut;
	static Multilang.Heartbeat heartbeat = Multilang.Heartbeat.newBuilder().build();
	static Multilang.NextCommand next = Multilang.NextCommand.newBuilder().build();
	static Multilang.ActivateCommand activate = Multilang.ActivateCommand.newBuilder().build();
	static Multilang.DeactivateCommand deactivate = Multilang.DeactivateCommand.newBuilder().build();

	public void initialize(OutputStream processIn, InputStream processOut) {
		this.processIn = processIn;
		this.processOut = processOut;
	}
    
    Object ofVariant(Multilang.Variant v) {
		switch (v.getKindCase()) {
			case STRVAL:
				return v.getStrVal();
			case INT32VAL:
				return v.getInt32Val();
			case INT64VAL:
				return v.getInt64Val();
			case FLOATVAL:
				return v.getFloatVal();
			case DOUBLEVAL:
				return v.getDoubleVal();
			case BOOLVAL:
				return v.getBoolVal();
			case BYTESVAL:
				return v.getBytesVal().toByteArray();
			case TIMESTAMPVAL:
				Timestamp ts = v.getTimestampVal();
				return new Date(ts.getSeconds()*1000+(ts.getNanos()/1000000));
			case NONEVAL:
				return null;
			default:
				throw new RuntimeException("Unmapped Variant type: "+v.getKindCase());
		}
    }
    
    Multilang.Variant toVariant(Object o) {
        Multilang.Variant.Builder v = Multilang.Variant.newBuilder();
		if(o == null) {
			v.setNoneVal(NullValue.NULL_VALUE);
		} else if(o instanceof Boolean) {
            v.setBoolVal((Boolean)o);
        } else if(o instanceof Double) {
            v.setDoubleVal((Double)o);
		} else if(o instanceof Float) {
			v.setFloatVal((Float)o);
        } else if(o instanceof Integer) {
            v.setInt32Val((Integer) o);
        } else if(o instanceof Long) {
            v.setInt64Val((Long)o);
        } else if(o instanceof String) {
            v.setStrVal((String)o);
        } else if(o instanceof Date) {
			Long millis = ((Date)o).getTime();
            v.setTimestampVal(Timestamp.newBuilder().setSeconds(millis / 1000).setNanos((int) ((millis % 1000) * 1000000)).build());
		} else if(o instanceof byte[]) {
			v.setBytesVal(ByteString.copyFrom((byte[])o));
        } else {
			throw new RuntimeException("Unexpected type: "+o.getClass());
		}
        return v.build();
    }

	Value toValue(Object o) {
		Value.Builder v = Value.newBuilder();
		if(o == null) {
			v.setNullValue(NullValue.NULL_VALUE);
		} else if(o instanceof Boolean) {
			v.setBoolValue((Boolean)o);
		} else if(o instanceof Double) {
			v.setNumberValue((Double)o);
		} else if(o instanceof Integer) {
			v.setNumberValue((Integer) o);
		} else if(o instanceof Long) {
			v.setNumberValue((Long)o);
		} else if(o instanceof String) {
			v.setStringValue((String)o);
		} else if(o instanceof clojure.lang.PersistentVector) {
			ListValue.Builder lb = ListValue.newBuilder();
			for(Object vo:(clojure.lang.PersistentVector)o) {
					lb.addValues(toValue(vo)) ;
			}
			v.setListValue(lb);
		} else {
			LOG.warn("Skipping unmapped Value type: "+o.getClass());
			v.setNullValue(NullValue.NULL_VALUE);
		}
		return v.build();
	}

	Map<String,Value> toValueMap(Map map) {
        Map<String,Value> varMap = new HashMap<String,Value>();
        for(Object o:map.entrySet()){
            Map.Entry e = (Map.Entry)o;
            varMap.put(e.getKey().toString(),toValue(e.getValue()));
        }
        return varMap;
    }

	private String compPid;

	public Number connect(Map conf, TopologyContext context) throws IOException, NoOutputException {
        Multilang.Context ctx = Multilang.Context.newBuilder()
			.setTaskId(context.getThisTaskId())
			.setComponentId(context.getThisComponentId())
			.putAllTaskComponents(context.getTaskToComponent())
			.build();

        LOG.info("Writing configuration to shell component");
        writeMessage(Multilang.StormMsg.newBuilder()
				.setHandshake(Multilang.Handshake.newBuilder()
						.setContext(ctx)
						.setPidDir(context.getPIDDir())
						.putAllConfig(toValueMap(conf))));

        LOG.info("Waiting for pid from component");
        Multilang.ShellMsg inMsg = readMessage(Multilang.ShellMsg.parser());
        LOG.info("Shell component connection established.");
		this.compPid = "["+context.getThisComponentId() + "@" +inMsg.getPid().getPid()+"]: ";
        return inMsg.getPid().getPid();
	}

	public ShellMsg readShellMsg() throws IOException, NoOutputException {
		Multilang.ShellMsg msg = readMessage(Multilang.ShellMsg.parser());

		ShellMsg shellMsg = new ShellMsg();
		switch (msg.getMsgCase()) {
			case EMIT:
				Multilang.EmitCommand cmd = msg.getEmit();
				String id = cmd.getId();
				if(id != null && !id.isEmpty()) { shellMsg.setId(id); }
				shellMsg.setCommand("emit");
				shellMsg.setAnchors(cmd.getAnchorsList());
				shellMsg.setStream(cmd.getStream());
				shellMsg.setTask(cmd.getTask());
				shellMsg.setNeedTaskIds(cmd.getNeedTaskIds());
				for (Multilang.Variant v: cmd.getTupleList()) {
					shellMsg.addTuple(ofVariant(v));
				}
				break;
			case SYNC:
            	shellMsg.setCommand("sync");
				break;
			case FAIL:
				Multilang.FailReply fail = msg.getFail();
				shellMsg.setCommand("fail");
				shellMsg.setId(fail.getId());
				break;
			case OK:
				Multilang.OkReply ack = msg.getOk();
				shellMsg.setCommand("ack");
				shellMsg.setId(ack.getId());
				break;
			case LOG:
				Multilang.LogCommand log = msg.getLog();
				shellMsg.setCommand("log");
				shellMsg.setMsg(log.getText());
				shellMsg.setLogLevel(log.getLevel().getNumber());
				break;
		}
// TODO: Metrics?
//		shellMsg.setMetricName(cmd.getName());
//		shellMsg.setMetricParams(cmd.getParamsList());

		return shellMsg;
	}

	public void writeBoltMsg(BoltMsg msg) throws IOException {
		Multilang.StormMsg.Builder b = Multilang.StormMsg.newBuilder();
		if (msg.getStream() == "__heartbeat") {
			b.setHeartbeat(heartbeat);
		} else {
			List<Multilang.Variant> fields = new ArrayList<Multilang.Variant>();
			for (Object o: msg.getTuple()) {
                fields.add(toVariant(o));
			}
			Multilang.StreamIn cmd = Multilang.StreamIn.newBuilder()
				.setId(msg.getId())
				.setComp(msg.getComp())
				.setStream(msg.getStream())
				.setTask((int)msg.getTask())
				.addAllTuple(fields)
				.build();
			b.setStreamIn(cmd);
		}
		writeMessage(b);
	}

	public void writeSpoutMsg(SpoutMsg msg) throws IOException {
		Multilang.StormMsg.Builder b = Multilang.StormMsg.newBuilder();
		if (msg.getCommand() == "next") {
			b.setNextCmd(next);
		} else if (msg.getCommand() == "ack") {
			b.setAckCmd(Multilang.AckCommand.newBuilder().setId(msg.getId().toString()));
		} else if (msg.getCommand() == "fail") {
			b.setNackCmd(Multilang.NackCommand.newBuilder().setId(msg.getId().toString()));
		} else if (msg.getCommand() == "activate") {
			b.setActivateCmd(activate);
		} else if (msg.getCommand() == "deactivate") {
			b.setDeactivateCmd(deactivate);
		} else {
			throw new IOException(this.compPid+"Unexpected spout message: "+msg.getCommand()	);
		}
        writeMessage(b);
	}

	public void writeTaskIds(List<Integer> taskIds) throws IOException {
        writeMessage(Multilang.StormMsg.newBuilder()
				.setTaskIds(Multilang.TaskIdsReply.newBuilder().addAllTaskIds(taskIds)));
	}

	private void writeMessage(Multilang.StormMsg.Builder builder) throws IOException {
        try {
			Multilang.StormMsg msg = builder.build();
			if(LOG.isDebugEnabled()) LOG.debug(this.compPid+"Sending: "+msg);
			msg.writeDelimitedTo(this.processIn);
            processIn.flush();
		} catch (Exception x) {
			throw new IOException(this.compPid+"Unable to write a message: "+x.getMessage(),x);
		}
    }

	private Multilang.ShellMsg readMessage(Parser parser) throws IOException {
		try {
			return (Multilang.ShellMsg)parser.parseDelimitedFrom(this.processOut);
		} catch (Exception x) {
			throw new IOException(this.compPid+"Unable to read a message: "+x.getMessage(),x);
		}
    }
}
