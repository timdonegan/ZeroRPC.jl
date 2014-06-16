module ZeroRPC

import ZMQ
import Msgpack
import Base.Random

function unpack(envelope::Array{ZMQ.Message})
  raw_msg = convert(IOStream, envelope[3])
  seek(raw_msg, 0)
  return Msgpack.unpack(raw_msg.data)
end

function generate_heartbeat(response_to)
  header = { "v" => 3, "message_id" => string(Random.uuid4()), "response_to" => response_to}
  response = Any[header, "_zpc_hb", [0]]
  raw_response = ZMQ.Message(Msgpack.pack(response))
  return raw_response
end

type Server
  functions::Dict{String, Function}
  ctx::ZMQ.Context
  socket::ZMQ.Socket

  function Server(_module::Module)

    functions = Dict{String, Function}()

    for func_symbol in names(_module)
      f = eval(_module, func_symbol)
      if isa(f, Function)
        functions[string(func_symbol)] = f
      end
    end

    ctx = ZMQ.Context(1)
    socket = ZMQ.Socket(ctx, ZMQ.ROUTER)
    return new (functions, ctx, socket)
  end
end

function bind(server::Server, dest)
  ZMQ.bind(server.socket, dest)
end

function run(server::Server)
  while true
    envelope::Array{ZMQ.Message} = ZMQ.recv_multipart(server.socket)
    message = unpack(envelope)

    if message[2] == "_zpc_hb"
      # ignore incoming heartbeats for now
      continue
    end

    response_type = "OK"
    ret = nothing
    f = function()
          try
            ret = Any[apply(server.functions[message[2]], message[3])]
          catch e
            buf = IOBuffer()
            showerror(STDERR, e, catch_backtrace())
            showerror(buf, e, catch_backtrace())
            ret = Any[string(e), string(e), takebuf_string(buf)]
            response_type = "ERR"
          end
        end
    task = @async f()
    a = time()
    while !istaskdone(task)
      b = time()
      if b - a > 5
        # send heartbeat
        env = deepcopy(envelope)
        env[3] = generate_heartbeat(message[1]["message_id"])
        ZMQ.send_multipart(server.socket, env)
        a = b
      end
      yield()
    end

    header = { "v" => 3, "message_id" => string(Random.uuid4()), "response_to" => message[1]["message_id"] }
    response = Any[header, response_type, ret]
    raw_response = ZMQ.Message(Msgpack.pack(response))
    envelope[3] = raw_response
    ZMQ.send_multipart(server.socket, envelope)
  end
end

end # module
