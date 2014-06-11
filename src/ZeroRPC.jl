module Zerorpc

import ZMQ
import Msgpack
import Base.Random

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
    socket = ZMQ.Socket(ctx, ZMQ.REP)
    return new (functions, ctx, socket)
  end
end

function bind(server::Server, dest)
  ZMQ.bind(server.socket, dest)
end

function run(server::Server)
  while true
      raw_in = convert(IOStream, ZMQ.recv(server.socket))
      seek(raw_in, 0)
      message = Msgpack.unpack(raw_in.data)

      f = server.functions[message[2]]
      ret = apply(f, message[3])

      header = { "v" => 3, "message_id" => string(Random.uuid4()), "response_to" => message[1]["message_id"] }
      response = Any[header, "OK", [ret]]
      raw_out = ZMQ.Message(Msgpack.pack(response))
      ZMQ.send(server.socket, raw_out)
  end
end

end # module
