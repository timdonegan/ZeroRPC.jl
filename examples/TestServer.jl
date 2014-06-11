import ZeroRPC

module TestServer

function hello(name)
  ret = @sprintf "Hello, %s" name
  return ret
end

function add(a, b)
  return a+b
end

export hello, add

end # module

server = ZeroRPC.Server(TestServer)
ZeroRPC.bind(server, "tcp://127.0.0.1:4242")
ZeroRPC.run(server)
