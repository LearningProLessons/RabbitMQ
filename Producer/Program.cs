using Producer.RPC;

Console.WriteLine("RPC Client");
string n = args.Length > 0 ? args[0] : "30";
await InvokeAsync(n);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();


async Task InvokeAsync(string n)
{
    var rpcClient = new RpcClient();
    await rpcClient.StartAsync();

    Console.WriteLine(" [x] Requesting fib({0})", n);
    var response = await rpcClient.CallAsync(n);
    Console.WriteLine(" [.] Got '{0}'", response);
}


 
