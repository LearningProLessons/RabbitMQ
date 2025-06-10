using Producer.RPC;

var rpcClient = new RpcClient();
await rpcClient.StartAsync();

var response = await rpcClient.PayAsync("Sep", 250000);
Console.WriteLine(response);

response = await rpcClient.PayAsync("Pasargad", 540000);
Console.WriteLine(response);

await rpcClient.DisposeAsync();
