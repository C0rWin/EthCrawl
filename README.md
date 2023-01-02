# Example of fetching block from the Ethereum network

In this example we get connected to the test Ethereum network via Infura, where we need to provide 
`PROJECT-ID` while running the main program, which you can get after registration at Infura services.

Example on how to run the program:

```
go run main.go PROJECT-ID
```

It will subscribe to the block notification web socket and start fetch new comming blocks, transfer result
with transaction into custom data struct extracting relevant information and printing results into console.
