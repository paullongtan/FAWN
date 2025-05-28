# FAWN
CSE223B Final Project - FAWN: A Fast Array of Wimpy Nodes


## Run the Client:
You need 4 terminals
1. cd into fawn/workspace/
2. run `cargo run -p fawn_frontend -- fawn_frontend/test/front_1.config` 
3. run `cargo run -p fawn_frontend -- fawn_frontend/test/front_2.config` 
4. for backend, `cd fawn/workspace/fawn_backend` because our test_data will be in here
5. run `cargo run -p fawn_backend -- test/config.json`
6. for client, `cd fawn/workspace/fawn_client` because our test_data will be in here
7. run `cargo run`


## Using the client:
Commands:
- put `<k>` `<v>`
- get `<k>`
- generate `<filename>` `<number_of_bytes>`


Example:
- generate 1kb.bin 1024 // this will generate a 1024b random file in test/put/1kb.bin
- put test1 1kb.bin
- get test1             // this will store the retrieved data in a file in test/get/test1.bin