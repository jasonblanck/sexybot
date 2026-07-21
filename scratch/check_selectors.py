from web3 import Web3

RPC = "https://polygon.drpc.org"
w3 = Web3(Web3.HTTPProvider(RPC))

onramp_addr = w3.to_checksum_address("0x93070a847efEf7F70739046A929D47a521F5B8ee")
bytecode = w3.eth.get_code(onramp_addr)
print("Bytecode length:", len(bytecode))

# Common signature hashes:
# Let's calculate the standard ones:
# 1. wrap(uint256) -> Web3.keccak(text="wrap(uint256)")[:4].hex()
# 2. wrap(address,uint256) -> Web3.keccak(text="wrap(address,uint256)")[:4].hex()
# 3. wrap(address,address,uint256) -> Web3.keccak(text="wrap(address,address,uint256)")[:4].hex()

selectors = {
    Web3.keccak(text="wrap(uint256)")[:4].hex(): "wrap(uint256)",
    Web3.keccak(text="wrap(address,uint256)")[:4].hex(): "wrap(address,uint256)",
    Web3.keccak(text="wrap(address,address,uint256)")[:4].hex(): "wrap(address,address,uint256)",
    Web3.keccak(text="deposit(uint256)")[:4].hex(): "deposit(uint256)",
    Web3.keccak(text="deposit(address,uint256)")[:4].hex(): "deposit(address,uint256)",
}

for sel, sig in selectors.items():
    if bytes.fromhex(sel) in bytecode:
        print(f"Bytecode contains selector {sel} -> {sig}")
    else:
        print(f"Bytecode does NOT contain selector {sel} -> {sig}")
