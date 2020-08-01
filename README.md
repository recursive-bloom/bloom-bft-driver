## run 4 nodes(bloom-bft-driver)

```
./target/debug/bloom-bft-driver 4 0
./target/debug/bloom-bft-driver 4 1
./target/debug/bloom-bft-driver 4 2
./target/debug/bloom-bft-driver 4 3
```

## run 4 pbft engine(sawtooth-pbft)

```
./target/debug/pbft-engine -C tcp://localhost:5051 -vv
./target/debug/pbft-engine -C tcp://localhost:5052 -vv
./target/debug/pbft-engine -C tcp://localhost:5053 -vv
./target/debug/pbft-engine -C tcp://localhost:5050 -vv
```
