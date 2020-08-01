## run 4 nodes(sawtooth-pbft-demo)

```
./target/debug/sawtooth-pbft-demo 4 0
./target/debug/sawtooth-pbft-demo 4 1
./target/debug/sawtooth-pbft-demo 4 2
./target/debug/sawtooth-pbft-demo 4 3
```

## run 4 pbft engine(sawtooth-pbft)

```
./target/debug/pbft-engine -C tcp://localhost:5051 -vv
./target/debug/pbft-engine -C tcp://localhost:5052 -vv
./target/debug/pbft-engine -C tcp://localhost:5053 -vv
./target/debug/pbft-engine -C tcp://localhost:5050 -vv
```
