# self hosted telemetry service

```bash
pip3 install -r requirements.txt
```

### Run simulation

```bash
make simulation
```

### Test API endpoint

```bash
make curl
```

### Run Individual Processes

```bash
make process p=p1
make process p=p2
make process p=p3
make process p=p4
```

Each process will stream data to:

```
output/<process_name>/<timestamp>.csv
```

---

## Output Example

For `p1`, the output folder structure will be:

```
output/
└── p1/
    ├── 1743834567.csv
    ├── 1743834568.csv
    └── ...
```
