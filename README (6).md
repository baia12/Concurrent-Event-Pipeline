# **Concurrent Event Pipeline**

## **1\. Project Overview**

This project implements a concurrent event-processing pipeline using C11 and POSIX threads (Pthreads). It supports serial and parallel execution, bounded queues, ordered result collection, error handling, and serial verification.

## **2\. Supported Platforms**

Tested environments:

* Ubuntu 26.04.1 LTS running under WSL2, GCC 15.2.0.  
* Windows 11 using MSYS2 UCRT64, GCC 16.2.0.

The following commands use the Ubuntu/WSL2 environment.

## **3\. Compilation**

Open a terminal in the directory containing `event_pipeline.c`.

Compile using:

```
gcc -std=c11 -Wall -Wextra -pthread event_pipeline.c -o event_pipeline
```

## **4\. Running the Program**

### **Serial Mode**

```
./event_pipeline --mode serial --input events.csv --output serial_alerts.csv
```

### **Parallel Mode**

```
./event_pipeline --mode parallel --input events.csv --output parallel_alerts.csv --threads 4 --queue 8 --batch 256 --verify
```

### **Command-Line Options**

| Option | Description |
| ----- | ----- |
| `--mode` | Select `serial` or `parallel` |
| `--input` | Input CSV file |
| `--output` | Output alert file |
| `--threads` | Number of worker threads (1–32) |
| `--queue` | Queue capacity (1–64) |
| `--batch` | Batch size (1–1024) |
| `--verify` | Compare parallel results against serial execution |

Parallel mode requires `--threads`, `--queue`, and `--batch`. The `--verify` option is available only in parallel mode.

## **5\. Input Format**

Each input record must follow this format:

```
id,level,amount
```

Example:

```
1,INFO,100
2,ERROR,250
3,WARN,150
4,ERROR,-50
5,INFO,200
6,WARN,300
7,ERROR,500
8,INFO,-100
```

The output file contains the ERROR records in their original input order.

## **6\. Functional Test Reproduction**

### **Test Preparation**

Create the sample input file:

```
cat > events.csv <<'EOF'
1,INFO,100
2,ERROR,250
3,WARN,150
4,ERROR,-50
5,INFO,200
6,WARN,300
7,ERROR,500
8,INFO,-100
EOF
```

### **Test A – Single Worker**

```
./event_pipeline --mode parallel --input events.csv --output caseA.csv --threads 1 --queue 2 --batch 2 --verify
```

### **Test B – Small Queue and Batch**

```
./event_pipeline --mode parallel --input events.csv --output caseB.csv --threads 4 --queue 1 --batch 1 --verify
```

### **Test C – Empty Input**

```
: > empty.csv
./event_pipeline --mode parallel --input empty.csv --output caseC.csv --threads 4 --queue 1 --batch 1 --verify
```

### **Test D – Final Line Without Newline**

```
printf '1,ERROR,11' > caseD.csv
./event_pipeline --mode parallel --input caseD.csv --output caseD_alerts.csv --threads 3 --queue 1 --batch 4 --verify
```

### **Test E – Invalid Record**

```
printf '1,INFO,5\n2,WARN,7\n3,DEBUG,9\n' > invalid.csv
./event_pipeline --mode parallel --input invalid.csv --output caseE.csv --threads 4 --queue 1 --batch 1 --verify
```

The program should reject the invalid `DEBUG` level.

### **Additional Input Validation – Long Record**

```
python3 -c "open('long.csv','w').write('1,INFO,' + '1'*130 + '\n')"
./event_pipeline --mode serial --input long.csv --output long_alerts.csv
```

The program should reject the oversized record.

### **Additional Input Validation – Output Protection**

```
echo "DO NOT CHANGE THIS FILE" > protected.csv
./event_pipeline --mode parallel --input invalid.csv --output protected.csv --threads 4 --queue 1 --batch 1
cat protected.csv
```

The existing output file should remain unchanged after failure.

### **Additional Input Validation – Invalid CLI Option**

```
./event_pipeline --mode parallel --input events.csv --output test.csv --threads 4 --queue 8 --batch 256 --unknown
```

The program should reject the unsupported option.

### **Test F – Large Input and Repeated Execution**

Generate 10,001 records:

```
python3 -c "with open('large.csv','w') as f:
 for i in range(1,10002):
  level=['INFO','WARN','ERROR'][(i-1)%3]
  amount=(i%101)-50
  f.write(f'{i},{level},{amount}\n')"
```

Run the test five times:

```
for i in {1..5}; do
    ./event_pipeline --mode parallel --input large.csv --output caseF_$i.csv --threads 8 --queue 2 --batch 17 --verify || break
done
```

All five executions should complete successfully with matching serial verification.

## **7\. Performance Benchmark**

The performance evaluation uses 1,000,000 deterministic records, queue capacity 8, batch size 256, and worker counts of 1, 2, 4, and 8\.

Each configuration is executed three times. The median execution time is used for comparison.

The benchmark results and performance discussion are presented in the project report.

## **8\. Expected Output**

For the eight-record sample input, the summary should be:

```
records=8 info=3 warn=2 error=3 amount_sum=1350 alerts=3 verify=PASS
```

The alert file should contain:

```
id,amount
2,250
4,-50
7,500
```

## **9\. Project Files**

```
Concurrent_Event_Pipeline/
├── event_pipeline.c
├── README.md
└── report.pdf
```

