# Description

### Input Data
The given JSON file contains 20K articles (1 object per line).
Each article contains set of attributes with one set value.

### Recommendation Engine Requirements
Calculate similarity of articles identified by SKU (product identifier) based on their attributes values.
The number of matching attributes is the most important metric for defining similarity.
In case of a draw, attributes with a name higher in alphabet (a is higher than z) is weighted heavier.

Example 1:
{"sku":"sku-1","attributes": {"att-a": "a1", "att-b": "b1", "att-c": "c1"}} is more similar to
{"sku":"sku-2","attributes": {"att-a": "a2", "att-b": "b1", "att-c": "c1"}} than to
{"sku":"sku-3","attributes": {"att-a": "a1", "att-b": "b3", "att-c": "c3"}}

Example 2:
{"sku":"sku-1","attributes":{"att-a": "a1", "att-b": "b1"}} is more similar to 
{"sku":"sku-2","attributes":{"att-a": "a1", "att-b": "b2"}} than to
{"sku":"sku-3","attributes":{"att-a": "a2", "att-b": "b1"}}

### Recommendation request example
sku-123 > ENGINE > 10 most similar SKUs based on criteria described above with their corresponding weights.

### Implementation requirements
programming Language: scala or python.

### Code requirements
Clean structured reusable code.

### Expected delivery format
Gzipped file containing solution with simple instructions how to run data import and how to execute recommendation request.

# How to run

**Make sure that you have installed at least Python 3.8 on your machine**

### 1. Please go to the task's home directory, and for launching the program, it's best to create a virtual environment as follows

```bash
$ python3 -m venv task

$ source task/bin/activate
```

### 2. Next, type in terminal this command and wait till all of the necessary dependencies are resolved

```bash
$ pip3 install -r requirements.txt
```

### 3. Aftewards please run following command to run unit tests

```bash
$ pytest test_recommendation_engine.py  
```

### 4. Now run the main.py script by typing the command below and passing necessary keyword parameters (--sku_name, --json_file, -num) for example recommendation request

```bash
$ python3 main.py --sku_name="sku-222" --json_file="test-data.json" --num=100
```

### After, when the venv is not needed anymore, the command for deactivating the virtual environment is shown below

```bash
$ deactivate
```
