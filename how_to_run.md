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

### After, when the venv is not needed anymore, the command for deactivating the virtual environment is as shown below

```bash
$ deactivate
```
