# Fresh-VM Setup

This guide takes a brand-new **Ubuntu 22.04 LTS** virtual machine and brings it to the point where every command in [README.md](README.md) works.

It has been verified on:

- **UTM** (Apple Silicon, ARM64) — used for development
- **VirtualBox** (x86_64) — used for the grading machine

The instructions are identical for both because every piece of the stack (OpenJDK, Kafka, PySpark) is byte-code or Python.

---

## 1. Recommended VM sizing

| Resource | Minimum | Comfortable |
| --- | ---: | ---: |
| vCPU | 2 | 4 |
| RAM | 6 GB | 8 GB |
| Disk | 25 GB | 40 GB |

Spark + Kafka + Zookeeper + a Streamlit dashboard fits in 6 GB, but the dataset and the trained model take ~3 GB on disk.

> **VirtualBox tip:** install the *Guest Additions* (Devices → Insert Guest Additions CD) so that the shared clipboard and shared folder work — you will move files in/out of the VM during the demo.

---

## 2. System packages

Open a terminal inside the VM and run:

```bash
sudo apt update
sudo apt install -y \
    openjdk-17-jdk \
    python3.10 python3.10-venv python3-pip \
    git curl wget unzip \
    build-essential
```

Verify Java:

```bash
java -version    # should print openjdk version "17.0.x"
```

Add `JAVA_HOME` to your shell profile:

```bash
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
source ~/.bashrc
echo $JAVA_HOME
```

---

## 3. Project files

You have two options:

### Option A — copy the folder in from the host

Use VirtualBox shared folders or `scp`:

```bash
# on the host
scp -r "big data mini project 3" student@<vm-ip>:~/big-data-mini-project-3
```

### Option B — create the folder on the VM and paste files

```bash
mkdir -p ~/big-data-mini-project-3
cd ~/big-data-mini-project-3
# then copy README.md, src/, scripts/, ... in
```

Either way, **work from `~/big-data-mini-project-3`** from this point on.

---

## 4. Python environment

```bash
cd ~/big-data-mini-project-3
python3.10 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`pyspark==3.5.1` ships its own Spark binaries, so you do **not** need a separate Spark download.

Test PySpark:

```bash
python -c "from pyspark.sql import SparkSession; \
    s=SparkSession.builder.appName('check').getOrCreate(); \
    print('Spark', s.version); s.stop()"
```

---

## 5. Kafka

Run the helper script — it downloads the official Apache Kafka 3.7.0 tarball, extracts it into `~/kafka`, and makes the bin scripts executable.

```bash
bash scripts/01_setup_vm.sh
```

You will end up with:

```
~/kafka/                       # extracted tarball
~/kafka-data/zookeeper/        # zookeeper state
~/kafka-data/kafka/            # broker log dir
```

Both data directories are kept out of `~/kafka` so you can wipe state without re-downloading.

---

## 6. Kaggle credentials (for the dataset)

`src/download_data.py` uses **kagglehub**, which reads `~/.kaggle/kaggle.json`.

1. Go to <https://www.kaggle.com/settings> → *API* → *Create New Token*.
2. Move the downloaded file to the VM and place it at `~/.kaggle/kaggle.json`.
3. Restrict its permissions:

```bash
mkdir -p ~/.kaggle
chmod 600 ~/.kaggle/kaggle.json
```

If you cannot get a Kaggle token onto the VM, `src/download_data.py` has a fallback that reads a local TSV that you copy in manually — see the script's docstring.

---

## 7. Smoke test the stack

In **three separate terminals**, all from `~/big-data-mini-project-3` with `.venv` activated:

```bash
# terminal A — kafka + zookeeper
bash scripts/02_start_kafka.sh

# terminal B — create topic + send/receive one test event
bash scripts/03_create_topic.sh
bash scripts/05_smoke_test.sh
```

You should see `{"user_id": 1, "item_id": 99, "rating": 5.0, "timestamp": "..."}` echoed back. Hit `Ctrl-C` to leave the consumer.

If that worked, you are done with setup — proceed to [README.md](README.md) for the actual project run.

---

## 8. Moving from UTM to VirtualBox (handoff)

When you copy the project to a teammate's VirtualBox VM:

1. Repeat sections **2, 4, 5, 6** on the new VM.
2. **Do not** copy `~/kafka` or `~/kafka-data` — they are arch-specific cache state.
3. **Do** copy `models/als/` if you want to skip retraining; it is plain Parquet and is portable.
4. Re-run the smoke test.

That is the only difference between the two environments.
