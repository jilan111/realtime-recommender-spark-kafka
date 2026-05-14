#!/usr/bin/env bash
# Bootstrap a fresh Ubuntu VM in ONE command.
#
# Designed for the case where you cannot copy/paste into the VM — drop the
# project folder onto the VM via VirtFS / shared folder, then in the VM run:
#
#     bash ~/big-data-mini-project-3/scripts/00_bootstrap_vm.sh
#
# (You may need to mount the share first — the script prints the exact
# command if it doesn't see itself in a mounted location.)
#
# What it does:
#   1. Installs apt packages (java, python, openssh, build tools)
#   2. Creates the python venv and installs requirements.txt
#   3. Runs scripts/01_setup_vm.sh to install Kafka
#   4. Enables sshd so you can copy-paste from the Mac Terminal afterwards
#   5. Prints the next step (data download + ALS training)
set -euo pipefail

GREEN="\033[32m"; YELLOW="\033[33m"; NC="\033[0m"
say() { echo -e "${GREEN}[bootstrap]${NC} $*"; }
warn() { echo -e "${YELLOW}[bootstrap]${NC} $*"; }

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${ROOT}"

# ---- 1. apt packages ------------------------------------------------------
say "installing apt packages (Java 17, Python, ssh, build tools)..."
sudo apt update
sudo apt install -y \
  openjdk-17-jdk \
  python3 python3-venv python3-pip \
  openssh-server \
  curl wget unzip git build-essential

# ---- 2. enable ssh so you can paste commands from the Mac afterwards ------
say "enabling sshd..."
sudo systemctl enable --now ssh
IP="$(hostname -I | awk '{print $1}')"
say "ssh is up. From the Mac Terminal you can now: ssh ${USER}@${IP}"

# ---- 3. python venv -------------------------------------------------------
if [ ! -d "${ROOT}/.venv" ]; then
  say "creating python venv..."
  python3 -m venv "${ROOT}/.venv"
fi
# shellcheck disable=SC1091
source "${ROOT}/.venv/bin/activate"
say "installing python requirements..."
pip install --upgrade pip
pip install -r requirements.txt

# ---- 4. kafka -------------------------------------------------------------
say "installing Kafka 3.7.0..."
bash "${ROOT}/scripts/01_setup_vm.sh"

# ---- 5. summary -----------------------------------------------------------
cat <<EOF

${GREEN}=============================================================${NC}
 bootstrap complete.

 Next steps (you can now paste these from your Mac via SSH):

   ssh ${USER}@${IP}                              # from Mac
   cd ${ROOT}
   source .venv/bin/activate

   # one-time: kaggle token at ~/.kaggle/kaggle.json (chmod 600)
   python src/download_data.py        # ~5–8 min
   python src/train_als.py            # ~6–10 min
   bash scripts/test_pipeline.sh      # ~3 min — must print PASS:11 FAIL:0

${GREEN}=============================================================${NC}
EOF
