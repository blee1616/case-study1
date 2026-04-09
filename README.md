# case1 trading bot

This repository contains scripts for the UTC exchange bot experiments.

## quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## run

```powershell
python -u pennyingA+totalEDA.py
```

## notes

- This repo ignores generated logs and local virtual environments.
- The dependency pins match the currently working protobuf/grpc runtime combination.
