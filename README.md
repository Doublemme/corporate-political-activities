# Corporate Political Activities

## Install

Put the datasets inside the folder `data/datasets`

Allowed files:

`dataset.xlsx`

`lobbying_full.xlsx`

`PAC_Full.xlsx`

`SCAC_edited.xlsx`


### Init the env

```bash
python -m venv .venv
```
On macOS/Linux
```bash
source .venv/bin/activate
```

On Windows
```bash
.venv/Scripts/activate.bat //In CMD
.venv/Scripts/Activate.ps1 //In Powershel
```

### Install the packages

```bash
pip install -r requirments.txt
```

## Run

For compile the dataset simply run
`
```bash
python .src/main.py
```

You can find the result file in the csv format inside `output/dataset.csv`