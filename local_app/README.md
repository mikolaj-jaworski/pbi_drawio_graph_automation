## Version of the script to run locally

### Environment setup

To set up virtual environment, run below commands in your bash terminal.
```bash
conda create --name pbidrawio python=3.7 --yes
conda activate pbidrawio
pip3 install --upgrade pip --user
pip3 install -r requirements.txt --user
```

### How to use?

In order to run the script, you need to run the script from your terminal.
```bash
python create_graph.py --user <your username> --client <your app client id> --tenant <your app tenant id> --ws_names <list of workspace names, leave empty if want all>
```

Examples:
```bash
python create_graph.py --user xyz@gmail.com --client bbbbbbb --tenant aaaaaaa --ws_names "workspace number one" "workspace number two"
python create_graph.py --user xyz@gmail.com --client bbbbbbb --tenant aaaaaaa --ws_names
```
