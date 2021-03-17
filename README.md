# PowerBI Service API & Draw.io automation with Python

Automatic creation of graph involving resources and relationships from PowerBI Service may be useful when documenting company's reporting system. 
This project may be helpful in achieving such goals.

Before running the code, you need to set up your PowerBI & Azure apps. 
[This article](https://www.datalineo.com/post/power-bi-rest-api-with-python-and-microsoft-authentication-library-msal) may be helpful (it also explains part of the code).


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
