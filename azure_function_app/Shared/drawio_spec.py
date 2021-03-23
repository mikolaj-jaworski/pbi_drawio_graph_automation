import pandas as pd
from io import StringIO

drawio_spec = \
'''# label: %name%<br><i>%type%</i>
#
# style: label;image=%image%;whiteSpace=wrap;html=1;rounded=1;fillColor=%fill%;horizontal=1;
#
# parentstyle: swimlane;whiteSpace=wrap;html=1;childLayout=stackLayout;horizontal=1;horizontalStack=1;resizeParent=1;resizeLast=0;collapsible=1;
#
# namespace: csvimport-
#
# connect: {"from": "parent", "to": "id", "invert": true, "style": "curved=1;endArrow=none;endFill=1;fontSize=11;"}
# connect: {"from": "relatives", "to": "id", "style": "curved=1;fontSize=11;dashed=1;endArrow=none;"}
#
## layout: horizontalflow
# nodespacing: 150
# levelspacing: 200
# edgespacing: 150
# ignore: image,fill
'''

html_spec = \
'''
type	fill	image
workspaces	#ff6666	https://img.icons8.com/carbon-copy/100/000000/power-plant.png
dataflows	#00ccff	https://img.icons8.com/ios/50/000000/database-import.png
datasets	#33cc33	https://img.icons8.com/ios/50/000000/database.png
reports	#ffff99	https://img.icons8.com/ios/50/000000/business-report.png
dashboards	#ff9900	https://img.icons8.com/dotty/80/000000/dashboard.png
users	#ffffff	https://img.icons8.com/windows/32/000000/user-male-circle.png
dataflows_datasources	#b3cccc	
datasets_datasources	#c2c2d6	
'''

html_spec = pd.read_csv(StringIO(html_spec), sep ='\t')