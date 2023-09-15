import os

os.environ['envn'] = 'Dev'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = 'EndtoEnd'

###Setup Directories
current = os.getcwd()
rawsource = current + '\RawSource'
cleansedsource = current + '\CleansedSource'
transformed = current + '\TransformedData'

#print(os.getcwd())