import ee

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

assets = ee.data.listAssets(params='projects/ee-yangluhao990714/assets/CCDC/tmp')

for asset in assets['assets']:
    print(asset['name'])
    ee.data.deleteAsset(asset['name'])