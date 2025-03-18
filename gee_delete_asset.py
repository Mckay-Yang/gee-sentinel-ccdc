import ee

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

assets = ee.data.listAssets(params='projects/ee-yangluhao990714/assets/ccdc_2nd_12_0995')
for asset in assets['assets']:
    if 'ccdc_result_24' in asset['name'] or 'ccdc_result_54' in asset['name']:
        print(asset['name'])
        ee.data.deleteAsset(asset['name'])