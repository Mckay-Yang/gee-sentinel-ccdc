import ee

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

assets = ee.data.listAssets(params='projects/ee-yangluhao990714/assets/CCDC/ccdc_4th_12_009')
for asset in assets['assets']:
    for index in [134]:
        if f'ccdc_result_{index}_3_0' in asset['name']:
            print(asset['name'])
            ee.data.deleteAsset(asset['name'])