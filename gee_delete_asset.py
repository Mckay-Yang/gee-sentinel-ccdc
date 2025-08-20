import ee

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

assets = ee.data.listAssets(params='projects/ee-yangluhao990714/assets/CCDC/ccdc_final_res')
for asset in assets['assets']:
    if f'ccdc_result_' in asset['name']:
        print(asset['name'])
        ee.data.deleteAsset(asset['name'])