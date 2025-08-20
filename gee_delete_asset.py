import ee

ee.Authenticate()
ee.Initialize(project='project-id')

assets = ee.data.listAssets(params='asset-id-to-delete')

for asset in assets['assets']:
    if f'ccdc_result_' in asset['name']:
        print(asset['name'])
        ee.data.deleteAsset(asset['name'])