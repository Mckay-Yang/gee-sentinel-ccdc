import ee
import geemap
import utils

utils.ee_init()

bands_list = ee.List([
    ee.Dictionary({'tBreak': [
        'tBreak_0', 'tBreak_1', 'tBreak_2', 'tBreak_3', 'tBreak_4',
        'tBreak_5', 'tBreak_6', 'tBreak_7', 'tBreak_8', 'tBreak_9'
    ]}),
    ee.Dictionary({'changeProb': [
        'changeProb_0', 'changeProb_1', 'changeProb_2', 'changeProb_3', 'changeProb_4',
        'changeProb_5', 'changeProb_6', 'changeProb_7', 'changeProb_8', 'changeProb_9'
    ]}),
    ee.Dictionary({'Blue_magnitude': [
        'Blue_magnitude_0', 'Blue_magnitude_1', 'Blue_magnitude_2', 'Blue_magnitude_3', 'Blue_magnitude_4',
        'Blue_magnitude_5', 'Blue_magnitude_6', 'Blue_magnitude_7', 'Blue_magnitude_8', 'Blue_magnitude_9'
    ]}),
    ee.Dictionary({'Green_magnitude': [
        'Green_magnitude_0', 'Green_magnitude_1', 'Green_magnitude_2', 'Green_magnitude_3', 'Green_magnitude_4',
        'Green_magnitude_5', 'Green_magnitude_6', 'Green_magnitude_7', 'Green_magnitude_8', 'Green_magnitude_9'
    ]}),
    ee.Dictionary({'Red_magnitude': [
        'Red_magnitude_0', 'Red_magnitude_1', 'Red_magnitude_2', 'Red_magnitude_3', 'Red_magnitude_4',
        'Red_magnitude_5', 'Red_magnitude_6', 'Red_magnitude_7', 'Red_magnitude_8', 'Red_magnitude_9'
    ]}),
    ee.Dictionary({'NIR_magnitude': [
        'NIR_magnitude_0', 'NIR_magnitude_1', 'NIR_magnitude_2', 'NIR_magnitude_3', 'NIR_magnitude_4',
        'NIR_magnitude_5', 'NIR_magnitude_6', 'NIR_magnitude_7', 'NIR_magnitude_8', 'NIR_magnitude_9'
    ]}),
    ee.Dictionary({'SWIR1_magnitude': [
        'SWIR1_magnitude_0', 'SWIR1_magnitude_1', 'SWIR1_magnitude_2', 'SWIR1_magnitude_3', 'SWIR1_magnitude_4',
        'SWIR1_magnitude_5', 'SWIR1_magnitude_6', 'SWIR1_magnitude_7', 'SWIR1_magnitude_8', 'SWIR1_magnitude_9'
    ]}),
    ee.Dictionary({'SWIR2_magnitude': [
        'SWIR2_magnitude_0', 'SWIR2_magnitude_1', 'SWIR2_magnitude_2', 'SWIR2_magnitude_3', 'SWIR2_magnitude_4',
        'SWIR2_magnitude_5', 'SWIR2_magnitude_6', 'SWIR2_magnitude_7', 'SWIR2_magnitude_8', 'SWIR2_magnitude_9'
    ]}),
    ee.Dictionary({'NDVI_magnitude': [
        'NDVI_magnitude_0', 'NDVI_magnitude_1', 'NDVI_magnitude_2', 'NDVI_magnitude_3', 'NDVI_magnitude_4',
        'NDVI_magnitude_5', 'NDVI_magnitude_6', 'NDVI_magnitude_7', 'NDVI_magnitude_8', 'NDVI_magnitude_9'
    ]}),
    ee.Dictionary({'EVI_magnitude': [
        'EVI_magnitude_0', 'EVI_magnitude_1', 'EVI_magnitude_2', 'EVI_magnitude_3', 'EVI_magnitude_4',
        'EVI_magnitude_5', 'EVI_magnitude_6', 'EVI_magnitude_7', 'EVI_magnitude_8', 'EVI_magnitude_9'
    ]}),
    ee.Dictionary({'TCB_magnitude': [
        'TCB_magnitude_0', 'TCB_magnitude_1', 'TCB_magnitude_2', 'TCB_magnitude_3', 'TCB_magnitude_4',
        'TCB_magnitude_5', 'TCB_magnitude_6', 'TCB_magnitude_7', 'TCB_magnitude_8', 'TCB_magnitude_9'
    ]}),
    ee.Dictionary({'TCG_magnitude': [
        'TCG_magnitude_0', 'TCG_magnitude_1', 'TCG_magnitude_2', 'TCG_magnitude_3', 'TCG_magnitude_4',
        'TCG_magnitude_5', 'TCG_magnitude_6', 'TCG_magnitude_7', 'TCG_magnitude_8', 'TCG_magnitude_9'
    ]}),
    ee.Dictionary({'TCW_magnitude': [
        'TCW_magnitude_0', 'TCW_magnitude_1', 'TCW_magnitude_2', 'TCW_magnitude_3', 'TCW_magnitude_4',
        'TCW_magnitude_5', 'TCW_magnitude_6', 'TCW_magnitude_7', 'TCW_magnitude_8', 'TCW_magnitude_9'
    ]}),
])

start_date = ee.Date('2015-06-27')
end_date = ee.Date('2025-01-01')
aoi_grid = ee.FeatureCollection('projects/ee-yangluhao990714/assets/AOIs/downstream_grid')
tp_forest_mask: ee.Image = ee.Image('projects/ee-yangluhao990714/assets/TP_Forest2015_Five').select('b1').neq(0)
index = 0
for aoi_grid_feature in aoi_grid.getInfo()['features']:

    # Caution: change here to skip some AOIs
    # Caution: change here to skip some AOIs
    # Caution: change here to skip some AOIs
    # if index != 0:
    #     index += 1
    #     continue

    aoi = ee.Feature(aoi_grid_feature['geometry']).geometry()
    # aoi = ee.FeatureCollection('projects/ee-yangluhao990714/assets/AOIs/downstream_tiny_aoi').geometry()
    sentinel_2_l2a = ee.ImageCollection('COPERNICUS/S2_HARMONIZED').filterBounds(aoi) \
        .filterDate(start_date, end_date)
    sentinel_2_l2a = sentinel_2_l2a.map(lambda img: img.clip(aoi))
    sentinel_2_l2a = sentinel_2_l2a.map(lambda img: img.updateMask(tp_forest_mask.clip(aoi)))
    sentinel_2_l2a = sentinel_2_l2a.remove_clouds()
    sentinel_2_l2a = sentinel_2_l2a.band_rename()
    sentinel_2_l2a = sentinel_2_l2a.map(lambda img: img.ndvi())
    sentinel_2_l2a = sentinel_2_l2a.map(lambda img: img.evi())
    sentinel_2_l2a = sentinel_2_l2a.map(lambda img: img.kt_transform())
    ccdc_input = sentinel_2_l2a.select(
        ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'NDVI', 'EVI', 'TCB', 'TCG', 'TCW']
    )
    ccdc_input = ccdc_input.map(lambda img: img.clip(aoi))
    ccdc_result: ee.Image = ee.Algorithms.TemporalSegmentation.Ccdc(
        ccdc_input,
        minObservations=12,
        dateFormat=1,
        chiSquareProbability=0.99,
        maxIterations=25000,
    )

    # keys() and values() will return ee.List with length 1, so we need to get(0) to get the actual parameters
    ccdc_result_flat_list = bands_list.map(
        lambda band: ccdc_result.select([ee.Dictionary(band).keys().get(0)]).arrayPad([10], 0) \
            .arrayFlatten([ee.Dictionary(band).values().get(0)])
    )
    ccdc_result_flat = ee.Image(ccdc_result_flat_list.get(0)).addBands(ee.Image(ccdc_result_flat_list.get(1))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(2))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(3))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(4))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(5))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(6))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(7))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(8))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(9))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(10))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(11))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(12)))
    file_name = f'ccdc_result_{index}'
    geemap.ee_export_image_to_asset(
        image=ccdc_result_flat,
        # image=ccdc_result,
        description='export_' + file_name,
        assetId=f'projects/ee-yangluhao990714/assets/ccdc_2nd_12_099/{file_name}',
        scale=10,
        region=aoi,
        maxPixels=1e13,
        crs='EPSG:4326',
    )
    index += 1