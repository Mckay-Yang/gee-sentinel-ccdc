import ee
import geemap
import utils

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

aoi = ee.FeatureCollection('projects/ee-yangluhao990714/assets/downstream_aoi')
image_collection = ee.ImageCollection('projects/ee-yangluhao990714/assets/ccdc_2nd_12_099')
image = image_collection.mosaic().clip(aoi)

change_info = image.select(
    [
        'tBreak_0',          'tBreak_1',          'tBreak_2',          'tBreak_3',          'tBreak_4',
        'tBreak_5',          'tBreak_6',          'tBreak_7',          'tBreak_8',          'tBreak_9',
        'Blue_magnitude_0',  'Blue_magnitude_1',  'Blue_magnitude_2',  'Blue_magnitude_3',  'Blue_magnitude_4',
        'Blue_magnitude_5',  'Blue_magnitude_6',  'Blue_magnitude_7',  'Blue_magnitude_8',  'Blue_magnitude_9',
        'Green_magnitude_0', 'Green_magnitude_1', 'Green_magnitude_2', 'Green_magnitude_3', 'Green_magnitude_4',
        'Green_magnitude_5', 'Green_magnitude_6', 'Green_magnitude_7', 'Green_magnitude_8', 'Green_magnitude_9',
        'Red_magnitude_0',   'Red_magnitude_1',   'Red_magnitude_2',   'Red_magnitude_3',   'Red_magnitude_4',
        'Red_magnitude_5',   'Red_magnitude_6',   'Red_magnitude_7',   'Red_magnitude_8',   'Red_magnitude_9',
        'NIR_magnitude_0',   'NIR_magnitude_1',   'NIR_magnitude_2',   'NIR_magnitude_3',   'NIR_magnitude_4',
        'NIR_magnitude_5',   'NIR_magnitude_6',   'NIR_magnitude_7',   'NIR_magnitude_8',   'NIR_magnitude_9',
        'SWIR1_magnitude_0', 'SWIR1_magnitude_1', 'SWIR1_magnitude_2', 'SWIR1_magnitude_3', 'SWIR1_magnitude_4',
        'SWIR1_magnitude_5', 'SWIR1_magnitude_6', 'SWIR1_magnitude_7', 'SWIR1_magnitude_8', 'SWIR1_magnitude_9',
        'SWIR2_magnitude_0', 'SWIR2_magnitude_1', 'SWIR2_magnitude_2', 'SWIR2_magnitude_3', 'SWIR2_magnitude_4',
        'SWIR2_magnitude_5', 'SWIR2_magnitude_6', 'SWIR2_magnitude_7', 'SWIR2_magnitude_8', 'SWIR2_magnitude_9',
        'NDVI_magnitude_0',  'NDVI_magnitude_1',  'NDVI_magnitude_2',  'NDVI_magnitude_3',  'NDVI_magnitude_4',
        'NDVI_magnitude_5',  'NDVI_magnitude_6',  'NDVI_magnitude_7',  'NDVI_magnitude_8',  'NDVI_magnitude_9',
        'EVI_magnitude_0',   'EVI_magnitude_1',   'EVI_magnitude_2',   'EVI_magnitude_3',   'EVI_magnitude_4',
        'EVI_magnitude_5',   'EVI_magnitude_6',   'EVI_magnitude_7',   'EVI_magnitude_8',   'EVI_magnitude_9',
        'TCB_magnitude_0',   'TCB_magnitude_1',   'TCB_magnitude_2',   'TCB_magnitude_3',   'TCB_magnitude_4',
        'TCB_magnitude_5',   'TCB_magnitude_6',   'TCB_magnitude_7',   'TCB_magnitude_8',   'TCB_magnitude_9',
        'TCG_magnitude_0',   'TCG_magnitude_1',   'TCG_magnitude_2',   'TCG_magnitude_3',   'TCG_magnitude_4',
        'TCG_magnitude_5',   'TCG_magnitude_6',   'TCG_magnitude_7',   'TCG_magnitude_8',   'TCG_magnitude_9',
        'TCW_magnitude_0',   'TCW_magnitude_1',   'TCW_magnitude_2',   'TCW_magnitude_3',   'TCW_magnitude_4',
        'TCW_magnitude_5',   'TCW_magnitude_6',   'TCW_magnitude_7',   'TCW_magnitude_8',   'TCW_magnitude_9',
    ]
)

change_prob = image.select(
    [
        'changeProb_0', 'changeProb_1', 'changeProb_2', 'changeProb_3', 'changeProb_4',
        'changeProb_5', 'changeProb_6', 'changeProb_7', 'changeProb_8', 'changeProb_9',
    ]
)

image = change_info.updateMask(change_prob.gt(0.95))

def get_image_intervel(image, interval) -> ee.Image:
    image = ee.Image(image)
    start_year = interval
    end_year = interval + 1
    t_break = image.select([
        'tBreak_0', 'tBreak_1', 'tBreak_2', 'tBreak_3', 'tBreak_4',
        'tBreak_5', 'tBreak_6', 'tBreak_7', 'tBreak_8', 'tBreak_9',
    ])
    blue_magnitude = image.select([
        'Blue_magnitude_0', 'Blue_magnitude_1', 'Blue_magnitude_2', 'Blue_magnitude_3', 'Blue_magnitude_4',
        'Blue_magnitude_5', 'Blue_magnitude_6', 'Blue_magnitude_7', 'Blue_magnitude_8', 'Blue_magnitude_9',
    ])
    green_magnitude = image.select([
        'Green_magnitude_0', 'Green_magnitude_1', 'Green_magnitude_2', 'Green_magnitude_3', 'Green_magnitude_4',
        'Green_magnitude_5', 'Green_magnitude_6', 'Green_magnitude_7', 'Green_magnitude_8', 'Green_magnitude_9',
    ])
    red_magnitude = image.select([
        'Red_magnitude_0', 'Red_magnitude_1', 'Red_magnitude_2', 'Red_magnitude_3', 'Red_magnitude_4',
        'Red_magnitude_5', 'Red_magnitude_6', 'Red_magnitude_7', 'Red_magnitude_8', 'Red_magnitude_9',
    ])
    nir_magnitude = image.select([
        'NIR_magnitude_0', 'NIR_magnitude_1', 'NIR_magnitude_2', 'NIR_magnitude_3', 'NIR_magnitude_4',
        'NIR_magnitude_5', 'NIR_magnitude_6', 'NIR_magnitude_7', 'NIR_magnitude_8', 'NIR_magnitude_9',
    ])
    swir1_magnitude = image.select([
        'SWIR1_magnitude_0', 'SWIR1_magnitude_1', 'SWIR1_magnitude_2', 'SWIR1_magnitude_3', 'SWIR1_magnitude_4',
        'SWIR1_magnitude_5', 'SWIR1_magnitude_6', 'SWIR1_magnitude_7', 'SWIR1_magnitude_8', 'SWIR1_magnitude_9',
    ])
    swir2_magnitude = image.select([
        'SWIR2_magnitude_0', 'SWIR2_magnitude_1', 'SWIR2_magnitude_2', 'SWIR2_magnitude_3', 'SWIR2_magnitude_4',
        'SWIR2_magnitude_5', 'SWIR2_magnitude_6', 'SWIR2_magnitude_7', 'SWIR2_magnitude_8', 'SWIR2_magnitude_9',
    ])
    ndvi_magnitude = image.select([
        'NDVI_magnitude_0', 'NDVI_magnitude_1', 'NDVI_magnitude_2', 'NDVI_magnitude_3', 'NDVI_magnitude_4',
        'NDVI_magnitude_5', 'NDVI_magnitude_6', 'NDVI_magnitude_7', 'NDVI_magnitude_8', 'NDVI_magnitude_9',
    ])
    evi_magnitude = image.select([
        'EVI_magnitude_0', 'EVI_magnitude_1', 'EVI_magnitude_2', 'EVI_magnitude_3', 'EVI_magnitude_4',
        'EVI_magnitude_5', 'EVI_magnitude_6', 'EVI_magnitude_7', 'EVI_magnitude_8', 'EVI_magnitude_9',
    ])
    tcb_magnitude = image.select([
        'TCB_magnitude_0', 'TCB_magnitude_1', 'TCB_magnitude_2', 'TCB_magnitude_3', 'TCB_magnitude_4',
        'TCB_magnitude_5', 'TCB_magnitude_6', 'TCB_magnitude_7', 'TCB_magnitude_8', 'TCB_magnitude_9',
    ])
    tcg_magnitude = image.select([
        'TCG_magnitude_0', 'TCG_magnitude_1', 'TCG_magnitude_2', 'TCG_magnitude_3', 'TCG_magnitude_4',
        'TCG_magnitude_5', 'TCG_magnitude_6', 'TCG_magnitude_7', 'TCG_magnitude_8', 'TCG_magnitude_9',
    ])
    tcw_magnitude = image.select([
        'TCW_magnitude_0', 'TCW_magnitude_1', 'TCW_magnitude_2', 'TCW_magnitude_3', 'TCW_magnitude_4',
        'TCW_magnitude_5', 'TCW_magnitude_6', 'TCW_magnitude_7', 'TCW_magnitude_8', 'TCW_magnitude_9',
    ])

    time_mask = t_break.gte(start_year).And(t_break.lt(end_year))
    t_break_mask = t_break.updateMask(time_mask)
    blue_magnitude_mask = blue_magnitude.updateMask(time_mask)
    green_magnitude_mask = green_magnitude.updateMask(time_mask)
    red_magnitude_mask = red_magnitude.updateMask(time_mask)
    nir_magnitude_mask = nir_magnitude.updateMask(time_mask)
    swir1_magnitude_mask = swir1_magnitude.updateMask(time_mask)
    swir2_magnitude_mask = swir2_magnitude.updateMask(time_mask)
    ndvi_magnitude_mask = ndvi_magnitude.updateMask(time_mask)
    evi_magnitude_mask = evi_magnitude.updateMask(time_mask)
    tcb_magnitude_mask = tcb_magnitude.updateMask(time_mask)
    tcg_magnitude_mask = tcg_magnitude.updateMask(time_mask)
    tcw_magnitude_mask = tcw_magnitude.updateMask(time_mask)

    def extract_band(image: ee.Image, band_name) -> ee.Image:
        return image.select(band_name).rename('band')
    image_extracted = image.bandNames().map(lambda band_name: extract_band(image, ee.String(band_name)))
    image_collection = ee.ImageCollection(image_extracted)
    image_combine = image_collection.median()
    return image_combine

image_combine = get_image_intervel(image, 2015).rename('year_2015')
image_combine = image_combine.addBands(get_image_intervel(image, 2016).rename('year_2016'))
image_combine = image_combine.addBands(get_image_intervel(image, 2017).rename('year_2017'))
image_combine = image_combine.addBands(get_image_intervel(image, 2018).rename('year_2018'))
image_combine = image_combine.addBands(get_image_intervel(image, 2019).rename('year_2019'))
image_combine = image_combine.addBands(get_image_intervel(image, 2020).rename('year_2020'))
image_combine = image_combine.addBands(get_image_intervel(image, 2021).rename('year_2021'))
image_combine = image_combine.addBands(get_image_intervel(image, 2022).rename('year_2022'))
image_combine = image_combine.addBands(get_image_intervel(image, 2023).rename('year_2023'))
image_combine = image_combine.addBands(get_image_intervel(image, 2024).rename('year_2024'))
image_combine = image_combine.addBands(get_image_intervel(image, 2025).rename('year_2025'))

def patch_cal(band: ee.Image) -> ee.Image:
    image_int = ee.Image(band).multiply(1000).toInt32()
    labels = image_int.connectedComponents(
        connectedness=ee.Kernel.circle(50),
        maxSize=1024,
    ).select(['labels'])
    patch_size = labels.connectedPixelCount(
        maxSize=1024,
        eightConnected=False,
    )
    band = ee.Image(band).updateMask(patch_size.gte(10))
    return band

list = image_combine.bandNames().map(lambda band_name: patch_cal(image_combine.select([ee.String(band_name)])))
image_combine = ee.Image(list.get(0))
for i in range(1, 11):
    image_combine = image_combine.addBands(list.get(i))

geemap.ee_export_image_to_asset(
    image=image_combine,
    description='ccdc_result_filtered_merging_patch_combined',
    assetId='projects/ee-yangluhao990714/assets/ccdc_result_filtered_merging_patch_combined',
    region=aoi.geometry(),
    scale=10,
    crs='EPSG:4326',
    maxPixels=1e13,
)
