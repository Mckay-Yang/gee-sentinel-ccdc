import ee
import geemap
import utils

utils.ee_init()

aoi = ee.FeatureCollection('projects/ee-yangluhao990714/assets/downstream_aoi')
image_collection = ee.ImageCollection([])
for i in range(0, 147):
    image = ee.Image(f'projects/ee-yangluhao990714/assets/ccdc_result_12/ccdc_result_{i}')
    image_collection = image_collection.merge(image)

image = image_collection.mosaic().clip(aoi)

tBreak = image.select(
    ['tBreak_0', 'tBreak_1', 'tBreak_2', 'tBreak_3', 'tBreak_4',
     'tBreak_5', 'tBreak_6', 'tBreak_7', 'tBreak_8', 'tBreak_9',]
)
changeProb = image.select(
    ['changeProb_0', 'changeProb_1', 'changeProb_2', 'changeProb_3', 'changeProb_4',
     'changeProb_5', 'changeProb_6', 'changeProb_7', 'changeProb_8', 'changeProb_9',]
)
image = tBreak.updateMask(changeProb.gt(0.95))

def get_image_intervel(image, interval) -> ee.Image:
    image = ee.Image(image)
    start_year = interval
    end_year = interval + 1
    image = image.updateMask(image.gte(start_year).And(image.lt(end_year)))
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
    ).select('labels')
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
