import ee
import geemap
import utils
import threading
import time

ee.Authenticate()
ee.Initialize(project='your_project_id')

EE_TASK_MONITORING_DICT: dict[dict: dict] = {}
EE_TASK_MONITORING_DICT_LOCK = threading.Lock()

BAND_LIST = ee.List([
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


START_DATE = ee.Date('2015-06-27')
END_DATE = ee.Date('2025-01-01')
AOI_GRID = ee.FeatureCollection('projects/ee-yangluhao990714/assets/AOIs/downstream_grid')
TP_FOREST_MASK: ee.Image = ee.Image('projects/ee-yangluhao990714/assets/TP_Forest2015_Five').select(['b1']).neq(0)
IMAGE_COLLECTION = sentinel_2_l2a = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')


def ccdc_image_collection_preprocess(aoi: ee.Geometry) -> ee.ImageCollection:
    img_col = IMAGE_COLLECTION.filterBounds(aoi) \
        .filterDate(START_DATE, END_DATE)
    img_col = img_col.map(lambda img: img.clip(aoi))
    img_col = img_col.map(lambda img: img.updateMask(TP_FOREST_MASK.clip(aoi)))
    img_col = img_col.remove_clouds()
    img_col = img_col.band_rename()
    img_col = img_col.map(lambda img: img.ndvi())
    img_col = img_col.map(lambda img: img.evi())
    img_col = img_col.map(lambda img: img.kt_transform())
    ret = img_col.select(
        ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'NDVI', 'EVI', 'TCB', 'TCG', 'TCW']
    )
    ret = ret.map(lambda img: img.clip(aoi))
    return ret


def ccdc(ccdc_input: ee.ImageCollection, aoi: ee.Geometry) -> ee.Image:
    ccdc_result: ee.Image = ee.Algorithms.TemporalSegmentation.Ccdc(
        ccdc_input,
        minObservations=12,
        dateFormat=1,
        chiSquareProbability=0.99,
        maxIterations=25000,
    )
    return ccdc_result


def ccdc_result_flaten(ccdc_result: ee.Image) -> ee.Image:
    ccdc_result_flat_list = BAND_LIST.map(
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
    return ccdc_result_flat


def ccdc_result_export(ccdc_result_flat: ee.Image, aoi: ee.Geometry, file_name: str):
    task = ee.batch.Export.image.toAsset(
        image=ccdc_result_flat,
        description='export_' + file_name,
        assetId=f'projects/ee-yangluhao990714/assets/ccdc_2nd_12_099/{file_name}',
        scale=10,
        region=aoi,
        maxPixels=1e13,
        crs='EPSG:4326',
    )
    task.start()
    with EE_TASK_MONITORING_DICT_LOCK:
        EE_TASK_MONITORING_DICT[task.id] = {
            # To cut current aoi into smaller pieces
            'aoi': aoi,

            # To get the task info, the three fields are necessary
            'id': task.id,
            'state': ee.batch.Task.State(task.status()['state']),
            'type': ee.batch.Task.Type(task.status()['task_type']),

            'file_name': file_name,
        }



def ccdc_main():
    index = 0
    for aoi_grid_feature in AOI_GRID.getInfo()['features']:
        if index in [54]:
            aoi = ee.Feature(aoi_grid_feature['geometry']).geometry()
            ccdc_input = ccdc_image_collection_preprocess(aoi)
            ccdc_result = ccdc(ccdc_input, aoi)
            ccdc_result_flat = ccdc_result_flaten(ccdc_result)
            file_name = f'ccdc_result_{index}'
            ccdc_result_export(ccdc_result_flat, aoi, file_name)
        index += 1


def ee_task_aoi_split_retry(task_id: str):
    # Get the task info and delete it from the dict
    with EE_TASK_MONITORING_DICT_LOCK:
        aoi = EE_TASK_MONITORING_DICT[task_id]['aoi']
        file_name = EE_TASK_MONITORING_DICT[task_id]['file_name']
        del EE_TASK_MONITORING_DICT[task_id]

    aoi = ee.Feature(aoi['geometry']).geometry()

    # If the aoi is too small, less then a pixel, just return
    if aoi.area().getInfo() < 100:
        return

    # Split the aoi into smaller pieces
    coords = ee.List(aoi.coordinates().get(0))
    xmin = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.min())
    ymin = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.min())
    xmax = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.max())
    ymax = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.max())

    xmin = ee.Number(xmin)
    ymin = ee.Number(ymin)
    xmax = ee.Number(xmax)
    ymax = ee.Number(ymax)

    num_rows = ee.Number(6)
    num_cols = ee.Number(6)

    dx = xmax.subtract(xmin).divide(num_cols)
    dy = ymax.subtract(ymin).divide(num_rows)

    aoi_list = ee.List([])
    for row in range(num_rows.getInfo()):
        for col in range(num_cols.getInfo()):
            x0 = xmin.add(dx.multiply(col))
            y0 = ymin.add(dy.multiply(row))
            x1 = x0.add(dx)
            y1 = y0.add(dy)
            aoi = ee.Geometry.Rectangle([x0, y0, x1, y1])
            aoi_list = aoi_list.add(aoi)

    for index in range(36):
        aoi = ee.Geometry(aoi_list.get(index))
        ccdc_input = ccdc_image_collection_preprocess(aoi)
        ccdc_result = ccdc(ccdc_input, aoi)
        ccdc_result_flat = ccdc_result_flaten(ccdc_result)
        file_name_cut = f'{file_name}_{index}'
        ccdc_result_export(ccdc_result_flat, aoi, file_name_cut)


def ee_task_monitor():
    waits_empty_times = 0
    while True:
        if len(EE_TASK_MONITORING_DICT) == 0:
            waits_empty_times += 1
            if waits_empty_times > 60:
                break
            else:
                time.sleep(1)
                continue

        for task_id in list(EE_TASK_MONITORING_DICT.keys()):
            task = ee.batch.Task(
                task_id, EE_TASK_MONITORING_DICT[task_id]['type'], EE_TASK_MONITORING_DICT[task_id]['state']
            )
            task_status = task.status()
            if task_status['state'] == 'COMPLETED':
                print(f'Task {task_id} completed')
                with EE_TASK_MONITORING_DICT_LOCK:
                    del EE_TASK_MONITORING_DICT[task_id]
            elif task_status['state'] == 'FAILED':
                print(f'Task {task_id} failed')
                ee_task_aoi_split_retry(task_id)
            elif task_status['state'] == 'CANCELLED' or task_status['state'] == 'CANCEL_REQUESTED':
                print(f'Task {task_id} cancelled')
                with EE_TASK_MONITORING_DICT_LOCK:
                    del EE_TASK_MONITORING_DICT[task_id]
        time.sleep(0.1)


if __name__ == '__main__':
    ccdc_main()
    task_monitor_thread = threading.Thread(target=ee_task_monitor)
    task_monitor_thread.start()
